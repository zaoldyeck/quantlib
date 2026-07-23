"""apex 事件驅動日頻回測引擎 — 純函式核心,資料進、結果出,零 IO。

時序慣例(全 campaign 統一):
  T 收盤決策(entries / exit 條件只用 ≤ T 的資訊)→ T+1 成交。
  fill_at="next_open":T+1 開盤成交(預設);"next_close":T+1 收盤成交。

執行現實(era-aware):
  - 漲跌停擋單:成交日相對前收盤參考價的漲跌幅 ≥ limit_buffer × era_limit
    時買單擋掉(不重試,訊號需自行再發);≤ −limit_buffer × era_limit 時賣單
    擋掉並自動隔日重試直到成交或下市。era_limit:2015-06-01 前 7%、之後 10%。
    使用調整後價格計算,除權息日自動等於「相對參考價」漲跌幅。
  - 停牌日無 bar 不能成交;持倉以最後有效收盤 mark。
  - 下市(最後一根 bar 後永遠無資料):以最後收盤強制清算(含賣出成本)。
  - 零槓桿:買進 notional = min(NAV_prev / n_slots, 可用現金);fractional shares。

成本模型:
  買:cash -= N × (1 + commission);shares = N / (px × (1 + slippage))
  賣:cash += shares × px × (1 − slippage) × (1 − commission − sell_tax)

出場條件優先序(同日多重觸發只記第一個):abs_stop → trailing → profit_take
  → signal(exit_flags)→ time_stop。trailing 以「持有期調整收盤峰值」為基準。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date as Date

import numpy as np
import polars as pl

LIMIT_CHANGE_DATE = Date(2015, 6, 1)

TRADE_SCHEMA: dict[str, pl.DataType] = {
    "company_code": pl.Utf8,
    "entry_date": pl.Date,
    "exit_date": pl.Date,
    "entry_px": pl.Float64,
    "exit_px": pl.Float64,
    "cost": pl.Float64,
    "ret_net": pl.Float64,
    "days_held": pl.Int32,
    "exit_reason": pl.Utf8,
}


@dataclass(frozen=True)
class ExecSpec:
    commission: float = 0.000285
    sell_tax: float = 0.003
    slippage: float = 0.001
    fill_at: str = "next_open"          # next_open | next_close | next_mid(分批 50/50)
    limit_buffer: float = 0.95          # 擋單門檻 = buffer × era_limit(無掛單資料時)


@dataclass(frozen=True)
class PortSpec:
    n_slots: int = 10
    capital: float = 3_000_000.0
    min_hold_days: int = 1              # 成交日(不含)起算,滿此天數才評估出場
    max_new_per_day: int | None = None
    pyramid_trigger: float | None = None  # 浮盈 ≥ trigger(相對均價)觸發加碼
    pyramid_max: int = 1                  # 每檔加碼次數上限
    pyramid_frac: float = 0.5             # 加碼 notional = NAV_prev × w × frac


@dataclass(frozen=True)
class ExitSpec:
    trailing_stop: float | None = None  # 相對持有期收盤峰值
    abs_stop: float | None = None       # 相對進場成交價(含滑價)
    profit_take: float | None = None
    time_stop: int | None = None        # 交易日
    underwater_trail: float | None = None  # 水下(mark<entry)時改用的較緊 trail
    loser_time_stop: int | None = None     # 水下時的較短時間止損(非對稱)
    same_day_exit: bool = False            # 門檻型出場(trail/abs/profit/time)當日收盤成交
                                           # (觸發線事先已知,實盤可 MOC;signal 型仍隔日)
    profit_recycle: tuple | None = None    # (threshold, fraction):浮盈 ≥ threshold 時
                                           # 一次性回收 fraction 部位(部分止盈,其餘續抱)


@dataclass
class _Pos:
    shares: float
    entry_px: float      # 有效成交均價(含滑價;加碼後為加權平均)
    entry_d: int         # 首倉成交日 index(加碼不重置——持有期以首倉起算)
    cost: float          # 總現金流出(含手續費)
    peak: float          # 持有期收盤峰值(mark 基準)
    pending_exit: str | None = None
    recycled: bool = False
    w: float = 0.0       # 進場目標權重(加碼 sizing 基準)
    adds: int = 0        # 已加碼次數


#: same_day_exit 模式下,t-1 路徑只評估 signal 型出場(門檻型移到當日路徑)
_SIGNAL_ONLY = None  # 於模組載入後設為 ExitSpec()(前向參照)
_EMPTY_FLAGS: set = set()


@dataclass(frozen=True)
class SimResult:
    nav: pl.DataFrame     # date, nav, cash, invested, n_pos
    trades: pl.DataFrame  # TRADE_SCHEMA(exit_reason="open" 為期末未平倉)
    meta: dict


def simulate(
    panel: pl.DataFrame,
    entries: pl.DataFrame,
    *,
    exit_flags: pl.DataFrame | None = None,
    eligibility: pl.DataFrame | None = None,
    exec_spec: ExecSpec = ExecSpec(),
    port_spec: PortSpec = PortSpec(),
    exit_spec: ExitSpec = ExitSpec(),
    start: Date | None = None,
    end: Date | None = None,
) -> SimResult:
    """跑一次模擬。

    Args:
      panel: 調整價 panel(date, company_code, open, close 必要;(code, date) 唯一)。
      entries: 決策日候選 (date, company_code, score[, weight])——date 收盤決策、
        隔日成交,score 高者優先。同檔已持有則跳過。可選 weight 欄 = 該倉目標
        NAV 比例(如 0.20);缺欄時等權 1/n_slots。零槓桿由現金約束保證。
      exit_flags: 訊號死亡出場 (date, company_code)——date 收盤判定、隔日賣出。
      eligibility: (date, company_code, eligible)——決策日資格,semi-join 過濾 entries。
      start: NAV 起算日(預設 panel 第 2 個交易日);之前的 panel 只當暖機。
    """
    if exec_spec.fill_at not in ("next_open", "next_close", "next_mid"):
        raise ValueError(
            f"fill_at must be next_open|next_close|next_mid, got {exec_spec.fill_at!r}"
        )

    exact_lock = "ask_missing" in panel.columns and "bid_missing" in panel.columns
    cols = ["date", "company_code", "open", "close"] + (
        ["ask_missing", "bid_missing"] if exact_lock else []
    )
    px = panel.select(cols).sort(["date", "company_code"])
    if end is not None:
        px = px.filter(pl.col("date") <= end)

    dates = px["date"].unique().sort().to_numpy()            # datetime64[D]
    codes = px["company_code"].unique().sort().to_numpy()    # object[str]
    D, C = len(dates), len(codes)
    if D < 2:
        raise ValueError("panel too short (need >= 2 trading days)")
    dates_py: list[Date] = dates.tolist()
    code_map = {c: i for i, c in enumerate(codes.tolist())}

    d_ix = np.searchsorted(dates, px["date"].to_numpy())
    c_ix = px["company_code"].replace_strict(code_map, return_dtype=pl.Int32).to_numpy()
    close = np.full((D, C), np.nan)
    open_ = np.full((D, C), np.nan)
    close[d_ix, c_ix] = px["close"].to_numpy()
    open_[d_ix, c_ix] = px["open"].to_numpy()

    has_bar = ~np.isnan(close)
    ff = np.where(has_bar, np.arange(D)[:, None], 0)
    np.maximum.accumulate(ff, axis=0, out=ff)
    mark = close[ff, np.arange(C)[None, :]]                  # ffill;首根 bar 前為 NaN
    prev_mark = np.empty_like(mark)
    prev_mark[0] = np.nan
    prev_mark[1:] = mark[:-1]

    era_lim = np.where(dates < np.datetime64(LIMIT_CHANGE_DATE), 0.07, 0.10)[:, None]
    if exec_spec.fill_at == "next_open":
        fill_px_mat = open_
    elif exec_spec.fill_at == "next_close":
        fill_px_mat = close
    else:                                # next_mid:50% 開盤 + 50% 收盤
        fill_px_mat = 0.5 * (open_ + close)
    if exact_lock:
        # E01 精準鎖死:掛單缺失 ∧ 接近停板位。open fill 需整日鎖死(open 亦在停板位)。
        ask0 = np.zeros((D, C), dtype=bool)
        bid0 = np.zeros((D, C), dtype=bool)
        ask0[d_ix, c_ix] = px["ask_missing"].to_numpy()
        bid0[d_ix, c_ix] = px["bid_missing"].to_numpy()
        lvl = era_lim - 0.005
        with np.errstate(invalid="ignore", divide="ignore"):
            close_ret = close / prev_mark - 1.0
            open_ret = open_ / prev_mark - 1.0
        if exec_spec.fill_at in ("next_open", "next_mid"):
            # 開盤(或分批)單:整日鎖死才視為不可成交
            buy_block = ask0 & (close_ret >= lvl) & (open_ret >= lvl)
            sell_block = bid0 & (close_ret <= -lvl) & (open_ret <= -lvl)
        else:
            buy_block = ask0 & (close_ret >= lvl)
            sell_block = bid0 & (close_ret <= -lvl)
    else:
        with np.errstate(invalid="ignore", divide="ignore"):
            fill_ret = fill_px_mat / prev_mark - 1.0
        thr = exec_spec.limit_buffer * era_lim
        buy_block = fill_ret >= thr                          # NaN 比較 → False
        sell_block = fill_ret <= -thr
    last_bar = (D - 1) - np.argmax(has_bar[::-1], axis=0)

    if exit_spec.same_day_exit:
        with np.errstate(invalid="ignore", divide="ignore"):
            _cr = close / prev_mark - 1.0
        if exact_lock:
            sd_sell_block = bid0 & (_cr <= -(era_lim - 0.005))
        else:
            sd_sell_block = _cr <= -(exec_spec.limit_buffer * era_lim)
    else:
        sd_sell_block = None

    # ── entries → fill-day 佇列 ─────────────────────────────────────────
    has_weight = "weight" in entries.columns
    ecols = ["date", "company_code", "score"] + (["weight"] if has_weight else [])
    ent = entries.drop_nulls(subset=["score"]).select(ecols)
    if eligibility is not None:
        ent = ent.join(
            eligibility.filter(pl.col("eligible")).select(["date", "company_code"]),
            on=["date", "company_code"],
            how="semi",
        )
    default_w = 1.0 / port_spec.n_slots
    n_dropped = 0
    by_day: dict[int, list[tuple[float, int, float]]] = {}
    if ent.height:
        e_di = np.searchsorted(dates, ent["date"].to_numpy())
        e_ci = ent["company_code"].replace_strict(
            code_map, default=-1, return_dtype=pl.Int32
        ).to_numpy()
        e_sc = ent["score"].cast(pl.Float64).to_numpy()
        e_w = (ent["weight"].cast(pl.Float64).fill_null(default_w).to_numpy()
               if has_weight else np.full(ent.height, default_w))
        date_ok = (e_di < D) & (dates[np.minimum(e_di, D - 1)] == ent["date"].to_numpy())
        keep = date_ok & (e_ci >= 0) & (e_di + 1 < D)
        n_dropped = int(ent.height - keep.sum())
        for dd, cc, ss, ww in zip(e_di[keep] + 1, e_ci[keep], e_sc[keep], e_w[keep]):
            by_day.setdefault(int(dd), []).append((float(ss), int(cc), float(ww)))
        for lst in by_day.values():
            lst.sort(key=lambda t: -t[0])

    flag: set[tuple[int, int]] = set()
    if exit_flags is not None and exit_flags.height:
        f_di = np.searchsorted(dates, exit_flags["date"].to_numpy())
        f_ci = exit_flags["company_code"].replace_strict(
            code_map, default=-1, return_dtype=pl.Int32
        ).to_numpy()
        f_ok = (
            (f_di < D)
            & (dates[np.minimum(f_di, D - 1)] == exit_flags["date"].to_numpy())
            & (f_ci >= 0)
        )
        flag = set(zip(f_di[f_ok].tolist(), f_ci[f_ok].tolist()))

    start_ix = 1 if start is None else max(1, int(np.searchsorted(dates, np.datetime64(start))))

    # ── 主迴圈 ──────────────────────────────────────────────────────────
    xc = exec_spec
    cash = port_spec.capital
    nav_prev = port_spec.capital
    positions: dict[int, _Pos] = {}
    trades: list[dict] = []
    r_date: list[Date] = []
    r_nav: list[float] = []
    r_cash: list[float] = []
    r_inv: list[float] = []
    r_npos: list[int] = []

    def _sell(i: int, pos: _Pos, px_raw: float, exit_d: int, reason: str) -> None:
        nonlocal cash
        px_out = px_raw * (1 - xc.slippage)
        proceeds = pos.shares * px_out * (1 - xc.commission - xc.sell_tax)
        cash += proceeds
        trades.append(
            {
                "company_code": codes[i],
                "entry_date": dates_py[pos.entry_d],
                "exit_date": dates_py[exit_d],
                "entry_px": pos.entry_px,
                "exit_px": px_out,
                "cost": pos.cost,
                "ret_net": proceeds / pos.cost - 1.0,
                "days_held": exit_d - pos.entry_d,
                "exit_reason": reason,
            }
        )

    for d in range(start_ix, D):
        # 1) 下市強制清算(最後 bar 已過)
        for i in [i for i in positions if last_bar[i] < d]:
            pos = positions.pop(i)
            _sell(i, pos, mark[last_bar[i], i], int(last_bar[i]), pos.pending_exit or "delist")

        # 2) 出場(T-1 決策 → T 成交;跌停鎖死自動重試)
        #    same_day_exit 模式:門檻型規則移到步驟 3.5 當日評估,此處只處理 signal 型
        xs_here = (_SIGNAL_ONLY if exit_spec.same_day_exit else exit_spec)
        for i, pos in list(positions.items()):
            reason = pos.pending_exit or _exit_reason(
                pos, d - 1, i, mark, flag, xs_here, port_spec.min_hold_days
            )
            if reason is None:
                continue
            if has_bar[d, i] and not sell_block[d, i] and fill_px_mat[d, i] > 0:
                positions.pop(i)
                _sell(i, pos, fill_px_mat[d, i], d, reason)
            else:
                pos.pending_exit = reason

        # 2.5) 部分止盈回收(T-1 浮盈達標 → T 賣 fraction,一次性)
        if exit_spec.profit_recycle is not None:
            rc_thr, rc_frac = exit_spec.profit_recycle
            for i, pos in list(positions.items()):
                if pos.recycled or pos.pending_exit is not None:
                    continue
                if mark[d - 1, i] <= 0 or pos.entry_px <= 0:
                    continue
                if mark[d - 1, i] / pos.entry_px - 1 < rc_thr:
                    continue
                if has_bar[d, i] and not sell_block[d, i] and fill_px_mat[d, i] > 0:
                    sell_shares = pos.shares * rc_frac
                    part = _Pos(shares=sell_shares, entry_px=pos.entry_px,
                                entry_d=pos.entry_d, cost=pos.cost * rc_frac,
                                peak=pos.peak)
                    _sell(i, part, fill_px_mat[d, i], d, "recycle")
                    pos.shares -= sell_shares
                    pos.cost *= (1 - rc_frac)
                    pos.recycled = True

        # 3) 進場(score 高者優先;漲停鎖死擋掉不重試)
        new_fills = 0
        for score, i, w in by_day.get(d, ()):
            if len(positions) >= port_spec.n_slots:
                break
            if port_spec.max_new_per_day is not None and new_fills >= port_spec.max_new_per_day:
                break
            if i in positions or not has_bar[d, i] or buy_block[d, i]:
                continue
            px_in = fill_px_mat[d, i]
            if not px_in > 0:
                continue
            notional = min(nav_prev * w, cash / (1 + xc.commission))
            if notional < 1000.0:
                break
            px_eff = px_in * (1 + xc.slippage)
            positions[i] = _Pos(
                shares=notional / px_eff,
                entry_px=px_eff,
                entry_d=d,
                cost=notional * (1 + xc.commission),
                peak=mark[d, i],
                w=w,
            )
            cash -= notional * (1 + xc.commission)
            new_fills += 1

        # 3.2) 獲利加碼(pyramiding):T-1 浮盈 ≥ trigger → T 加碼;
        #      新倉優先用完當日節流額度,加碼與新倉共用 max_new_per_day
        if port_spec.pyramid_trigger is not None:
            cands = sorted(
                ((mark[d - 1, i] / pos.entry_px - 1.0, i)
                 for i, pos in positions.items()
                 if pos.pending_exit is None and pos.adds < port_spec.pyramid_max
                 and mark[d - 1, i] > 0 and pos.entry_px > 0
                 and mark[d - 1, i] / pos.entry_px - 1.0 >= port_spec.pyramid_trigger),
                key=lambda t: -t[0])
            for _, i in cands:
                if (port_spec.max_new_per_day is not None
                        and new_fills >= port_spec.max_new_per_day):
                    break
                if not has_bar[d, i] or buy_block[d, i]:
                    continue
                px_in = fill_px_mat[d, i]
                if not px_in > 0:
                    continue
                pos = positions[i]
                notional = min(nav_prev * pos.w * port_spec.pyramid_frac,
                               cash / (1 + xc.commission))
                if notional < 1000.0:
                    break
                px_eff = px_in * (1 + xc.slippage)
                add_sh = notional / px_eff
                pos.entry_px = ((pos.shares * pos.entry_px + add_sh * px_eff)
                                / (pos.shares + add_sh))
                pos.shares += add_sh
                pos.cost += notional * (1 + xc.commission)
                pos.adds += 1
                cash -= notional * (1 + xc.commission)
                new_fills += 1

        # 3.5) same_day_exit:門檻型出場當日收盤成交(pos.peak 仍為 d-1 前峰值;
        #      當日新倉 held=0 < min_hold 不會被同日賣出)
        if exit_spec.same_day_exit:
            for i, pos in list(positions.items()):
                if pos.pending_exit is not None:
                    continue
                reason = _exit_reason(pos, d, i, mark, _EMPTY_FLAGS, exit_spec,
                                      port_spec.min_hold_days)
                if reason is None:
                    continue
                if has_bar[d, i] and not sd_sell_block[d, i] and close[d, i] > 0:
                    positions.pop(i)
                    _sell(i, pos, close[d, i], d, reason)
                else:
                    pos.pending_exit = reason

        # 4) 收盤 mark + 峰值更新
        invested = 0.0
        for i, pos in positions.items():
            m = mark[d, i]
            invested += pos.shares * m
            if m > pos.peak:
                pos.peak = m
        nav = cash + invested
        r_date.append(dates_py[d])
        r_nav.append(nav)
        r_cash.append(cash)
        r_inv.append(invested)
        r_npos.append(len(positions))
        nav_prev = nav

    # 期末未平倉:NAV 序列已收完(以 mark 記帳),此處只補「假想淨出場」trade 供
    # 交易統計;_sell 對 cash 的修改發生在序列之後,不影響任何輸出 NAV。
    for i, pos in positions.items():
        _sell(i, pos, mark[D - 1, i], D - 1, "open")

    nav_df = pl.DataFrame(
        {"date": r_date, "nav": r_nav, "cash": r_cash, "invested": r_inv, "n_pos": r_npos}
    )
    trades_df = (
        pl.DataFrame(trades, schema=TRADE_SCHEMA) if trades else pl.DataFrame(schema=TRADE_SCHEMA)
    )
    meta = {
        "n_days": len(r_date),
        "start": str(r_date[0]) if r_date else None,
        "end": str(r_date[-1]) if r_date else None,
        "dropped_entry_rows": n_dropped,
        "exec": {
            "commission": xc.commission, "sell_tax": xc.sell_tax,
            "slippage": xc.slippage, "fill_at": xc.fill_at, "limit_buffer": xc.limit_buffer,
        },
        "port": {
            "n_slots": port_spec.n_slots, "capital": port_spec.capital,
            "min_hold_days": port_spec.min_hold_days,
            "max_new_per_day": port_spec.max_new_per_day,
            "pyramid": (port_spec.pyramid_trigger, port_spec.pyramid_max,
                        port_spec.pyramid_frac),
        },
        "exit": {
            "trailing_stop": exit_spec.trailing_stop, "abs_stop": exit_spec.abs_stop,
            "profit_take": exit_spec.profit_take, "time_stop": exit_spec.time_stop,
        },
    }
    return SimResult(nav=nav_df, trades=trades_df, meta=meta)


_SIGNAL_ONLY = ExitSpec()


def _exit_reason(
    pos: _Pos,
    t: int,
    i: int,
    mark: np.ndarray,
    flag: set[tuple[int, int]],
    xs: ExitSpec,
    min_hold: int,
) -> str | None:
    """T 收盤出場判定(t = 決策日 index)。"""
    if t - pos.entry_d < min_hold:
        return None
    m = mark[t, i]
    if not m > 0:
        return None
    r_entry = m / pos.entry_px - 1.0
    if xs.abs_stop is not None and r_entry <= -xs.abs_stop:
        return "abs_stop"
    trail = xs.trailing_stop
    if xs.underwater_trail is not None and r_entry < 0:
        trail = xs.underwater_trail
    if trail is not None and pos.peak > 0 and m / pos.peak - 1.0 <= -trail:
        return "trail"
    if xs.profit_take is not None and r_entry >= xs.profit_take:
        return "profit"
    if (t, i) in flag:
        return "signal"
    if (xs.loser_time_stop is not None and r_entry < 0
            and t - pos.entry_d >= xs.loser_time_stop):
        return "time_loser"
    if xs.time_stop is not None and t - pos.entry_d >= xs.time_stop:
        return "time"
    return None
