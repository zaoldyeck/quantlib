"""x_regime_cond — S 策略「regime 條件化參數」實驗(不是 regime 過濾)。

問題:S 用單一固定 trail(35%)/ slots(5)/ max_new(2) 跑完 12 年;若最優鬆緊
隨市場波動/趨勢/寬度而異,固定參數只是所有 regime 的折衷。本 harness 測
「不同 regime 用不同參數(但不停止交易)」能否勝過 canonical。

兩條實作路線(各有各的對照組,不混用):

  Track A(零人工假象):regime 只條件化**進場節流**(每日新倉上限),靠
    `_entries_fn` 在候選層截斷 → 單次連續回測,對照 = canonical 本人。

  Track B(需分段):regime 條件化**出場鬆緊 / slots**——這兩個是引擎內部
    狀態(ExitSpec / PortSpec 為整段常數),只能把時間軸依 regime 切段、
    每段各跑一次 `run_s_full` 再串接 NAV。分段本身會產生假象(每段從空手
    重新建倉、段末等同清倉),故 **對照組不是 canonical,而是「同一組切點、
    兩態都用 canonical 參數」的 placebo**——兩臂共享分段假象,配對比較才
    只剩參數效果。placebo vs canonical 的落差另外報,量化假象大小。

PIT 硬要求:regime 只用 ≤ 當日的資料——20 日波動 / 200 日均線 / 寬度皆為
落後窗;高低門檻用**滾動 3 年分位**(非全期分位,後者用到未來);狀態切換
再加 k 日確認(遲滯),只會更慢、不會偷看未來。

統計判準:配對 block bootstrap(block=21、n_boot=4000,對同一組日期同時
重抽變體與對照的日報酬 → CAGR 差分佈),CI 跨 0 即判噪音。

Run:
  uv run --project . python -m quantlib.strat_lab.x_regime_cond           # 全跑
  uv run --project . python -m quantlib.strat_lab.x_regime_cond --probe   # 只看 regime 統計與單跑耗時

依賴 cache: 是(prep_cached 讀 industry_taxonomy_pit / daily_quote / market_index)。
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import date as Date

import numpy as np
import polars as pl

from quantlib import paths
from quantlib.apex import data
from quantlib.apex.engine import ExitSpec, PortSpec
from quantlib.apex.metrics import perf_stats, trade_stats, yearly_table
from quantlib.apex.strategy_s import C, DS, prep_cached, run_s_full

TRADING_DAYS = 252
TAIEX = "發行量加權股價指數"
# 段界清倉摩擦:賣出成本(手續費 0.0285% + 證交稅 0.3% + 滑價 0.1%)。分段串接
# 不會自動收這筆錢(每段 NAV 期末以 mark 記帳),故對「所有分段臂」一律等額收取。
BOUNDARY_COST = 0.000285 + 0.003 + 0.001
CONFIRM_DAYS = 5      # 遲滯:raw 狀態連續 k 日一致才換檔(PIT,只會更慢)
DWELL = 40            # 換檔後最短停留交易日:綁 S 自己的持有週期(time_stop=30
                      # 交易日),讓每段至少能跑完一個完整持倉循環;否則分段回測
                      # 全在建倉/清倉的暫態裡,量到的是切段假象不是參數效果。
QWIN = 756            # 門檻用的滾動視窗(≈3 年交易日)
QMIN = 250            # 門檻最少樣本(≈1 年)


# ── regime 建構(全部 PIT)────────────────────────────────────────────────

def _hysteresis(raw: np.ndarray, k: int, dwell: int = 0) -> np.ndarray:
    """遲滯濾波:raw 狀態連續 k 日一致才切換,且距上次切換 ≥ dwell 日。

    兩者都只看過去 → PIT 安全(效果是「換檔更遲鈍」,絕不提前)。
    """
    out = np.empty_like(raw)
    cur, last = raw[0], -10**9
    for t in range(len(raw)):
        if (t >= k - 1 and raw[t] != cur and np.all(raw[t - k + 1: t + 1] == raw[t])
                and t - last >= dwell):
            cur, last = raw[t], t
        out[t] = cur
    return out


def build_regimes(con) -> pl.DataFrame:
    """回 (date, vol_hi, bear, breadth_state) —— 皆為當日收盤即可知的狀態。

    vol_hi   : TAIEX 20 日報酬標準差 > 其滾動 3 年 P70(波動叢聚 → 高波動期)
    bear     : TAIEX 收盤 < 200 日均線(經典長期趨勢分界)
    breadth_state: 全市場上漲家數比 10 日均,對滾動 3 年 P30/P70 分三檔(0 窄 /1 中 /2 寬)
    """
    idx = con.sql(
        f"SELECT date, close FROM market_index "
        f"WHERE market='twse' AND name='{TAIEX}' ORDER BY date").pl()
    idx = (idx.with_columns(pl.col("close").pct_change().alias("ret"))
           .with_columns([
               pl.col("ret").rolling_std(20).alias("vol20"),
               pl.col("close").rolling_mean(200).alias("ma200"),
           ])
           .with_columns(pl.col("vol20").rolling_quantile(
               0.70, window_size=QWIN, min_samples=QMIN).alias("vol_thr")))

    # 寬度:上漲家數比(用原始收盤——除息日會略微低估,是市場寬度指標的通例)
    br = con.sql("""
        WITH q AS (
          SELECT date, company_code, closing_price,
                 lag(closing_price) OVER (PARTITION BY market, company_code ORDER BY date) pc
          FROM daily_quote WHERE closing_price > 0
        )
        SELECT date, avg(CASE WHEN closing_price > pc THEN 1.0 ELSE 0.0 END) AS adv
        FROM q WHERE pc IS NOT NULL GROUP BY date ORDER BY date
    """).pl()
    br = (br.with_columns(pl.col("adv").rolling_mean(10).alias("adv10"))
          .with_columns([
              pl.col("adv10").rolling_quantile(0.30, window_size=QWIN,
                                               min_samples=QMIN).alias("lo"),
              pl.col("adv10").rolling_quantile(0.70, window_size=QWIN,
                                               min_samples=QMIN).alias("hi"),
          ]))

    df = (idx.join(br, on="date", how="left").sort("date")
          .with_columns([
              (pl.col("vol20") > pl.col("vol_thr")).fill_null(False).alias("raw_volhi"),
              (pl.col("close") < pl.col("ma200")).fill_null(False).alias("raw_bear"),
              pl.when(pl.col("lo").is_null()).then(1)
              .when(pl.col("adv10") < pl.col("lo")).then(0)
              .when(pl.col("adv10") > pl.col("hi")).then(2)
              .otherwise(1).alias("raw_breadth"),
          ]))
    df = df.with_columns(pl.col("vol20").rolling_median(
        window_size=QWIN, min_samples=QMIN).alias("vol_med"))
    return df.with_columns([
        pl.Series("vol_hi", _hysteresis(df["raw_volhi"].to_numpy(), CONFIRM_DAYS, DWELL)),
        pl.Series("bear", _hysteresis(df["raw_bear"].to_numpy(), CONFIRM_DAYS, DWELL)),
        # Track A(進場節流)不切段,不需要 dwell;保留較靈敏的寬度狀態
        pl.Series("breadth_state",
                  _hysteresis(df["raw_breadth"].to_numpy(), CONFIRM_DAYS)),
    ]).select(["date", "vol_hi", "bear", "breadth_state", "vol20", "vol_med"])


def state_on_dates(reg: pl.DataFrame, col: str, dates: list[Date]) -> np.ndarray:
    """把 regime 狀態對齊到交易日序列(asof backward,缺值補中性)。"""
    d = pl.DataFrame({"date": dates}).sort("date")
    j = d.join_asof(reg.select(["date", col]).sort("date"), on="date", strategy="backward")
    return j[col].fill_null(0).to_numpy()


def segments(dates: list[Date], state: np.ndarray) -> list[tuple[Date, Date, int]]:
    """把日期軸依狀態切成極大連續段 [(t0, t1, state), ...]。"""
    segs, s0 = [], 0
    for t in range(1, len(dates) + 1):
        if t == len(dates) or state[t] != state[s0]:
            segs.append((dates[s0], dates[t - 1], int(state[s0])))
            s0 = t
    return segs


# ── 分段串接(Track B)────────────────────────────────────────────────────

WARM = 20   # 段前暖機交易日數(供 prev_mark / 漲跌停判定;部位一律 ≥ t0 才開)


def _seg_run(panel, feat, elig, t0: Date, t1: Date, kw: dict, dates: list[Date]):
    """跑單一段:panel 只留 [t0-WARM, t1](等價於全 panel 跑到 t1,但快數倍)。

    等價性:所有部位都在 ≥ t0 才進場,引擎只需要 t0 前一根 bar 做 prev_mark/漲跌停
    參考;暖機留 20 日綽綽有餘。`--verify` 會逐位比對截尾與不截尾的 NAV。
    """
    i0 = max(0, dates.index(t0) - WARM)
    sub = panel.filter((pl.col("date") >= pl.lit(dates[i0]))
                       & (pl.col("date") <= pl.lit(t1)))
    return run_s_full(sub, feat, elig, t0.isoformat(), **kw)


def chained_nav(panel, feat, elig, segs, params_by_state: dict,
                dates: list[Date]) -> pl.DataFrame:
    """分段跑 run_s_full 再串接:段內用該段自己的日報酬,段界日記 0 報酬 + 清倉摩擦。"""
    r_dates: list[Date] = []
    r_rets: list[float] = []
    for k, (t0, t1, st) in enumerate(segs):
        kw = params_by_state[st]
        nav, _ = _seg_run(panel, feat, elig, t0, t1, kw, dates)
        v = nav["nav"].to_numpy()
        dd = nav["date"].to_list()
        if k > 0:                        # 段界日:前段已清倉,當日空手 + 收清倉摩擦
            r_dates.append(dd[0])
            r_rets.append(-BOUNDARY_COST)
        r_dates.extend(dd[1:])
        r_rets.extend((v[1:] / v[:-1] - 1.0).tolist())
    nav = np.cumprod(np.concatenate([[1.0], np.array(r_rets) + 1.0]))
    return pl.DataFrame({"date": [segs[0][0]] + r_dates, "nav": nav})


# ── 配對 block bootstrap ────────────────────────────────────────────────

def paired_block_bootstrap(nav_a: pl.DataFrame, nav_b: pl.DataFrame,
                           block: int = 21, n_boot: int = 4000, seed: int = 7) -> dict:
    """對同一組日期**同時**重抽兩條曲線的日報酬(配對),回 CAGR 差的分佈。

    配對抽樣保留兩曲線的同期相關(高相關時統計力遠優於各自獨立抽樣)。
    """
    j = (nav_a.rename({"nav": "a"}).join(nav_b.rename({"nav": "b"}), on="date", how="inner")
         .sort("date"))
    a, b = j["a"].to_numpy(), j["b"].to_numpy()
    ra, rb = a[1:] / a[:-1] - 1.0, b[1:] / b[:-1] - 1.0
    t = len(ra)
    rng = np.random.default_rng(seed)
    nb = int(np.ceil(t / block))
    starts = rng.integers(0, t, size=(n_boot, nb))
    ix = ((starts[:, :, None] + np.arange(block)[None, None, :]) % t).reshape(n_boot, -1)[:, :t]
    la, lb = np.log1p(ra), np.log1p(rb)
    ga = np.exp(la[ix].mean(axis=1) * TRADING_DAYS) - 1.0
    gb = np.exp(lb[ix].mean(axis=1) * TRADING_DAYS) - 1.0
    d = ga - gb
    # 對數年化成長差:與 CAGR 差**同號**(exp 單調 → p 值完全一致),但不隨
    # 重抽樣的基準水位縮放,量級才對得上實際落差(CAGR 差的中位數會被右偏
    # 分佈壓縮,只可看方向不宜看大小)。
    dl = (la[ix].mean(axis=1) - lb[ix].mean(axis=1)) * TRADING_DAYS
    real = ((np.exp(la.mean() * TRADING_DAYS) - 1.0)
            - (np.exp(lb.mean() * TRADING_DAYS) - 1.0))
    return {
        "realized_cagr_diff": float(real),
        "ann_diff": float(np.median(d)),
        "ci_lo": float(np.percentile(d, 2.5)),
        "ci_hi": float(np.percentile(d, 97.5)),
        "p_le0": float((d <= 0).mean()),
        "ann_log_diff": float(np.median(dl)),
        "log_ci_lo": float(np.percentile(dl, 2.5)),
        "log_ci_hi": float(np.percentile(dl, 97.5)),
        "boot_lo_variant": float(np.percentile(ga, 2.5)),
    }


def regime_attribution(nav: pl.DataFrame, states: dict[str, np.ndarray],
                       dates: list[Date]) -> dict:
    """canonical 報酬在各 regime 的歸因:日數占比、該狀態下的年化報酬、
    對總對數報酬的貢獻占比、狀態內下行波動。

    這是「條件化是否可能有效」的前置證據:若 S 在高波動/空頭日賺得**更多**,
    任何「壞 regime 就縮手」的條件化必然賠掉報酬,再怎麼調參數都救不回來。
    """
    n = nav.sort("date")
    dmap = dict(zip(n["date"].to_list(), n["nav"].to_list()))
    v = np.array([dmap.get(d, np.nan) for d in dates], dtype=float)
    r = v[1:] / v[:-1] - 1.0
    out = {}
    for key, st in states.items():
        s = st[1:]
        blocks = {}
        for lvl in sorted(set(s.tolist())):
            m = (s == lvl) & np.isfinite(r)
            rr = r[m]
            if len(rr) < 20:
                continue
            lg = np.log1p(rr).sum()
            blocks[str(lvl)] = {
                "days_share": float(m.mean()),
                "ann_ret": float(np.expm1(lg / len(rr) * TRADING_DAYS)),
                "logret_share": float(lg / np.log1p(r[np.isfinite(r)]).sum()),
                "downside_vol_ann": float(np.sqrt(np.mean(np.minimum(rr, 0) ** 2))
                                          * np.sqrt(TRADING_DAYS)),
            }
        out[key] = blocks
    return out


def drawdown_regime(nav: pl.DataFrame, states: dict[str, np.ndarray],
                    dates: list[Date]) -> dict:
    """最大回撤區間(峰 → 谷)與其 regime 組成——回答「最痛的時候是哪種市場」。"""
    n = nav.sort("date")
    v, d = n["nav"].to_numpy(), n["date"].to_list()
    dd = v / np.maximum.accumulate(v) - 1.0
    j = int(np.argmin(dd))
    i = int(np.argmax(v[: j + 1]))
    ix = {dt: k for k, dt in enumerate(dates)}
    win = [ix[x] for x in d[i: j + 1] if x in ix]
    return {"peak": str(d[i]), "trough": str(d[j]), "mdd": float(dd[j]),
            "n_days": len(win),
            **{f"{k}_mean": float(np.mean(st[win])) for k, st in states.items()}}


def did_by_regime(nav_v: pl.DataFrame, nav_b: pl.DataFrame, state: np.ndarray,
                  dates: list[Date], block: int = 21, n_boot: int = 4000,
                  seed: int = 11) -> dict:
    """差異中的差異(DiD):把「換參數的日報酬差」再依 regime 拆開。

    d_t = r_變體,t − r_基準,t(兩者皆為**全期連續**回測 → 零分段假象)。
    統計量 = 年化 mean(d | 高 state) − 年化 mean(d | 低 state)。
    這正是「條件化值不值得」的定義:>0 才代表該參數在該 regime 特別有效;
    跨 0 就是「參數好壞與 regime 無關」,條件化只是換個方式改平均值。
    """
    j = (nav_v.rename({"nav": "v"}).join(nav_b.rename({"nav": "b"}), on="date")
         .sort("date"))
    d_all = j["date"].to_list()
    v, b = j["v"].to_numpy(), j["b"].to_numpy()
    d = (v[1:] / v[:-1]) - (b[1:] / b[:-1])
    ix = {x: k for k, x in enumerate(dates)}
    s = np.array([state[ix[x]] for x in d_all[1:]])
    hi, lo = s == s.max(), s == s.min()

    def stat(dd, hh, ll):
        return float(dd[hh].mean() * TRADING_DAYS - dd[ll].mean() * TRADING_DAYS)

    t = len(d)
    rng = np.random.default_rng(seed)
    nb = int(np.ceil(t / block))
    st = rng.integers(0, t, size=(n_boot, nb))
    bix = ((st[:, :, None] + np.arange(block)[None, None, :]) % t).reshape(n_boot, -1)[:, :t]
    vals = []
    for row in bix:
        dd, hh, ll = d[row], hi[row], lo[row]
        if hh.sum() > 20 and ll.sum() > 20:
            vals.append(stat(dd, hh, ll))
    a = np.array(vals)
    return {"did": stat(d, hi, lo),
            "hi_ann": float(d[hi].mean() * TRADING_DAYS),
            "lo_ann": float(d[lo].mean() * TRADING_DAYS),
            "ci_lo": float(np.percentile(a, 2.5)), "ci_hi": float(np.percentile(a, 97.5)),
            "p_le0": float((a <= 0).mean())}


def kpi(nav: pl.DataFrame) -> dict:
    s = perf_stats(nav.sort("date"))
    return {k: s[k] for k in ("cagr", "sortino", "calmar", "mdd", "sharpe", "ann_vol")}


def halves(nav: pl.DataFrame, cut: str = "2020-07-01") -> dict:
    out = {}
    for tag, f in (("h1", pl.col("date") < pl.lit(cut).str.to_date()),
                   ("h2", pl.col("date") >= pl.lit(cut).str.to_date())):
        sub = nav.filter(f)
        out[tag] = {k: round(v, 4) for k, v in kpi(sub).items()} if sub.height > 30 else None
    return out


# ── 主流程 ───────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe", action="store_true", help="只印 regime 統計 + 單跑耗時")
    ap.add_argument("--verify", action="store_true",
                    help="驗證段截尾(WARM 暖機)與不截尾逐位等價")
    ap.add_argument("--gate", action="store_true",
                    help="讀已存 NAV,對 canonical 與各臂跑同一支 block bootstrap "
                         "(D2 KPI 閘門的 bootstrap 下界要同法比較才算數)")
    ap.add_argument("--nboot", type=int, default=4000)
    args = ap.parse_args()

    out_dir = paths.OUT_STRAT_LAB / "x_regime_cond"
    if args.gate:
        from quantlib.apex.validate import block_bootstrap_cagr
        rows = []
        for f in sorted(out_dir.glob("nav_*.parquet")):
            nv = pl.read_parquet(f).sort("date")
            s, b = perf_stats(nv), block_bootstrap_cagr(nv, n_boot=args.nboot, block=21)
            rows.append({"arm": f.stem[4:], "cagr": s["cagr"], "sortino": s["sortino"],
                         "calmar": s["calmar"], "mdd": s["mdd"],
                         "boot_ci_lo": b["ci_lo"], "boot_median": b["median"]})
        t = pl.DataFrame(rows).sort("cagr", descending=True)
        c = t.filter(pl.col("arm") == "canonical").row(0, named=True)
        t = t.with_columns([
            (pl.col("sortino") >= c["sortino"]).alias("ge_sortino"),
            (pl.col("calmar") >= c["calmar"]).alias("ge_calmar"),
            (pl.col("mdd") >= c["mdd"]).alias("ge_mdd"),
            (pl.col("boot_ci_lo") >= c["boot_ci_lo"]).alias("ge_bootlo"),
        ]).with_columns(pl.all_horizontal(["ge_sortino", "ge_calmar", "ge_mdd",
                                           "ge_bootlo"]).alias("PASS_D2"))
        with pl.Config(tbl_rows=60, tbl_width_chars=200):
            print(t)
        return

    con = data.connect()
    t0 = time.time()
    panel, feat, elig = prep_cached(con)
    print(f"[prep] {time.time()-t0:.1f}s  panel={panel.height:,}")

    dates = panel["date"].unique().sort().to_list()
    reg = build_regimes(con)
    st_vol = state_on_dates(reg, "vol_hi", dates).astype(int)
    st_bear = state_on_dates(reg, "bear", dates).astype(int)
    st_brd = state_on_dates(reg, "breadth_state", dates).astype(int)
    seg_vol, seg_bear = segments(dates, st_vol), segments(dates, st_bear)
    print(f"[regime] days={len(dates)}  vol_hi={st_vol.mean():.1%} segs={len(seg_vol)}  "
          f"bear={st_bear.mean():.1%} segs={len(seg_bear)}  "
          f"breadth lo/mid/hi={np.bincount(st_brd, minlength=3)/len(st_brd)}")

    t0 = time.time()
    nav_c, tr_c = run_s_full(panel, feat, elig, DS)
    t_one = time.time() - t0
    print(f"[canonical] {t_one:.1f}s  {kpi(nav_c)}")

    if args.verify:
        CANON0 = dict(_exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30,
                                          loser_time_stop=15),
                      _port_spec=PortSpec(n_slots=5, max_new_per_day=2))
        for (s0, s1, _) in [seg_vol[3], seg_vol[len(seg_vol) // 2], seg_vol[-2]]:
            a, _ = _seg_run(panel, feat, elig, s0, s1, CANON0, dates)
            b, _ = run_s_full(panel.filter(pl.col("date") <= pl.lit(s1)), feat, elig,
                              s0.isoformat(), **CANON0)
            b = b.filter(pl.col("date") >= pl.lit(s0))
            j = a.rename({"nav": "a"}).join(b.rename({"nav": "b"}), on="date")
            md = float((j["a"] - j["b"]).abs().max())
            print(f"[verify] {s0}~{s1} rows={j.height}/{a.height} max|Δnav|={md:.3e}")
        # 參數敏感度:同一段換 trail,NAV 必須真的改變(否則就是 kw 沒傳到)
        s0, s1, _ = seg_vol[len(seg_vol) // 2]
        for tr in (0.25, 0.35, 0.45):
            kwt = dict(_exit_spec=ExitSpec(trailing_stop=tr, time_stop=30,
                                           loser_time_stop=15),
                       _port_spec=PortSpec(n_slots=5, max_new_per_day=2))
            n, t = _seg_run(panel, feat, elig, s0, s1, kwt, dates)
            print(f"[verify:trail={tr}] {s0}~{s1} final_nav={n['nav'][-1]:.6f} "
                  f"exits={dict(t.group_by('exit_reason').len().iter_rows())}")
        return

    if args.probe:
        t0 = time.time()
        CANON0 = dict(_exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30,
                                          loser_time_stop=15),
                      _port_spec=PortSpec(n_slots=5, max_new_per_day=2))
        _seg_run(panel, feat, elig, *seg_vol[len(seg_vol) // 2][:2], CANON0, dates)
        ts = time.time() - t0
        print(f"[est] 單段 {ts:.1f}s → Track B 每臂 ≈ {ts*len(seg_vol)/60:.1f} min")
        return

    # 日期→狀態查表(entries_fn 用)
    dmap_bear = dict(zip(dates, st_bear.tolist()))
    dmap_vol = dict(zip(dates, st_vol.tolist()))
    dmap_brd = dict(zip(dates, st_brd.tolist()))

    def cap_by_state(dmap: dict, cap: dict):
        """每日新倉上限依 regime 變動:保留該日 score 前 cap[state] 名候選。"""
        def fn(entries: pl.DataFrame) -> pl.DataFrame:
            e = entries.with_columns(
                pl.col("date").replace_strict(dmap, default=0,
                                              return_dtype=pl.Int32).alias("_st"))
            return (e.with_columns([
                        pl.col("score").rank(method="ordinal", descending=True)
                        .over("date").alias("_rk"),
                        pl.col("_st").replace_strict(cap, return_dtype=pl.Int32).alias("_cap"),
                    ])
                    .filter(pl.col("_rk") <= pl.col("_cap"))
                    .drop(["_st", "_rk", "_cap"]))
        return fn

    def size_by_state(dmap: dict, wmap: dict):
        """每倉目標權重依進場當日 regime 變動(canonical = 1/5 = 0.20)。"""
        def fn(entries: pl.DataFrame) -> pl.DataFrame:
            return entries.with_columns(
                pl.col("date").replace_strict(dmap, default=0, return_dtype=pl.Int32)
                .replace_strict(wmap, return_dtype=pl.Float64).alias("weight"))
        return fn

    def size_vol_target(scale: float = 1.0, floor: float = 0.5):
        """連續型波動目標(Moreira & Muir 2017 volatility-managed portfolio):
        w_t = 0.20 × clip(滾動中位波動 / 當下 20 日波動, floor, 1.0)——只縮不放
        (上限 1.0 = 絕不加槓桿),分母是當日可知的 TAIEX 20 日實現波動。"""
        w = (reg.select(["date", "vol20", "vol_med"])
             .with_columns((0.20 * (pl.col("vol_med") / pl.col("vol20") * scale)
                            .clip(floor, 1.0)).alias("weight"))
             .select(["date", "weight"]))

        def fn(entries: pl.DataFrame) -> pl.DataFrame:
            return (entries.join(w, on="date", how="left")
                    .with_columns(pl.col("weight").fill_null(0.20)))
        return fn

    results: dict[str, dict] = {}
    navs: dict[str, pl.DataFrame] = {"canonical": nav_c}
    results["canonical"] = {"kpi": kpi(nav_c), "halves": halves(nav_c)}
    # 出場理由分佈:trail 佔比小 → 調 trail(不論條件化與否)本來就撼動不了 S
    results["canonical"]["trade_stats"] = {
        k: v for k, v in trade_stats(tr_c).items() if k != "exit_reasons"}
    results["canonical"]["exit_reasons"] = trade_stats(tr_c)["exit_reasons"]
    print(f"[exits] {results['canonical']['exit_reasons']}  "
          f"n={results['canonical']['trade_stats']['n_trades']}")
    st_all = {"vol_hi": st_vol, "bear": st_bear, "breadth": st_brd}
    results["_attribution"] = regime_attribution(nav_c, st_all, dates)
    results["_mdd_window"] = drawdown_regime(nav_c, st_all, dates)
    print(f"[attrib] {json.dumps(results['_attribution'], ensure_ascii=False)}")
    print(f"[mdd_win] {results['_mdd_window']}")

    # ── Track A:進場節流條件化(連續回測,對照 = canonical)──────────────
    # 機制:壞 tape 少放新倉(部位自然衰減 → 曝險降低但不歸零);好 tape 多放。
    # max_new 只能「往下砍」用 entries_fn,要往上開需同步放寬 PortSpec。
    track_a = {
        # 空頭(200MA 下)每日只放 1 檔新倉(canonical 2)
        "A1_bear_slow": dict(port=PortSpec(n_slots=5, max_new_per_day=2),
                             fn=cap_by_state(dmap_bear, {0: 5, 1: 1})),
        # 高波動期每日只放 1 檔
        "A2_vol_slow": dict(port=PortSpec(n_slots=5, max_new_per_day=2),
                            fn=cap_by_state(dmap_vol, {0: 5, 1: 1})),
        # 寬度分三檔:窄 1 / 中 2 / 寬 3(需 PortSpec 放寬到 3 才吃得到寬檔)
        "A3_breadth_3": dict(port=PortSpec(n_slots=5, max_new_per_day=3),
                             fn=cap_by_state(dmap_brd, {0: 1, 1: 2, 2: 3})),
        # 只開上檔(寬度寬時 3,其餘 2)——隔離「加速」與「減速」何者有效
        "A3b_breadth_up": dict(port=PortSpec(n_slots=5, max_new_per_day=3),
                               fn=cap_by_state(dmap_brd, {0: 2, 1: 2, 2: 3})),
        # 只開下檔(寬度窄時 1,其餘 2)
        "A3c_breadth_dn": dict(port=PortSpec(n_slots=5, max_new_per_day=2),
                               fn=cap_by_state(dmap_brd, {0: 1, 1: 5, 2: 5})),
        # ── Track D:regime 條件化「部位大小」(同樣零分段假象)─────────
        # 機制:高波動期新倉開小、留現金;不停止交易,只調整每筆風險預算。
        "D1_vol_size_15": dict(port=PortSpec(n_slots=5, max_new_per_day=2),
                               fn=size_by_state(dmap_vol, {0: 0.20, 1: 0.15})),
        "D1inv_vol_size_inv": dict(port=PortSpec(n_slots=5, max_new_per_day=2),
                                   fn=size_by_state(dmap_vol, {0: 0.15, 1: 0.20})),
        "D2_bear_size_15": dict(port=PortSpec(n_slots=5, max_new_per_day=2),
                                fn=size_by_state(dmap_bear, {0: 0.20, 1: 0.15})),
        # 連續型 vol targeting(教科書形態,無離散門檻可調)
        "D3_voltarget": dict(port=PortSpec(n_slots=5, max_new_per_day=2),
                             fn=size_vol_target()),
        # 高原驗證:地板放寬到 0.35(縮得更兇)——效果為真應同向
        "D3b_voltarget_f35": dict(port=PortSpec(n_slots=5, max_new_per_day=2),
                                  fn=size_vol_target(floor=0.35)),
    }
    for name, cfg in track_a.items():
        t0 = time.time()
        nav, _ = run_s_full(panel, feat, elig, DS, _port_spec=cfg["port"],
                            _entries_fn=cfg["fn"])
        navs[name] = nav
        results[name] = {
            "kpi": kpi(nav), "halves": halves(nav),
            "vs_canonical": paired_block_bootstrap(nav, nav_c, n_boot=args.nboot),
        }
        print(f"[A] {name} {time.time()-t0:.0f}s "
              f"CAGR {results[name]['kpi']['cagr']*100:.1f}% "
              f"Sortino {results[name]['kpi']['sortino']:.2f} "
              f"MDD {results[name]['kpi']['mdd']*100:.1f}% "
              f"| Δ {results[name]['vs_canonical']['ann_diff']*100:+.1f}pp "
              f"CI[{results[name]['vs_canonical']['ci_lo']*100:+.1f},"
              f"{results[name]['vs_canonical']['ci_hi']*100:+.1f}] "
              f"P≤0 {results[name]['vs_canonical']['p_le0']:.3f}")

    # ── Track B:出場/組合條件化(分段串接,對照 = 同切點 placebo)────────
    CANON = dict(_exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30, loser_time_stop=15),
                 _port_spec=PortSpec(n_slots=5, max_new_per_day=2))

    def ex(trail: float, tstop: int = 30, ltstop: int = 15, slots: int = 5,
           w: float | None = None) -> dict:
        """w=None 時每倉權重 = 1/slots(引擎預設,降 slots = 集中不降曝險);
        給 w 則固定每倉權重(降 slots = 真的降曝險,其餘留現金)。"""
        d = dict(_exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop,
                                     loser_time_stop=ltstop),
                 _port_spec=PortSpec(n_slots=slots, max_new_per_day=2))
        if w is not None:
            d["_entries_fn"] = lambda e, _w=w: e.with_columns(pl.lit(_w).alias("weight"))
        return d

    track_b = {
        # 波動切分(state 1 = 高波動)
        "vol": (seg_vol, {
            "P0_vol_placebo": {0: CANON, 1: CANON},
            # (a) 高波動收緊 trail、低波動放鬆
            "B1_trail_25_45": {0: ex(0.45), 1: ex(0.25)},
            # 溫和版(高原驗證:效果若真,應同向且單調)
            "B1m_trail_30_40": {0: ex(0.40), 1: ex(0.30)},
            # 反向(符號控制:若機制為真,反向應顯著較差)
            "B1inv_trail_45_25": {0: ex(0.25), 1: ex(0.45)},
            # 高波動縮短時間止損(換另一個鬆緊旋鈕測同一假設)
            "B3_vol_time_20": {0: CANON, 1: ex(0.35, tstop=20, ltstop=10)},
        }),
        # 趨勢切分(state 1 = 空頭)
        "bear": (seg_bear, {
            "P0_bear_placebo": {0: CANON, 1: CANON},
            # (b) 空頭降 slots 5→3:引擎預設等權 1/3 → 曝險不變、只是更集中
            "B2_bear_slots3": {0: CANON, 1: ex(0.35, slots=3)},
            # (b') 空頭 3 席但每倉仍 20% → 最高曝險 60%,其餘現金(真降曝險)
            "B2b_bear_slots3_w20": {0: CANON, 1: ex(0.35, slots=3, w=0.20)},
            # 空頭收緊 trail(不動 slots)
            "B4_bear_trail25": {0: CANON, 1: ex(0.25)},
        }),
    }
    for part, (segs, arms) in track_b.items():
        placebo_name = next(k for k in arms if k.startswith("P0"))
        for name, pbs in arms.items():
            t0 = time.time()
            nav = chained_nav(panel, feat, elig, segs, pbs, dates)
            navs[name] = nav
            r = {"kpi": kpi(nav), "halves": halves(nav), "n_segs": len(segs)}
            if name == placebo_name:
                r["vs_canonical"] = paired_block_bootstrap(nav, nav_c, n_boot=args.nboot)
            else:
                r["vs_placebo"] = paired_block_bootstrap(nav, navs[placebo_name],
                                                         n_boot=args.nboot)
                r["vs_canonical"] = paired_block_bootstrap(nav, nav_c, n_boot=args.nboot)
            results[name] = r
            cmpk = "vs_placebo" if "vs_placebo" in r else "vs_canonical"
            print(f"[B:{part}] {name} {time.time()-t0:.0f}s "
                  f"CAGR {r['kpi']['cagr']*100:.1f}% Sortino {r['kpi']['sortino']:.2f} "
                  f"MDD {r['kpi']['mdd']*100:.1f}% | vs {cmpk} "
                  f"Δ {r[cmpk]['ann_diff']*100:+.1f}pp "
                  f"CI[{r[cmpk]['ci_lo']*100:+.1f},{r[cmpk]['ci_hi']*100:+.1f}] "
                  f"P≤0 {r[cmpk]['p_le0']:.3f}")

    # ── DiD:同一個參數改動,效果是否隨 regime 而異(全期連續跑,零假象)──
    # 條件化要有價值的**充要前提**:某參數在 A regime 比在 B regime 更有利。
    # 用「統一參數」的兩條連續曲線相減,再依 regime 拆,直接量這件事。
    uni = {
        "u_trail25": dict(_exit_spec=ExitSpec(trailing_stop=0.25, time_stop=30,
                                              loser_time_stop=15)),
        "u_trail45": dict(_exit_spec=ExitSpec(trailing_stop=0.45, time_stop=30,
                                              loser_time_stop=15)),
        "u_size15": dict(_entries_fn=lambda e: e.with_columns(
            pl.lit(0.15).alias("weight"))),
    }
    results["_did"] = {}
    for tag, kw in uni.items():
        nav, _ = run_s_full(panel, feat, elig, DS, **kw)
        navs[tag] = nav
        results[tag] = {"kpi": kpi(nav),
                        "vs_canonical": paired_block_bootstrap(nav, nav_c, n_boot=args.nboot)}
        print(f"[uni] {tag} CAGR {results[tag]['kpi']['cagr']*100:.1f}% "
              f"Sortino {results[tag]['kpi']['sortino']:.2f} "
              f"MDD {results[tag]['kpi']['mdd']*100:.1f}%")
        for sname, st in (("vol_hi", st_vol), ("bear", st_bear), ("breadth", st_brd)):
            r = did_by_regime(nav, nav_c, st, dates, n_boot=args.nboot)
            results["_did"][f"{tag}|{sname}"] = r
            print(f"[DiD] {tag} × {sname}: hi {r['hi_ann']*100:+.1f}pp/y  "
                  f"lo {r['lo_ann']*100:+.1f}pp/y  DiD {r['did']*100:+.1f}pp "
                  f"CI[{r['ci_lo']*100:+.1f},{r['ci_hi']*100:+.1f}] P≤0 {r['p_le0']:.3f}")

    # ── 對稱對照:同一組參數、只把 regime 標籤對調 ────────────────────────
    # 這是整個維度最乾淨的檢定:兩臂的參數集合、平均曝險幾乎相同,唯一差別是
    # 「哪個 regime 拿到哪組參數」。差異若跨 0 → regime 標籤本身零資訊,
    # 條件化再怎麼調都只是在改平均值,不是在擇時。
    contrasts = {
        "D1_vs_D1inv(高波動縮碼 vs 低波動縮碼)":
            ("D1_vol_size_15", "D1inv_vol_size_inv"),
        "B1_vs_B1inv(高波動緊 trail vs 低波動緊 trail)":
            ("B1_trail_25_45", "B1inv_trail_45_25"),
    }
    results["_contrasts"] = {}
    for label, (a, b) in contrasts.items():
        if a in navs and b in navs:
            r = paired_block_bootstrap(navs[a], navs[b], n_boot=args.nboot)
            results["_contrasts"][label] = r
            print(f"[contrast] {label}: Δ {r['ann_diff']*100:+.1f}pp "
                  f"CI[{r['ci_lo']*100:+.1f},{r['ci_hi']*100:+.1f}] P≤0 {r['p_le0']:.3f}")

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "results.json").write_text(
        json.dumps(results, indent=2, ensure_ascii=False, default=float))
    for n, v in navs.items():
        v.write_parquet(out_dir / f"nav_{n}.parquet")
    print(f"\n[out] {out_dir}/results.json")
    print("\n逐年報酬(canonical):")
    print(yearly_table(nav_c))


if __name__ == "__main__":
    main()
