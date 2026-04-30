"""iter_24 — Catalyst Breakout + Pyramid Scale-In（贏家自然加碼）。

iter_20 v8 baseline 已 work 但等權 15% 限制了贏家上限。
iter_24 加 pyramid: position +30% 再加 10%、+60% 再加 10%（cap 30%/stock）。
讓真正的圓展 +277% 大贏家自然 scale 到最大 30%。

設計原則（user 明示）：
  - 進場：每日 close 評估觸發條件
  - 出場：條件觸發，無時間框
  - 倉位：不必等權，自然漂移
  - 最多 10 檔同時持有
  - 未投入 → 0050 buffer

進場觸發（同日全 true）：
  s1: today close > max(close[t-60..t-1])（60d 突破）
  s2: today volume > 1.5 × avg(vol[t-60..t-1])（量增 1.5x）
  s3: 最近已公告月營收 YoY ≥ 30%（catalyst 已存在）
  s4: 普通流動性過濾 ADV ≥ NT$50M、上市 ≥ 90 天、非 ETF/金融

出場（任一觸發）：
  e1: 從 entry 後高點 trailing -15%（趨勢反轉）
  e2: today close < 200d MA（長期破壞）
  e3: 最近已公告月營收 YoY < 0%（catalyst 失效）

倉位管理：
  每新進場 = 當下 NAV 的 10%（從 0050 buffer 賣出資金）
  既有 position 任其漂移、不 rebalance
  exit 後資金 → 0050 buffer
  max 10 同時持倉

評估窗口（依鐵則）：永遠 2005-01-03 → 2026-04-25 完整 21 年。

Pricing convention (2026-04-30 fix)
===================================
ALL OHLC 透過 `research.prices.fetch_adjusted_panel` 取 back-adjusted（cash
dividend + capital reduction）。Trailing stop / 60d-max / 200d-MA / breakout
/ ATR 全部在 adjusted space 計算 — 數學上等價 DRIP 持有 + 再投入。

Volume 與 trade_value 維持 raw（成交股數本就不受配息影響）。ADV 過濾改用
`trade_value` (NTD) 60d rolling mean，避免 raw close × adjusted close 混用。
"""
from __future__ import annotations

import argparse
import math
import os
import sys
import time
from datetime import date

import numpy as np
import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import connect
from prices import fetch_adjusted_panel


# Signal 參數 (v4: v1 entry/exit + 15% position size)
BREAKOUT_LOOKBACK   = 60
VOL_MULTIPLIER      = 1.5
MA_LOOKBACK         = 200
MIN_REV_YOY_ENTRY   = 30.0
MAX_REV_YOY_FADE    = 0.0
TRAILING_STOP       = 0.15
# ATR-relative trailing（--atr-trailing 啟用時生效）：
#   trail_pct = clip(entry_atr/entry_px × ATR_MULTIPLIER, MIN, MAX)
# 標準 vol (ATR/px≈3%) × 3 = 9% → clipped 至 10%；高 vol (ATR/px≈5%) × 3 = 15%；極高 vol → 25% cap
ATR_MULTIPLIER      = 3.0
ATR_LOOKBACK        = 14
TRAIL_PCT_MIN       = 0.10
TRAIL_PCT_MAX       = 0.25
USE_ATR_TRAILING    = False  # default 用固定 -15%；CLI --atr-trailing 切換
MIN_ADV             = 50_000_000
USE_MA200_EXIT      = True

TARGET_WEIGHT_NEW   = 0.10       # iter_24: 初始 10%
PYRAMID_TRIGGER_1   = 1.30
PYRAMID_TRIGGER_2   = 1.60
PYRAMID_ADD_WEIGHT  = 0.10
MAX_POSITIONS       = 10         # 還原：選項 B 邏輯（內部 max 10、最終合 portfolio cap 10）
USE_REGIME_GATE     = False      # v6 試過反而傷害，關閉
REGIME_BEAR_THRESH  = -0.10
REGIME_BULL_THRESH  = +0.05

COMMISSION          = 0.000285
SELL_TAX            = 0.003
TDPY                = 252


def build_daily_panel(con, start: date, end: date) -> pl.DataFrame:
    """建立每日 (date, code) panel + 計算 60d max close、60d avg vol、200d MA、ATR、最近已公告 yoy。

    OHLC 為 dividend / capital-reduction 還原（research.prices.fetch_adjusted_panel）。
    Volume / trade_value 維持 raw。ATR 在 adjusted space 計算（trailing stop 語意一致）。
    Universe 含 ETF / 金融 / KY 股，TWSE + TPEx 雙市場。
    """
    print("[iter24] Step 1: fetch adjusted OHLCV (twse + tpex, includes high/low for ATR) ...")
    panels = []
    for mkt in ("twse", "tpex"):
        p = fetch_adjusted_panel(
            con, start.isoformat(), end.isoformat(),
            codes=None, market=mkt,
            include_extra_history_days=300,
        )
        if not p.is_empty():
            panels.append(p)
    if not panels:
        raise RuntimeError("no data fetched from twse + tpex")
    df = (pl.concat(panels)
            .filter(
                (pl.col("close") > 0)
                & (pl.col("volume") > 0)
                & (pl.col("high") > 0)
                & (pl.col("low") > 0)
            )
            .filter(pl.col("company_code").str.contains(r"^0?[0-9]{4}[A-Z]?$"))
            .sort(["company_code", "date"])
            .rename({"volume": "vol"}))
    print(f"  raw rows: {len(df):,}")

    print("[iter24] Step 2: 計算 60d max / vol / 200d MA / next-day open / ATR(14) ...")
    # True Range 需要 prev_close（adjusted space）
    df = df.with_columns(
        pl.col("close").shift(1).over("company_code").alias("prev_close"),
    )
    df = df.with_columns(
        pl.max_horizontal([
            pl.col("high") - pl.col("low"),
            (pl.col("high") - pl.col("prev_close")).abs(),
            (pl.col("low") - pl.col("prev_close")).abs(),
        ]).alias("tr"),
    )
    df = df.with_columns([
        pl.col("close").shift(1).rolling_max(BREAKOUT_LOOKBACK).over("company_code").alias("close_60d_max"),
        pl.col("vol").shift(1).rolling_mean(BREAKOUT_LOOKBACK).over("company_code").alias("vol_60d_avg"),
        pl.col("close").rolling_mean(MA_LOOKBACK).over("company_code").alias("close_200d_ma"),
        # 60d ADV in NTD — 用 raw trade_value (actual transactions)
        pl.col("trade_value").rolling_mean(60).over("company_code").alias("adv60_ntd"),
        pl.col("tr").rolling_mean(ATR_LOOKBACK).over("company_code").alias("atr14"),
        pl.col("open").shift(-1).over("company_code").alias("next_open"),
        pl.col("date").shift(-1).over("company_code").alias("next_date"),
    ])

    print("[iter24] Step 3: 取月營收 + 計算「日 → 最近已公告 yoy」...")
    rev_sql = f"""
    SELECT company_code, year, month, monthly_revenue_yoy AS yoy
    FROM operating_revenue
    WHERE monthly_revenue_yoy IS NOT NULL
      AND year >= 2004
    """
    rev = con.sql(rev_sql).pl()
    # publish_date = (year, month) 對應月的次月 11 日（保守：12 日後 PIT-safe）
    rev = rev.with_columns([
        pl.date(pl.col("year"), pl.col("month"), 1).dt.offset_by("1mo").dt.offset_by("11d").alias("publish_d"),
    ])
    print(f"  monthly revenue rows: {len(rev):,}")

    # asof join：每個 (date, code) → 最近 publish_d ≤ date 的 yoy
    rev_pit = rev.select(["company_code", "publish_d", "yoy"]).sort(["company_code", "publish_d"])
    df = df.sort(["company_code", "date"])

    df = df.join_asof(
        rev_pit, left_on="date", right_on="publish_d", by="company_code", strategy="backward"
    ).rename({"yoy": "latest_yoy"})

    # 過濾到 backtest 區間
    df = df.filter((pl.col("date") >= start) & (pl.col("date") <= end))
    print(f"  panel size: {len(df):,} rows × {df['company_code'].n_unique()} codes")
    return df


def fetch_etf_codes(con) -> set[str]:
    return {r[0] for r in con.sql("SELECT DISTINCT company_code FROM etf").fetchall()}


def fetch_listing_first_day(con) -> dict[str, date]:
    rs = con.sql("""
        SELECT company_code, MIN(date) FROM daily_quote
        WHERE closing_price > 0 GROUP BY company_code
    """).fetchall()
    return {r[0]: r[1] for r in rs}


def fetch_industry(con) -> dict[str, str]:
    rs = con.sql("""
        SELECT DISTINCT ON (company_code) company_code, industry
        FROM operating_revenue WHERE industry IS NOT NULL
        ORDER BY company_code, year DESC, month DESC
    """).fetchall()
    return {r[0]: r[1] for r in rs}


def run_backtest(start: date, end: date, capital: float,
                  out_dir: str = "research/strat_lab/results",
                  out_suffix: str | None = None) -> dict:
    t0 = time.time()
    con = connect()

    panel = build_daily_panel(con, start, end)
    etf_codes = fetch_etf_codes(con)
    listing = fetch_listing_first_day(con)
    ind = fetch_industry(con)

    # 取 0050 daily price 作 cash buffer + 計算 regime state — 還原股價（90d warmup for ret_60d）
    px_0050_df = (fetch_adjusted_panel(
            con, start.isoformat(), end.isoformat(),
            codes=["0050"], market="twse",
            include_extra_history_days=90,
        )
        .sort("date")
        .select(["date", "close"]))
    # 60d return + regime state machine
    px_0050_df = px_0050_df.with_columns(
        (pl.col("close") / pl.col("close").shift(60) - 1).alias("ret_60d"),
    )
    states = []
    state = "bull"
    for r60 in px_0050_df["ret_60d"].to_list():
        if state == "bull":
            if r60 is not None and r60 < REGIME_BEAR_THRESH: state = "bear"
        else:
            if r60 is not None and r60 > REGIME_BULL_THRESH: state = "bull"
        states.append(state)
    px_0050_df = px_0050_df.with_columns(pl.Series("regime", states))
    px_0050_df = px_0050_df.filter((pl.col("date") >= start) & (pl.col("date") <= end))
    px_0050 = {r[0]: r[1] for r in px_0050_df.select(["date", "close"]).iter_rows()}
    regime_state = {r[0]: r[1] for r in px_0050_df.select(["date", "regime"]).iter_rows()}
    days = sorted(px_0050.keys())
    bear_days = sum(1 for d in days if regime_state[d] == "bear")
    print(f"[iter24] 交易日數: {len(days)}, bear regime: {bear_days} ({bear_days/len(days):.1%})")

    # 一次 partition_by("date") O(N) → dict 後續 O(1)
    print(f"[iter24] partition panel by date (one-shot O(N)) ...")
    t_p = time.time()
    panel_sorted = panel.sort("date")
    panel_by_date: dict[date, pl.DataFrame] = {
        d[0]: g for d, g in panel_sorted.group_by("date", maintain_order=True)
    }
    print(f"  partition in {time.time()-t_p:.1f}s ({len(panel_by_date)} dates)")

    print(f"[iter24] building (date,code) index...")
    t_p = time.time()
    panel_lookup: dict[tuple[date, str], dict] = {}
    for r in panel.iter_rows(named=True):
        panel_lookup[(r["date"], r["company_code"])] = r
    print(f"  index in {time.time()-t_p:.1f}s")

    print(f"[iter24] starting daily loop ...")

    # Position state
    positions: dict[str, dict] = {}  # code -> {entry_d, shares, entry_px, peak_px, last_px}
    cash_0050_units = capital / px_0050[days[0]]  # initial all in 0050

    nav_hist = []
    trades = []

    for di, d in enumerate(days):
        # Step 1: 更新所有持股的 last_px / peak_px + 計算 NAV
        for c in list(positions.keys()):
            p = positions[c]
            row = panel_lookup.get((d, c))
            if row is not None:
                p["last_px"] = row["close"]
                p["peak_px"] = max(p["peak_px"], row["close"])

        nav = cash_0050_units * px_0050[d] + sum(p["shares"] * p["last_px"] for p in positions.values())

        # Step 1.5: Pyramid scale-in for existing winners
        # 用初始 entry_px 算 current_factor (不是 average cost basis)
        for c in list(positions.keys()):
            p = positions[c]
            cur_factor = p["last_px"] / p["entry_px"]
            if p.get("pyramid_lvl", 0) == 0 and cur_factor >= PYRAMID_TRIGGER_1:
                # 加碼 PYRAMID_ADD_WEIGHT
                add_dollar = nav * PYRAMID_ADD_WEIGHT
                avail = cash_0050_units * px_0050[d]
                if add_dollar > avail: add_dollar = avail
                if add_dollar > 0:
                    add_shares = add_dollar / p["last_px"] / (1 + COMMISSION)
                    cost = add_shares * p["last_px"] * (1 + COMMISSION)
                    cash_0050_units -= cost / px_0050[d]
                    p["shares"] += add_shares
                    p["pyramid_lvl"] = 1
                    trades.append({"date": d, "code": c, "action": "pyramid1",
                                    "entry_d": p["entry_d"], "entry_px": p["last_px"],
                                    "exit_px": None, "ret": None})
            elif p.get("pyramid_lvl", 0) == 1 and cur_factor >= PYRAMID_TRIGGER_2:
                add_dollar = nav * PYRAMID_ADD_WEIGHT
                avail = cash_0050_units * px_0050[d]
                if add_dollar > avail: add_dollar = avail
                if add_dollar > 0:
                    add_shares = add_dollar / p["last_px"] / (1 + COMMISSION)
                    cost = add_shares * p["last_px"] * (1 + COMMISSION)
                    cash_0050_units -= cost / px_0050[d]
                    p["shares"] += add_shares
                    p["pyramid_lvl"] = 2
                    trades.append({"date": d, "code": c, "action": "pyramid2",
                                    "entry_d": p["entry_d"], "entry_px": p["last_px"],
                                    "exit_px": None, "ret": None})

        # 重新算 nav（pyramid 後）
        nav = cash_0050_units * px_0050[d] + sum(p["shares"] * p["last_px"] for p in positions.values())

        # Step 2: 檢查出場條件
        to_exit = []
        for c, p in positions.items():
            row = panel_lookup.get((d, c))
            if row is None:
                continue
            close = row["close"]
            ma200 = row["close_200d_ma"]
            yoy = row["latest_yoy"]
            # e1: trailing — 固定 -15% 或 ATR-relative（依 USE_ATR_TRAILING）
            if USE_ATR_TRAILING:
                entry_atr = p.get("entry_atr")
                entry_px = p.get("entry_px")
                if entry_atr is not None and entry_atr > 0 and entry_px and entry_px > 0:
                    atr_pct = entry_atr / entry_px
                    trail_pct = max(TRAIL_PCT_MIN, min(TRAIL_PCT_MAX, atr_pct * ATR_MULTIPLIER))
                else:
                    trail_pct = TRAILING_STOP  # fallback when ATR unavailable
            else:
                trail_pct = TRAILING_STOP
            if p["peak_px"] > 0 and close / p["peak_px"] - 1 <= -trail_pct:
                to_exit.append((c, "trailing_atr" if USE_ATR_TRAILING else "trailing"))
            # e2: 跌破 200d MA (toggleable)
            elif USE_MA200_EXIT and ma200 is not None and close < ma200:
                to_exit.append((c, "below_ma200"))
            # e3: revenue catalyst fade
            elif yoy is not None and yoy < MAX_REV_YOY_FADE:
                to_exit.append((c, "yoy_fade"))

        # 執行出場：v8 改用次日 open 賣
        for c, reason in to_exit:
            p = positions.pop(c)
            row = panel_lookup.get((d, c))
            exit_px = row["next_open"] if (row and row["next_open"]) else p["last_px"]
            proceeds = p["shares"] * exit_px * (1 - SELL_TAX - COMMISSION)
            # 次日才換到 0050，所以用 next-day 價
            cash_0050_units += proceeds / px_0050[d]
            trades.append({
                "date": d, "code": c, "action": f"exit_{reason}",
                "entry_d": p["entry_d"],
                "entry_px": p["entry_px"], "exit_px": exit_px,
                "ret": exit_px / p["entry_px"] - 1,
            })

        # Step 3: 掃當日新訊號 (regime gate: bear 時暫停新進場)
        regime_ok = (not USE_REGIME_GATE) or (regime_state.get(d, "bull") == "bull")
        if regime_ok and len(positions) < MAX_POSITIONS:
            today = panel_by_date.get(d, pl.DataFrame())
            if not today.is_empty():
                # 訊號條件 (用 polars filter)
                signals = today.filter(
                    (pl.col("close") > pl.col("close_60d_max"))
                    & (pl.col("vol") > pl.col("vol_60d_avg") * VOL_MULTIPLIER)
                    & (pl.col("latest_yoy") >= MIN_REV_YOY_ENTRY)
                    & (pl.col("adv60_ntd") >= MIN_ADV)
                ).sort(by=[(pl.col("close") / pl.col("close_60d_max") - 1)], descending=True)

                for r in signals.iter_rows(named=True):
                    if len(positions) >= MAX_POSITIONS: break
                    c = r["company_code"]
                    if c in positions: continue
                    if c in etf_codes: continue
                    industry = ind.get(c, "")
                    if "金融" in industry or "證券" in industry or "保險" in industry: continue
                    listed_d = listing.get(c)
                    if not listed_d or (d - listed_d).days < 90: continue

                    # 進場：用今日 close 計算 shares = 10% NAV / close × (1 - commission)
                    target_dollar = nav * TARGET_WEIGHT_NEW
                    if target_dollar > cash_0050_units * px_0050[d]:
                        # 不夠 cash → skip（保守，不強迫 sell 0050）
                        target_dollar = cash_0050_units * px_0050[d]
                        if target_dollar <= 0: break
                    shares = target_dollar / r["close"] / (1 + COMMISSION)
                    cost_dollar = shares * r["close"] * (1 + COMMISSION)
                    cash_0050_units -= cost_dollar / px_0050[d]
                    positions[c] = {
                        "entry_d": d, "shares": shares,
                        "entry_px": r["close"], "peak_px": r["close"],
                        "last_px": r["close"],
                        "pyramid_lvl": 0,
                        "entry_atr": r.get("atr14"),  # lock-in ATR at entry (for ATR-trailing)
                    }
                    trades.append({
                        "date": d, "code": c, "action": "entry",
                        "entry_d": d, "entry_px": r["close"], "exit_px": None, "ret": None,
                    })

        # final NAV after all transactions
        nav = cash_0050_units * px_0050[d] + sum(p["shares"] * p["last_px"] for p in positions.values())
        nav_hist.append((d, nav, len(positions)))

        if di % 1000 == 0:
            n_active = len(positions)
            n_entry = sum(1 for t in trades if t["action"] == "entry")
            n_exit = sum(1 for t in trades if t["action"].startswith("exit"))
            print(f"  [iter24] day {di:>5}/{len(days)} {d} active={n_active:>2} "
                  f"nav=${nav:,.0f} entries={n_entry} exits={n_exit}")

    print(f"[iter24] 完成 backtest {time.time()-t0:.1f}s")
    n_entry = sum(1 for t in trades if t["action"] == "entry")
    print(f"[iter24] 總進場: {n_entry}")
    for reason in ["trailing", "below_ma200", "yoy_fade"]:
        n = sum(1 for t in trades if t["action"] == f"exit_{reason}")
        print(f"  exit_{reason}: {n}")

    nav_arr = np.array([n for _, n, _ in nav_hist])
    rets = np.diff(np.concatenate([[capital], nav_arr])) / np.concatenate([[capital], nav_arr[:-1]])

    years = max((days[-1] - days[0]).days / 365.25, 1e-9)
    cagr = (nav_arr[-1] / capital) ** (1 / years) - 1
    vol_ann = rets.std(ddof=1) * math.sqrt(TDPY)
    downside = rets[rets < 0]
    downvol_ann = (downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 1e-9
    sharpe = (cagr - 0.01) / vol_ann if vol_ann > 0 else 0
    sortino = (cagr - 0.01) / downvol_ann if downvol_ann > 0 else 0
    peak, mdd = capital, 0.0
    for v in nav_arr:
        peak = max(peak, v); mdd = min(mdd, (v - peak) / peak)

    os.makedirs(out_dir, exist_ok=True)
    suffix = f"_{out_suffix}" if out_suffix else ""
    pl.DataFrame({"date": [d for d, _, _ in nav_hist],
                   "nav": nav_arr,
                   "n_active": [n for _, _, n in nav_hist]}
                ).write_csv(os.path.join(out_dir, f"iter_24{suffix}_daily.csv"))
    pl.DataFrame(trades).write_csv(os.path.join(out_dir, f"iter_24{suffix}_trades.csv"))

    return {
        "iter": 20, "runtime_s": time.time() - t0,
        "params": {
            "BREAKOUT_LOOKBACK": BREAKOUT_LOOKBACK, "VOL_MULTIPLIER": VOL_MULTIPLIER,
            "MA_LOOKBACK": MA_LOOKBACK, "MIN_REV_YOY_ENTRY": MIN_REV_YOY_ENTRY,
            "TRAILING_STOP": TRAILING_STOP, "TARGET_WEIGHT_NEW": TARGET_WEIGHT_NEW,
            "MAX_POSITIONS": MAX_POSITIONS,
        },
        "n_trades_entry": n_entry,
        "CAGR": cagr, "Sharpe": sharpe, "Sortino": sortino,
        "MDD": mdd, "Calmar": cagr / abs(mdd) if mdd < 0 else 0,
        "vol_ann": vol_ann, "final": float(nav_arr[-1]),
    }


def main():
    global MAX_POSITIONS, USE_REGIME_GATE, USE_ATR_TRAILING
    global BREAKOUT_LOOKBACK, VOL_MULTIPLIER, MIN_REV_YOY_ENTRY, MAX_REV_YOY_FADE
    global ATR_MULTIPLIER, TRAILING_STOP
    ap = argparse.ArgumentParser(description="iter_24 catalyst breakout + pyramid scale-in")
    ap.add_argument("--start", default="2005-01-03")
    ap.add_argument("--end",   default="2026-04-25")
    ap.add_argument("--capital", type=float, default=1_000_000)
    ap.add_argument("--max-positions", type=int, default=MAX_POSITIONS,
                    help=f"Max simultaneous positions (default {MAX_POSITIONS}). 取代過去 iter_24_max{{1..9}}.py 共 9 個檔。")
    ap.add_argument("--regime-gate", action="store_true",
                    help="Enable bear-regime entry gate (default off; v6 試過反而傷害).")
    ap.add_argument("--atr-trailing", action="store_true",
                    help=f"Use ATR-relative trailing stop (clip [{TRAIL_PCT_MIN:.0%}, {TRAIL_PCT_MAX:.0%}] × {ATR_MULTIPLIER}) instead of fixed -{TRAILING_STOP:.0%}.")
    # Phase B: entry/exit 參數 sweep flags
    ap.add_argument("--breakout-lookback", type=int, default=BREAKOUT_LOOKBACK,
                    help=f"Days for breakout high (default {BREAKOUT_LOOKBACK}).")
    ap.add_argument("--vol-multiplier", type=float, default=VOL_MULTIPLIER,
                    help=f"Volume must be > N × 60d avg vol (default {VOL_MULTIPLIER}).")
    ap.add_argument("--yoy-entry", type=float, default=MIN_REV_YOY_ENTRY,
                    help=f"Min monthly revenue YoY to enter (default {MIN_REV_YOY_ENTRY}%).")
    ap.add_argument("--yoy-exit", type=float, default=MAX_REV_YOY_FADE,
                    help=f"Exit if YoY drops below this (default {MAX_REV_YOY_FADE}%).")
    ap.add_argument("--atr-mult", type=float, default=ATR_MULTIPLIER,
                    help=f"ATR multiplier for trailing stop (default {ATR_MULTIPLIER}).")
    ap.add_argument("--trailing-stop", type=float, default=TRAILING_STOP,
                    help=f"Fixed trailing stop pct (used when --atr-trailing OFF, default {TRAILING_STOP:.0%}).")
    ap.add_argument("--out-suffix", default=None,
                    help="Output filename suffix (e.g. 'max5' → iter_24_max5_daily.csv). Default: max{N}[_atr].")
    args = ap.parse_args()

    # Apply CLI overrides
    MAX_POSITIONS = args.max_positions
    USE_REGIME_GATE = args.regime_gate
    USE_ATR_TRAILING = args.atr_trailing
    BREAKOUT_LOOKBACK = args.breakout_lookback
    VOL_MULTIPLIER = args.vol_multiplier
    MIN_REV_YOY_ENTRY = args.yoy_entry
    MAX_REV_YOY_FADE = args.yoy_exit
    ATR_MULTIPLIER = args.atr_mult
    TRAILING_STOP = args.trailing_stop
    default_suffix = f"max{MAX_POSITIONS}" + ("_atr" if USE_ATR_TRAILING else "")
    out_suffix = args.out_suffix or default_suffix

    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)

    print("=" * 78)
    print(f"iter_20 = Catalyst-Confirmed Breakout (daily, no time stop)")
    print(f"  Entry: 60d 突破 + 量 ≥ {VOL_MULTIPLIER}× 60d avg + 月營收 YoY ≥ {MIN_REV_YOY_ENTRY}%")
    print(f"  Sizing: 每新進場 {TARGET_WEIGHT_NEW:.0%} NAV (自然漂移、無 rebalance)")
    print(f"  Exit: trailing -{TRAILING_STOP:.0%} OR close < 200d MA OR yoy < {MAX_REV_YOY_FADE}%")
    print(f"  Max positions: {MAX_POSITIONS}, 未投入 → 0050 buffer")
    print(f"  窗口（鐵則 21y）: {start} → {end}")
    print("=" * 78)

    res = run_backtest(start, end, args.capital, out_suffix=out_suffix)
    print(f"\n--- iter_20 結果 ---")
    print(f"  CAGR:            {res['CAGR']:+.2%}")
    print(f"  Sharpe:          {res['Sharpe']:.3f}")
    print(f"  Sortino:         {res['Sortino']:.3f}  ★")
    print(f"  MDD:             {res['MDD']:.2%}")
    print(f"  finalNAV:        ${res['final']:,.0f}")
    print(f"  進場次數:        {res['n_trades_entry']}")

    print(f"\n--- 對照 ---")
    print(f"  hold_2330: CAGR +24.23% Sortino 1.333 MDD -45.86%")
    print(f"  iter_13 mcap (best legit): CAGR +22.76% Sortino 1.352 MDD -44.00%")
    print(f"  hold_0050: CAGR +13.45% Sortino 0.823 MDD -55.66%")

    print(f"\n--- 是否破 2330？ ---")
    print(f"  CAGR    > 2330: {'✓' if res['CAGR'] > 0.2423 else '✗'} ({res['CAGR']:+.2%})")
    print(f"  Sortino > 2330: {'✓' if res['Sortino'] > 1.333 else '✗'} ({res['Sortino']:.3f})")
    print(f"  MDD     > 2330: {'✓' if res['MDD'] > -0.4586 else '✗'} ({res['MDD']:.2%})")


if __name__ == "__main__":
    main()
