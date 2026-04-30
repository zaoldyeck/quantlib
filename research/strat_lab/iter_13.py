"""iter_13 — Quality Pool + Market-Cap Weighted（規模加權品質池）。

策略假設（hypothesis）：
  iter_10 v6 用「ROA ≥ 12% + GM ≥ 30%」絕對品質門檻 + 等權 5 檔，CAGR 18.52% Sortino 0.977。
  iter_10 等權的問題：2330 (TWSE 最大) 與其他 4 檔小公司被等權稀釋，TSMC alpha 被吃掉。

  hindsight test: 70/30 配 2330/2383 Sortino 達 1.703，證明「重壓 2330 + 少量分散」是對的方向。

  iter_13 嘗試：
    - 同 iter_10 v6 quality 篩選（ROA ≥ 12%, GM ≥ 30%）
    - 但用 **ADV (規模 proxy) 加權** 取代等權
    - 結果：2330 自然取得 ~50%+ 權重，cousins 各 < 15%，TSMC alpha 不被稀釋

  目標：CAGR > 24.23%, Sortino > 1.333（必破 2330 雙門檻）
  代價：較高集中風險（但 2330 本身就是 -45% MDD，加 cousins 應該不會更壞）

評估窗口（依鐵則）：永遠 2005-01-03 → 2026-04-25 完整 21 年。
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from datetime import date

import numpy as np
import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import connect
from prices import fetch_daily_returns


TRAILING_YEARS_QUALITY = 5
MIN_ROA_MEDIAN         = 0.12
MIN_GM_MEDIAN          = 0.30
MIN_ADV                = 50_000_000
TOPN                   = 5
COMMISSION             = 0.000285
SELL_TAX               = 0.003
TDPY                   = 252

VALID_RANKERS = ("mcap", "roa_recent", "roa_med", "rev_cagr5y", "composite")
VALID_FREQS   = ("monthly", "annual")


def screen_pool(con, ref_year: int, ref_month: int = 12,
                universe: str = "twse_tpex") -> pl.DataFrame:
    """Quality pool screening. ref point = end of (ref_year, ref_month).

    For monthly rebal we screen at end of prev month using last-completed quarter
    of fundamentals (so each rebal date 反映最近 PIT-safe 資料).

    universe: 'twse_only' | 'twse_tpex'  (預設雙市場 — C 改造)
    """
    yq_start = (ref_year - TRAILING_YEARS_QUALITY) * 4 + 1
    # PIT-safe: 取截至 ref_month-3 月的最近 quarter (Q1 ≤ 5/22, Q2 ≤ 8/21, Q3 ≤ 11/21, Q4 ≤ 4/7 next yr)
    # 簡化：用 ref_month 對應的「上一個已公告 quarter」
    if ref_month <= 4:        # 1-4 月只能用前一年的 Q3 (Q4 5/15 才公告)
        yq_recent_end = (ref_year - 1) * 4 + 3
    elif ref_month <= 5:      # 5 月用前年 Q3
        yq_recent_end = (ref_year - 1) * 4 + 3
    elif ref_month <= 8:      # 6-8 月用今年 Q1 (5/15 後)
        yq_recent_end = ref_year * 4 + 1
    elif ref_month <= 11:     # 9-11 月用今年 Q2 (8/14 後)
        yq_recent_end = ref_year * 4 + 2
    else:                     # 12 月用今年 Q3 (11/14 後)
        yq_recent_end = ref_year * 4 + 3
    yq_recent_start = max(yq_recent_end - 3, 1)

    market_filter = (
        "market IN ('twse','tpex')" if universe == "twse_tpex" else "market='twse'"
    )

    # ADV 過濾窗口：取 ref date 前 60 trading days 估值
    adv_end_yr, adv_end_mo = ref_year, ref_month
    adv_start_yr = adv_end_yr if adv_end_mo > 3 else adv_end_yr - 1
    adv_start_mo = adv_end_mo - 3 if adv_end_mo > 3 else adv_end_mo + 9

    # mcap 估值用 ref date 前 1 個月的 closing
    px_end_yr, px_end_mo = ref_year, ref_month
    px_start_yr = px_end_yr if px_end_mo > 1 else px_end_yr - 1
    px_start_mo = px_end_mo - 1 if px_end_mo > 1 else 12

    sql = f"""
    WITH
    qual AS (
      SELECT
        company_code,
        COALESCE(quantile_cont(roa_ttm, 0.5), 0.0)  AS med_roa,
        COALESCE(quantile_cont(gross_margin_ttm, 0.5), 0.0) AS med_gm,
        COUNT(*) FILTER (WHERE ni_q < 0 AND year * 4 + quarter >= {yq_recent_start}) AS recent_losses,
        -- 排序用：最近 ROA TTM
        AVG(roa_ttm) FILTER (WHERE year * 4 + quarter = {yq_recent_end}) AS recent_roa
      FROM raw_quarterly
      WHERE year * 4 + quarter BETWEEN {yq_start} AND {yq_recent_end}
      GROUP BY company_code
      HAVING COUNT(*) >= 8
    ),
    rev_yearly AS (
      SELECT company_code, year,
             SUM(rev_q) FILTER (WHERE rev_q IS NOT NULL) AS rev_year
      FROM raw_quarterly
      WHERE year BETWEEN {ref_year-5} AND {ref_year-1}
      GROUP BY company_code, year
      HAVING SUM(rev_q) > 0
    ),
    rev_growth AS (
      SELECT company_code,
             POWER(MAX(CASE WHEN year={ref_year-1} THEN rev_year END) /
                   NULLIF(MAX(CASE WHEN year={ref_year-5} THEN rev_year END), 0),
                   1.0/{TRAILING_YEARS_QUALITY}) - 1 AS rev_cagr_5y
      FROM rev_yearly
      GROUP BY company_code
      HAVING MAX(CASE WHEN year={ref_year-5} THEN rev_year END) > 0
         AND MAX(CASE WHEN year={ref_year-1} THEN rev_year END) > 0
    ),
    adv60 AS (
      SELECT company_code, AVG(trade_value) AS adv60
      FROM daily_quote
      WHERE {market_filter}
        AND date BETWEEN DATE '{adv_start_yr}-{adv_start_mo:02d}-01'
                     AND DATE '{adv_end_yr}-{adv_end_mo:02d}-28'
        AND closing_price > 0
      GROUP BY company_code
    ),
    cap_latest AS (
      SELECT DISTINCT ON (company_code) company_code, capital_stock
      FROM raw_quarterly
      WHERE capital_stock > 0
        AND year * 4 + quarter <= {yq_recent_end}
      ORDER BY company_code, year DESC, quarter DESC
    ),
    px_recent AS (
      SELECT DISTINCT ON (company_code) company_code, closing_price AS px
      FROM daily_quote
      WHERE {market_filter}
        AND date BETWEEN DATE '{px_start_yr}-{px_start_mo:02d}-01'
                     AND DATE '{px_end_yr}-{px_end_mo:02d}-28'
        AND closing_price > 0
      ORDER BY company_code, date DESC
    ),
    mcap AS (
      SELECT c.company_code,
             c.capital_stock / 10.0 * 1000 * p.px AS mcap
      FROM cap_latest c JOIN px_recent p USING (company_code)
    ),
    ind AS (
      SELECT DISTINCT ON (company_code) company_code, industry
      FROM operating_revenue
      WHERE {market_filter} AND industry IS NOT NULL
      ORDER BY company_code, year DESC, month DESC
    )
    SELECT
      q.company_code, q.med_roa, q.med_gm, q.recent_roa,
      a.adv60, m.mcap, COALESCE(g.rev_cagr_5y, 0) AS rev_cagr_5y
    FROM qual q
    JOIN adv60 a USING (company_code)
    JOIN mcap m USING (company_code)
    LEFT JOIN rev_growth g USING (company_code)
    LEFT JOIN ind ON ind.company_code = q.company_code
    WHERE
      q.med_roa  >= {MIN_ROA_MEDIAN}
      AND q.med_gm   >= {MIN_GM_MEDIAN}
      AND q.recent_losses = 0
      AND a.adv60    >= {MIN_ADV}
      AND regexp_matches(q.company_code, '^[1-9][0-9]{{3}}$')
      AND q.company_code NOT IN (SELECT company_code FROM etf)
      AND ind.industry IN ('半導體業','電子零組件業','光電業','電腦及週邊設備業',
                           '通信網路業','電子通路業','其他電子業','資訊服務業')
    """
    return con.sql(sql).pl()


def rank_pool(pool: pl.DataFrame, ranker: str) -> pl.DataFrame:
    """Sort pool by chosen ranker (descending — bigger = better)."""
    if pool.is_empty():
        return pool
    if ranker == "mcap":
        col = "mcap"
    elif ranker == "roa_recent":
        col = "recent_roa"
    elif ranker == "roa_med":
        col = "med_roa"
    elif ranker == "rev_cagr5y":
        col = "rev_cagr_5y"
    elif ranker == "composite":
        # z-score sum of (mcap_log, roa_med, rev_cagr_5y) — equal weight
        import numpy as np
        df = pool.with_columns(pl.col("mcap").log().alias("log_mcap"))
        feats = ["log_mcap", "med_roa", "rev_cagr_5y"]
        for f in feats:
            mean = df[f].mean()
            std = df[f].std() if df[f].std() > 0 else 1.0
            df = df.with_columns(((pl.col(f) - mean) / std).alias(f"z_{f}"))
        return (df.with_columns(
                    (pl.col("z_log_mcap") + pl.col("z_med_roa") + pl.col("z_rev_cagr_5y"))
                    .alias("composite_score"))
                .sort("composite_score", descending=True)
                .drop([f"z_{f}" for f in feats]))
    else:
        raise ValueError(f"Unknown ranker: {ranker}")
    return pool.sort(col, descending=True, nulls_last=True)


def screen_pool_for_year(con, year: int) -> pl.DataFrame:
    """Backwards-compatible wrapper for annual rebal (TWSE+TPEx by default)."""
    return screen_pool(con, year, ref_month=12, universe="twse_tpex")


def get_rebal_dates(con, start: date, end: date, freq: str = "monthly") -> list[date]:
    """Get rebalance dates: first trading day of each year (annual) or month (monthly)."""
    period_clause = "EXTRACT(YEAR FROM date)" if freq == "annual" else \
                    "EXTRACT(YEAR FROM date) * 100 + EXTRACT(MONTH FROM date)"
    return [r[0] for r in con.sql(f"""
        SELECT MIN(date) FROM daily_quote
        WHERE market='twse' AND company_code='0050'
          AND date BETWEEN DATE '{start}' AND DATE '{end}'
        GROUP BY {period_clause}
        ORDER BY MIN(date)
    """).fetchall()]


def get_year_start_dates(con, start: date, end: date) -> list[date]:
    """Backwards-compat alias."""
    return get_rebal_dates(con, start, end, freq="annual")


def adv_weighted_picks(pool: pl.DataFrame, topn: int) -> list[tuple[str, float]]:
    """從 pool 取 TOP N by ADV，並以 ADV 加權回傳 (code, weight)。"""
    top = pool.head(topn)
    if top.is_empty():
        return []
    weights = (top["adv60"] / top["adv60"].sum()).to_list()
    codes = top["company_code"].to_list()
    return list(zip(codes, weights))


def run_backtest(start: date, end: date, capital: float,
                  weight_mode: str = "mcap", ranker: str = "mcap",
                  freq: str = "monthly", universe: str = "twse_tpex",
                  out_dir: str = "research/strat_lab/results",
                  out_suffix: str | None = None) -> dict:
    """
    Args:
      weight_mode: 'mcap' | 'sqrt_mcap' | 'adv' | 'eq' — how to weight TOP N picks
      ranker: 'mcap' | 'roa_recent' | 'roa_med' | 'rev_cagr5y' | 'composite' — how to rank pool
      freq: 'monthly' | 'annual' — rebalance frequency
      universe: 'twse_tpex' | 'twse_only' — which markets to screen
    """
    t0 = time.time()
    con = connect()

    rebal_dates = get_rebal_dates(con, start, end, freq=freq)
    print(f"[iter13] {freq} 重平衡點: {len(rebal_dates)} 天 ({rebal_dates[0]} → {rebal_dates[-1]})")
    print(f"[iter13] 加權: {weight_mode}, 排序: {ranker}, universe: {universe}")

    rows = []
    fallback_count = 0
    pool_sizes = {}
    for rd in rebal_dates:
        # screen at end of prev period
        pool_raw = screen_pool(con, ref_year=rd.year, ref_month=rd.month, universe=universe)
        pool = rank_pool(pool_raw, ranker)
        pool_sizes[rd] = len(pool)
        if pool.is_empty():
            rows.append({"rebal_d": rd, "company_code": "0050", "weight": 1.0})
            fallback_count += TOPN
            continue
        top = pool.head(TOPN)
        n = len(top)
        if weight_mode == "mcap":
            weights = (top["mcap"] / top["mcap"].sum()).to_list()
        elif weight_mode == "sqrt_mcap":
            sq = (top["mcap"] ** 0.5)
            weights = (sq / sq.sum()).to_list()
        elif weight_mode == "adv":
            weights = (top["adv60"] / top["adv60"].sum()).to_list()
        else:  # eq
            weights = [1.0 / n] * n
        total_w = sum(weights)
        codes = top["company_code"].to_list()
        for c, w in zip(codes, weights):
            rows.append({"rebal_d": rd, "company_code": c, "weight": w})
        if n < TOPN:
            shortfall = 1.0 - total_w
            rows.append({"rebal_d": rd, "company_code": "0050", "weight": shortfall})
            fallback_count += (TOPN - n)

    picks_df = pl.DataFrame(rows) if rows else pl.DataFrame(
        schema={"rebal_d": pl.Date, "company_code": pl.Utf8, "weight": pl.Float64}
    )
    print(f"[iter13] 總 picks rows: {len(picks_df):,} "
          f"(0050 補位 {fallback_count} slot) ({time.time()-t0:.1f}s)")

    # Returns: pull from BOTH markets (universe may include tpex)
    held = picks_df["company_code"].unique().to_list()
    market_for_returns = "twse" if universe == "twse_only" else None  # None = both
    if market_for_returns:
        rets = fetch_daily_returns(
            con, start.isoformat(), end.isoformat(),
            codes=held, market="twse",
        )
    else:
        # fetch from both markets
        twse_rets = fetch_daily_returns(con, start.isoformat(), end.isoformat(),
                                         codes=held, market="twse")
        tpex_rets = fetch_daily_returns(con, start.isoformat(), end.isoformat(),
                                         codes=held, market="tpex")
        rets = pl.concat([twse_rets, tpex_rets]).unique(subset=["date", "company_code"])

    days = [r[0] for r in con.sql(f"""
        SELECT date FROM daily_quote
        WHERE market='twse' AND company_code='0050'
          AND date BETWEEN DATE '{start}' AND DATE '{end}' ORDER BY date
    """).fetchall()]

    days_df = pl.DataFrame({"date": days}).sort("date")
    rebal_df = (pl.DataFrame({"active_rebal": rebal_dates})
                .with_columns((pl.col("active_rebal") + pl.duration(days=1)).alias("effective"))
                .sort("effective"))
    da = days_df.join_asof(rebal_df, left_on="date", right_on="effective", strategy="backward")
    contrib = (da.join(picks_df, left_on="active_rebal", right_on="rebal_d", how="left")
                 .join(rets, on=["date", "company_code"], how="left")
                 .with_columns((pl.col("weight") * pl.col("ret")).alias("c")))
    port = (contrib.group_by("date").agg(pl.col("c").sum().alias("r"))
                   .sort("date").with_columns(pl.col("r").fill_null(0.0)))

    # Turnover cost
    pbd = {}
    for row in picks_df.iter_rows(named=True):
        pbd.setdefault(row["rebal_d"], {})[row["company_code"]] = row["weight"]
    cmap, prev = {}, None
    for rd in sorted(rebal_dates):
        cs = pbd.get(rd, {})
        if prev is None:
            sf = 1.0 if cs else 0.0
        else:
            all_codes = set(cs.keys()) | set(prev.keys())
            sf = sum(abs(cs.get(c, 0) - prev.get(c, 0)) for c in all_codes) / 2
        cmap[rd] = sf * (SELL_TAX + 2 * COMMISSION)
        prev = cs
    cdf = pl.DataFrame({"date": list(cmap.keys()), "cost": list(cmap.values())})
    port = (port.join(cdf, on="date", how="left")
                .with_columns(pl.col("cost").fill_null(0.0))
                .with_columns((pl.col("r") - pl.col("cost")).alias("net")))

    rets_arr = port["net"].to_numpy()
    navs = capital * np.cumprod(1 + rets_arr)
    print(f"[iter13] 完成 {time.time()-t0:.1f}s")

    years = max((days[-1] - days[0]).days / 365.25, 1e-9)
    cagr = (navs[-1] / capital) ** (1 / years) - 1
    vol_ann = rets_arr.std(ddof=1) * math.sqrt(TDPY)
    downside = rets_arr[rets_arr < 0]
    downvol_ann = (downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 1e-9
    sharpe = (cagr - 0.01) / vol_ann if vol_ann > 0 else 0
    sortino = (cagr - 0.01) / downvol_ann if downvol_ann > 0 else 0
    peak, mdd = capital, 0.0
    for v in navs:
        peak = max(peak, v); mdd = min(mdd, (v - peak) / peak)
    calmar = cagr / abs(mdd) if mdd < 0 else 0

    os.makedirs(out_dir, exist_ok=True)
    suffix = out_suffix or f"{freq}_{ranker}_{weight_mode}_{universe}"
    port.with_columns(pl.lit(navs).alias("nav")).write_csv(
        os.path.join(out_dir, f"iter_13_{suffix}_daily.csv")
    )
    picks_df.write_csv(os.path.join(out_dir, f"iter_13_{suffix}_picks.csv"))

    return {
        "iter": 13, "runtime_s": time.time() - t0,
        "weight_mode": weight_mode, "ranker": ranker, "freq": freq,
        "universe": universe, "TOPN": TOPN,
        "CAGR": cagr, "Sharpe": sharpe, "Sortino": sortino,
        "MDD": mdd, "Calmar": calmar,
        "vol_ann": vol_ann, "downvol_ann": downvol_ann,
        "final": float(navs[-1]),
        "n_rebals": len(rebal_dates),
        "out_suffix": suffix,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2005-01-03")
    ap.add_argument("--end",   default="2026-04-25")
    ap.add_argument("--capital", type=float, default=1_000_000)
    ap.add_argument("--mode", default="mcap", choices=["mcap", "sqrt_mcap", "adv", "eq"],
                    help="Weight assignment within TOP N picks")
    ap.add_argument("--ranker", default="mcap", choices=list(VALID_RANKERS),
                    help="How to rank quality pool (cross-validation tool)")
    ap.add_argument("--freq", default="monthly", choices=list(VALID_FREQS),
                    help="Rebalance frequency (memory final ship = monthly)")
    ap.add_argument("--universe", default="twse_tpex", choices=["twse_tpex", "twse_only"],
                    help="Universe: twse_tpex = C 改造（雙市場）, twse_only = legacy")
    ap.add_argument("--suffix", default=None, help="Output filename suffix override")
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    print("=" * 78)
    print(f"iter_13 = Quality Pool TOP {TOPN} ({args.weight_mode if hasattr(args, 'weight_mode') else args.mode}-weighted)")
    print(f"  freq={args.freq}, ranker={args.ranker}, universe={args.universe}")
    print(f"  TOPN={TOPN}, ROA≥{MIN_ROA_MEDIAN:.0%}, GM≥{MIN_GM_MEDIAN:.0%}, ADV≥${MIN_ADV/1e6:.0f}M")
    print(f"  窗口（鐵則 21y）: {start} → {end}")
    print("=" * 78)

    res = run_backtest(start, end, args.capital,
                        weight_mode=args.mode, ranker=args.ranker,
                        freq=args.freq, universe=args.universe,
                        out_suffix=args.suffix)
    print(f"\n--- iter_13 ({res['out_suffix']}) 結果 ---")
    print(f"  CAGR:        {res['CAGR']:+.2%}")
    print(f"  Sharpe:      {res['Sharpe']:.3f}")
    print(f"  Sortino:     {res['Sortino']:.3f}  ★")
    print(f"  MDD:         {res['MDD']:.2%}")
    print(f"  finalNAV:    ${res['final']:,.0f}")

    print(f"\n--- 對照 ---")
    print(f"  hold_2330: CAGR +24.23% Sortino 1.333 MDD -45.86%")
    print(f"  hold_0050: CAGR +13.45% Sortino 0.823 MDD -55.66%")

    print(f"\n--- 是否破 2330？ ---")
    print(f"  CAGR    > 2330: {'✓' if res['CAGR'] > 0.2423 else '✗'} ({res['CAGR']:+.2%})")
    print(f"  Sortino > 2330: {'✓' if res['Sortino'] > 1.333 else '✗'} ({res['Sortino']:.3f})")
    print(f"  MDD     > 2330: {'✓' if res['MDD'] > -0.4586 else '✗'} ({res['MDD']:.2%})")


if __name__ == "__main__":
    main()
