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


def screen_pool_for_year(con, year: int) -> pl.DataFrame:
    yq_start = (year - TRAILING_YEARS_QUALITY) * 4 + 1
    yq_end   = (year - 1) * 4 + 4
    yq_recent_start = (year - 1) * 4 + 1

    sql = f"""
    WITH
    qual AS (
      SELECT
        company_code,
        COALESCE(quantile_cont(roa_ttm, 0.5), 0.0)  AS med_roa,
        COALESCE(quantile_cont(gross_margin_ttm, 0.5), 0.0) AS med_gm,
        COUNT(*) FILTER (WHERE ni_q < 0 AND year * 4 + quarter >= {yq_recent_start}) AS recent_losses
      FROM raw_quarterly
      WHERE year * 4 + quarter BETWEEN {yq_start} AND {yq_end}
      GROUP BY company_code
      HAVING COUNT(*) >= 8
    ),
    -- 5y 營收成長過濾（停滯股剔除）
    rev_yearly AS (
      SELECT company_code, year,
             SUM(rev_q) FILTER (WHERE rev_q IS NOT NULL) AS rev_year
      FROM raw_quarterly
      WHERE year BETWEEN {year-5} AND {year-1}
      GROUP BY company_code, year
      HAVING SUM(rev_q) > 0
    ),
    rev_growth AS (
      SELECT company_code,
             POWER(MAX(CASE WHEN year={year-1} THEN rev_year END) /
                   NULLIF(MAX(CASE WHEN year={year-5} THEN rev_year END), 0),
                   1.0/{TRAILING_YEARS_QUALITY}) - 1 AS rev_cagr_5y
      FROM rev_yearly
      GROUP BY company_code
      HAVING MAX(CASE WHEN year={year-5} THEN rev_year END) > 0
         AND MAX(CASE WHEN year={year-1} THEN rev_year END) > 0
    ),
    adv60 AS (
      SELECT company_code, AVG(trade_value) AS adv60
      FROM daily_quote
      WHERE market='twse'
        AND date BETWEEN DATE '{year-1}-10-01' AND DATE '{year-1}-12-31'
        AND closing_price > 0
      GROUP BY company_code
    ),
    -- 市值近似（最近一個 BS 期 capital_stock 千元 × 100 NTD/share / par-10 × 最後價格）
    -- 簡化：mcap_proxy = capital_stock × last_close（capital 已是千元，price 是 NTD/share）
    cap_latest AS (
      SELECT DISTINCT ON (company_code) company_code, capital_stock
      FROM raw_quarterly
      WHERE capital_stock > 0
        AND year * 4 + quarter <= {(year-1) * 4 + 4}
      ORDER BY company_code, year DESC, quarter DESC
    ),
    px_eoy AS (
      SELECT DISTINCT ON (company_code) company_code, closing_price AS px
      FROM daily_quote
      WHERE market='twse'
        AND date BETWEEN DATE '{year-1}-12-01' AND DATE '{year-1}-12-31'
        AND closing_price > 0
      ORDER BY company_code, date DESC
    ),
    mcap AS (
      SELECT c.company_code,
             c.capital_stock / 10.0 * 1000 * p.px AS mcap
      FROM cap_latest c JOIN px_eoy p USING (company_code)
    ),
    ind AS (
      SELECT DISTINCT ON (company_code) company_code, industry
      FROM operating_revenue
      WHERE market='twse' AND industry IS NOT NULL
      ORDER BY company_code, year DESC, month DESC
    )
    SELECT
      q.company_code, q.med_roa, q.med_gm, a.adv60, m.mcap
    FROM qual q
    JOIN adv60 a USING (company_code)
    JOIN mcap m USING (company_code)
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
    ORDER BY m.mcap DESC NULLS LAST
    """
    return con.sql(sql).pl()


def get_year_start_dates(con, start: date, end: date) -> list[date]:
    return [r[0] for r in con.sql(f"""
        SELECT MIN(date) FROM daily_quote
        WHERE market='twse' AND company_code='0050'
          AND date BETWEEN DATE '{start}' AND DATE '{end}'
        GROUP BY EXTRACT(YEAR FROM date)
        ORDER BY MIN(date)
    """).fetchall()]


def adv_weighted_picks(pool: pl.DataFrame, topn: int) -> list[tuple[str, float]]:
    """從 pool 取 TOP N by ADV，並以 ADV 加權回傳 (code, weight)。"""
    top = pool.head(topn)
    if top.is_empty():
        return []
    weights = (top["adv60"] / top["adv60"].sum()).to_list()
    codes = top["company_code"].to_list()
    return list(zip(codes, weights))


def run_backtest(start: date, end: date, capital: float, weight_mode: str = "adv",
                  out_dir: str = "research/strat_lab/results") -> dict:
    """weight_mode: 'adv' = ADV-weighted, 'sqrt_adv' = sqrt(ADV)-weighted, 'eq' = equal."""
    t0 = time.time()
    con = connect()

    year_starts = get_year_start_dates(con, start, end)
    print(f"[iter13] 年初再平衡點: {len(year_starts)} 天 ({year_starts[0]} → {year_starts[-1]})")
    print(f"[iter13] 加權方式: {weight_mode}")

    rows = []
    fallback_count = 0
    pool_sizes = {}
    for yr_d in year_starts:
        pool = screen_pool_for_year(con, yr_d.year)
        pool_sizes[yr_d] = len(pool)
        if pool.is_empty():
            rows.append({"rebal_d": yr_d, "company_code": "0050", "weight": 1.0})
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
        # 若 n < TOPN，剩餘權重交給 0050
        total_w = sum(weights)
        codes = top["company_code"].to_list()
        for c, w in zip(codes, weights):
            rows.append({"rebal_d": yr_d, "company_code": c, "weight": w})
        if n < TOPN:
            shortfall = 1.0 - total_w
            rows.append({"rebal_d": yr_d, "company_code": "0050", "weight": shortfall})
            fallback_count += (TOPN - n)
        # 印 picks + weights
        weights_str = " ".join([f"{c}={w:.0%}" for c, w in zip(codes, weights)])
        print(f"  [iter13] {yr_d.year}: pool={len(pool):>3} {weights_str}")

    picks_df = pl.DataFrame(rows) if rows else pl.DataFrame(
        schema={"rebal_d": pl.Date, "company_code": pl.Utf8, "weight": pl.Float64}
    )
    print(f"[iter13] 總 picks rows: {len(picks_df):,} "
          f"(0050 補位 {fallback_count} slot) ({time.time()-t0:.1f}s)")

    held = picks_df["company_code"].unique().to_list()
    rets = fetch_daily_returns(
        con, start.isoformat(), end.isoformat(),
        codes=held, market="twse",
    )

    days = [r[0] for r in con.sql(f"""
        SELECT date FROM daily_quote
        WHERE market='twse' AND company_code='0050'
          AND date BETWEEN DATE '{start}' AND DATE '{end}' ORDER BY date
    """).fetchall()]

    days_df = pl.DataFrame({"date": days}).sort("date")
    rebal_df = (pl.DataFrame({"active_rebal": year_starts})
                .with_columns((pl.col("active_rebal") + pl.duration(days=1)).alias("effective"))
                .sort("effective"))
    da = days_df.join_asof(rebal_df, left_on="date", right_on="effective", strategy="backward")
    contrib = (da.join(picks_df, left_on="active_rebal", right_on="rebal_d", how="left")
                 .join(rets, on=["date", "company_code"], how="left")
                 .with_columns((pl.col("weight") * pl.col("ret")).alias("c")))
    port = (contrib.group_by("date").agg(pl.col("c").sum().alias("r"))
                   .sort("date").with_columns(pl.col("r").fill_null(0.0)))

    # Turnover cost — 加權版本：sold_fraction = Σ |w_new - w_old| / 2
    pbd = {}
    for row in picks_df.iter_rows(named=True):
        pbd.setdefault(row["rebal_d"], {})[row["company_code"]] = row["weight"]
    cmap, prev = {}, None
    for rd in sorted(year_starts):
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
    port.with_columns(pl.lit(navs).alias("nav")).write_csv(
        os.path.join(out_dir, f"iter_13_{weight_mode}_daily.csv")
    )
    picks_df.write_csv(os.path.join(out_dir, f"iter_13_{weight_mode}_picks.csv"))

    return {
        "iter": 13, "runtime_s": time.time() - t0,
        "weight_mode": weight_mode, "TOPN": TOPN,
        "CAGR": cagr, "Sharpe": sharpe, "Sortino": sortino,
        "MDD": mdd, "Calmar": calmar,
        "vol_ann": vol_ann, "downvol_ann": downvol_ann,
        "final": float(navs[-1]),
        "n_rebals": len(year_starts),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2005-01-03")
    ap.add_argument("--end",   default="2026-04-25")
    ap.add_argument("--capital", type=float, default=1_000_000)
    ap.add_argument("--mode", default="mcap", choices=["mcap", "sqrt_mcap", "adv", "eq"])
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    print("=" * 78)
    print(f"iter_13 = ADV-weighted Quality Pool (mode={args.mode})")
    print(f"  TOPN={TOPN}, 年度 rebal, ROA≥{MIN_ROA_MEDIAN:.0%}, GM≥{MIN_GM_MEDIAN:.0%}")
    print(f"  窗口（鐵則 21y）: {start} → {end}")
    print("=" * 78)

    res = run_backtest(start, end, args.capital, weight_mode=args.mode)
    print(f"\n--- iter_13 ({args.mode}) 結果 ---")
    print(f"  CAGR:        {res['CAGR']:+.2%}")
    print(f"  Sharpe:      {res['Sharpe']:.3f}")
    print(f"  Sortino:     {res['Sortino']:.3f}  ★")
    print(f"  MDD:         {res['MDD']:.2%}")
    print(f"  finalNAV:    ${res['final']:,.0f}")

    print(f"\n--- 對照 ---")
    print(f"  hold_2330: CAGR +24.23% Sortino 1.333 MDD -45.86%")
    print(f"  hold_0050: CAGR +13.45% Sortino 0.823 MDD -55.66%")
    print(f"  iter_10 v6 (eq): CAGR +18.52% Sortino 0.977 MDD -55.66%")

    print(f"\n--- 是否破 2330？ ---")
    print(f"  CAGR    > 2330: {'✓' if res['CAGR'] > 0.2423 else '✗'} ({res['CAGR']:+.2%})")
    print(f"  Sortino > 2330: {'✓' if res['Sortino'] > 1.333 else '✗'} ({res['Sortino']:.3f})")
    print(f"  MDD     > 2330: {'✓' if res['MDD'] > -0.4586 else '✗'} ({res['MDD']:.2%})")


if __name__ == "__main__":
    main()
