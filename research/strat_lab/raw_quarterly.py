"""First-principles quarterly factor builder.

Builds standalone quarterly panel from RAW base tables, NOT from views:
  - concise_income_statement_progressive  (raw, but YTD progressive)
  - concise_balance_sheet                 (raw, point-in-time)
  - cash_flows_progressive                (raw, YTD progressive)

We DELIBERATELY do NOT use:
  - growth_analysis_ttm (VIEW with hand-rolled F-Score logic)
  - financial_index_quarterly (VIEW with margin computation we can't fully verify)
  - financial_index_ttm (VIEW)

Computed factors per (year, quarter, code):
  IS-based:     rev_q, cogs_q, op_income_q, ni_q, gross_margin_q, operating_margin_q, net_margin_q
  IS-TTM:       rev_ttm, ni_ttm
  BS:           total_assets, current_ratio (CA/CL), lt_debt_ratio (LT_debt/TA)
  CF-based:     cfo_q, cfo_ttm
  Derived TTM:  roa_ttm, asset_turnover_ttm
  F-Score (9):  Piotroski (each criterion 0/1, sum = 0..9)

Then mapped to daily PIT via v4 deadline logic
  (Q1 ≤ 5/22, Q2 ≤ 8/21, Q3 ≤ 11/21, Q4 ≤ 4/7 next year)

Validation: TSMC 2024Q4 standalone gross_margin should be ~0.59
            (Q4 YTD - Q3 YTD = 8685 / 5124 = 0.59)

Output: research/strat_lab/results/raw_quarterly.parquet
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date

import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import connect


# Income Statement line items.
# IMPORTANT: TWSE pre-IFRS-2013 用 ASCII 半形括號 (), post-IFRS 用全形（）。
# Schema drift discovered 2026-04-27 (iter_10 debug)：必須兩種都收，否則 2005-2012
# 整段 IS 資料變 NULL，downstream ROA/margin TTM 全空。
IS_TITLES = {
    "rev":       ["營業收入", "營業收入淨額"],
    "cogs":      ["營業成本"],
    "gross_pf":  ["營業毛利（毛損）", "營業毛利（毛損）淨額",
                  "營業毛利(毛損)", "營業毛利(毛損)淨額"],
    "op_income": ["營業利益（損失）", "營業利益（淨損）",
                  "營業利益(損失)", "營業利益(淨損)"],
    "ni":        ["本期淨利（淨損）", "繼續營業單位本期淨利（淨損）",
                  "繼續營業單位淨利(淨損)", "本期淨利(淨損)",
                  "本期稅後淨利（淨損）", "本期稅後淨利(淨損)"],
}

BS_TITLES = {
    # Schema drift discovered 2026-04-27: TWSE BS title 變化:
    #   2008-2012: 「資產總計」、「股東權益總計」  (pre-IFRS)
    #   2013-2017: 「資產總額」、「權益總額」      (early IFRS)
    #   2018+:    「資產總計」、「權益總計」       (current)
    "total_assets":         ["資產總計", "資產總額"],
    "current_assets":       ["流動資產"],
    "current_liabilities":  ["流動負債"],
    "non_current_liab":     ["非流動負債"],
    "total_equity":         ["權益總計", "權益總額", "股東權益總計"],
    "capital_stock":        ["股本"],
}

CF_TITLES = {
    "cfo":  ["營業活動之淨現金流入（流出）", "營業活動之淨現金流入(流出)"],
}


def _pivot_titles(con, cache_table: str, mapping: dict[str, list[str]],
                   start_year: int, end_year: int) -> pl.DataFrame:
    """Pivot long-form (year, quarter, code, title, value) to wide.

    Reads from cache.duckdb tables (is_progressive_raw, bs_concise_raw,
    cf_progressive_raw) which are already filtered to twse market.
    """
    select_clauses = [
        f"MAX(value) FILTER (WHERE title IN ({','.join(repr(t) for t in titles)})) AS {key}"
        for key, titles in mapping.items()
    ]
    # Cache tables: is/bs filter market IN ('twse','tpex'); cf uses 'tw' (no split)
    if cache_table == "cf_progressive_raw":
        # cf has no market split — assume same code = same company across markets
        # We'll join by company_code only; downstream code uses market from is/bs
        sql = f"""
        SELECT year, quarter, company_code,
               {', '.join(select_clauses)}
        FROM {cache_table}
        WHERE year BETWEEN {start_year} AND {end_year}
        GROUP BY year, quarter, company_code
        """
    else:
        sql = f"""
        SELECT market, year, quarter, company_code,
               {', '.join(select_clauses)}
        FROM {cache_table}
        WHERE market IN ('twse','tpex') AND year BETWEEN {start_year} AND {end_year}
        GROUP BY market, year, quarter, company_code
        """
    return con.sql(sql).pl()


def build_raw_quarterly(con, start: date, end: date) -> pl.DataFrame:
    print(f"[raw_quarterly] building for {start} → {end}")
    t0 = time.time()

    # Pull more history than strictly needed (for TTM + YoY comparison).
    sy = start.year - 2
    ey = end.year + 1

    # ============ IS pivot (YTD progressive) ============
    print("  [is] pivoting is_progressive_raw...")
    is_ytd = _pivot_titles(con, "is_progressive_raw",
                            IS_TITLES, sy, ey).sort(["company_code", "year", "quarter"])
    print(f"  [is] {len(is_ytd):,} rows ({time.time()-t0:.1f}s)")

    # Convert to standalone quarterly via lag-diff:
    # Q1 standalone = Q1 progressive (since YTD up to Q1 = Q1)
    # Q2/3/4 standalone = current YTD - prior YTD (within same year)
    is_q = is_ytd.with_columns([
        pl.when(pl.col("quarter") == 1).then(pl.col("rev"))
          .otherwise(pl.col("rev") - pl.col("rev").shift(1)
                     .over(["company_code", "year"], order_by="quarter"))
          .alias("rev_q"),
        pl.when(pl.col("quarter") == 1).then(pl.col("cogs"))
          .otherwise(pl.col("cogs") - pl.col("cogs").shift(1)
                     .over(["company_code", "year"], order_by="quarter"))
          .alias("cogs_q"),
        pl.when(pl.col("quarter") == 1).then(pl.col("gross_pf"))
          .otherwise(pl.col("gross_pf") - pl.col("gross_pf").shift(1)
                     .over(["company_code", "year"], order_by="quarter"))
          .alias("gross_pf_q"),
        pl.when(pl.col("quarter") == 1).then(pl.col("op_income"))
          .otherwise(pl.col("op_income") - pl.col("op_income").shift(1)
                     .over(["company_code", "year"], order_by="quarter"))
          .alias("op_income_q"),
        pl.when(pl.col("quarter") == 1).then(pl.col("ni"))
          .otherwise(pl.col("ni") - pl.col("ni").shift(1)
                     .over(["company_code", "year"], order_by="quarter"))
          .alias("ni_q"),
    ])

    # Standalone margins
    is_q = is_q.with_columns([
        pl.when(pl.col("rev_q") > 0).then(pl.col("gross_pf_q") / pl.col("rev_q"))
          .alias("gross_margin_q"),
        pl.when(pl.col("rev_q") > 0).then(pl.col("op_income_q") / pl.col("rev_q"))
          .alias("operating_margin_q"),
        pl.when(pl.col("rev_q") > 0).then(pl.col("ni_q") / pl.col("rev_q"))
          .alias("net_margin_q"),
    ])

    # TTM (rolling 4 quarters): need to sort across years too.
    is_q = is_q.sort(["company_code", "year", "quarter"]).with_columns([
        pl.col("rev_q").rolling_sum(window_size=4).over("company_code").alias("rev_ttm"),
        pl.col("ni_q").rolling_sum(window_size=4).over("company_code").alias("ni_ttm"),
    ])

    # ============ BS pivot (point-in-time end of quarter) ============
    print("  [bs] pivoting bs_concise_raw...")
    bs = _pivot_titles(con, "bs_concise_raw", BS_TITLES, sy, ey)
    print(f"  [bs] {len(bs):,} rows ({time.time()-t0:.1f}s)")

    # ============ CF pivot (YTD progressive → standalone) ============
    print("  [cf] pivoting cf_progressive_raw...")
    cf_ytd = _pivot_titles(con, "cf_progressive_raw",
                            CF_TITLES, sy, ey).sort(["company_code", "year", "quarter"])
    # cf 用 'tw' 不分市場 → 不加 market 欄位，後面 join 只用 (year, quarter, company_code)
    cf_q = cf_ytd.with_columns(
        pl.when(pl.col("quarter") == 1).then(pl.col("cfo"))
          .otherwise(pl.col("cfo") - pl.col("cfo").shift(1)
                     .over(["company_code", "year"], order_by="quarter"))
          .alias("cfo_q")
    ).sort(["company_code", "year", "quarter"]).with_columns(
        pl.col("cfo_q").rolling_sum(window_size=4).over("company_code").alias("cfo_ttm")
    )
    print(f"  [cf] {len(cf_q):,} rows ({time.time()-t0:.1f}s)")

    # ============ Join all ============
    panel = (is_q.select("market", "year", "quarter", "company_code",
                          "rev_q", "cogs_q", "gross_pf_q", "op_income_q", "ni_q",
                          "gross_margin_q", "operating_margin_q", "net_margin_q",
                          "rev_ttm", "ni_ttm")
             .join(bs.select("market", "year", "quarter", "company_code",
                             *BS_TITLES.keys()),
                   on=["market", "year", "quarter", "company_code"], how="left")
             .join(cf_q.select("year", "quarter", "company_code",
                                "cfo_q", "cfo_ttm"),
                   on=["year", "quarter", "company_code"], how="left"))

    # ============ Derived ratios ============
    panel = panel.with_columns([
        # ROA TTM = NI_TTM / TA
        pl.when(pl.col("total_assets") > 0)
          .then(pl.col("ni_ttm") / pl.col("total_assets")).alias("roa_ttm"),
        # Asset turnover TTM = Rev_TTM / TA
        pl.when(pl.col("total_assets") > 0)
          .then(pl.col("rev_ttm") / pl.col("total_assets")).alias("asset_turnover_ttm"),
        # Current ratio
        pl.when(pl.col("current_liabilities") > 0)
          .then(pl.col("current_assets") / pl.col("current_liabilities"))
          .alias("current_ratio"),
        # LT debt ratio (proxy: non-current liabilities / TA)
        pl.when(pl.col("total_assets") > 0)
          .then(pl.col("non_current_liab") / pl.col("total_assets"))
          .alias("lt_debt_ratio"),
        # CFO / NI ratio (accruals quality, > 1 = good)
        pl.when(pl.col("ni_ttm").abs() > 0)
          .then(pl.col("cfo_ttm") / pl.col("ni_ttm").abs()).alias("cfo_ni_ratio_ttm"),
        # Gross margin TTM
        pl.when(pl.col("rev_ttm") > 0)
          .then(pl.col("gross_pf_q").rolling_sum(4).over("company_code")
                / pl.col("rev_ttm")).alias("gross_margin_ttm"),
    ])

    # ============ YoY differences (Δ vs same Q last year) ============
    # Need 4-quarter shift on (code, year, quarter ordering). sort properly.
    panel = panel.sort(["company_code", "year", "quarter"]).with_columns([
        (pl.col("roa_ttm") - pl.col("roa_ttm").shift(4).over("company_code"))
          .alias("d_roa_yoy"),
        (pl.col("current_ratio") - pl.col("current_ratio").shift(4).over("company_code"))
          .alias("d_current_ratio_yoy"),
        (pl.col("lt_debt_ratio") - pl.col("lt_debt_ratio").shift(4).over("company_code"))
          .alias("d_lt_debt_yoy"),
        (pl.col("gross_margin_ttm") - pl.col("gross_margin_ttm").shift(4).over("company_code"))
          .alias("d_gross_margin_yoy"),
        (pl.col("asset_turnover_ttm") - pl.col("asset_turnover_ttm").shift(4).over("company_code"))
          .alias("d_asset_turnover_yoy"),
        (pl.col("capital_stock") - pl.col("capital_stock").shift(4).over("company_code"))
          .alias("d_capital_stock_yoy"),
    ])

    # ============ Piotroski F9 (each 0/1) ============
    panel = panel.with_columns([
        # 1. ROA > 0
        pl.when(pl.col("roa_ttm") > 0).then(1).otherwise(0).alias("f1_roa_pos"),
        # 2. CFO > 0 (using cfo_ttm)
        pl.when(pl.col("cfo_ttm") > 0).then(1).otherwise(0).alias("f2_cfo_pos"),
        # 3. ΔROA > 0
        pl.when(pl.col("d_roa_yoy") > 0).then(1).otherwise(0).alias("f3_d_roa_pos"),
        # 4. Accrual: CFO_TTM > NI_TTM (CF earnings quality)
        pl.when(pl.col("cfo_ttm") > pl.col("ni_ttm")).then(1).otherwise(0).alias("f4_cfo_gt_ni"),
        # 5. ΔLT-Debt < 0 (debt is decreasing)
        pl.when(pl.col("d_lt_debt_yoy") < 0).then(1).otherwise(0).alias("f5_d_debt_neg"),
        # 6. ΔCurrent ratio > 0
        pl.when(pl.col("d_current_ratio_yoy") > 0).then(1).otherwise(0).alias("f6_d_curr_pos"),
        # 7. No new shares (ΔCapitalStock <= 0). Use small epsilon to allow rounding.
        pl.when(pl.col("d_capital_stock_yoy") <= 1).then(1).otherwise(0).alias("f7_no_new_eq"),
        # 8. ΔGross margin > 0
        pl.when(pl.col("d_gross_margin_yoy") > 0).then(1).otherwise(0).alias("f8_d_gm_pos"),
        # 9. ΔAsset turnover > 0
        pl.when(pl.col("d_asset_turnover_yoy") > 0).then(1).otherwise(0).alias("f9_d_at_pos"),
    ]).with_columns(
        (pl.col("f1_roa_pos") + pl.col("f2_cfo_pos") + pl.col("f3_d_roa_pos")
         + pl.col("f4_cfo_gt_ni") + pl.col("f5_d_debt_neg") + pl.col("f6_d_curr_pos")
         + pl.col("f7_no_new_eq") + pl.col("f8_d_gm_pos") + pl.col("f9_d_at_pos"))
        .alias("f_score_raw")
    )

    # Filter to requested window (we kept extra history for TTM/YoY)
    panel = panel.filter(pl.col("year") >= start.year)
    print(f"  [done] {len(panel):,} rows × {len(panel.columns)} cols ({time.time()-t0:.1f}s)")
    return panel


def validate_against_tsmc(panel: pl.DataFrame) -> None:
    """Sanity check: TSMC 2024Q4 standalone gross_margin should be ~0.59."""
    print("\n=== Validation: TSMC 2024Q4 ===")
    row = panel.filter(
        (pl.col("company_code") == "2330") &
        (pl.col("year") == 2024) & (pl.col("quarter") == 4)
    )
    if row.is_empty():
        print("  [WARN] no row found for 2330 2024Q4")
        return
    r = row.row(0, named=True)
    expected_gm = 0.59
    actual_gm = r.get("gross_margin_q")
    print(f"  Expected (view value):     gross_margin ≈ 0.59")
    print(f"  Computed (first principles): {actual_gm:.4f}")
    print(f"  Match: {'✓' if actual_gm and abs(actual_gm - expected_gm) < 0.01 else '✗'}")
    print(f"\n  Other Q4 standalone values:")
    print(f"    rev_q (4Q standalone): {r.get('rev_q'):,.0f}")
    print(f"    operating_margin_q:    {r.get('operating_margin_q'):.4f}")
    print(f"    net_margin_q:          {r.get('net_margin_q'):.4f}")
    print(f"    roa_ttm:               {r.get('roa_ttm'):.4f}")
    print(f"    asset_turnover_ttm:    {r.get('asset_turnover_ttm'):.4f}")
    print(f"    current_ratio:         {r.get('current_ratio'):.4f}")
    print(f"    f_score_raw:           {r.get('f_score_raw')}/9")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2005-01-01")
    ap.add_argument("--end",   default="2026-04-25")
    ap.add_argument("--out",   default="research/raw_quarterly.parquet")
    args = ap.parse_args()

    # IS/BS/CF base tables now in cache (is_progressive_raw, bs_concise_raw,
    # cf_progressive_raw). Use cache for fast builds.
    con = connect()
    panel = build_raw_quarterly(con,
                                 date.fromisoformat(args.start),
                                 date.fromisoformat(args.end))

    validate_against_tsmc(panel)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    panel.write_parquet(args.out, compression="zstd")
    print(f"\nSaved: {args.out}  ({os.path.getsize(args.out)/1e6:.1f} MB)")


if __name__ == "__main__":
    main()
