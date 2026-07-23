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

Output: var/out/strat_lab/raw_quarterly.parquet
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date

import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from research.db import connect


# Income Statement line items.
# IMPORTANT: TWSE pre-IFRS-2013 用 ASCII 半形括號 (), post-IFRS 用全形（）。
# Schema drift discovered 2026-04-27 (iter_10 debug)：必須兩種都收，否則 2005-2012
# 整段 IS 資料變 NULL，downstream ROA/margin TTM 全空。
# 清單順序 = COALESCE 優先序(見 _pivot_titles)。**Piotroski (2000) 明定用
# 「除非常項目與停業單位前之淨利」**,故 ni 優先取「繼續營業單位本期淨利」,
# 取不到才退回「本期淨利」;毛利優先取「淨額」。
IS_TITLES = {
    "rev":       ["營業收入", "營業收入淨額"],
    "cogs":      ["營業成本"],
    "gross_pf":  ["營業毛利（毛損）淨額", "營業毛利(毛損)淨額",
                  "營業毛利（毛損）", "營業毛利(毛損)"],
    "op_income": ["營業利益（損失）", "營業利益（淨損）",
                  "營業利益(損失)", "營業利益(淨損)"],
    "ni":        ["繼續營業單位本期淨利（淨損）", "繼續營業單位淨利(淨損)",
                  "本期淨利（淨損）", "本期淨利(淨損)",
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
    # **固定優先序 COALESCE,不是 MAX**(2026-07-23 FC4/BUG-10)。
    # 舊寫法 MAX(value) FILTER (title IN (...)) 在同一格有兩個候選科目時挑「數字大的」
    # ——系統性偏差,且哪個較大隨季別變動 → Δ毛利率/淨利被灌雜訊(88,036/89,006 格
    # 同時有 2+ 候選科目,1,168 格淨利差達 120 億)。改成照 mapping 清單的順序逐一
    # 回退:繼續營業單位淨利(≈ 除非常項目前淨利,Piotroski 明定用它)優先於本期淨利;
    # 毛利淨額優先於毛利。
    select_clauses = [
        "COALESCE(" + ", ".join(
            f"MAX(value) FILTER (WHERE title = {t!r})" for t in titles
        ) + f") AS {key}"
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

    # ============ BS pivot (point-in-time end of quarter) ============
    print("  [bs] pivoting bs_concise_raw...")
    bs = _pivot_titles(con, "bs_concise_raw", BS_TITLES, sy, ey)
    print(f"  [bs] {len(bs):,} rows ({time.time()-t0:.1f}s)")

    # ============ CF pivot (YTD progressive) ============
    print("  [cf] pivoting cf_progressive_raw...")
    cf_ytd = _pivot_titles(con, "cf_progressive_raw", CF_TITLES, sy, ey)

    # ============ Join YTD IS + PIT BS + YTD CF (尚未做單季差分) ============
    # cf 用 'tw' 不分市場 → join 只用 (year, quarter, company_code)。
    panel = (is_ytd.join(bs, on=["market", "year", "quarter", "company_code"], how="left")
             .join(cf_ytd.select("year", "quarter", "company_code", "cfo"),
                   on=["year", "quarter", "company_code"], how="left"))

    # ============ 日曆季格線(2026-07-23 FC4/FC5:BUG-9 根治)============
    # **所有位移一律在完整日曆季格線上做**,不是按實體列。舊寫法 shift(1)/shift(4)/
    # rolling_sum(4) 都是「往前 N 列」,一旦公司缺報一季,窗口就跨超過 4 個日曆季、
    # 「去年同季」錯位、YTD 差分把兩季當一季(5.5% 的列受影響)。densify 成每
    # (market, code) 一條連續 yq 序列後,缺的季變成 null 列,位移即日曆精確,
    # 缺料自然傳播成 null(而非算出錯值)。
    panel = panel.with_columns([
        (pl.col("year") * 4 + pl.col("quarter") - 1).alias("yq"),
        pl.lit(True).alias("_present"),   # 標記原始申報列;densify 的佔位列為 null
    ])
    bounds = panel.group_by(["market", "company_code"]).agg(
        pl.col("yq").min().alias("lo"), pl.col("yq").max().alias("hi"))
    grid = (bounds.with_columns(pl.int_ranges("lo", pl.col("hi") + 1).alias("yq"))
            .explode("yq").select("market", "company_code", "yq"))
    panel = (grid.join(panel.drop("year", "quarter"),
                       on=["market", "company_code", "yq"], how="left")
             .with_columns([(pl.col("yq") // 4).alias("year"),
                            (pl.col("yq") % 4 + 1).alias("quarter")])
             .sort(["market", "company_code", "yq"]))

    G = ["market", "company_code"]        # 位移的分組鍵(每公司一條 yq 序列)

    def _standalone(col: str) -> pl.Expr:
        """YTD → 單季:Q1 = YTD;Q2-4 = 本季 YTD − 上一日曆季 YTD(缺季 → null)。"""
        return (pl.when(pl.col("quarter") == 1).then(pl.col(col))
                .otherwise(pl.col(col) - pl.col(col).shift(1).over(G, order_by="yq"))
                .alias(col + "_q"))

    panel = panel.with_columns([_standalone(c) for c in
                                ("rev", "cogs", "gross_pf", "op_income", "ni")]
                               + [_standalone("cfo")])

    def _ttm(col_q: str) -> pl.Expr:
        """單季 → TTM(4 季和);**4 季必須全部有值**才輸出,否則 null。"""
        s = pl.col(col_q)
        valid = s.is_not_null().cast(pl.Int32).rolling_sum(4).over(G, order_by="yq")
        return (pl.when(valid == 4)
                .then(s.rolling_sum(4).over(G, order_by="yq"))
                .alias(col_q.replace("_q", "") + "_ttm"))

    panel = panel.with_columns([_ttm(c) for c in
                                ("rev_q", "ni_q", "cfo_q", "gross_pf_q")])

    # 標準化單季利潤率(其他消費者用)
    panel = panel.with_columns([
        pl.when(pl.col("rev_q") > 0).then(pl.col("gross_pf_q") / pl.col("rev_q"))
          .alias("gross_margin_q"),
        pl.when(pl.col("rev_q") > 0).then(pl.col("op_income_q") / pl.col("rev_q"))
          .alias("operating_margin_q"),
        pl.when(pl.col("rev_q") > 0).then(pl.col("ni_q") / pl.col("rev_q"))
          .alias("net_margin_q"),
    ])

    # ============ 衍生比率(Piotroski 分母口徑,2026-07-23 FC4/BUG-8)============
    # Piotroski (2000):ROA 與資產週轉率的分母用**年初總資產**(= 4 季前期末),
    # 不是期末;槓桿(LEVER)用**平均總資產**。舊寫法一律用期末 → 38.2% 的格子
    # 分數不同、系統性寬鬆 0.29 分。
    panel = panel.with_columns(
        pl.col("total_assets").shift(4).over(G, order_by="yq").alias("total_assets_begin"))
    panel = panel.with_columns([
        # ROA_TTM = NI_TTM / 年初總資產
        pl.when(pl.col("total_assets_begin") > 0)
          .then(pl.col("ni_ttm") / pl.col("total_assets_begin")).alias("roa_ttm"),
        # 資產週轉率 = Rev_TTM / 年初總資產
        pl.when(pl.col("total_assets_begin") > 0)
          .then(pl.col("rev_ttm") / pl.col("total_assets_begin")).alias("asset_turnover_ttm"),
        # 流動比率(期末)
        pl.when(pl.col("current_liabilities") > 0)
          .then(pl.col("current_assets") / pl.col("current_liabilities")).alias("current_ratio"),
        # 槓桿 = 非流動負債 / 平均總資產((期末 + 年初)/2)。非流動負債是「長期負債」
        # 的可得近似(concise 表層級無更細分),與 Piotroski ΔLEVER 定義一致。
        pl.when(((pl.col("total_assets") + pl.col("total_assets_begin")) / 2) > 0)
          .then(pl.col("non_current_liab")
                / ((pl.col("total_assets") + pl.col("total_assets_begin")) / 2))
          .alias("leverage"),
        # 現金流品質:CFO_TTM / |NI_TTM|(獨立訊號,非 Piotroski 項)
        pl.when(pl.col("ni_ttm").abs() > 0)
          .then(pl.col("cfo_ttm") / pl.col("ni_ttm").abs()).alias("cfo_ni_ratio_ttm"),
        # 毛利率 TTM = 毛利 TTM / 營收 TTM
        pl.when(pl.col("rev_ttm") > 0)
          .then(pl.col("gross_pf_ttm") / pl.col("rev_ttm")).alias("gross_margin_ttm"),
    ])
    # 期末口徑的沿用欄位(非 Piotroski;為既有消費者保留原名原義):
    #   roa_ttm_eop      = NI_TTM / 期末總資產
    #   lt_debt_ratio    = 非流動負債 / 期末總資產(g04c_fundamentals 等實驗沿用)
    panel = panel.with_columns([
        pl.when(pl.col("total_assets") > 0)
          .then(pl.col("ni_ttm") / pl.col("total_assets")).alias("roa_ttm_eop"),
        pl.when(pl.col("total_assets") > 0)
          .then(pl.col("non_current_liab") / pl.col("total_assets")).alias("lt_debt_ratio"),
    ])

    # ============ YoY 差分(Δ vs 去年同季,日曆對齊)============
    panel = panel.with_columns([
        (pl.col("roa_ttm") - pl.col("roa_ttm").shift(4).over(G, order_by="yq")).alias("d_roa_yoy"),
        (pl.col("current_ratio") - pl.col("current_ratio").shift(4).over(G, order_by="yq")).alias("d_current_ratio_yoy"),
        (pl.col("leverage") - pl.col("leverage").shift(4).over(G, order_by="yq")).alias("d_leverage_yoy"),
        (pl.col("gross_margin_ttm") - pl.col("gross_margin_ttm").shift(4).over(G, order_by="yq")).alias("d_gross_margin_yoy"),
        (pl.col("asset_turnover_ttm") - pl.col("asset_turnover_ttm").shift(4).over(G, order_by="yq")).alias("d_asset_turnover_yoy"),
        (pl.col("capital_stock") - pl.col("capital_stock").shift(4).over(G, order_by="yq")).alias("d_capital_stock_yoy"),
    ])

    # ============ Piotroski F9(每項 0/1,**缺料 → null 不當 0**)============
    # BUG-7 根治:每一項的輸入若為 null,該項給 null(不是 0)。舊寫法 .otherwise(0)
    # 把「算不出來」當「不加分」→ (a) 系統性低估 (b) 金融業毛利恆 null → f8/f9 恆 0、
    # 平均分被壓到 3.0,形成沒人宣告過的隱形濾網 (c) 2011 前現金流缺料 → f2/f4 恆 0,
    # 「F-Score 逐年上升」其實是資料補齊軌跡。改成 null 傳播後:
    #   f_score_raw     = 有效項之和
    #   f_score_n_valid = 有效項數(消費端要求 == 9 才採用,金融業/歷史不足自動排除)
    def _crit(cond: pl.Expr, inputs: list[str]) -> pl.Expr:
        null_any = None
        for c in inputs:
            e = pl.col(c).is_null()
            null_any = e if null_any is None else (null_any | e)
        return pl.when(null_any).then(None).when(cond).then(1).otherwise(0)

    panel = panel.with_columns([
        _crit(pl.col("roa_ttm") > 0, ["roa_ttm"]).alias("f1_roa_pos"),
        _crit(pl.col("cfo_ttm") > 0, ["cfo_ttm"]).alias("f2_cfo_pos"),
        _crit(pl.col("d_roa_yoy") > 0, ["d_roa_yoy"]).alias("f3_d_roa_pos"),
        # 應計品質:CFO_TTM > NI_TTM(同分母下 CFO/資產 > ROA)
        _crit(pl.col("cfo_ttm") > pl.col("ni_ttm"), ["cfo_ttm", "ni_ttm"]).alias("f4_cfo_gt_ni"),
        # ΔLEVER < 0(槓桿下降)
        _crit(pl.col("d_leverage_yoy") < 0, ["d_leverage_yoy"]).alias("f5_d_debt_neg"),
        _crit(pl.col("d_current_ratio_yoy") > 0, ["d_current_ratio_yoy"]).alias("f6_d_curr_pos"),
        # 未發新股:ΔStock ≤ 0(嚴格,無魔術 epsilon)。**台股 caveat**:盈餘/資本
        # 公積轉增資(股票股利)也會讓股本增加卻非對外募資,此代理會保守扣分;理想解
        # 需 MOPS 現增事件(見 docs/data_audit,FC4 尾註),此管線暫無該資料。
        _crit(pl.col("d_capital_stock_yoy") <= 0, ["d_capital_stock_yoy"]).alias("f7_no_new_eq"),
        _crit(pl.col("d_gross_margin_yoy") > 0, ["d_gross_margin_yoy"]).alias("f8_d_gm_pos"),
        _crit(pl.col("d_asset_turnover_yoy") > 0, ["d_asset_turnover_yoy"]).alias("f9_d_at_pos"),
    ])
    _fcols = ["f1_roa_pos", "f2_cfo_pos", "f3_d_roa_pos", "f4_cfo_gt_ni", "f5_d_debt_neg",
              "f6_d_curr_pos", "f7_no_new_eq", "f8_d_gm_pos", "f9_d_at_pos"]
    panel = panel.with_columns([
        pl.sum_horizontal([pl.col(c).fill_null(0) for c in _fcols]).alias("f_score_raw"),
        pl.sum_horizontal([pl.col(c).is_not_null().cast(pl.Int32) for c in _fcols])
          .alias("f_score_n_valid"),
    ])

    # 只留原始申報列(densify 的佔位列是 spacer,只為位移日曆對齊,不進輸出),
    # 並裁到請求視窗(前面多留了歷史供 TTM/YoY)。
    panel = panel.filter(pl.col("_present") & (pl.col("year") >= start.year)).drop("_present")
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
