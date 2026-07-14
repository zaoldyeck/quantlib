"""發掘漏斗:用引擎「一模一樣的動能子分數 + 硬門檻」掃全市場,找尚未入冊的候選。

定位(curation SOP §3):**發掘輸入,非買訊**——機械篩選 0 alpha(置換檢定),
本漏斗的價值是找「市場已開始定價、但我們還沒策展」的瓶頸候選,補策展缺口。
動能公式與 replay_2025.score_candidates 完全同源(同 clip、同權重),血統一致。

用法:
    uv run --project research python research/serenity/discovery_funnel.py [--top 25]
輸出:非池內名單依動能子分數排序(附產業/估值/法人流,供策展檢核)。
過濾原則:只按結構排除(純通路/組裝/金融),不按產業別(theme-agnostic 鐵律)。
"""
from __future__ import annotations

import argparse

import duckdb
import polars as pl

REGISTRY = "research/serenity/registry/thesis_registry_2025.csv"
CACHE = "research/cache.duckdb"

# 與 replay_2025.score_candidates 同源的動能子分數(僅價格動能三項)
MOM_TERMS = (("ret_60d", -0.5, 1.8, 14.0), ("ret_20d", -0.35, 0.9, 5.0), ("ret_252d", -0.8, 3.2, 3.0))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=25)
    args = ap.parse_args()

    pool = set(pl.read_csv(REGISTRY, infer_schema_length=0)["company_code"].to_list())
    con = duckdb.connect(CACHE, read_only=True)
    df = con.execute("""
        WITH r AS (
          SELECT company_code, market, date, closing_price, trade_value,
                 ROW_NUMBER() OVER (PARTITION BY company_code ORDER BY date DESC) rn
          FROM daily_quote WHERE date >= CURRENT_DATE - INTERVAL 420 DAY
        ),
        px AS (
          SELECT company_code,
                 ANY_VALUE(market) market,
                 MAX(CASE WHEN rn=1 THEN closing_price END) c0,
                 MAX(CASE WHEN rn=21 THEN closing_price END) c20,
                 MAX(CASE WHEN rn=61 THEN closing_price END) c60,
                 MAX(CASE WHEN rn=253 THEN closing_price END) c252,
                 MAX(CASE WHEN rn<=252 THEN closing_price END) hi252,
                 AVG(CASE WHEN rn<=20 THEN trade_value END) adv20
          FROM r GROUP BY company_code
        ),
        pe AS (
          SELECT company_code, ANY_VALUE(price_to_earning_ratio) pe, ANY_VALUE(price_book_ratio) pb
          FROM (SELECT company_code, price_to_earning_ratio, price_book_ratio,
                       ROW_NUMBER() OVER (PARTITION BY company_code ORDER BY date DESC) rn
                FROM stock_per_pbr) WHERE rn = 1 GROUP BY company_code
        ),
        inst AS (
          SELECT company_code, SUM(total_difference) inst20
          FROM (SELECT company_code, total_difference,
                       ROW_NUMBER() OVER (PARTITION BY company_code ORDER BY date DESC) rn
                FROM daily_trading_details) WHERE rn <= 20 GROUP BY company_code
        ),
        ind AS (SELECT company_code, ANY_VALUE(industry) industry FROM operating_revenue GROUP BY company_code)
        SELECT px.company_code code, px.market, ind.industry, px.c0 px_close,
               px.c0/px.c20-1 ret_20d, px.c0/px.c60-1 ret_60d, px.c0/px.c252-1 ret_252d,
               px.c0/px.hi252-1 dd_252, px.adv20, pe.pe, pe.pb, inst.inst20
        FROM px LEFT JOIN pe USING(company_code)
                LEFT JOIN inst USING(company_code)
                LEFT JOIN ind USING(company_code)
        WHERE px.c0 IS NOT NULL AND px.c20 IS NOT NULL AND px.c60 IS NOT NULL
          AND px.c252 IS NOT NULL AND LENGTH(px.company_code) = 4
    """).pl()
    con.close()

    # 引擎同源硬門檻(可交易 + 軟門檻;battle 11:filters 有回測背書)
    df = df.filter(
        (pl.col("adv20") >= 50_000_000) & (pl.col("px_close") >= 20)
        & (pl.col("ret_60d") >= -0.35) & (pl.col("ret_252d") >= -0.35)
        & (pl.col("dd_252") >= -0.55)
        & (pl.col("pe").is_null() | (pl.col("pe") <= 250))
        & (pl.col("pb").is_null() | (pl.col("pb") <= 45))
        & (~pl.col("code").is_in(list(pool)))
    )
    mom = pl.lit(0.0)
    for col, lo, hi, w in MOM_TERMS:
        mom = mom + pl.col(col).clip(lo, hi).fill_null(0.0) * w
    df = df.with_columns(mom_score=mom).sort("mom_score", descending=True)

    pl.Config.set_tbl_rows(args.top + 2)
    pl.Config.set_fmt_str_lengths(20)
    out = df.head(args.top).select(
        "code", "market", "industry",
        pl.col("px_close").round(1),
        pl.col("mom_score").round(1),
        (100 * pl.col("ret_20d")).round(0).alias("r20d_%"),
        (100 * pl.col("ret_60d")).round(0).alias("r60d_%"),
        (100 * pl.col("ret_252d")).round(0).alias("r252d_%"),
        pl.col("pe").round(1), pl.col("pb").round(1),
        (pl.col("inst20") / 1e3).round(0).alias("inst20d_k"),
        (pl.col("adv20") / 1e6).round(0).alias("adv_M"),
    )
    print(f"發掘漏斗(引擎同源動能子分數,排除池內 {len(pool)} 檔;發掘輸入非買訊):")
    print(out)


if __name__ == "__main__":
    main()
