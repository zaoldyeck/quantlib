"""Phase 3.4 新 edge 探勘:S 偏營收+動能,測能否加入**分散化**因子(step ①②)。

S 現有 6 因子全是營收動能 + 價格動能(同向)。分散化候選(低相關、不同經濟來源):
- **價值**(-PBR):便宜=逆動能,可能降回撤。thesis:被低估的公司均值回歸。
- **籌碼**(法人 20 日淨買/流通股):聰明錢累積領先。thesis:法人資訊優勢。
- **品質**(毛利/總資產 GP/A,Novy-Marx):高獲利力公司長期超額。thesis:品質溢酬。

依框架 step ②直接驗每個候選的截面 IC(有訊號才有資格進 step ③);與 S 現有動能因子低相關者
才是真分散。reuse prep_cached(秒回)+ data loaders + factors.evaluate_factor。**這是探勘,不是
拍板**——有 IC 的候選才進下一步 whole-strategy 測試。

Run: uv run --project . python -m quantlib.strat_lab.candidate_edges
"""
from __future__ import annotations

import polars as pl

from quantlib.apex import data, factors
from quantlib.apex.strategy_s import C, prep_cached


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    fwd = factors.forward_returns(panel)
    lo = panel["date"].min().isoformat()
    hi = panel["date"].max().isoformat()

    cands: dict[str, pl.DataFrame] = {}

    # 價值:-PBR(便宜=高分)。PBR>0 才有意義。
    val = data.load_valuation(con, lo, hi)
    cands["value(-PBR)"] = (val.filter(pl.col("pbr") > 0)
                            .select(["date", C, (-pl.col("pbr")).alias("value")]))
    # 盈餘殖利率 1/PER(PER>0)
    cands["earnings_yield(1/PER)"] = (val.filter(pl.col("per") > 0)
                                      .select(["date", C, (1.0 / pl.col("per")).alias("value")]))

    # 籌碼:法人 20 日淨買股數(外資+投信+自營)——聰明錢累積
    fl = data.load_flows(con, lo, hi).sort([C, "date"]).with_columns(
        (pl.col("foreign_diff").fill_null(0) + pl.col("trust_diff").fill_null(0)
         + pl.col("dealer_diff").fill_null(0)).alias("net"))
    cands["chip(法人20日淨買)"] = (fl.with_columns(
        pl.col("net").rolling_sum(20).over(C).alias("value"))
        .select(["date", C, "value"]).drop_nulls())

    # 品質:毛利/總資產(GP/A,Novy-Marx)——raw_quarterly PIT,as-of 到日
    rq = con.sql(
        "SELECT company_code, year, quarter, gross_pf_ttm, total_assets FROM raw_quarterly "
        "WHERE gross_pf_ttm IS NOT NULL AND total_assets > 0").pl().with_columns(
        (pl.col("gross_pf_ttm") / pl.col("total_assets")).alias("gpa"),
        # 季報 as-of:保守用季末 + 45 日(申報延遲)可得日
        pl.date(pl.col("year") + (pl.col("quarter") * 3 // 12),
                (pl.col("quarter") * 3) % 12 + 1, 15).alias("avail"))
    gpa = (feat.select(["date", C]).sort("date")
           .join_asof(rq.select([C, "avail", "gpa"]).sort("avail"),
                      left_on="date", right_on="avail", by=C, strategy="backward")
           .select(["date", C, pl.col("gpa").alias("value")]).drop_nulls())
    cands["quality(GP/A)"] = gpa

    print("=== 候選新 edge 直接檢驗(h21 IC;分散化 S 的營收+動能)===")
    print("  (IC>0 且 t>3 = 有訊號;與 S 動能低相關者才是真分散)\n")
    for name, fac in cands.items():
        r = factors.evaluate_factor(name, fac, fwd, elig, family="cand",
                                    batch="phase3.4-cand", log=False)
        print(f"  {name}")
        print(f"     {factors.fmt_factor(r)}\n")
    print("  下一步:IC 顯著的候選 → 算與 rev_yoy_accel/high_52w 的相關,低相關者進 whole-strategy 測試。")


if __name__ == "__main__":
    main()
