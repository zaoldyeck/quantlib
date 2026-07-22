"""進場池唯一真源守護:逐日池 (`pool_history`) 的今日切片必須與單日算法逐位一致。

為什麼要這支測試:2026-07-22 把「部位身分」從『某台機器的成交紀錄檔』改成
『市場資料算得出的池籍』後,`pool_history` 同時餵**今日買誰**與**歷史部位的
進場錨**。它一旦與原本的單日算法漂移,錯誤會同時汙染買進與出場兩條 money-path
(且是靜默的:池少一檔 = 該賣的沒賣 / 該留的被當外人砍掉)。

本測試把單日算法當作**參考實作**寫死在這裡,對真實 cache 逐位比對成員集合與
geo 分數。任何對 `pool_history` 的修改都必須先讓這支綠燈。

依賴:`var/cache/cache.duckdb` 需為最新(見 CLAUDE.md「Data Refresh Workflow」)。
Run: uv run --project research python -m research.tri.tests.test_pool_history
"""
from __future__ import annotations

from datetime import date as Date

import duckdb
import polars as pl

from research.apex import data
from research.apex.assemble import apply_avail_override, build_features
from research.tri.advisors import C, S_WTS, entry_anchors, pool_history
from research import paths

TODAY = Date(2026, 7, 22)


def _build(con):
    """重建 s_advisor 的特徵輸入(與 advisors.s_advisor 前半段同構)。"""
    import os
    ov = None
    fs = f"{paths.RECORDS}/revenue_first_seen.parquet"
    if os.path.exists(fs):
        ov = (pl.read_parquet(fs)
              .with_columns(pl.col("first_seen").str.to_date().alias("avail_date"))
              .select([C, "year", "month", "avail_date"]))
    ws = TODAY.replace(year=TODAY.year - 2).isoformat()
    panel, feat, elig = build_features(con, ws, TODAY.isoformat(), avail_override=ov)
    rev = (data.load_monthly_revenue(con, TODAY.isoformat())
           .sort([C, "year", "month"])
           .with_columns([
               pl.date(pl.col("year") + pl.col("month") // 12,
                       pl.col("month") % 12 + 1, 10).alias("avail"),
               (pl.col("monthly_revenue").rolling_sum(3)
                / pl.col("monthly_revenue").rolling_sum(3).shift(3) - 1)
               .over(C).alias("rev_seq"),
           ]))
    rev = (apply_avail_override(rev, ov)
           .select([C, "avail", "rev_seq"]).drop_nulls().sort("avail"))
    feat = (feat.sort("date")
            .join_asof(rev, left_on="date", right_on="avail", by=C,
                       strategy="backward", tolerance="70d").sort([C, "date"]))
    raw = duckdb.connect(f"{paths.CACHE_DB}", read_only=True)
    tax = raw.sql("SELECT company_code, effective_date, industry FROM "
                  "industry_taxonomy_pit WHERE industry IS NOT NULL "
                  "ORDER BY effective_date").pl()
    d0 = max(d for d in panel.select("date").unique()["date"].to_list() if d <= TODAY)
    return panel, feat, elig, tax, d0


def _legacy_pool(feat, elig, tax, d0) -> pl.DataFrame:
    """參考實作:2026-07-22 重構前的單日池算法(逐行搬過來,不得「順手改良」)。"""
    day = (feat.filter(pl.col("date") == d0)
           .join_asof(tax.sort("effective_date"), left_on="date",
                      right_on="effective_date", by=C, strategy="backward"))
    ind_med = (day.filter(pl.col("industry").is_not_null())
               .group_by("industry")
               .agg(pl.col("rev_yoy_accel").median().alias("_im")))
    day = (day.join(ind_med, on="industry", how="left")
           .with_columns((pl.col("rev_yoy_accel") - pl.col("_im")).alias("accel_rel")))
    el = elig.filter((pl.col("date") == d0) & pl.col("eligible")).select(C)
    pool = (day.filter(pl.col("rev_fresh_days") <= 7)
            .join(el, on=C, how="semi").drop_nulls(subset=list(S_WTS)))
    med = pool["cfo_ni_ratio_ttm"].median()
    n_cov = pool["cfo_ni_ratio_ttm"].drop_nulls().len()
    if n_cov >= 0.3 * pool.height and med is not None:
        pool = pool.filter(pl.col("cfo_ni_ratio_ttm") >= med)
    expr = None
    for cname, wt in S_WTS.items():
        term = (pl.col(cname).rank() / pl.len()) ** wt
        expr = term if expr is None else expr * term
    return pool.with_columns(expr.alias("geo")).sort("geo", descending=True)


def main() -> None:
    con = data.connect()
    panel, feat, elig, tax, d0 = _build(con)
    ph = pool_history(feat, elig, tax)
    new = ph.filter(pl.col("date") == d0).sort("geo", descending=True)
    old = _legacy_pool(feat, elig, tax, d0)

    assert new.height == old.height, f"池大小不符:new {new.height} vs old {old.height}"
    assert new[C].to_list() == old[C].to_list(), "池成員或排序不符"
    dg = max(abs(a - b) for a, b in zip(new["geo"], old["geo"]))
    assert dg < 1e-12, f"geo 分數漂移 {dg:g}"
    print(f"✓ 今日池逐位一致({new.height} 檔,geo 最大差 {dg:g})")

    # 逐日池必須涵蓋多個交易日,否則「歷史進場錨」根本無從查起
    nd = ph.select(pl.col("date").n_unique()).item()
    assert nd > 100, f"逐日池只有 {nd} 天,錨查詢會失真"
    print(f"✓ 逐日池涵蓋 {nd} 個交易日")

    # 錨語義:取「≤ 取得日」的最後一次入池;未曾入池者不得出現在結果中
    top = new[C][0]
    anchors = entry_anchors(ph, {top: d0, "0000": d0})
    assert anchors.get(top) == d0, f"今日在池者的錨應為今日,得到 {anchors.get(top)}"
    assert "0000" not in anchors, "從未入池者不得有錨"
    hist = ph.filter(pl.col(C) == top).select("date").to_series().to_list()
    past = [d for d in hist if d < d0]
    if past:
        a2 = entry_anchors(ph, {top: max(past)})
        assert a2[top] == max(past), "錨必須取 ≤ 取得日的最後一次入池"
        print(f"✓ 錨取 ≤ 取得日的最後一次入池({top}: {max(past)})")
    print("✓ pool_history 全過")


if __name__ == "__main__":
    main()
