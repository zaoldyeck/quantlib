"""apex_revcycle_S 官方 replay 引擎(唯一真源)。

S 的特徵組裝(prep)+ 規格回測(run_s,STRATEGY.md §4-§6,5 檔 20%、trail 35%、
時間止損 30/15、無絕對停損、6 因子)。生產與研究路徑(tri.pnl_dashboard s_nav、
s01 分佈診斷、chart 對比圖)一律 import 這份,禁止各自重寫(2026-07-20:從
experiments/chart_s_vs_benchmarks 搬到正式模組,消除「策略引擎住在畫圖檔」壞味道)。
冠軍規格與研發史見 apex/STRATEGY.md、apex/REPORT.md。

依賴 cache: 是(prep 讀 industry_taxonomy_pit;run_s 走 apex.engine.simulate)。
"""
from __future__ import annotations

from datetime import date as Date

import polars as pl

from research.apex import data
from research.apex.assemble import build_features, entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
DS = "2014-10-31"          # 特徵 panel 載入起點(正2 上市日;固定錨,非資料截止)
WREL = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0,
        "mom_126_5": 0.5, "rev_seq": 0.5, "accel_rel": 0.5}


def prep(con, end: str | None = None):
    """S 的特徵組裝(六軸 + PIT 環比/同業相對加速)。end 預設 = cache 最新日
    (動態,見 data.latest_date);LIVE 儀表板重用本函式時必須傳入同一 end。"""
    de = end or data.latest_date(con).isoformat()
    panel, feat, _ = build_features(con, DS, de)
    rev = (data.load_monthly_revenue(con, de)
           .sort([C, "year", "month"])
           .with_columns([
               pl.date(pl.col("year") + pl.col("month") // 12,
                       pl.col("month") % 12 + 1, 10).alias("avail"),
               (pl.col("monthly_revenue").rolling_sum(3)
                / pl.col("monthly_revenue").rolling_sum(3).shift(3) - 1)
               .over(C).alias("rev_seq"),
           ])
           .select([C, "avail", "rev_seq"]).drop_nulls().sort("avail"))
    feat = (feat.sort("date")
            .join_asof(rev, left_on="date", right_on="avail", by=C,
                       strategy="backward", tolerance="70d")
            .sort([C, "date"]))
    tax = con.sql(
        "SELECT company_code, effective_date, industry FROM industry_taxonomy_pit "
        "WHERE industry IS NOT NULL ORDER BY effective_date").pl()
    fx = (feat.select(["date", C, "rev_yoy_accel"]).drop_nulls().sort("date")
          .join_asof(tax.sort("effective_date"), left_on="date",
                     right_on="effective_date", by=C, strategy="backward")
          .drop_nulls(subset=["industry"]))
    ind_med = fx.group_by(["date", "industry"]).agg(
        pl.col("rev_yoy_accel").median().alias("m"))
    rel = (fx.join(ind_med, on=["date", "industry"], how="left")
           .with_columns((pl.col("rev_yoy_accel") - pl.col("m")).alias("accel_rel"))
           .select(["date", C, "accel_rel"]))
    feat = feat.join(rel, on=["date", C], how="left")
    elig = data.eligibility(panel, min_adv=5_000_000.0)
    return panel, feat, elig


def run_s(panel, feat, elig, start: str) -> pl.DataFrame:
    """S 規格回測(STRATEGY.md §4-§6),回傳歸一化 NAV。"""
    pool = feat.filter(pl.col("rev_fresh_days") <= 7)
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi")
          .drop_nulls(subset=list(WREL))
          .filter(pl.col("cfo_ni_ratio_ttm")
                  >= pl.col("cfo_ni_ratio_ttm").median().over("date")))
    expr = None
    for c_, wt in WREL.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = (df.with_columns(expr.alias("score"))
          .select(["date", C, "score"])
          .filter(pl.col("date") >= pl.lit(start).str.to_date()))
    entries, _ = entries_and_flags(sc, 5, 10**9)
    stale = (feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C])
             .filter(pl.col("date") >= pl.lit(start).str.to_date()))
    res = simulate(panel, entries, exit_flags=stale, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30,
                                      loser_time_stop=15),
                   start=Date.fromisoformat(start))
    nav = res.nav.select(["date", "nav"]).sort("date")
    return nav.with_columns(pl.col("nav") / pl.col("nav").first())
