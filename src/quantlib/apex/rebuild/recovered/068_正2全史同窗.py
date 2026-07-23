"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T16:29:16.953Z(工具 Bash)
涵蓋 trials(3):正2全史同窗_R3, 正2全史同窗_r08a, 正2全史同窗_v6
"""
"""S vs 0050 vs 00631L 同窗 NAV(正2全史同窗 2014-10-31 起 + 現代 era 2019 起)。"""
import time
import polars as pl
from datetime import date as Date
from quantlib.apex import data
from quantlib.apex.assemble import build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

OUT = "/private/tmp/claude-501/-Users-zaoldyeck-Documents-scala-quantlib/3d5413eb-b7db-45c8-bf62-efdef11c1375/scratchpad"
C = "company_code"
WREL = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5,
        "rev_seq": 0.5, "accel_rel": 0.5}
t0 = time.time()
con = data.connect()
DS, DE = "2014-10-31", "2026-07-09"
panel, feat, _ = build_features(con, DS, DE)
rev = (data.load_monthly_revenue(con, DE)
       .sort([C, "year", "month"])
       .with_columns([
           pl.date(pl.col("year") + pl.col("month") // 12, pl.col("month") % 12 + 1, 10).alias("avail"),
           (pl.col("monthly_revenue").rolling_sum(3)
            / pl.col("monthly_revenue").rolling_sum(3).shift(3) - 1).over(C).alias("rev_seq"),
       ])
       .select([C, "avail", "rev_seq"]).drop_nulls().sort("avail"))
feat = (feat.sort("date")
        .join_asof(rev, left_on="date", right_on="avail", by=C, strategy="backward", tolerance="70d")
        .sort([C, "date"]))
tax = con.sql("SELECT company_code, effective_date, industry FROM industry_taxonomy_pit "
              "WHERE industry IS NOT NULL ORDER BY effective_date").pl()
fx = (feat.select(["date", C, "rev_yoy_accel"]).drop_nulls().sort("date")
      .join_asof(tax.sort("effective_date"), left_on="date", right_on="effective_date",
                 by=C, strategy="backward").drop_nulls(subset=["industry"]))
ind_med = fx.group_by(["date", "industry"]).agg(pl.col("rev_yoy_accel").median().alias("m"))
rel = (fx.join(ind_med, on=["date", "industry"], how="left")
       .with_columns((pl.col("rev_yoy_accel") - pl.col("m")).alias("accel_rel"))
       .select(["date", C, "accel_rel"]))
feat = feat.join(rel, on=["date", C], how="left")
E5 = data.eligibility(panel, min_adv=5_000_000.0)
print(f"prep {time.time()-t0:.0f}s")

def run_s(start: str):
    pool = feat.filter(pl.col("rev_fresh_days") <= 7)
    df = (pool.join(E5.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=list(WREL)))
    df = df.filter(pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date"))
    expr = None
    for c_, wt in WREL.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(start).str.to_date())
    e, _ = entries_and_flags(sc, 5, 10**9)
    f = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(start).str.to_date())
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30, loser_time_stop=15),
                   start=Date.fromisoformat(start))
    return res.nav.select(["date", "nav"])

for tag, start in [("full", "2014-10-31"), ("modern", "2019-01-02")]:
    nav_s = run_s(start)
    nav_s.write_parquet(f"{OUT}/nav_s_{tag}.parquet")
    for code in ["0050", "00631L"]:
        b = data.benchmark_nav(con, start, DE, code=code)
        b.write_parquet(f"{OUT}/nav_{code}_{tag}.parquet")
    yrs = (nav_s["date"][-1] - nav_s["date"][0]).days / 365.25
    print(f"{tag}: S 終值 {nav_s['nav'][-1]:.1f}x, CAGR {(nav_s['nav'][-1])**(1/yrs)-1:+.1%}, {time.time()-t0:.0f}s")
print("done")
