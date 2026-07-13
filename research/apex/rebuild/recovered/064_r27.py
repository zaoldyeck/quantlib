"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T13:02:57.645Z(工具 Bash)
涵蓋 trials(9):r27_lts10, r27_lts15, r27_lts20, r27_uw15, r27_uw20, r27_uw20_lts15, r27_uw25, r27_uw25_lts15, r27_uw25_lts20
"""
"""R27 — 非對稱出場網格 @ n5+rel(KPI v2:Sortino 為主;預註冊:
晉級 = Sortino > 4.13 ∧ CAGR ≥ 100 ∧ MDD ≥ −40;晉級者附舊時代披露)。"""
import time
import polars as pl
from datetime import date as Date
from research.apex import data, ledger, metrics
from research.apex.assemble import build_features, entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
WREL = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5,
        "rev_seq": 0.5, "accel_rel": 0.5}
t0 = time.time()
con = data.connect()

def prep(ws, we):
    panel, feat, _ = build_features(con, ws, we)
    rev = (data.load_monthly_revenue(con, we)
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
    return panel, feat.join(rel, on=["date", C], how="left")

def go(panel_, feat_, ws, *, uw=None, lts=None):
    el = data.eligibility(panel_, min_adv=5_000_000.0)
    pool = feat_.filter(pl.col("rev_fresh_days") <= 7)
    df = (pool.join(el.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=list(WREL)))
    for cond in GATE:
        df = df.filter(cond)
    expr = None
    for c_, wt in WREL.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    e, _ = entries_and_flags(sc, 5, 10**9)
    f = feat_.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    return simulate(panel_, e, exit_flags=f, exec_spec=ExecSpec(),
                    port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                    exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30,
                                       underwater_trail=uw, loser_time_stop=lts),
                    start=Date.fromisoformat(ws))

DS, DE = "2019-01-02", "2025-06-30"
panel, feat = prep(DS, DE)
bench = data.benchmark_nav(con, DS, DE)
runs = []
for name, uw, lts in [("uw20", 0.20, None), ("uw25", 0.25, None), ("uw15", 0.15, None),
                      ("lts10", None, 10), ("lts15", None, 15), ("lts20", None, 20),
                      ("uw20_lts15", 0.20, 15), ("uw25_lts15", 0.25, 15),
                      ("uw25_lts20", 0.25, 20)]:
    res = go(panel, feat, DS, uw=uw, lts=lts)
    s = metrics.summarize(res.nav, res.trades, bench)
    ledger.log_trial(family="mod_line", name=f"r27_{name}", hypothesis="非對稱出場(Sortino KPI)",
                     config={"uw": uw, "lts": lts}, window=f"{DS}..{DE}", metrics=s,
                     batch="R27", curve=res.nav)
    runs.append({"name": name, **{k: s[k] for k in ("cagr", "sharpe", "sortino", "mdd")}})
cmp = pl.DataFrame(runs).sort("sortino", descending=True)
with pl.Config(tbl_rows=11, tbl_width_chars=110):
    print(cmp)
print("\n基準 n5+rel:CAGR 121.7 / Sharpe 2.55 / Sortino 4.13 / MDD −35.6")
print("晉級:Sortino > 4.13 ∧ CAGR ≥ 100 ∧ MDD ≥ −40")
print(f"total {time.time()-t0:.1f}s")
