"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T12:03:27.276Z(工具 Bash)
涵蓋 trials(6):r17a_adv5n6_pead, r17b_adv5_pead, r17c_adv5n6_t40, r17d_adv5n6_stack, r17e_adv5n6_pead_t40, r17f_adv5n6_seq60
"""
"""R17 — MOD 線疊加(6 trials)+ 各勝者舊時代披露。"""
import time
import polars as pl
from datetime import date as Date
from research.apex import data, ledger, metrics
from research.apex.assemble import build_features, entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W5 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5, "rev_seq": 0.5}
W5a8 = {**W5, "rev_yoy_accel": 0.8}
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
    # pead_persist
    td = pl.DataFrame({"td": panel["date"].unique().sort()}).sort("td")
    rel = (data.load_monthly_revenue(con, we)
           .with_columns(pl.date(pl.col("year") + pl.col("month") // 12, pl.col("month") % 12 + 1, 10).alias("d0"))
           .select([C, "d0"]).unique().sort("d0")
           .join_asof(td, left_on="d0", right_on="td", strategy="forward")
           .rename({"td": "rel_day"}).drop_nulls(subset=["rel_day"]))
    px = (panel.sort([C, "date"])
          .with_columns((pl.col("close").shift(-22) / pl.col("close").shift(-1) - 1).over(C).alias("cyc"))
          .select(["date", C, "cyc"]))
    pead = (rel.join(px, left_on=["rel_day", C], right_on=["date", C], how="inner")
            .sort([C, "rel_day"])
            .with_columns(pl.col("cyc").rolling_mean(6).shift(1).over(C).alias("pead"))
            .drop_nulls(subset=["pead"])
            .select([C, pl.col("rel_day").alias("pavail"), "pead"]).sort("pavail"))
    feat = (feat.sort("date")
            .join_asof(pead, left_on="date", right_on="pavail", by=C, strategy="backward", tolerance="70d")
            .sort([C, "date"]))
    return panel, feat

def go(panel, feat, el, ws, *, w=None, fresh=5, tstop=30, trail=0.35, topn=8, pead=False):
    w = w or W5
    pool = feat.filter(pl.col("rev_fresh_days") <= fresh)
    df = (pool.join(el.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=list(w)))
    for cond in GATE:
        df = df.filter(cond)
    if pead:
        df = df.filter(pl.col("pead") > 0)
    expr = None
    for c_, wt in w.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    e, _ = entries_and_flags(sc, topn, 10**9)
    f = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    return simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                    port_spec=PortSpec(n_slots=topn, max_new_per_day=5),
                    exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop),
                    start=Date.fromisoformat(ws))

DS, DE = "2019-01-02", "2025-06-30"
panel, feat = prep(DS, DE)
E5 = data.eligibility(panel, min_adv=5_000_000.0)
bench = data.benchmark_nav(con, DS, DE)

def trial(name, res):
    s = metrics.summarize(res.nav, res.trades, bench)
    tid = ledger.log_trial(family="mod_line", name=name, hypothesis="MOD 線 100% 攻堅",
                           config={"name": name}, window=f"{DS}..{DE}", metrics=s,
                           batch="R17", curve=res.nav)
    return {"trial_id": tid, "name": name, **{k: s[k] for k in ("cagr", "sharpe", "mdd")}}

runs = [
    trial("r17a_adv5n6_pead", go(panel, feat, E5, DS, topn=6, pead=True)),
    trial("r17b_adv5_pead", go(panel, feat, E5, DS, pead=True)),
    trial("r17c_adv5n6_t40", go(panel, feat, E5, DS, topn=6, trail=0.40)),
    trial("r17d_adv5n6_stack", go(panel, feat, E5, DS, topn=6, w=W5a8, fresh=6, tstop=24)),
    trial("r17e_adv5n6_pead_t40", go(panel, feat, E5, DS, topn=6, pead=True, trail=0.40)),
    trial("r17f_adv5n6_seq60", go(panel, feat, E5, DS, topn=6, w=dict(W5) | {"rev_seq": 0.6})),
]
cmp = pl.DataFrame(runs).sort("cagr", descending=True)
with pl.Config(tbl_rows=8, tbl_width_chars=100):
    print(cmp)
print("\nMOD 基座 adv5:87.3/2.31 | adv5_n6:90.7/2.18 | 晉級 ≥95 或(2.35∧90)| 目標 ≥100")
print(f"total {time.time()-t0:.1f}s")
