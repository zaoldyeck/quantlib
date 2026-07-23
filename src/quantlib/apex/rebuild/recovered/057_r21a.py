"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T12:20:06.667Z(工具 Bash)
涵蓋 trials(6):r21a_n5_pxreact, r21b_n4_pxreact, r21c_n4_volreact, r21d_n4_both, r21e_n4_pxr_f7, r21f_n5_pxr100
"""
"""R21 — 釋放日反應軸(價/量)× n4/n5(6 trials;晉級 ≥130 或 2.5∧120)。"""
import time
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger, metrics
from quantlib.apex.assemble import build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W5 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5, "rev_seq": 0.5}
W5s40 = dict(W5) | {"rev_seq": 0.4}
t0 = time.time()
con = data.connect()
DS, DE = "2019-01-02", "2025-06-30"
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
E5 = data.eligibility(panel, min_adv=5_000_000.0)
bench = data.benchmark_nav(con, DS, DE)

# 釋放日錨點:每 (code, 月) 的首個交易日與其收盤/量能
td = pl.DataFrame({"td": panel["date"].unique().sort()}).sort("td")
rel = (data.load_monthly_revenue(con, DE)
       .with_columns(pl.date(pl.col("year") + pl.col("month") // 12, pl.col("month") % 12 + 1, 10).alias("d0"))
       .select([C, "d0"]).unique().sort("d0")
       .join_asof(td, left_on="d0", right_on="td", strategy="forward")
       .rename({"td": "rel_day"}).drop_nulls(subset=["rel_day"]))
adv = data.eligibility(panel).select(["date", C, "adv20"])
relpx = (rel.join(panel.select(["date", C, "close", "trade_value"]),
                  left_on=["rel_day", C], right_on=["date", C], how="inner")
         .join(adv, left_on=["rel_day", C], right_on=["date", C], how="left")
         .with_columns((pl.col("trade_value") / (pl.col("adv20") + 1)).alias("vol_react"))
         .select([C, "rel_day", pl.col("close").alias("close_rel"), "vol_react"])
         .sort("rel_day"))
feat = (feat.sort("date")
        .join_asof(relpx, left_on="date", right_on="rel_day", by=C,
                   strategy="backward", tolerance="40d")
        .sort([C, "date"])
        .with_columns((pl.col("close") / pl.col("close_rel") - 1).alias("px_react")))

def go(name, *, topn, fresh=6, w=None, max_new=2):
    w = w or W5
    pool = feat.filter(pl.col("rev_fresh_days") <= fresh)
    df = (pool.join(E5.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=list(w)))
    for cond in GATE:
        df = df.filter(cond)
    expr = None
    for c_, wt in w.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(DS).str.to_date())
    e, _ = entries_and_flags(sc, topn, 10**9)
    f = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(DS).str.to_date())
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=topn, max_new_per_day=max_new),
                   exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30),
                   start=Date.fromisoformat(DS))
    s = metrics.summarize(res.nav, res.trades, bench)
    ledger.log_trial(family="mod_line", name=name, hypothesis="釋放日反應軸",
                     config={"name": name}, window=f"{DS}..{DE}", metrics=s,
                     batch="R21", curve=res.nav)
    return {"name": name, **{k: s[k] for k in ("cagr", "sharpe", "mdd")}}

runs = [
    go("r21a_n5_pxreact", topn=5, w=dict(W5) | {"px_react": 0.5}),
    go("r21b_n4_pxreact", topn=4, w=dict(W5s40) | {"px_react": 0.5}),
    go("r21c_n4_volreact", topn=4, w=dict(W5s40) | {"vol_react": 0.5}),
    go("r21d_n4_both", topn=4, w=dict(W5s40) | {"px_react": 0.5, "vol_react": 0.25}),
    go("r21e_n4_pxr_f7", topn=4, fresh=7, w=dict(W5s40) | {"px_react": 0.5}),
    go("r21f_n5_pxr100", topn=5, w=dict(W5) | {"px_react": 1.0}),
]
cmp = pl.DataFrame(runs).sort("cagr", descending=True)
with pl.Config(tbl_rows=8, tbl_width_chars=100):
    print(cmp)
print("\n現況最佳 r20d:126.1/2.40/−37.9 | 晉級 ≥130 或(2.5∧120)| 目標 ≥200")
print(f"total {time.time()-t0:.1f}s")
