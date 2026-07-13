"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T12:18:45.883Z(工具 Bash)
涵蓋 trials(8):r20a_n4, r20b_n4_seq40, r20c_n4_fresh7, r20d_n4_s40_f7, r20e_n3, r20f_n3_s40_f7, r20g_n2, r20h_n4_mn3
"""
"""R20 — 深集中 × 微堆疊 + oracle 上界。"""
import time
import polars as pl
from datetime import date as Date
from research.apex import data, ledger, metrics
from research.apex.assemble import build_features, entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W5 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5, "rev_seq": 0.5}
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

# 未來 21 日報酬(oracle 專用,標明作弊)
fut = (panel.sort([C, "date"])
       .with_columns((pl.col("close").shift(-22) / pl.col("close").shift(-1) - 1).over(C).alias("fut21"))
       .select(["date", C, "fut21"]))
feat = feat.join(fut, on=["date", C], how="left")

def go(name, *, topn, fresh=6, max_new=2, w=None, oracle=False, log=True):
    w = w or W5
    pool = feat.filter(pl.col("rev_fresh_days") <= fresh)
    need = list(w) if not oracle else ["fut21"]
    df = (pool.join(E5.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=need))
    for cond in GATE:
        df = df.filter(cond)
    if oracle:
        sc = df.with_columns(pl.col("fut21").alias("score"))
    else:
        expr = None
        for c_, wt in w.items():
            term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
            expr = term if expr is None else expr * term
        sc = df.with_columns(expr.alias("score"))
    sc = sc.select(["date", C, "score"]).filter(pl.col("date") >= pl.lit(DS).str.to_date())
    e, _ = entries_and_flags(sc, topn, 10**9)
    f = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(DS).str.to_date())
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=topn, max_new_per_day=max_new),
                   exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30),
                   start=Date.fromisoformat(DS))
    s = metrics.summarize(res.nav, res.trades, bench)
    if log:
        ledger.log_trial(family="mod_line", name=name, hypothesis="R20 200% 攻堅",
                         config={"name": name, "oracle": oracle}, window=f"{DS}..{DE}",
                         metrics=s, batch="R20", curve=res.nav)
    return {"name": name, **{k: s[k] for k in ("cagr", "sharpe", "mdd")}}

W5s40 = dict(W5) | {"rev_seq": 0.4}
runs = [
    go("r20a_n4", topn=4),
    go("r20b_n4_seq40", topn=4, w=W5s40),
    go("r20c_n4_fresh7", topn=4, fresh=7),
    go("r20d_n4_s40_f7", topn=4, w=W5s40, fresh=7),
    go("r20e_n3", topn=3),
    go("r20f_n3_s40_f7", topn=3, w=W5s40, fresh=7),
    go("r20g_n2", topn=2),
    go("r20h_n4_mn3", topn=4, max_new=3),
]
cmp = pl.DataFrame(runs).sort("cagr", descending=True)
with pl.Config(tbl_rows=10, tbl_width_chars=100):
    print(cmp)

print("\n── ORACLE 上界(作弊:以未來 21 日報酬選股,同容器同成本同鎖死)──")
for topn in (5, 4, 3):
    o = go(f"oracle_n{topn}", topn=topn, oracle=True, log=False)
    print(f"oracle n{topn}: CAGR {o['cagr']:+.1%} | Sharpe {o['sharpe']:.2f} | MDD {o['mdd']:+.1%}")
print("\nR3 基準:113.7/2.44/−33.3 | 晉級 ≥130 或(2.5∧120)| 目標 ≥200")
print(f"total {time.time()-t0:.1f}s")
