"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T12:01:56.731Z(工具 Bash)
涵蓋 trials(6):r16a_adv3, r16b_adv5_n6, r16c_adv5_n10, r16d_adv5_t40, r16e_adv5_stack, r16f_adv5_maxnew8
"""
"""adv5 舊時代確認 + R16(adv 響應曲線 × 疊加;晉級 ≥90 或 Sharpe≥2.4∧≥87)。"""
import time
import polars as pl
from datetime import date as Date
from quantlib.apex import data, metrics
from quantlib.apex.assemble import build_features, entries_and_flags, run_trial
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W5 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5, "rev_seq": 0.5}
t0 = time.time()
con = data.connect()

def prep(ws, we):
    panel, feat, elig_d = build_features(con, ws, we)
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
    return panel, feat

def go(panel, feat, el, ws, *, w=None, fresh=5, tstop=30, trail=0.35, topn=8, max_new=5,
       log=None):
    w = w or W5
    pool = feat.filter(pl.col("rev_fresh_days") <= fresh)
    df = (pool.join(el.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=list(w)))
    for cond in GATE:
        df = df.filter(cond)
    expr = None
    for c_, wt in w.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    e, _ = entries_and_flags(sc, topn, 10**9)
    f = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=topn, max_new_per_day=max_new),
                   exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop),
                   start=Date.fromisoformat(ws))
    return res

# 舊時代確認(adv5)
OW, OE = "2012-01-02", "2018-12-28"
p_old, f_old = prep(OW, OE)
E5_old = data.eligibility(p_old, min_adv=5_000_000.0)
s_old = metrics.perf_stats(go(p_old, f_old, E5_old, OW).nav)
ok = s_old["cagr"] >= 0.36
print(f"adv5 舊時代 2012-18:{s_old['cagr']:+.1%}/{s_old['sharpe']:.2f}/{s_old['mdd']:+.1%} "
      f"(v6 同段 38.0%)→ {'✅ 確認' if ok else '❌ 出局'}")

# R16 dev
DS, DE = "2019-01-02", "2025-06-30"
panel, feat = prep(DS, DE)
bench = data.benchmark_nav(con, DS, DE)
E5 = data.eligibility(panel, min_adv=5_000_000.0)
E3 = data.eligibility(panel, min_adv=3_000_000.0)
W5a8 = {**W5, "rev_yoy_accel": 0.8}

def trial(name, res):
    s = metrics.summarize(res.nav, res.trades, bench)
    from quantlib.apex import ledger
    tid = ledger.log_trial(family="r16", name=name, hypothesis="adv5 基座疊加",
                           config={"name": name}, window=f"{DS}..{DE}", metrics=s,
                           batch="R16", curve=res.nav)
    return {"trial_id": tid, "name": name, **{k: s[k] for k in ("cagr", "sharpe", "mdd", "exposure")}}

runs = [
    trial("r16a_adv3", go(panel, feat, E3, DS)),
    trial("r16b_adv5_n6", go(panel, feat, E5, DS, topn=6)),
    trial("r16c_adv5_n10", go(panel, feat, E5, DS, topn=10)),
    trial("r16d_adv5_t40", go(panel, feat, E5, DS, trail=0.40)),
    trial("r16e_adv5_stack", go(panel, feat, E5, DS, w=W5a8, fresh=6, tstop=24)),
    trial("r16f_adv5_maxnew8", go(panel, feat, E5, DS, max_new=8)),
]
cmp = pl.DataFrame(runs).sort("cagr", descending=True)
with pl.Config(tbl_rows=8, tbl_width_chars=110):
    print(cmp)
print("\nadv5 基準:87.3/2.31/−32.3 | 晉級:≥90 或(Sharpe≥2.4∧≥87)| 目標 ≥100")
print(f"total {time.time()-t0:.1f}s")
