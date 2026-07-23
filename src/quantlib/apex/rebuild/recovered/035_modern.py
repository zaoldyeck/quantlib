"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T08:32:44.916Z(工具 Bash)
涵蓋 trials(15):modern_r06e_geo_n8_t30, modern_r07d_revlevel, modern_r07f_t35, oos_r03d_n8, oos_r03h_n10_momw75, oos_r06e_geo_n8_t30, oos_r07d_revlevel, oos_r07f_t35, oos_r08a_t35_revlevel, r07d_revlevel, r07f_t35, 現代era_R3, 現代era_S, 現代era_r08a, 現代era_v6
"""
"""R07 top-2 OOS 熱年 + 現代era連續。"""
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger, metrics
from quantlib.apex.assemble import build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W4 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5}
con = data.connect()

def run_geo(ws, we, *, exps, trail):
    panel, feat, elig = build_features(con, ws, we)
    pool = feat.filter(pl.col("rev_fresh_days") <= 5)
    cols = list(exps)
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=cols))
    for cond in GATE:
        df = df.filter(cond)
    expr = None
    for c_, wt in exps.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    e, _ = entries_and_flags(sc, 8, 10**9)
    fl = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    return simulate(panel, e, exit_flags=fl, exec_spec=ExecSpec(),
                    port_spec=PortSpec(n_slots=8, max_new_per_day=5),
                    exit_spec=ExitSpec(trailing_stop=trail, time_stop=30),
                    start=Date.fromisoformat(ws))

CFGS = {
    "r07f_t35": {"exps": W4, "trail": 0.35},
    "r07d_revlevel": {"exps": dict(W4) | {"rev_yoy": 0.5}, "trail": 0.30},
}
for nm, cfg in CFGS.items():
    res = run_geo("2025-07-01", "2026-07-07", **cfg)
    s = metrics.perf_stats(res.nav)
    tot = s["final_nav_ratio"] - 1
    ledger.log_trial(family="oos_hot_year", name=f"oos_{nm}", hypothesis="R07 top-2 OOS",
                     config=str(cfg), window="2025-07-01..2026-07-07", metrics=s,
                     batch="R07-OOS", curve=res.nav)
    m = run_geo("2019-01-02", "2026-07-07", **cfg)
    sm = metrics.perf_stats(m.nav)
    ledger.log_trial(family="fullspan", name=f"modern_{nm}", hypothesis="現代era vs 正2",
                     config=str(cfg), window="2019-01-02..2026-07-07", metrics=sm,
                     batch="R07-OOS", curve=m.nav)
    print(f"{nm:16s} OOS 熱年 {tot:+.1%}/S{s['sharpe']:.2f}/MDD{s['mdd']:+.1%} "
          f"{'✅' if tot >= 0.722 and s['mdd'] >= -0.30 else '❌'} | "
          f"現代era CAGR {sm['cagr']:+.1%}/S{sm['sharpe']:.2f}/MDD{sm['mdd']:+.1%} "
          f"({sm['final_nav_ratio']:.0f}x) {'🏆>正2' if sm['cagr'] > 0.559 else '❌'}")
