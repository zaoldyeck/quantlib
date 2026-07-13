"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T01:33:30.774Z(工具 Bash)
涵蓋 trials(3):holdout_v3_next_close, holdout_v3_next_mid, holdout_v3_next_open
"""
"""FINAL HOLDOUT — apex_revcycle_v3 on 2025-07-01 → 2026-07-07(evaluation-only,動用 #1)。"""
import polars as pl
from datetime import date as Date
from research.apex import data, ledger, metrics
from research.apex.assemble import blend_score, build_features, entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
HS, HE = "2025-07-01", "2026-07-07"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W4 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5}

con = data.connect()
panel, feat, elig = build_features(con, HS, HE)
bench = data.benchmark_nav(con, HS, HE)
b = bench.sort("date")
byears = (b["date"][-1] - b["date"][0]).days / 365.25
bench_ret = float(b["nav"][-1] / b["nav"][0] - 1)

pool = feat.filter(pl.col("rev_fresh_days") <= 5)
sc = blend_score(pool, elig, W4, require=GATE).filter(pl.col("date") >= pl.lit(HS).str.to_date())
flags = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
    pl.col("date") >= pl.lit(HS).str.to_date())
e, _ = entries_and_flags(sc, 20, 10**9)

print(f"holdout 窗:{HS} → {HE}(~{byears:.2f}y)| 0050 同期:{bench_ret:+.1%}\n")
for fill in ("next_open", "next_mid", "next_close"):
    res = simulate(panel, e, exit_flags=flags, exec_spec=ExecSpec(fill_at=fill),
                   port_spec=PortSpec(n_slots=20, max_new_per_day=5),
                   exit_spec=ExitSpec(trailing_stop=0.25, time_stop=30),
                   start=Date.fromisoformat(HS))
    s = metrics.summarize(res.nav, res.trades, bench)
    total_ret = s["final_nav_ratio"] - 1
    tid = ledger.log_trial(
        family="holdout", name=f"holdout_v3_{fill}",
        hypothesis="A1-R 終局 evaluation-only 披露(動用 #1)",
        config={"champion": "apex_revcycle_v3", "fill": fill},
        window=f"{HS}..{HE}", metrics=s, batch="HOLDOUT", curve=res.nav)
    print(f"{tid} {fill:10s} 總報酬 {total_ret:+.1%} | CAGR {s['cagr']:+.1%} | "
          f"Sharpe {s['sharpe']:.2f} | MDD {s['mdd']:+.1%} | trades {s['n_trades']} "
          f"| win {s['win_rate']:.0%}")
    if fill == "next_open":
        print(metrics.fmt_report("v3 holdout(next_open)", res.nav, res.trades, bench))
        print()
