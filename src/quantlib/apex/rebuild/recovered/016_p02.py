"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T00:59:24.906Z(工具 Bash)
涵蓋 trials(2):p02_fill_close_dev, p02_fill_close_val
"""
"""v1s26 fill 雙測(dev/close、val/close;判準:CAGR 衰減 ≤ 8pp vs open 版)。"""
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger, metrics
from quantlib.apex.assemble import blend_score, build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
TRI = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0}
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
con = data.connect()
OPEN_REF = {"dev": 0.3077, "val": 0.2083}
for wname, (ws, we) in {"dev": ("2012-01-02", "2023-12-29"),
                        "val": ("2024-01-02", "2025-06-30")}.items():
    panel, feat, elig = build_features(con, ws, we)
    pool = feat.filter(pl.col("rev_fresh_days") <= 5)
    sc = blend_score(pool, elig, TRI, require=GATE).filter(pl.col("date") >= pl.lit(ws).str.to_date())
    flags = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    e, _ = entries_and_flags(sc, 20, 10**9)
    res = simulate(panel, e, exit_flags=flags, exec_spec=ExecSpec(fill_at="next_close"),
                   port_spec=PortSpec(n_slots=20, max_new_per_day=5),
                   exit_spec=ExitSpec(trailing_stop=0.25, time_stop=30),
                   start=Date.fromisoformat(ws))
    s = metrics.perf_stats(res.nav)
    decay = OPEN_REF[wname] - s["cagr"]
    tid = ledger.log_trial(family="validation", name=f"p02_fill_close_{wname}",
                           hypothesis="fill 慣例雙測", config={"fill": "next_close", "stale": 26},
                           window=f"{ws}..{we}", metrics=s, batch="P02", curve=res.nav)
    print(f"{tid} {wname}/next_close: cagr {s['cagr']:+.2%} sharpe {s['sharpe']:.2f} "
          f"mdd {s['mdd']:+.2%} | 衰減 {decay*100:.1f}pp {'✅' if decay <= 0.08 else '❌'}")
