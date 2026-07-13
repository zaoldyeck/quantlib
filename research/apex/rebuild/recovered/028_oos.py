"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T08:06:28.696Z(工具 Bash)
涵蓋 trials(8):oos_r03d_n8, oos_r03h_n10_momw75, oos_r06e_geo_n8_t30, oos_r07d_revlevel, oos_r07f_t35, oos_r08a_t35_revlevel, r03d_n8, r03h_n10_momw75
"""
"""R03 top-2 OOS 熱年終審(2025-07 → 2026-07;R-line OOS touch #1-2)。"""
import polars as pl
from datetime import date as Date
from research.apex import data, ledger, metrics
from research.apex.assemble import blend_score, build_features, entries_and_flags, run_trial
from research.apex.engine import ExecSpec, ExitSpec, PortSpec

C = "company_code"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]

con = data.connect()

# 年段正數驗證(dev 曲線)
for tid, nm in [("T0148", "r03h_n10_momw75"), ("T0144", "r03d_n8")]:
    yt = metrics.yearly_table(ledger.load_curve(tid))
    pos = int((yt["ret"] > 0).sum())
    print(f"{nm}: 年段正 {pos}/{yt.height} | " +
          " ".join(f"{y}:{r*100:+.0f}%" for y, r in zip(yt["year"], yt["ret"])))

HS, HE = "2025-07-01", "2026-07-07"
panel, feat, elig = build_features(con, HS, HE)
bench = data.benchmark_nav(con, HS, HE)

def go(name, topn, mom_w, trail=0.25):
    w = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": mom_w}
    pool = feat.filter(pl.col("rev_fresh_days") <= 5)
    sc = blend_score(pool, elig, w, require=GATE).filter(pl.col("date") >= pl.lit(HS).str.to_date())
    e, _ = entries_and_flags(sc, topn, 10**9)
    f = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(HS).str.to_date())
    return run_trial(
        name=name, hypothesis="R03 top-2 OOS 熱年終審", family="oos_hot_year", batch="R03-OOS",
        panel=panel, entries=e, exit_flags=f, bench=bench, window=f"{HS}..{HE}",
        start=Date.fromisoformat(HS),
        config={"topn": topn, "mom_w": mom_w, "trail": trail},
        port_spec=PortSpec(n_slots=topn, max_new_per_day=5),
        exit_spec=ExitSpec(trailing_stop=trail, time_stop=30), verbose=False)

print(f"\n0050 同窗:+118.3% | v3-n20 同窗:+49.2%\n")
for r in [go("oos_r03h_n10_momw75", 10, 0.75), go("oos_r03d_n8", 8, 0.5)]:
    tot = r["final_nav_ratio"] - 1
    ok = tot >= 0.65 and r["mdd"] >= -0.30
    print(f"{r['name']:22s} 總報酬 {tot:+.1%} | Sharpe {r['sharpe']:.2f} | "
          f"MDD {r['mdd']:+.1%} | win {r['win_rate']:.0%} | "
          f"{'✅ 過(≥+65%∧MDD≥−30%)' if ok else '❌'}{' 🚀 stretch ≥+100%' if tot >= 1.0 else ''}")
