"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T08:20:12.094Z(工具 Bash)
涵蓋 trials(2):fullspan_v3_n20, fullspan_v3_n8
"""
"""全跨度連續模擬(2012-01 → 2026-07)逐年績效:v3-n20 / v3-n8 / 0050。"""
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger, metrics
from quantlib.apex.assemble import blend_score, build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
FS, FE = "2012-01-02", "2026-07-07"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W4 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5}

con = data.connect()
panel, feat, elig = build_features(con, FS, FE)
bench = data.benchmark_nav(con, FS, FE)

def run_n(topn):
    pool = feat.filter(pl.col("rev_fresh_days") <= 5)
    sc = blend_score(pool, elig, W4, require=GATE).filter(pl.col("date") >= pl.lit(FS).str.to_date())
    e, _ = entries_and_flags(sc, topn, 10**9)
    f = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(FS).str.to_date())
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=topn, max_new_per_day=5),
                   exit_spec=ExitSpec(trailing_stop=0.25, time_stop=30),
                   start=Date.fromisoformat(FS))
    return res

out = {}
for topn, tag in [(20, "n20"), (8, "n8")]:
    res = run_n(topn)
    s = metrics.perf_stats(res.nav)
    yt = metrics.yearly_table(res.nav).rename({"ret": f"ret_{tag}", "mdd": f"mdd_{tag}"})
    out[tag] = (s, yt, res.nav)
    tid = ledger.log_trial(family="fullspan", name=f"fullspan_v3_{tag}",
                           hypothesis="全跨度連續逐年報告", config={"topn": topn},
                           window=f"{FS}..{FE}", metrics=s, batch="REPORT", curve=res.nav)
    print(f"{tid} v3-{tag} 全跨度(14.5y): CAGR {s['cagr']:+.2%} Sharpe {s['sharpe']:.2f} "
          f"MDD {s['mdd']:+.2%} 終值 {s['final_nav_ratio']:.1f}x")

byr = (bench.sort("date").with_columns(pl.col("date").dt.year().alias("year"))
       .group_by("year").agg(pl.col("nav").last()).sort("year"))
byr = byr.with_columns((pl.col("nav") / pl.col("nav").shift(1, fill_value=1.0) - 1).alias("ret_0050"))

tbl = (out["n20"][1].join(out["n8"][1], on="year")
       .join(byr.select(["year", "ret_0050"]), on="year"))
print("\n year | v3-n20 ret / mdd | v3-n8 ret / mdd | 0050")
for r in tbl.iter_rows(named=True):
    print(f" {r['year']} | {r['ret_n20']*100:+7.1f}% / {r['mdd_n20']*100:5.1f}% | "
          f"{r['ret_n8']*100:+7.1f}% / {r['mdd_n8']*100:5.1f}% | {r['ret_0050']*100:+6.1f}%")
