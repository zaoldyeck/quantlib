"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T09:50:58.961Z(工具 Bash)
涵蓋 trials(11):r11c_pead_gate, 全跨度14.5y_R3, 全跨度14.5y_S, 全跨度14.5y_v6, 正2全史同窗_R3, 正2全史同窗_r08a, 正2全史同窗_v6, 現代era_R3, 現代era_S, 現代era_r08a, 現代era_v6
"""
"""R11 確認:r11c 於 2012-2018 舊時代段 vs 旗艦同段(判準:不劣於旗艦 −2pp)。
通過則跑連續窗報告(現代era + 正2全史同窗)。"""
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger, metrics
from quantlib.apex.assemble import build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W4 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5}
con = data.connect()

def build_pead(panel, feat):
    td = pl.DataFrame({"td": panel["date"].unique().sort()}).sort("td")
    rel = (data.load_monthly_revenue(con, "2026-07-07")
           .with_columns(pl.date(pl.col("year") + pl.col("month") // 12, pl.col("month") % 12 + 1, 10).alias("d0"))
           .select([C, "d0"]).unique().sort("d0")
           .join_asof(td, left_on="d0", right_on="td", strategy="forward")
           .rename({"td": "rel_day"}).drop_nulls(subset=["rel_day"]))
    px = (panel.sort([C, "date"])
          .with_columns((pl.col("close").shift(-22) / pl.col("close").shift(-1) - 1).over(C).alias("cyc_ret"))
          .select(["date", C, "cyc_ret"]))
    pead = (rel.join(px, left_on=["rel_day", C], right_on=["date", C], how="inner")
            .sort([C, "rel_day"])
            .with_columns(pl.col("cyc_ret").rolling_mean(6).shift(1).over(C).alias("pead_persist"))
            .drop_nulls(subset=["pead_persist"])
            .select([C, pl.col("rel_day").alias("avail"), "pead_persist"]).sort("avail"))
    return (feat.sort("date")
            .join_asof(pead, left_on="date", right_on="avail", by=C, strategy="backward", tolerance="70d")
            .sort([C, "date"]))

def run(ws, we, *, pead_gate):
    panel, feat, elig = build_features(con, ws, we)
    if pead_gate:
        feat = build_pead(panel, feat)
    pool = feat.filter(pl.col("rev_fresh_days") <= 5)
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=list(W4)))
    for cond in GATE:
        df = df.filter(cond)
    if pead_gate:
        df = df.filter(pl.col("pead_persist") > 0)
    expr = None
    for c_, wt in W4.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    e, _ = entries_and_flags(sc, 8, 10**9)
    f = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    return simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                    port_spec=PortSpec(n_slots=8, max_new_per_day=5),
                    exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30),
                    start=Date.fromisoformat(ws))

# 舊時代段確認
OW, OE = "2012-01-02", "2018-12-28"
flag = metrics.perf_stats(run(OW, OE, pead_gate=False).nav)
cand = metrics.perf_stats(run(OW, OE, pead_gate=True).nav)
ok = cand["cagr"] >= flag["cagr"] - 0.02
print(f"舊時代段 2012-2018:旗艦 {flag['cagr']:+.1%}/{flag['sharpe']:.2f} vs "
      f"r11c {cand['cagr']:+.1%}/{cand['sharpe']:.2f} → {'✅ 確認通過' if ok else '❌ 出局'}")

if ok:
    for ws, we, tag, target in [("2019-01-02", "2026-07-07", "現代era", 0.559),
                                 ("2014-11-03", "2026-07-07", "正2全史同窗", 0.377),
                                 ("2012-01-02", "2026-07-07", "全跨度14.5y", None)]:
        r = run(ws, we, pead_gate=True)
        s = metrics.perf_stats(r.nav)
        ledger.log_trial(family="fullspan", name=f"{tag}_r11c", hypothesis="R2 冠軍連續窗",
                         config={"pead_gate": True}, window=f"{ws}..{we}", metrics=s,
                         batch="R11", curve=r.nav)
        vs = f"{'🏆>正2' if target and s['cagr'] > target else ''}"
        print(f"{tag}: CAGR {s['cagr']:+.1%} | Sharpe {s['sharpe']:.2f} | MDD {s['mdd']:+.1%} | "
              f"{s['final_nav_ratio']:.0f}x {vs}")
        if tag == "全跨度14.5y":
            yt = metrics.yearly_table(r.nav)
            print("逐年:", "  ".join(f"{y}:{v*100:+.0f}%" for y, v in zip(yt["year"], yt["ret"])))
