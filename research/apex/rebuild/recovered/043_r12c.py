"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T10:10:44.212Z(工具 Bash)
涵蓋 trials(1):r12c_seq_axis
"""
"""r12c 三重檢查:(1) 舊時代確認 2012-2018;(2) 交易解剖;(3) PIT 突襲檢查。"""
import polars as pl
from datetime import date as Date
from research.apex import data, metrics
from research.apex.assemble import build_features, entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W5 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5,
      "rev_seq": 0.5}
con = data.connect()

def run(ws, we, *, w, seed_check=False):
    panel, feat, elig = build_features(con, ws, we)
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
    if seed_check:
        # PIT 突襲:抽 3 個樣本列,驗證 rev_seq 只用 avail 之前的月份
        s = (feat.filter((pl.col("rev_seq").is_not_null()) & (pl.col("rev_fresh_days") <= 5))
             .sample(3, seed=7).select(["date", C, "rev_fresh_days", "rev_seq"]))
        print("PIT 抽樣(date 應 ≥ 對應 avail):")
        print(s)
    pool = feat.filter(pl.col("rev_fresh_days") <= 5)
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=list(w)))
    for cond in GATE:
        df = df.filter(cond)
    expr = None
    for c_, wt in w.items():
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

W4 = {k: v for k, v in W5.items() if k != "rev_seq"}
OW, OE = "2012-01-02", "2018-12-28"
flag_old = metrics.perf_stats(run(OW, OE, w=W4).nav)
res_old = run(OW, OE, w=W5)
cand_old = metrics.perf_stats(res_old.nav)
ok = cand_old["cagr"] >= flag_old["cagr"] - 0.02
print(f"舊時代 2012-2018:旗艦 {flag_old['cagr']:+.1%}/{flag_old['sharpe']:.2f} vs "
      f"r12c {cand_old['cagr']:+.1%}/{cand_old['sharpe']:.2f}/MDD{cand_old['mdd']:+.1%} "
      f"→ {'✅ 確認通過' if ok else '❌ 出局'}")

# dev 交易解剖 + PIT 抽樣
res_dev = run("2019-01-02", "2025-06-30", w=W5, seed_check=True)
s = metrics.summarize(res_dev.nav, res_dev.trades, None)
print(f"\ndev 解剖:trades {s['n_trades']} | win {s['win_rate']:.0%} | "
      f"avgW {s['avg_win']:+.1%}/avgL {s['avg_loss']:+.1%} | PF {s['profit_factor']:.2f} | "
      f"medHold {s['med_days_held']:.0f}d | turnover {s['turnover_ann']:.1f}x")
top = res_dev.trades.sort("ret_net", descending=True).head(5)
print("最大 5 筆:", [(r["company_code"], f"{r['ret_net']:+.0%}") for r in top.to_dicts()])
tot_pnl = (res_dev.trades["ret_net"] * res_dev.trades["cost"]).sum()
top_pnl = (top["ret_net"] * top["cost"]).sum()
print(f"前 5 筆貢獻佔比:{top_pnl/tot_pnl:.0%}")
yt = metrics.yearly_table(res_dev.nav)
print("dev 逐年:", "  ".join(f"{y}:{v*100:+.0f}%" for y, v in zip(yt["year"], yt["ret"])))
