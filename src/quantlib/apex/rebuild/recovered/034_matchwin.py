"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T08:31:47.454Z(工具 Bash)
涵蓋 trials(14):matchwin_r06e_vs_00631L, matchwin_t35_vs_00631L, r06e_geo_n8_t30, r07a_exfin, r07b_excon, r07c_exboth, r07d_revlevel, r07e_dualgate, r07f_t35, r07g_mome75, r07h_fresh6, 正2全史同窗_R3, 正2全史同窗_r08a, 正2全史同窗_v6
"""
"""R07 — 語義排除 × 軸擴 × 微調(8 trials)+ r06e 正2 全史同窗對決。"""
import time
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger, metrics
from quantlib.apex.assemble import blend_score, build_features, entries_and_flags, run_trial
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
DS, DE = "2019-01-02", "2025-06-30"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W4 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5}

t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DS, DE)
bench = data.benchmark_nav(con, DS, DE)

# PIT 產業旗標(金融/特殊類)與營建
tax = con.sql("""
  SELECT company_code, effective_date, is_financial,
         (industry LIKE '%營造%' OR industry LIKE '%營建%' OR industry LIKE '%建材%') AS is_construction
  FROM industry_taxonomy_pit ORDER BY effective_date
""").pl()
feat = (feat.sort("date")
        .join_asof(tax.sort("effective_date"), left_on="date", right_on="effective_date",
                   by=C, strategy="backward")
        .sort([C, "date"])
        .with_columns([pl.col("is_financial").fill_null(False),
                       pl.col("is_construction").fill_null(False)]))

def W_(df):
    return df.filter(pl.col("date") >= pl.lit(DS).str.to_date())

def go(name, *, topn=8, trail=0.30, weights=None, exps=None, require=None,
       pool_filter=None, fresh=5):
    w = exps or weights or W4
    pool = feat.filter(pl.col("rev_fresh_days") <= fresh)
    if pool_filter is not None:
        pool = pool.filter(pool_filter)
    req = GATE if require is None else require
    cols = list(w)
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=cols))
    for cond in req:
        df = df.filter(cond)
    expr = None
    for c_, wt in w.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = W_(df.with_columns(expr.alias("score")).select(["date", C, "score"]))
    e, _ = entries_and_flags(sc, topn, 10**9)
    f = W_(feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]))
    return run_trial(
        name=name, hypothesis="R07 語義排除×軸擴", family="r07", batch="R07",
        panel=panel, entries=e, exit_flags=f, bench=bench, window=f"{DS}..{DE}",
        start=Date.fromisoformat(DS),
        config={"topn": topn, "trail": trail, "w": {k: v for k, v in w.items()}},
        port_spec=PortSpec(n_slots=topn, max_new_per_day=5),
        exit_spec=ExitSpec(trailing_stop=trail, time_stop=30), verbose=False)

FIN = ~pl.col("is_financial")
CON = ~pl.col("is_construction")
runs = [
    go("r07a_exfin", pool_filter=FIN),
    go("r07b_excon", pool_filter=CON),
    go("r07c_exboth", pool_filter=FIN & CON),
    go("r07d_revlevel", exps=dict(W4) | {"rev_yoy": 0.5}),
    go("r07e_dualgate", require=GATE + [pl.col("f_score_raw").is_null() | (pl.col("f_score_raw") >= 5)] if "f_score_raw" in feat.columns else GATE),
    go("r07f_t35", trail=0.35),
    go("r07g_mome75", exps=dict(W4) | {"mom_126_5": 0.75}),
    go("r07h_fresh6", fresh=6),
]
cmp = pl.DataFrame(
    [{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd"]} for r in runs]
).sort("cagr", descending=True)
with pl.Config(tbl_rows=10, tbl_width_chars=105):
    print(cmp)
print("對照 r06e:55.9/1.64/−38.0 | 晉級:CAGR≥56.9 或(Sharpe≥1.72∧CAGR≥55.9)\n")

# 正2 全史同窗對決(2014-11-03 → 2026-07-07)
def run_geo_window(ws, we):
    p2, f2, e2 = build_features(con, ws, we)
    pool = f2.filter(pl.col("rev_fresh_days") <= 5)
    cols = list(W4)
    df = (pool.join(e2.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=cols))
    for cond in GATE:
        df = df.filter(cond)
    expr = None
    for c_, wt in W4.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    e, _ = entries_and_flags(sc, 8, 10**9)
    fl = f2.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    return simulate(p2, e, exit_flags=fl, exec_spec=ExecSpec(),
                    port_spec=PortSpec(n_slots=8, max_new_per_day=5),
                    exit_spec=ExitSpec(trailing_stop=0.30, time_stop=30),
                    start=Date.fromisoformat(ws))

res = run_geo_window("2014-11-03", "2026-07-07")
s = metrics.perf_stats(res.nav)
ledger.log_trial(family="fullspan", name="matchwin_r06e_vs_00631L",
                 hypothesis="正2 全史同窗對決", config={"geo": True, "topn": 8, "trail": 0.30},
                 window="2014-11-03..2026-07-07", metrics=s, batch="R06-OOS", curve=res.nav)
print(f"r06e 正2全史同窗(2014-11→2026-07):CAGR {s['cagr']:+.1%} | Sharpe {s['sharpe']:.2f} | "
      f"MDD {s['mdd']:+.1%} | {s['final_nav_ratio']:.0f}x")
print(f"正2 同窗:+37.7% / 1.08 / −55.1% / 42x → {'🏆 全史同窗超越' if s['cagr'] > 0.377 else '❌'}")
print(f"total {time.time()-t0:.1f}s")
