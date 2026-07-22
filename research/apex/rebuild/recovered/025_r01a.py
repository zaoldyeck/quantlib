# transcript 逐字復原(零改動)。
#
# 來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T08:01:33.908Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/research/apex/experiments/r01_momentum_containers.py)
# 涵蓋 trials(16):r01a_52wh_top10, r01b_52wh_top20, r01c_52wh_t40, r01d_52wh_adv300, r01e_mom61_adv100, r01f_mom61_mega5, r01g_blend, r01h_52wh_cfo, r01i_52wh_revpos, r01j_v3_baseline, r01k_breakout_hold, r01l_frn_modern, 現代era_R3, 現代era_S, 現代era_r08a, 現代era_v6
"""R01 — 動能為主容器 × 現代 dev 窗(12 trials;預註冊見 ledger/batches.md R-LINE)。

Run: uv run --project research python -m research.apex.experiments.r01_momentum_containers
"""
from __future__ import annotations

import time
from datetime import date as Date

import polars as pl

from research.apex import data, metrics
from research.apex.assemble import blend_score, build_features, entries_and_flags, run_trial
from research.apex.engine import ExecSpec, ExitSpec, PortSpec

C = "company_code"
DEV_START, DEV_END = "2019-01-02", "2025-06-30"
BATCH = "R01"
WINDOW = f"{DEV_START}..{DEV_END}"
START = Date.fromisoformat(DEV_START)
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W4 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5}

t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DEV_START, DEV_END)
bench = data.benchmark_nav(con, DEV_START, DEV_END)

mom61 = (panel.sort([C, "date"])
         .with_columns((pl.col("close").shift(21) / pl.col("close").shift(126) - 1)
                       .over(C).alias("mom_6_1"))
         .select(["date", C, "mom_6_1"]))
adv = data.eligibility(panel).select(["date", C, "adv20"])
feat = (feat.join(mom61, on=["date", C], how="left")
        .join(adv, on=["date", C], how="left")
        .with_columns(pl.col("adv20").rank("ordinal", descending=True).over("date").alias("adv_rk")))

td = panel.select(pl.col("date").unique().sort()).with_columns(
    [pl.col("date").dt.month().alias("m"), pl.col("date").dt.year().alias("y")])
DAY1 = td.group_by(["y", "m"]).agg(pl.col("date").min()).get_column("date")


def W_(df):
    return df.filter(pl.col("date") >= pl.lit(DEV_START).str.to_date())


def go(name, hypothesis, family, entries, flags, *, topn, trail=0.30, tstop=None, max_new=None):
    return run_trial(
        name=name, hypothesis=hypothesis, family=family, batch=BATCH,
        panel=panel, entries=entries, exit_flags=flags, bench=bench,
        window=WINDOW, start=START,
        config={"topn": topn, "trail": trail, "time_stop": tstop},
        port_spec=PortSpec(n_slots=topn, max_new_per_day=max_new or topn),
        exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop),
        verbose=False,
    )


def monthly(score_expr_cols: dict[str, float], *, topn, require=None, extra=None):
    """月頻容器:每月首個交易日 rank top-N、requal 出場。"""
    sc = W_(blend_score(feat, elig, score_expr_cols, require=require)).filter(
        pl.col("date").is_in(DAY1.implode()))
    if extra is not None:
        sc = sc.join(extra, on=["date", C], how="semi")
    r = sc.with_columns(pl.col("score").rank("ordinal", descending=True).over("date").alias("rk"))
    e = r.filter(pl.col("rk") <= topn).select(["date", C, "score"])
    f = (panel.select(["date", C]).filter(pl.col("date").is_in(DAY1.implode()))
         .join(e.select(["date", C]), on=["date", C], how="anti"))
    return e, f


runs = []
e, f = monthly({"high_52w": 1.0}, topn=10)
runs.append(go("r01a_52wh_top10", "52wH 月頻 top10", "mom_monthly", e, f, topn=10))
e2, f2 = monthly({"high_52w": 1.0}, topn=20)
runs.append(go("r01b_52wh_top20", "52wH 月頻 top20", "mom_monthly", e2, f2, topn=20))
runs.append(go("r01c_52wh_t40", "52wH top10 寬 trail", "mom_monthly", e, f, topn=10, trail=0.40))

adv300 = W_(feat.filter(pl.col("adv_rk") <= 300).select(["date", C]))
e, f = monthly({"high_52w": 1.0}, topn=10, extra=adv300)
runs.append(go("r01d_52wh_adv300", "52wH ∩ ADV前300", "mom_monthly", e, f, topn=10))

adv100 = W_(feat.filter(pl.col("adv_rk") <= 100).select(["date", C]))
e, f = monthly({"mom_6_1": 1.0}, topn=10, extra=adv100)
runs.append(go("r01e_mom61_adv100", "mom6-1 ADV前100 top10", "mom_monthly", e, f, topn=10))

adv50 = W_(feat.filter(pl.col("adv_rk") <= 50).select(["date", C]))
e, f = monthly({"mom_6_1": 1.0}, topn=5, extra=adv50)
runs.append(go("r01f_mom61_mega5", "mom6-1 mega top5", "mom_monthly", e, f, topn=5, trail=0.25))

e, f = monthly({"high_52w": 1.0, "mom_6_1": 1.0}, topn=10)
runs.append(go("r01g_blend", "52wH+mom6-1 blend top10", "mom_monthly", e, f, topn=10))

e, f = monthly({"high_52w": 1.0}, topn=10, require=GATE)
runs.append(go("r01h_52wh_cfo", "52wH + cfo 閘", "mom_quality", e, f, topn=10))

e, f = monthly({"high_52w": 1.0}, topn=10, require=[pl.col("rev_yoy_accel") > 0])
runs.append(go("r01i_52wh_revpos", "52wH + 營收加速>0 閘", "mom_marriage", e, f, topn=10))

# j: v3 冠軍同窗對照
pool = feat.filter(pl.col("rev_fresh_days") <= 5)
sc = W_(blend_score(pool, elig, W4, require=GATE))
ev3, _ = entries_and_flags(sc, 20, 10**9)
fv3 = W_(feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]))
runs.append(go("r01j_v3_baseline", "v3 冠軍同窗對照", "baseline", ev3, fv3,
               topn=20, trail=0.25, tstop=30, max_new=5))

# k: 突破持有事件
brk = W_(feat.filter((pl.col("donchian_60") > 1.0) & (pl.col("high_52w") >= 0.95))
         .join(elig.filter(pl.col("eligible")).select(["date", C]), on=["date", C], how="semi")
         .select(["date", C, pl.col("mom_126_5").alias("score")]).drop_nulls())
runs.append(go("r01k_breakout_hold", "突破持有(donchian∩52wH)", "trend_event", brk, None,
               topn=10, trail=0.35, tstop=120, max_new=3))

# l: frn_60 月頻(現代era)
e, f = monthly({"frn_60": 1.0}, topn=20)
runs.append(go("r01l_frn_modern", "外資流月頻(現代era重測)", "flow", e, f, topn=20, trail=0.25))

cmp = pl.DataFrame(
    [{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd", "calmar", "turnover_ann"]} for r in runs]
).sort("cagr", descending=True)
with pl.Config(tbl_rows=14, tbl_width_chars=120):
    print(cmp)
b = bench.sort("date")
yrs = (b["date"][-1] - b["date"][0]).days / 365.25
print(f"\n0050 同窗 CAGR:{(b['nav'][-1]/b['nav'][0])**(1/yrs)-1:+.1%}")
print("晉級門檻:CAGR≥30 ∧ Sharpe≥1.2 ∧ MDD≥−40 ∧ 年段正≥5/7;top-2 進 OOS")
print(f"total {time.time()-t0:.1f}s")

