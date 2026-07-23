# transcript 逐字復原(零改動)。
#
# 來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T08:21:48.652Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/src/quantlib/apex/experiments/r05_winner_ind.py)
# 涵蓋 trials(9):r03d_n8, r05a_n8_extend, r05b_n20_extend, r05c_n8_ind, r05d_n20_ind, r05e_n8_geo, r05f_n8_streak, r05g_n8_ind_extend, r05h_n12_ind
"""R05 — 贏家展期 × 產業動能軸 × 幾何集中 × streak(8 trials;預註冊見 batches.md)。

Run: uv run --project . python -m quantlib.apex.experiments.r05_winner_ind
"""
from __future__ import annotations

import time
from datetime import date as Date

import polars as pl

from quantlib.apex import data, metrics
from quantlib.apex.assemble import blend_score, build_features, entries_and_flags, run_trial
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec

C = "company_code"
DS, DE = "2019-01-02", "2025-06-30"
BATCH = "R05"
WINDOW = f"{DS}..{DE}"
START = Date.fromisoformat(DS)
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W4 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5}

t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DS, DE)
bench = data.benchmark_nav(con, DS, DE)

# ── 產業動能軸(PIT 產業 60 日等權報酬)──────────────────────────────
tax = con.sql(
    "SELECT company_code, effective_date, industry FROM industry_taxonomy_pit "
    "WHERE industry IS NOT NULL ORDER BY effective_date"
).pl()
rets = (panel.sort([C, "date"])
        .with_columns((pl.col("close") / pl.col("close").shift(1) - 1).over(C).alias("ret"))
        .select(["date", C, "ret"]).drop_nulls())
r2 = (rets.sort("date")
      .join_asof(tax.sort("effective_date"), left_on="date", right_on="effective_date",
                 by=C, strategy="backward")
      .drop_nulls(subset=["industry"]))
ind_mom = (r2.group_by(["date", "industry"]).agg(pl.col("ret").mean())
           .sort(["industry", "date"])
           .with_columns(pl.col("ret").rolling_mean(60).over("industry").alias("ind_mom"))
           .drop_nulls(subset=["ind_mom"])
           .select(["date", "industry", "ind_mom"]))
stock_ind = r2.select(["date", C, "industry"])
ind_axis = stock_ind.join(ind_mom, on=["date", "industry"], how="inner").select(
    ["date", C, "ind_mom"])
feat = feat.join(ind_axis, on=["date", C], how="left")

# ── 營收連續加速 streak(月頻 → PIT snap)────────────────────────────
rev = (data.load_monthly_revenue(con, DE)
       .sort([C, "year", "month"])
       .with_columns(
           [
               pl.date(pl.col("year") + pl.col("month") // 12, pl.col("month") % 12 + 1, 10)
               .alias("avail"),
               (pl.col("monthly_revenue_yoy").rolling_mean(3)
                - pl.col("monthly_revenue_yoy").rolling_mean(12)).over(C).alias("acc"),
           ]
       )
       .drop_nulls(subset=["acc"])
       .with_columns((pl.col("acc") <= 0).cast(pl.Int32).cum_sum().over(C).alias("run_id"))
       .with_columns(
           pl.when(pl.col("acc") > 0)
           .then(pl.int_range(pl.len()).over([C, "run_id"]) + 1)
           .otherwise(0)
           .clip(0, 6)
           .cast(pl.Float64)
           .alias("streak")
       )
       .select([C, "avail", "streak"])
       .sort("avail"))
feat = (feat.sort("date")
        .join_asof(rev, left_on="date", right_on="avail", by=C,
                   strategy="backward", tolerance="70d")
        .sort([C, "date"]))

# 展期:60 日新高者不掛 stale 旗
donch_hi = feat.filter(pl.col("donchian_60") > 1.0).select(["date", C])


def W_(df):
    return df.filter(pl.col("date") >= pl.lit(DS).str.to_date())


def go(name, *, topn, weights=None, geometric=False, extend=False, tstop=30, trail=0.25):
    w = weights or W4
    pool = feat.filter(pl.col("rev_fresh_days") <= 5)
    if geometric:
        cols = list(w)
        df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                        on=["date", C], how="semi").drop_nulls(subset=cols))
        for cond in GATE:
            df = df.filter(cond)
        expr = None
        for c_, wt in w.items():
            term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
            expr = term if expr is None else expr * term
        sc = W_(df.with_columns(expr.alias("score")).select(["date", C, "score"]))
    else:
        sc = W_(blend_score(pool, elig, w, require=GATE))
    e, _ = entries_and_flags(sc, topn, 10**9)
    f = W_(feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]))
    if extend:
        f = f.join(donch_hi, on=["date", C], how="anti")
    return run_trial(
        name=name, hypothesis="R05 槓桿", family="r05", batch=BATCH,
        panel=panel, entries=e, exit_flags=f, bench=bench, window=WINDOW, start=START,
        config={"topn": topn, "weights": {k: v for k, v in w.items()}, "geo": geometric,
                "extend": extend, "tstop": tstop},
        port_spec=PortSpec(n_slots=topn, max_new_per_day=5),
        exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop),
        verbose=False)


WIND = dict(W4) | {"ind_mom": 0.5}
WSTK = dict(W4) | {"streak": 0.5}
runs = [
    go("r05a_n8_extend", topn=8, extend=True, tstop=60),
    go("r05b_n20_extend", topn=20, extend=True, tstop=60),
    go("r05c_n8_ind", topn=8, weights=WIND),
    go("r05d_n20_ind", topn=20, weights=WIND),
    go("r05e_n8_geo", topn=8, geometric=True),
    go("r05f_n8_streak", topn=8, weights=WSTK),
    go("r05g_n8_ind_extend", topn=8, weights=WIND, extend=True, tstop=60),
    go("r05h_n12_ind", topn=12, weights=WIND),
]
cmp = pl.DataFrame(
    [{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd", "calmar"]} for r in runs]
).sort("cagr", descending=True)
with pl.Config(tbl_rows=10, tbl_width_chars=110):
    print(cmp)
print("\n對照:r03d_n8 52.2/1.58/−38.6 | v3-n20 modern 42.9/1.75/−26.6")
print("晉級:n8系 CAGR≥55.2 或(Sharpe≥1.68∧CAGR≥52.2);n20系 CAGR≥45.9 或(Sharpe≥1.85∧CAGR≥42.9)")
print(f"total {time.time()-t0:.1f}s")

