"""B15 — 最後一擊探索批(8 trials × 8 家族;預註冊見 ledger/batches.md)。

Run: uv run --project . python -m quantlib.apex.experiments.b15_last_push
"""
from __future__ import annotations

import time
from datetime import date as Date

import polars as pl

from quantlib.apex import data
from quantlib.apex.assemble import blend_score, build_features, entries_and_flags, run_trial
from quantlib.apex.engine import ExitSpec, PortSpec

C = "company_code"
DEV_START, DEV_END = "2012-01-02", "2023-12-29"
BATCH = "B15"
WINDOW = f"{DEV_START}..{DEV_END}"
START = Date.fromisoformat(DEV_START)
W4 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5}
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]

t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DEV_START, DEV_END)
bench = data.benchmark_nav(con, DEV_START, DEV_END)

# rev_mom_sa:月營收 vs 同月 3 年均(PIT 對齊)
rev_sa = (
    data.load_monthly_revenue(con, DEV_END)
    .sort([C, "year", "month"])
    .with_columns(
        [
            pl.date(pl.col("year") + pl.col("month") // 12, pl.col("month") % 12 + 1, 10)
            .alias("avail"),
            (
                pl.col("monthly_revenue")
                / (
                    (pl.col("monthly_revenue").shift(12) + pl.col("monthly_revenue").shift(24)
                     + pl.col("monthly_revenue").shift(36)) / 3.0
                )
                - 1
            ).over(C).alias("rev_mom_sa"),
        ]
    )
    .select([C, "avail", "rev_mom_sa"])
    .drop_nulls()
    .sort("avail")
)
feat = (feat.sort("date")
        .join_asof(rev_sa, left_on="date", right_on="avail", by=C,
                   strategy="backward", tolerance="70d")
        .sort([C, "date"]))

# NI>0(季頻 PIT;raw_quarterly ni_ttm)
rq = (
    pl.read_parquet(data.RAW_QUARTERLY_PARQUET)
    .sort([C, "year", "quarter"])
    .with_columns(
        pl.when(pl.col("quarter") == 1).then(pl.date(pl.col("year"), 5, 15))
        .when(pl.col("quarter") == 2).then(pl.date(pl.col("year"), 8, 14))
        .when(pl.col("quarter") == 3).then(pl.date(pl.col("year"), 11, 14))
        .otherwise(pl.date(pl.col("year") + 1, 3, 31)).alias("q_avail")
    )
    .select([C, "q_avail", pl.col("ni_ttm")])
    .drop_nulls().sort("q_avail")
)
feat = (feat.sort("date")
        .join_asof(rq, left_on="date", right_on="q_avail", by=C,
                   strategy="backward", tolerance="150d")
        .sort([C, "date"]))

# close_pos_60
p60 = (
    panel.sort([C, "date"])
    .with_columns(
        pl.when(pl.col("high") > pl.col("low"))
        .then((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low")))
        .otherwise(None).rolling_mean(60, min_samples=30).over(C).alias("close_pos_60")
    )
    .select(["date", C, "close_pos_60"])
)
feat = feat.join(p60, on=["date", C], how="left")


def W_(df):
    return df.filter(pl.col("date") >= pl.lit(DEV_START).str.to_date())


def go(name, hypothesis, family, entries, flags, *, trail=0.25, tstop=30,
       profit_take=None, max_new=5, topn=20):
    return run_trial(
        name=name, hypothesis=hypothesis, family=family, batch=BATCH,
        panel=panel, entries=entries, exit_flags=flags, bench=bench,
        window=WINDOW, start=START,
        config={"scaffold": "v3", "trail": trail, "profit_take": profit_take},
        port_spec=PortSpec(n_slots=topn, max_new_per_day=max_new),
        exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop, profit_take=profit_take),
        verbose=False,
    )


def revcycle_entries(weights, *, require=None, fresh=5, pool_filter=None, topn=20,
                     geometric=False, two_stage=None):
    pool = feat.filter(pl.col("rev_fresh_days") <= fresh)
    if pool_filter is not None:
        pool = pool.filter(pool_filter)
    req = GATE if require is None else require
    if geometric:
        cols = list(weights)
        df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                        on=["date", C], how="semi").drop_nulls(subset=cols))
        for cond in req:
            df = df.filter(cond)
        expr = None
        for c, w in weights.items():
            term = ((pl.col(c).rank() / pl.len()).over("date")) ** w
            expr = term if expr is None else expr * term
        sc = W_(df.with_columns(expr.alias("score")).select(["date", C, "score"]))
    elif two_stage is not None:
        first_col, first_k, second = two_stage
        df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                        on=["date", C], how="semi")
              .drop_nulls(subset=[first_col] + list(second)))
        for cond in req:
            df = df.filter(cond)
        df = df.with_columns(
            pl.col(first_col).rank("ordinal", descending=True).over("date").alias("_r1")
        ).filter(pl.col("_r1") <= first_k)
        sc = W_(df.with_columns(
            sum((pl.col(c).rank() / pl.len()).over("date") * w for c, w in second.items())
            .alias("score")
        ).select(["date", C, "score"]))
    else:
        sc = W_(blend_score(pool, elig, weights, require=req))
    flags = W_(feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]))
    e, _ = entries_and_flags(sc, topn, 10**9)
    return e, flags


runs = []
wa = dict(W4)
wa.pop("rev_yoy_accel")
e, f = revcycle_entries({"rev_mom_sa": 1.0, **wa})
runs.append(go("b15a_mom_sa", "季調營收動能換 accel", "factor_def", e, f))

e, f = revcycle_entries({"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0,
                         "mom_126_5": 0.5}, geometric=True)
runs.append(go("b15b_geometric", "rank 幾何乘積分數", "score_comp", e, f))

e, f = revcycle_entries(W4, require=GATE + [pl.col("ni_ttm") > 0])
runs.append(go("b15c_dualgate_ni", "cfo ∧ NI>0 雙閘", "gate_design", e, f))

w60 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_60": 1.0, "mom_126_5": 0.5}
e, f = revcycle_entries(w60)
runs.append(go("b15d_pos60", "吸收窗 20→60", "window", e, f))

e, f = revcycle_entries(W4, two_stage=("rev_yoy_accel", 40,
                                       {"high_52w": 1.0, "close_pos_20": 1.0}))
runs.append(go("b15e_twostage", "accel top40 → 技術排序", "sort_struct", e, f))

e, f = revcycle_entries(W4)
runs.append(go("b15f_profit100", "profit_take +100%", "exit_design", e, f, profit_take=1.0))
runs.append(go("b15g_maxnew3", "max_new 3", "throttle", e, f, max_new=3))

e, f = revcycle_entries(W4, pool_filter=None)
px15 = panel.filter(pl.col("raw_close") >= 15).select(["date", C])
e15 = e.join(px15, on=["date", C], how="semi")
runs.append(go("b15h_price15", "價 ≥ 15 資格", "eligibility", e15, f))

cmp = pl.DataFrame(
    [{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd", "calmar"]} for r in runs]
).sort("cagr", descending=True)
with pl.Config(tbl_rows=10, tbl_width_chars=110):
    print(cmp)
print("\nfrontier 目標:v3 dev 33.4%/1.66/−26.8(charter 規則 a/b/c)")
print(f"total {time.time()-t0:.1f}s")
