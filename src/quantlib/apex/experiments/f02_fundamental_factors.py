"""F02 — 月營收 + 品質單因子廣篩(PIT 對齊;預註冊見 ledger/batches.md)。

Run: uv run --project . python -m quantlib.apex.experiments.f02_fundamental_factors
"""
from __future__ import annotations

import time

import polars as pl

from quantlib.apex import data, factors

DEV_START, DEV_END = "2012-01-02", "2023-12-29"
BATCH = "F02"
C = "company_code"


def win(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(
        pl.col("date").is_between(
            pl.lit(DEV_START).str.to_date(), pl.lit(DEV_END).str.to_date()
        )
    )


def to_daily(event: pl.DataFrame, grid: pl.DataFrame, value_col: str, tolerance: str) -> pl.DataFrame:
    """事件 frame (company_code, avail, value) → 每日 as-of frame (date, code, value)。

    tolerance 過期即失效(公司停止揭露時舊值不得無限延用)。
    """
    # join_asof 契約:兩側按 asof key「全域」排序;禁止對 (code, key) 排序的欄
    # set_sorted(謊 flag 會讓下游 over/group_by/filter 走有序快徑而損壞)。
    ev = (
        event.drop_nulls(subset=[value_col])
        .filter(pl.col(value_col).is_finite())
        .sort("avail")
    )
    return (
        grid.sort("date")
        .join_asof(
            ev.select([C, "avail", value_col]),
            left_on="date", right_on="avail", by=C,
            strategy="backward", tolerance=tolerance,
        )
        .select(["date", C, pl.col(value_col).alias("value")])
        .drop_nulls(subset=["value"])
    )


t0 = time.time()
con = data.connect()
panel = data.common_stocks(data.load_panel(con, DEV_START, DEV_END, warmup_days=420))
elig = win(data.eligibility(panel))
fwd = factors.forward_returns(panel)
grid = win(panel.select(["date", C]))
trading_days = panel.select(pl.col("date").unique().sort()).get_column("date")


def _snap(df: pl.DataFrame, date_col: str) -> pl.DataFrame:
    """把 date_col 對齊到 ≥ 它的首個交易日(forward asof)。產出欄 avail。"""
    td = pl.DataFrame({"td": trading_days}).sort("td")
    return (
        df.sort(date_col)
        .join_asof(td, left_on=date_col, right_on="td", strategy="forward")
        .rename({"td": "avail"})
        .drop_nulls(subset=["avail"])
    )


# ── 月營收因子 ──────────────────────────────────────────────────────────
rev = data.load_monthly_revenue(con, DEV_END)
rev = (
    rev.sort([C, "year", "month"])
    .with_columns(
        [
            pl.date(
                pl.col("year") + pl.col("month") // 12,
                pl.col("month") % 12 + 1,
                10,
            ).alias("deadline"),
            pl.col("monthly_revenue_yoy").alias("rev_yoy"),
        ]
    )
    .with_columns(
        [
            (
                (pl.col("monthly_revenue").rolling_sum(3)
                 / pl.col("monthly_revenue").rolling_sum(3).shift(12) - 1) * 100
            ).over(C).alias("rev_3m_yoy"),
            (
                pl.col("rev_yoy").rolling_mean(3) - pl.col("rev_yoy").rolling_mean(12)
            ).over(C).alias("rev_yoy_accel"),
            (pl.col("rev_yoy") - pl.col("rev_yoy").shift(1)).over(C).alias("rev_yoy_chg"),
            (
                pl.col("monthly_revenue").rolling_sum(12)
                / pl.col("monthly_revenue").rolling_sum(12).cum_max()
            ).over(C).alias("rev_ttm_high"),
        ]
    )
)
rev = _snap(rev, "deadline")

# ── 品質因子(raw_quarterly + 法定期限)─────────────────────────────────
rq = pl.read_parquet(data.RAW_QUARTERLY_PARQUET)
rq = (
    rq.sort([C, "year", "quarter"])
    .with_columns(
        pl.when(pl.col("quarter") == 1).then(pl.date(pl.col("year"), 5, 15))
        .when(pl.col("quarter") == 2).then(pl.date(pl.col("year"), 8, 14))
        .when(pl.col("quarter") == 3).then(pl.date(pl.col("year"), 11, 14))
        .otherwise(pl.date(pl.col("year") + 1, 3, 31))
        .alias("deadline")
    )
    .with_columns(
        [
            pl.col("f_score_raw").cast(pl.Float64),
            (
                (pl.col("ni_ttm") - pl.col("ni_ttm").shift(4)) / pl.col("total_assets")
            ).over(C).alias("ni_mom_ta"),
        ]
    )
)
rq = _snap(rq, "deadline")

print(f"data ready in {time.time()-t0:.1f}s\n")

SPECS: list[tuple[str, pl.DataFrame, str, list[str]]] = [
    ("revenue", rev, "70d", ["rev_yoy", "rev_3m_yoy", "rev_yoy_accel", "rev_yoy_chg", "rev_ttm_high"]),
    ("quality", rq, "150d", ["f_score_raw", "roa_ttm", "d_gross_margin_yoy", "cfo_ni_ratio_ttm", "ni_mom_ta"]),
]

for family, frame, tol, names in SPECS:
    for name in names:
        fac = to_daily(frame, grid, name, tolerance=tol)
        r = factors.evaluate_factor(name, fac, fwd, elig, family=family, batch=BATCH)
        print(factors.fmt_factor(r))

print(f"\ntotal {time.time()-t0:.1f}s")
