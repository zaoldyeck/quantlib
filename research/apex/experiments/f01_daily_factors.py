"""F01 — daily 單因子廣篩(預註冊見 ledger/batches.md)。

24 因子 × 3 horizons 的截面 IC + decile spread,dev 窗 2012-2023。
Run: uv run --project research python -m research.apex.experiments.f01_daily_factors
"""
from __future__ import annotations

import time

import polars as pl

from research.apex import data, factors

DEV_START, DEV_END = "2012-01-02", "2023-12-29"
BATCH = "F01"
C = "company_code"


def win(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(
        pl.col("date").is_between(
            pl.lit(DEV_START).str.to_date(), pl.lit(DEV_END).str.to_date()
        )
    )


t0 = time.time()
con = data.connect()
panel = data.common_stocks(data.load_panel(con, DEV_START, DEV_END, warmup_days=420))
elig = win(data.eligibility(panel))
fwd = factors.forward_returns(panel)

# ── tech 因子(單一 panel 派生)────────────────────────────────────────
p = (
    panel.sort([C, "date"])
    .with_columns(
        [
            (pl.col("close") / pl.col("close").shift(1) - 1).over(C).alias("ret"),
            pl.col("close").shift(1).over(C).alias("prev_c"),
        ]
    )
    .with_columns(
        pl.max_horizontal(
            pl.col("high") - pl.col("low"),
            (pl.col("high") - pl.col("prev_c")).abs(),
            (pl.col("low") - pl.col("prev_c")).abs(),
        ).alias("tr")
    )
)

tech = (
    p.with_columns(
        [
            (pl.col("close").shift(5) / pl.col("close").shift(126) - 1).over(C).alias("mom_126_5"),
            (pl.col("close").shift(5) / pl.col("close").shift(63) - 1).over(C).alias("mom_63_5"),
            (pl.col("close").shift(21) / pl.col("close").shift(252) - 1).over(C).alias("mom_252_21"),
            (-(pl.col("close") / pl.col("close").shift(5) - 1)).over(C).alias("rev_5"),
            (-(pl.col("close") / pl.col("close").shift(21) - 1)).over(C).alias("rev_21"),
            (pl.col("close") / pl.col("close").rolling_max(252)).over(C).alias("high_52w"),
            (-pl.col("ret").rolling_std(60)).over(C).alias("lowvol_60"),
            (-(pl.col("tr").rolling_mean(20) / pl.col("tr").rolling_mean(120))).over(C).alias("vcp_atr"),
            (pl.col("close") / pl.col("close").shift(1).rolling_max(60)).over(C).alias("donchian_60"),
            (pl.col("trade_value").rolling_mean(5) / pl.col("trade_value").rolling_mean(60))
            .over(C).alias("val_surge_5_60"),
            ((pl.col("ret").abs() / (pl.col("trade_value") + 1.0)).rolling_mean(60) * 1e9)
            .over(C).alias("illiq_60"),
            pl.col("close").rolling_mean(20).over(C).alias("ma20"),
            pl.col("close").rolling_mean(60).over(C).alias("ma60"),
            pl.col("close").rolling_mean(120).over(C).alias("ma120"),
            pl.col("ret").rolling_std(60).over(C).alias("sigma60"),
            pl.when(pl.col("ret") > 0).then(pl.col("volume"))
            .when(pl.col("ret") < 0).then(-pl.col("volume"))
            .otherwise(0).cast(pl.Float64).alias("signed_vol"),
        ]
    )
    .with_columns(
        [
            (
                (pl.col("close") > pl.col("ma20")).cast(pl.Int8)
                + (pl.col("ma20") > pl.col("ma60")).cast(pl.Int8)
                + (pl.col("ma60") > pl.col("ma120")).cast(pl.Int8)
            ).cast(pl.Float64).alias("ma_align"),
            (pl.col("signed_vol").rolling_sum(20) / pl.col("volume").cast(pl.Float64).rolling_sum(20))
            .over(C).alias("ofi_20"),
            (1.0 / (1.0 + (-1.702 * pl.col("ret") / pl.col("sigma60")).exp())).alias("buy_frac"),
        ]
    )
    .with_columns(
        ((2 * pl.col("buy_frac") - 1).abs().rolling_mean(60)).over(C).alias("vpin_60")
    )
)

# ── flow 因子 ───────────────────────────────────────────────────────────
vol = panel.select(["date", C, "volume"])

fl = (
    data.load_flows(con, DEV_START, DEV_END)
    .join(vol, on=["date", C], how="inner")
    .sort([C, "date"])
    .with_columns(pl.col("volume").cast(pl.Float64).alias("vf"))
    .with_columns(
        [
            (pl.col("foreign_diff").cast(pl.Float64).rolling_sum(20) / pl.col("vf").rolling_sum(20))
            .over(C).alias("frn_20"),
            (pl.col("foreign_diff").cast(pl.Float64).rolling_sum(60) / pl.col("vf").rolling_sum(60))
            .over(C).alias("frn_60"),
            (pl.col("trust_diff").cast(pl.Float64).rolling_sum(20) / pl.col("vf").rolling_sum(20))
            .over(C).alias("trust_20"),
            (pl.col("trust_diff").cast(pl.Float64).rolling_sum(60) / pl.col("vf").rolling_sum(60))
            .over(C).alias("trust_60"),
        ]
    )
)

fh = (
    data.load_foreign_holding(con, DEV_START, DEV_END)
    .sort([C, "date"])
    .with_columns(
        (pl.col("foreign_held_ratio") - pl.col("foreign_held_ratio").shift(20))
        .over(C).alias("fh_chg_20")
    )
)

mg = (
    data.load_margin(con, DEV_START, DEV_END)
    .join(vol, on=["date", C], how="inner")
    .sort([C, "date"])
    .with_columns(
        (
            -(
                (pl.col("margin_balance") - pl.col("margin_balance").shift(20)).cast(pl.Float64)
                * 1000.0
            )
            / pl.col("volume").cast(pl.Float64).rolling_sum(20)
        )
        .over(C).alias("mgn_neg_chg_20")
    )
)

sb = (
    data.load_sbl(con, DEV_START, DEV_END)
    .join(vol, on=["date", C], how="inner")
    .sort([C, "date"])
    .with_columns(
        (
            (pl.col("daily_balance") - pl.col("daily_balance").shift(20)).cast(pl.Float64)
            / pl.col("volume").cast(pl.Float64).rolling_sum(20)
        )
        .over(C).alias("sbl_chg_20")
    )
)

# ── value 因子 ──────────────────────────────────────────────────────────
va = (
    data.load_valuation(con, DEV_START, DEV_END)
    .sort([C, "date"])
    .with_columns(
        [
            (-(pl.col("pbr") / pl.col("pbr").rolling_median(1260, min_samples=756)))
            .over(C).alias("pbr_rel_5y"),
            pl.when(pl.col("per") > 0).then(1.0 / pl.col("per")).otherwise(None).alias("ep"),
        ]
    )
)

print(f"data+signals ready in {time.time()-t0:.1f}s\n")

SPECS: list[tuple[str, pl.DataFrame, list[str]]] = [
    ("tech", tech, [
        "mom_126_5", "mom_63_5", "mom_252_21", "rev_5", "rev_21", "high_52w",
        "lowvol_60", "vcp_atr", "donchian_60", "ma_align", "val_surge_5_60",
        "illiq_60", "ofi_20", "vpin_60",
    ]),
    ("flow", fl, ["frn_20", "frn_60", "trust_20", "trust_60"]),
    ("flow", fh, ["fh_chg_20"]),
    ("flow", mg, ["mgn_neg_chg_20"]),
    ("flow", sb, ["sbl_chg_20"]),
    ("value", va, ["pbr_rel_5y", "dy", "ep"]),
]

for family, frame, names in SPECS:
    for name in names:
        fac = win(frame.select(["date", C, pl.col(name).alias("value")]))
        r = factors.evaluate_factor(name, fac, fwd, elig, family=family, batch=BATCH)
        print(factors.fmt_factor(r))

print(f"\ntotal {time.time()-t0:.1f}s")
