"""apex Phase 2 組裝層 — base features 一次算、多 trial 重用(純函式)。

pipeline:build_features → blend_score(rank-pct 加權)→ entries/exit_flags → run_trial
"""
from __future__ import annotations

import polars as pl

from quantlib.apex import data, ledger, metrics
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"

#: build_features 產出的可組裝欄位
FEATURE_COLS = [
    "high_52w", "mom_126_5", "hvn_dist", "range_pos_60", "updays_20", "fvg_20",
    "close_pos_20", "donchian_60", "rev_yoy", "rev_yoy_accel", "rev_fresh_days",
    "cfo_ni_ratio_ttm", "frn_60", "dy", "lowvol_60",
]


def apply_avail_override(
    rev: pl.DataFrame, override: pl.DataFrame | None
) -> pl.DataFrame:
    """以實際首見日逐筆覆蓋月營收 `avail`;無 override 者維持原值。

    override 欄位:(company_code, year, month, avail_date)。純函式。
    """
    if override is None or override.is_empty():
        return rev
    return (
        rev.join(
            override.select([C, "year", "month",
                             pl.col("avail_date").alias("_ov")]),
            on=[C, "year", "month"], how="left")
        .with_columns(pl.coalesce("_ov", "avail").alias("avail"))
        .drop("_ov")
    )


def build_features(
    con, start: str, end: str, warmup_days: int = 420,
    avail_override: pl.DataFrame | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """回傳 (panel, features, elig)。features = (date, code, FEATURE_COLS...)決策日值。

    avail_override: (company_code, year, month, avail_date) — 逐筆覆蓋月營收
    生效日。live 決策傳入實際首見日(`src/quantlib/records/revenue_first_seen.parquet`,
    事件驅動:資料庫一有該公司新月報即生效,不等法定 10 日);回測不傳,
    維持保守「次月 10 日」語義(歷史無逐公司公告日資料,保守下界)。
    """
    panel = data.common_stocks(data.load_panel(con, start, end, warmup_days=warmup_days))
    elig = data.eligibility(panel)

    p = (
        panel.sort([C, "date"])
        .with_columns((pl.col("close") / pl.col("close").shift(1) - 1).over(C).alias("ret"))
        .with_columns(
            [
                (pl.col("close") / pl.col("close").rolling_max(252)).over(C).alias("high_52w"),
                (pl.col("close").shift(5) / pl.col("close").shift(126) - 1).over(C).alias("mom_126_5"),
                (
                    pl.col("raw_close")
                    / (
                        pl.col("trade_value").cast(pl.Float64).rolling_sum(120)
                        / pl.col("volume").cast(pl.Float64).rolling_sum(120)
                    ).over(C)
                    - 1
                ).alias("hvn_dist"),
                (
                    (pl.col("close") - pl.col("low").rolling_min(60))
                    / (pl.col("high").rolling_max(60) - pl.col("low").rolling_min(60) + 1e-12)
                ).over(C).alias("range_pos_60"),
                ((pl.col("ret") > 0).cast(pl.Float64).rolling_mean(20)).over(C).alias("updays_20"),
                (
                    (pl.col("low") > pl.col("high").shift(2)).cast(pl.Float64).rolling_sum(20)
                ).over(C).alias("fvg_20"),
                pl.when(pl.col("high") > pl.col("low"))
                .then((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low")))
                .otherwise(None)
                .rolling_mean(20, min_samples=10)
                .over(C)
                .alias("close_pos_20"),
                (pl.col("close") / pl.col("close").shift(1).rolling_max(60)).over(C).alias("donchian_60"),
                (-pl.col("ret").rolling_std(60)).over(C).alias("lowvol_60"),
            ]
        )
        .select(["date", C, "high_52w", "mom_126_5", "hvn_dist", "range_pos_60",
                 "updays_20", "fvg_20", "close_pos_20", "donchian_60", "lowvol_60"])
    )

    # 月營收(PIT:次月 10 日起生效)
    rev = (
        data.load_monthly_revenue(con, end)
        .sort([C, "year", "month"])
        .with_columns(
            [
                pl.date(
                    pl.col("year") + pl.col("month") // 12, pl.col("month") % 12 + 1, 10
                ).alias("avail"),
                (
                    pl.col("monthly_revenue_yoy").rolling_mean(3)
                    - pl.col("monthly_revenue_yoy").rolling_mean(12)
                ).over(C).alias("rev_yoy_accel"),
                pl.col("monthly_revenue_yoy").alias("rev_yoy"),
            ]
        )
    )
    rev = (
        apply_avail_override(rev, avail_override)
        .select([C, "avail", "rev_yoy", "rev_yoy_accel"])
        .drop_nulls()
        .sort("avail")  # join_asof 契約:兩側都按 asof key 全域排序(不可對
    )                   # (code, date) 排序的欄 set_sorted——謊 flag 會讓下游
                        # over/group_by/filter 走有序快徑而全面損壞

    # 季報品質(法定期限生效)
    rq = (
        pl.read_parquet(data.RAW_QUARTERLY_PARQUET)
        .sort([C, "year", "quarter"])
        .with_columns(
            pl.when(pl.col("quarter") == 1).then(pl.date(pl.col("year"), 5, 15))
            .when(pl.col("quarter") == 2).then(pl.date(pl.col("year"), 8, 14))
            .when(pl.col("quarter") == 3).then(pl.date(pl.col("year"), 11, 14))
            .otherwise(pl.date(pl.col("year") + 1, 3, 31))
            .alias("q_avail")
        )
        .select([C, "q_avail", "cfo_ni_ratio_ttm"])
        .drop_nulls()
        .sort("q_avail")
    )

    # 外資 60 日流(F01 定義)
    fl = (
        data.load_flows(con, start, end)
        .join(panel.select(["date", C, "volume"]), on=["date", C], how="inner")
        .sort([C, "date"])
        .with_columns(
            (
                pl.col("foreign_diff").cast(pl.Float64).rolling_sum(60)
                / pl.col("volume").cast(pl.Float64).rolling_sum(60)
            ).over(C).alias("frn_60")
        )
        .select(["date", C, "frn_60"])
        .unique(subset=["date", C], keep="first")
    )

    va = (
        data.load_valuation(con, start, end, warmup_days=warmup_days)
        .select(["date", C, pl.col("dy")])
        .unique(subset=["date", C], keep="first")
    )

    feat = (
        p.sort("date")
        .join_asof(rev, left_on="date", right_on="avail", by=C,
                   strategy="backward", tolerance="70d")
        .join_asof(rq, left_on="date", right_on="q_avail", by=C,
                   strategy="backward", tolerance="150d")
        .join(fl, on=["date", C], how="left")
        .join(va, on=["date", C], how="left")
        .with_columns(
            (pl.col("date") - pl.col("avail")).dt.total_days().alias("rev_fresh_days")
        )
        .select(["date", C, *FEATURE_COLS])
        .sort([C, "date"])
    )
    return panel, feat, elig


def blend_score(
    feat: pl.DataFrame,
    elig: pl.DataFrame,
    weights: dict[str, float],
    *,
    require: list[pl.Expr] | None = None,
) -> pl.DataFrame:
    """rank-pct 加權組合分數:(date, code, score)。

    只計 eligible 股票;任一成分 null 的列剔除;require 為額外布林過濾條件。
    """
    cols = list(weights)
    df = (
        feat.join(
            elig.filter(pl.col("eligible")).select(["date", C]),
            on=["date", C], how="semi",
        )
        .drop_nulls(subset=cols)
    )
    for cond in require or []:
        df = df.filter(cond)
    total = sum(abs(w) for w in weights.values())
    return df.with_columns(
        sum(
            (pl.col(c).rank() / pl.len()).over("date") * (w / total)
            for c, w in weights.items()
        ).alias("score")
    ).select(["date", C, "score"])


def entries_and_flags(
    score: pl.DataFrame, topn: int, exit_rank: int
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """score → (entries top-N, exit_flags rank>exit_rank)。"""
    ranked = score.with_columns(
        pl.col("score").rank("ordinal", descending=True).over("date").alias("rk")
    )
    entries = ranked.filter(pl.col("rk") <= topn).select(["date", C, "score"])
    flags = ranked.filter(pl.col("rk") > exit_rank).select(["date", C])
    return entries, flags


def run_trial(
    *,
    name: str,
    hypothesis: str,
    family: str,
    batch: str,
    panel: pl.DataFrame,
    entries: pl.DataFrame,
    exit_flags: pl.DataFrame | None,
    bench: pl.DataFrame,
    window: str,
    start,
    config: dict,
    exec_spec: ExecSpec = ExecSpec(),
    port_spec: PortSpec = PortSpec(),
    exit_spec: ExitSpec = ExitSpec(trailing_stop=0.25),
    verbose: bool = True,
) -> dict:
    res = simulate(
        panel, entries, exit_flags=exit_flags,
        exec_spec=exec_spec, port_spec=port_spec, exit_spec=exit_spec, start=start,
    )
    summ = metrics.summarize(res.nav, res.trades, bench)
    trial_id = ledger.log_trial(
        family=family, name=name, hypothesis=hypothesis, config=config,
        window=window, metrics=summ, batch=batch, curve=res.nav,
    )
    if verbose:
        print(metrics.fmt_report(f"{trial_id} {name}", res.nav, res.trades, bench))
        print()
    return {"trial_id": trial_id, "name": name, **summ}
