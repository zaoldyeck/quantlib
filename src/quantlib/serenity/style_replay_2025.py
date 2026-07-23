"""Point-in-time Serenity-style bottleneck momentum replay from 2025 onward.

This is not a clone of Serenity's discretionary process.  It is the mechanical,
auditable proxy:

- use only data available as of each signal date;
- rebalance after monthly revenue should have been public;
- rank all non-financial, non-special TWSE/TPEx stocks by structural scarcity,
  revenue acceleration, total-return momentum, liquidity, fund flow, and
  valuation penalties;
- execute the next trading day with simple turnover costs.

The output is meant to answer whether the database-observable part of the
Serenity-style thesis has alpha before adding human research logs or news text.
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
from quantlib import paths

REPO_ROOT = Path(__file__).resolve().parents[3]
RESEARCH_ROOT = REPO_ROOT / "src" / "quantlib"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(RESEARCH_ROOT))

from quantlib.constants import CAPITAL, COMMISSION, SELL_TAX, TDPY  # noqa: E402
from quantlib.db import connect  # noqa: E402
from quantlib.prices import fetch_adjusted_panel, total_return_series  # noqa: E402


RESULTS = paths.OUT_STRAT_LAB
OUT_PREFIX = "serenity_style_replay_2025"


@dataclass(frozen=True)
class Variant:
    name: str
    top_n: int
    weighting: str


VARIANTS = (
    Variant("top10_equal", 10, "equal"),
    Variant("top10_score_weighted", 10, "score"),
    Variant("top5_equal", 5, "equal"),
    Variant("top15_equal", 15, "equal"),
)


def month_add(year: int, month: int, delta: int = 1) -> tuple[int, int]:
    month0 = year * 12 + (month - 1) + delta
    return month0 // 12, month0 % 12 + 1


def revenue_report_date(year: int, month: int) -> date:
    report_year, report_month = month_add(year, month, 1)
    return date(report_year, report_month, 10)


def first_trading_day_on_or_after(days: list[date], target: date) -> date | None:
    for day in days:
        if day >= target:
            return day
    return None


def build_rebalance_pairs(days: list[date], start: date, end: date) -> list[tuple[date, date]]:
    pairs: list[tuple[date, date]] = []
    year, month = start.year, start.month
    while date(year, month, 1) <= end:
        signal = first_trading_day_on_or_after(days, date(year, month, 11))
        if signal is not None and signal >= start and signal < end:
            idx = days.index(signal)
            if idx + 1 < len(days):
                pairs.append((signal, days[idx + 1]))
        year, month = month_add(year, month, 1)
    return pairs


def scarcity_score(row: pd.Series) -> float:
    """Static, auditable proxy for structural scarcity.

    The point of this score is to avoid injecting hindsight stock-specific
    stories into the backtest.  It deliberately uses stable industry buckets
    plus broad company-name keywords that describe bottleneck *types*.
    """

    industry = str(row.get("industry") or "")
    sector = str(row.get("broad_sector") or "")
    name = str(row.get("company_name") or "")
    text = f"{industry} {sector} {name}"

    score = 2.4
    if any(key in text for key in ("半導體", "電子零組件", "其他電子", "通信網路", "電腦及週邊")):
        score += 0.9
    if any(key in text for key in ("電機機械", "電器電纜")):
        score += 0.8
    if any(key in text for key in ("生技醫療", "化學", "綠能環保", "航運", "鋼鐵", "塑膠")):
        score += 0.35
    if any(key in name for key in ("材料", "光", "電", "微", "晶", "矽", "華城", "中興電", "士電", "亞力")):
        score += 0.20
    return min(score, 4.2)


def load_universe(con) -> pd.DataFrame:
    frame = con.sql(
        """
        WITH latest_tax AS (
            SELECT market, company_code, company_name, industry, broad_sector,
                   is_financial, is_special_category
            FROM industry_taxonomy_pit
            QUALIFY row_number() OVER (
                PARTITION BY company_code
                ORDER BY effective_date DESC NULLS LAST, source_ym DESC NULLS LAST
            ) = 1
        ),
        latest_px AS (
            SELECT market, company_code, closing_price, trade_value
            FROM daily_quote
            WHERE date = (SELECT max(date) FROM daily_quote)
              AND closing_price > 0
        )
        SELECT t.*
        FROM latest_tax t
        JOIN latest_px p USING (market, company_code)
        WHERE t.is_financial = false
          AND t.is_special_category = false
          AND regexp_matches(t.company_code, '^[0-9]{4}$')
        """
    ).pl()
    return frame.with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4)).to_pandas()


def load_price_features(con, universe: pd.DataFrame, start: date, end: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    panels: list[pl.DataFrame] = []
    for market in ("twse", "tpex"):
        codes = universe.loc[universe["market"] == market, "company_code"].tolist()
        if not codes:
            continue
        panels.append(
            fetch_adjusted_panel(
                con,
                start.isoformat(),
                end.isoformat(),
                codes=codes,
                market=market,
                include_extra_history_days=320,
            )
        )
    panel = (
        pl.concat(panels, how="diagonal")
        .with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4))
        .sort(["company_code", "date"])
    )
    featured = (
        panel.with_columns(
            [
                (pl.col("close") / pl.col("close").shift(1).over("company_code") - 1.0).alias("ret_1d"),
                (pl.col("close") / pl.col("close").shift(20).over("company_code") - 1.0).alias("ret_20d"),
                (pl.col("close") / pl.col("close").shift(60).over("company_code") - 1.0).alias("ret_60d"),
                (pl.col("close") / pl.col("close").shift(120).over("company_code") - 1.0).alias("ret_120d"),
                (pl.col("close") / pl.col("close").shift(252).over("company_code") - 1.0).alias("ret_252d"),
                pl.col("trade_value").rolling_mean(20).over("company_code").shift(1).alias("adv20"),
                (pl.col("close") / pl.col("close").rolling_max(252).over("company_code") - 1.0).alias("drawdown_252"),
            ]
        )
        .filter(pl.col("date") >= pl.lit(start).cast(pl.Date))
        .select(
            [
                "date",
                "company_code",
                "close",
                "raw_close",
                "ret_1d",
                "ret_20d",
                "ret_60d",
                "ret_120d",
                "ret_252d",
                "adv20",
                "drawdown_252",
            ]
        )
    )
    daily_returns = featured.select(["date", "company_code", "ret_1d"]).to_pandas()
    return featured.to_pandas(), daily_returns


def load_revenue_features(con) -> pd.DataFrame:
    rev = con.sql(
        """
        SELECT company_code, year, month, monthly_revenue_yoy
        FROM operating_revenue
        WHERE regexp_matches(company_code, '^[0-9]{4}$')
        """
    ).pl().with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4)).to_pandas()
    rev = rev.sort_values(["company_code", "year", "month"]).copy()
    rev["report_date"] = [revenue_report_date(int(y), int(m)) for y, m in zip(rev["year"], rev["month"])]
    rev["yoy_3m"] = rev.groupby("company_code")["monthly_revenue_yoy"].transform(lambda s: s.rolling(3, min_periods=2).mean())
    prev3 = rev.groupby("company_code")["monthly_revenue_yoy"].transform(
        lambda s: s.shift(3).rolling(3, min_periods=2).mean()
    )
    rev["yoy_accel"] = rev["yoy_3m"] - prev3
    return rev


def load_point_in_time_table(con, table: str, fields: list[str], date_field: str = "date") -> pd.DataFrame:
    select_fields = ", ".join(["company_code", date_field, *fields])
    frame = con.sql(f"SELECT {select_fields} FROM {table}").pl()
    return frame.with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4)).to_pandas()


def row_latest_before(df: pd.DataFrame, day: date, date_col: str) -> pd.DataFrame:
    work = df.copy()
    work[date_col] = pd.to_datetime(work[date_col]).dt.date
    view = work[work[date_col] <= day].sort_values(["company_code", date_col])
    return view.groupby("company_code", as_index=False).tail(1)


def score_candidates(frame: pd.DataFrame) -> pd.DataFrame:
    data = frame.copy()
    data["scarcity_score"] = data.apply(scarcity_score, axis=1)
    for col in (
        "monthly_revenue_yoy",
        "yoy_3m",
        "yoy_accel",
        "ret_20d",
        "ret_60d",
        "ret_252d",
        "drawdown_252",
        "adv20",
        "price_to_earning_ratio",
        "price_book_ratio",
        "inst_20d",
    ):
        if col not in data:
            data[col] = np.nan
        data[col] = pd.to_numeric(data[col], errors="coerce")

    data = data[
        (data["adv20"] >= 80_000_000)
        & (data["raw_close"] >= 20)
        & (data[["monthly_revenue_yoy", "yoy_3m"]].max(axis=1) >= 15)
        & (data["ret_60d"] >= -0.25)
        & (data["ret_252d"] >= -0.20)
    ].copy()
    if data.empty:
        return data.assign(score=[])

    score = data["scarcity_score"] * 20.0
    score += data["monthly_revenue_yoy"].clip(-30, 150).fillna(0.0) * 0.16
    score += data["yoy_3m"].clip(-30, 120).fillna(0.0) * 0.10
    score += data["yoy_accel"].clip(-50, 80).fillna(0.0) * 0.06
    score += data["ret_60d"].clip(-0.5, 1.5).fillna(0.0) * 18.0
    score += data["ret_20d"].clip(-0.35, 0.9).fillna(0.0) * 7.0
    score += data["ret_252d"].clip(-0.8, 3.0).fillna(0.0) * 4.0
    score += (np.log10(data["adv20"].clip(lower=1.0)) - 8.0).clip(upper=3.0) * 4.0
    score += (data["inst_20d"].fillna(0.0) / 1_000 / 10_000).clip(-3.0, 3.0) * 1.2

    pe = data["price_to_earning_ratio"]
    score += np.select(
        [pe.isna() | (pe <= 0), pe > 180, pe > 120, pe > 80, pe < 25],
        [-5.0, -10.0, -7.0, -4.0, 2.0],
        default=0.0,
    )
    pb = data["price_book_ratio"]
    score += np.select([pb > 35, pb > 25], [-6.0, -3.0], default=0.0)
    dd = data["drawdown_252"]
    score += np.select([dd < -0.40, dd < -0.30], [-8.0, -4.0], default=0.0)
    data["score"] = score
    return data.sort_values("score", ascending=False)


def target_weights(scored: pd.DataFrame, variant: Variant) -> dict[str, float]:
    picks = scored.head(variant.top_n).copy()
    if picks.empty:
        return {}
    if variant.weighting == "equal":
        return {code: 1.0 / len(picks) for code in picks["company_code"]}
    raw = picks["score"].clip(lower=0.0)
    if float(raw.sum()) <= 0:
        return {code: 1.0 / len(picks) for code in picks["company_code"]}
    weights = raw / raw.sum()
    weights = weights.clip(upper=0.20)
    weights = weights / weights.sum()
    return {code: float(weight) for code, weight in zip(picks["company_code"], weights)}


def cagr(start_nav: float, end_nav: float, days: int) -> float:
    if start_nav <= 0 or end_nav <= 0 or days <= 0:
        return float("nan")
    return (end_nav / start_nav) ** (365.25 / days) - 1.0


def max_drawdown(nav: pd.Series) -> float:
    peak = nav.cummax()
    return float((nav / peak - 1.0).min())


def recent_1y_cagr(daily: pd.DataFrame) -> tuple[float, str]:
    end = pd.to_datetime(daily["date"].iloc[-1]).date()
    anchor = end - timedelta(days=365)
    start_rows = daily[pd.to_datetime(daily["date"]).dt.date <= anchor]
    if start_rows.empty:
        return cagr(CAPITAL, float(daily["nav"].iloc[-1]), (end - pd.to_datetime(daily["date"].iloc[0]).date()).days), (
            f"{daily['date'].iloc[0]}~{end}"
        )
    start_row = start_rows.tail(1).iloc[0]
    start = pd.to_datetime(start_row["date"]).date()
    return cagr(float(start_row["nav"]), float(daily["nav"].iloc[-1]), (end - start).days), f"{start}~{end}"


def summarize_nav(name: str, daily: pd.DataFrame, total_turnover: float, rebalances: int) -> dict[str, object]:
    ordered = daily.sort_values("date").reset_index(drop=True)
    start = pd.to_datetime(ordered["date"].iloc[0]).date()
    end = pd.to_datetime(ordered["date"].iloc[-1]).date()
    returns = ordered["nav"].pct_change().fillna(0.0)
    sharpe = float(np.sqrt(TDPY) * returns.mean() / returns.std()) if returns.std() > 0 else float("nan")
    r1y, r1y_window = recent_1y_cagr(ordered)
    return {
        "name": name,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "days": len(ordered),
        "cagr": cagr(float(ordered["nav"].iloc[0]), float(ordered["nav"].iloc[-1]), (end - start).days),
        "recent_1y_cagr": r1y,
        "recent_1y_window": r1y_window,
        "sharpe": sharpe,
        "mdd": max_drawdown(ordered["nav"]),
        "final_nav": float(ordered["nav"].iloc[-1]),
        "total_turnover": total_turnover,
        "avg_rebalance_turnover": total_turnover / max(rebalances, 1),
        "rebalances": rebalances,
    }


def simulate_variant(
    variant: Variant,
    trading_days: list[date],
    rebalance_targets: dict[date, dict[str, float]],
    returns_by_day: dict[date, dict[str, float]],
) -> tuple[pd.DataFrame, float]:
    nav = CAPITAL
    current: dict[str, float] = {}
    rows: list[dict[str, object]] = []
    total_turnover = 0.0
    fee_buy = COMMISSION + 0.0005
    fee_sell = COMMISSION + SELL_TAX + 0.0005

    for day in trading_days:
        if day in rebalance_targets:
            target = rebalance_targets[day]
            keys = set(current) | set(target)
            buys = sum(max(target.get(code, 0.0) - current.get(code, 0.0), 0.0) for code in keys)
            sells = sum(max(current.get(code, 0.0) - target.get(code, 0.0), 0.0) for code in keys)
            turnover = buys + sells
            nav *= 1.0 - (buys * fee_buy + sells * fee_sell)
            total_turnover += turnover
            current = dict(target)
        day_rets = returns_by_day.get(day, {})
        port_ret = sum(weight * day_rets.get(code, 0.0) for code, weight in current.items())
        nav *= 1.0 + port_ret
        rows.append({"date": day, "nav": nav, "active": len(current)})
    return pd.DataFrame(rows), total_turnover


def benchmark_nav(con, code: str, market: str, start: date, end: date, name: str) -> pd.DataFrame:
    series = total_return_series(con, code, start.isoformat(), end.isoformat(), market=market).to_pandas()
    series = series.sort_values("date").reset_index(drop=True)
    series["nav"] = CAPITAL * series["adj_close"] / float(series["adj_close"].iloc[0])
    return series[["date", "nav"]].assign(name=name)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--capital", type=float, default=CAPITAL)
    args = parser.parse_args()
    if args.capital != CAPITAL:
        raise ValueError("This script currently uses quantlib.constants.CAPITAL for comparable outputs.")

    con = connect(read_only=True)
    try:
        cutoff = con.sql("select max(date) from daily_quote").fetchone()[0]
        start = date.fromisoformat(args.start)
        load_start = start - timedelta(days=420)
        universe = load_universe(con)
        price_features, daily_returns = load_price_features(con, universe, load_start, cutoff)
        revenue = load_revenue_features(con)
        per = load_point_in_time_table(con, "stock_per_pbr", ["price_to_earning_ratio", "price_book_ratio"])
        flows = con.sql(
            """
            SELECT date, company_code, total_difference AS inst_diff
            FROM daily_trading_details
            """
        ).pl().with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4)).to_pandas()
        flows = flows.sort_values(["company_code", "date"]).copy()
        flows["inst_20d"] = flows.groupby("company_code")["inst_diff"].transform(
            lambda s: s.rolling(20, min_periods=5).sum()
        )

        trading_days = sorted(pd.to_datetime(price_features["date"]).dt.date.unique())
        trading_days = [day for day in trading_days if day >= start and day <= cutoff]
        pairs = build_rebalance_pairs(trading_days, start, cutoff)

        daily_returns_map = {
            day: group.set_index("company_code")["ret_1d"].fillna(0.0).to_dict()
            for day, group in daily_returns[pd.to_datetime(daily_returns["date"]).dt.date.isin(trading_days)].groupby(
                pd.to_datetime(daily_returns[pd.to_datetime(daily_returns["date"]).dt.date.isin(trading_days)]["date"]).dt.date
            )
        }

        targets_by_variant: dict[str, dict[date, dict[str, float]]] = {variant.name: {} for variant in VARIANTS}
        pick_rows: list[dict[str, object]] = []

        universe = universe.assign(company_code=universe["company_code"].astype(str).str.zfill(4))
        for signal_day, exec_day in pairs:
            px_day = price_features[pd.to_datetime(price_features["date"]).dt.date == signal_day].copy()
            rev_day = row_latest_before(revenue, signal_day, "report_date")
            per_day = row_latest_before(per, signal_day, "date")
            flow_day = row_latest_before(flows[["date", "company_code", "inst_20d"]], signal_day, "date")
            joined = (
                universe.merge(px_day, on="company_code", how="inner")
                .merge(rev_day, on="company_code", how="left", suffixes=("", "_rev"))
                .merge(per_day, on="company_code", how="left")
                .merge(flow_day, on="company_code", how="left")
            )
            scored = score_candidates(joined)
            if scored.empty:
                continue
            for rank, row in enumerate(scored.head(30).itertuples(index=False), 1):
                pick_rows.append(
                    {
                        "signal_date": signal_day,
                        "execution_date": exec_day,
                        "rank": rank,
                        "company_code": row.company_code,
                        "company_name": row.company_name,
                        "industry": row.industry,
                        "score": float(row.score),
                        "scarcity_score": float(row.scarcity_score),
                        "ret_20d": float(row.ret_20d),
                        "ret_60d": float(row.ret_60d),
                        "ret_252d": float(row.ret_252d),
                        "monthly_revenue_yoy": float(row.monthly_revenue_yoy)
                        if not pd.isna(row.monthly_revenue_yoy)
                        else np.nan,
                        "yoy_3m": float(row.yoy_3m) if not pd.isna(row.yoy_3m) else np.nan,
                        "price_to_earning_ratio": float(row.price_to_earning_ratio)
                        if not pd.isna(row.price_to_earning_ratio)
                        else np.nan,
                        "price_book_ratio": float(row.price_book_ratio)
                        if not pd.isna(row.price_book_ratio)
                        else np.nan,
                    }
                )
            for variant in VARIANTS:
                targets_by_variant[variant.name][exec_day] = target_weights(scored, variant)

        summaries: list[dict[str, object]] = []
        for variant in VARIANTS:
            daily, turnover = simulate_variant(
                variant, trading_days, targets_by_variant[variant.name], daily_returns_map
            )
            daily.to_csv(RESULTS / f"{OUT_PREFIX}_{variant.name}_daily.csv", index=False)
            summaries.append(summarize_nav(variant.name, daily, turnover, len(targets_by_variant[variant.name])))

        bench_start = trading_days[0]
        bench_end = trading_days[-1]
        for code, market, name in (("0050", "twse", "hold_0050"), ("2330", "twse", "hold_2330")):
            daily = benchmark_nav(con, code, market, bench_start, bench_end, name)
            daily.to_csv(RESULTS / f"{OUT_PREFIX}_{name}_daily.csv", index=False)
            summaries.append(summarize_nav(name, daily, 0.0, 0))

        picks = pd.DataFrame(pick_rows)
        picks.to_csv(RESULTS / f"{OUT_PREFIX}_picks.csv", index=False)
        summary = pd.DataFrame(summaries).sort_values("cagr", ascending=False)
        summary.to_csv(RESULTS / f"{OUT_PREFIX}_summary.csv", index=False)

        print(f"data_cutoff={cutoff}")
        print(f"trading_window={trading_days[0]}~{trading_days[-1]} rebalances={len(pairs)}")
        print("outputs:")
        print(f"  {RESULTS / f'{OUT_PREFIX}_summary.csv'}")
        print(f"  {RESULTS / f'{OUT_PREFIX}_picks.csv'}")
        display = summary.copy()
        for col in ("cagr", "recent_1y_cagr", "sharpe", "mdd", "total_turnover", "avg_rebalance_turnover"):
            display[col] = display[col].astype(float)
        print(
            display[
                [
                    "name",
                    "cagr",
                    "recent_1y_cagr",
                    "recent_1y_window",
                    "sharpe",
                    "mdd",
                    "final_nav",
                    "total_turnover",
                    "avg_rebalance_turnover",
                    "rebalances",
                ]
            ].to_string(
                index=False,
                formatters={
                    "cagr": "{:.2%}".format,
                    "recent_1y_cagr": "{:.2%}".format,
                    "sharpe": "{:.3f}".format,
                    "mdd": "{:.2%}".format,
                    "final_nav": "{:,.0f}".format,
                    "total_turnover": "{:.2f}x".format,
                    "avg_rebalance_turnover": "{:.2f}x".format,
                },
            )
        )
        latest_signal = max((row["signal_date"] for row in pick_rows), default=None)
        if latest_signal is not None:
            latest_picks = picks[picks["signal_date"] == latest_signal].head(15)
            print("\nlatest_signal_top15")
            print(
                latest_picks[
                    [
                        "rank",
                        "company_code",
                        "company_name",
                        "industry",
                        "score",
                        "ret_60d",
                        "monthly_revenue_yoy",
                        "price_to_earning_ratio",
                    ]
                ].to_string(
                    index=False,
                    formatters={
                        "score": "{:.1f}".format,
                        "ret_60d": "{:.1%}".format,
                        "monthly_revenue_yoy": "{:.1f}".format,
                        "price_to_earning_ratio": "{:.1f}".format,
                    },
                )
            )
    finally:
        con.close()


if __name__ == "__main__":
    main()
