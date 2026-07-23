"""Signal builders and data loaders for TAIFEX futures research."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import polars as pl
from stockstats import wrap

from .rpt_features import load_rpt_daily_features


@dataclass(frozen=True)
class StrategyCandidate:
    name: str
    product: str
    kind: str
    params: dict[str, float | int | str]


_FEATURE_CACHE: dict[tuple[int, int, str], pl.DataFrame] = {}

_STOCKSTATS_COLUMNS = [
    "rsi",
    "rsi_6",
    "rsi_12",
    "macd",
    "macds",
    "macdh",
    "boll",
    "boll_ub",
    "boll_lb",
    "kdjk",
    "kdjd",
    "kdjj",
    "adx",
    "adxr",
    "pdi",
    "ndi",
    "cci",
    "cci_20",
    "atr",
    "wr_10",
    "wr_14",
    "mfi",
    "trix",
    "tema",
    "close_5_sma",
    "close_10_sma",
    "close_20_sma",
    "close_50_sma",
    "close_120_sma",
    "close_200_sma",
    "close_5_ema",
    "close_10_ema",
    "close_20_ema",
    "close_50_ema",
    "close_120_ema",
    "close_200_ema",
]


def _stockstats_name(name: str) -> str:
    return "ss_" + name.replace(",", "_").replace("-", "_")


def _with_optional_columns(frame: pl.DataFrame) -> pl.DataFrame:
    defaults = {
        "volume": 0.0,
        "tx_spot_basis_pct": 0.0,
        "tx_next_term_spread_pct": 0.0,
        "foreign_tx_net_oi": 0.0,
        "trust_tx_net_oi": 0.0,
        "dealer_tx_net_oi": 0.0,
        "foreign_tx_net_volume": 0.0,
        "cash_foreign_net": 0.0,
        "cash_trust_net": 0.0,
        "cash_dealer_net": 0.0,
        "cash_total_net": 0.0,
        "margin_balance_sum": 0.0,
        "short_balance_sum": 0.0,
        "sbl_daily_sold": 0.0,
        "sbl_daily_returned": 0.0,
        "sbl_balance_sum": 0.0,
        "foreign_holding_ratio_mkt": 0.0,
        "rpt_5m_total_ret": 0.0,
        "rpt_5m_regular_ret": 0.0,
        "rpt_5m_night_ret": 0.0,
        "rpt_5m_first30_ret": 0.0,
        "rpt_5m_last60_ret": 0.0,
        "rpt_5m_rv": 0.0,
        "rpt_5m_range_pct": 0.0,
        "rpt_5m_regular_volume_share": 0.0,
        "rpt_5m_night_volume_share": 0.0,
        "rpt_15m_total_ret": 0.0,
        "rpt_15m_regular_ret": 0.0,
        "rpt_15m_night_ret": 0.0,
        "rpt_15m_first30_ret": 0.0,
        "rpt_15m_last60_ret": 0.0,
        "rpt_15m_rv": 0.0,
        "rpt_15m_range_pct": 0.0,
        "rpt_60m_total_ret": 0.0,
        "rpt_60m_regular_ret": 0.0,
        "rpt_60m_night_ret": 0.0,
        "rpt_60m_last60_ret": 0.0,
        "rpt_60m_rv": 0.0,
        "rpt_60m_range_pct": 0.0,
    }
    expressions = []
    for name, value in defaults.items():
        if name in frame.columns:
            expressions.append(pl.col(name).fill_null(value).alias(name))
        else:
            expressions.append(pl.lit(value).alias(name))
    return frame.with_columns(expressions)


def _with_stockstats_indicators(frame: pl.DataFrame) -> pl.DataFrame:
    pdf = (
        frame.select(["date", "open", "high", "low", "close", "volume"])
        .with_columns(
            [
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("volume").cast(pl.Float64),
            ]
        )
        .to_pandas()
    )
    stock = wrap(pdf, index_column="date")
    generated: list[str] = []
    for name in _STOCKSTATS_COLUMNS:
        try:
            _ = stock[name]
            generated.append(name)
        except Exception:
            continue
    if not generated:
        return frame
    out = {"date": pdf["date"].to_numpy()}
    for name in generated:
        out[_stockstats_name(name)] = stock[name].to_numpy()
    out = pl.DataFrame(out).with_columns(pl.col("date").cast(pl.Date))
    return frame.join(out, on="date", how="left")


def load_product_frame(db_path: str | Path, product: str = "TX") -> pl.DataFrame:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        frame = con.sql(
            f"""
            WITH cash_flow AS (
                SELECT
                    date,
                    SUM(foreign_investors_difference)::DOUBLE AS cash_foreign_net,
                    SUM(trust_difference)::DOUBLE AS cash_trust_net,
                    SUM(dealers_difference)::DOUBLE AS cash_dealer_net,
                    SUM(total_difference)::DOUBLE AS cash_total_net
                FROM daily_trading_details
                GROUP BY date
            ),
            margin_flow AS (
                SELECT
                    date,
                    SUM(margin_balance)::DOUBLE AS margin_balance_sum,
                    SUM(short_balance)::DOUBLE AS short_balance_sum
                FROM margin_transactions
                GROUP BY date
            ),
            sbl_flow AS (
                SELECT
                    date,
                    SUM(daily_sold)::DOUBLE AS sbl_daily_sold,
                    SUM(daily_returned)::DOUBLE AS sbl_daily_returned,
                    SUM(daily_balance)::DOUBLE AS sbl_balance_sum
                FROM sbl_borrowing
                GROUP BY date
            ),
            foreign_holding AS (
                SELECT
                    date,
                    CASE
                        WHEN SUM(outstanding_shares) > 0
                        THEN SUM(foreign_held_shares)::DOUBLE / SUM(outstanding_shares)::DOUBLE
                        ELSE NULL
                    END AS foreign_holding_ratio_mkt
                FROM foreign_holding_ratio
                GROUP BY date
            )
            SELECT
                r.date,
                r.product,
                r.contract_month,
                COALESCE(r.open, r.close) AS open,
                COALESCE(r.high, r.open, r.close) AS high,
                COALESCE(r.low, r.open, r.close) AS low,
                r.close,
                COALESCE(r.settlement_price, r.final_settlement_price, r.close) AS mark,
                r.final_settlement_price,
                r.volume,
                r.open_interest,
                c.continuous_close,
                c.daily_return,
                f.tx_spot_basis_pct,
                f.tx_next_term_spread_pct,
                f.foreign_tx_net_oi,
                f.trust_tx_net_oi,
                f.dealer_tx_net_oi,
                f.foreign_tx_net_volume,
                f.taiex_close,
                f.te_close,
                f.tf_close,
                cf.cash_foreign_net,
                cf.cash_trust_net,
                cf.cash_dealer_net,
                cf.cash_total_net,
                mf.margin_balance_sum,
                mf.short_balance_sum,
                sf.sbl_daily_sold,
                sf.sbl_daily_returned,
                sf.sbl_balance_sum,
                fh.foreign_holding_ratio_mkt
            FROM taifex_futures_contract_rank r
            LEFT JOIN taifex_futures_continuous c
              ON c.date = r.date
             AND c.product = r.product
             AND c.contract_month = r.contract_month
            LEFT JOIN taifex_futures_daily_factors f
              ON f.date = r.date
            LEFT JOIN cash_flow cf
              ON cf.date = r.date
            LEFT JOIN margin_flow mf
              ON mf.date = r.date
            LEFT JOIN sbl_flow sf
              ON sf.date = r.date
            LEFT JOIN foreign_holding fh
              ON fh.date = r.date
            WHERE r.product = '{product}'
              AND r.month_rank = 1
              AND r.trading_session = '一般'
              AND r.close IS NOT NULL
            ORDER BY r.date
            """
        ).pl()
    finally:
        con.close()
    for timeframe in ["5m", "15m", "60m"]:
        try:
            rpt = load_rpt_daily_features(product=product, timeframe=timeframe)
        except FileNotFoundError:
            continue
        frame = frame.join(rpt.drop(["product"], strict=False), on=["date", "contract_month"], how="left")
    return frame


def add_common_features(frame: pl.DataFrame) -> pl.DataFrame:
    cache_key = (id(frame), frame.height, frame["date"][-1].isoformat() if frame.height else "")
    cached = _FEATURE_CACHE.get(cache_key)
    if cached is not None:
        return cached

    ordered = _with_stockstats_indicators(_with_optional_columns(frame.sort("date")))
    ret = (pl.col("continuous_close") / pl.col("continuous_close").shift(1) - 1.0).fill_null(0.0)
    true_range = pl.max_horizontal(
        pl.col("high") - pl.col("low"),
        (pl.col("high") - pl.col("close").shift(1)).abs(),
        (pl.col("low") - pl.col("close").shift(1)).abs(),
    )
    up_ret = pl.when(ret > 0).then(ret).otherwise(0.0)
    down_ret = pl.when(ret < 0).then(-ret).otherwise(0.0)
    typical = (pl.col("high") + pl.col("low") + pl.col("close")) / 3.0
    high_delta = pl.col("high") - pl.col("high").shift(1)
    low_delta = pl.col("low").shift(1) - pl.col("low")
    plus_dm = pl.when((high_delta > low_delta) & (high_delta > 0)).then(high_delta).otherwise(0.0)
    minus_dm = pl.when((low_delta > high_delta) & (low_delta > 0)).then(low_delta).otherwise(0.0)

    featured = (
        ordered.with_columns(
            [
                ret.alias("ret"),
                true_range.alias("true_range"),
                (pl.col("te_close") / pl.col("tf_close") - 1.0).alias("te_tf_rel"),
                typical.alias("typical_price"),
                up_ret.alias("up_ret"),
                down_ret.alias("down_ret"),
                plus_dm.alias("plus_dm"),
                minus_dm.alias("minus_dm"),
            ]
        )
        .with_columns(
            [
                pl.col("true_range").rolling_mean(14).alias("atr14"),
                pl.col("ret").rolling_std(20).alias("vol20"),
                pl.col("ret").rolling_std(63).alias("vol63"),
                pl.col("continuous_close").rolling_mean(20).alias("ma20"),
                pl.col("continuous_close").rolling_mean(50).alias("ma50"),
                pl.col("continuous_close").rolling_mean(120).alias("ma120"),
                pl.col("continuous_close").rolling_mean(200).alias("ma200"),
                pl.col("continuous_close").rolling_std(20).alias("price_std20"),
                pl.col("continuous_close").rolling_min(14).alias("low14"),
                pl.col("continuous_close").rolling_max(14).alias("high14"),
                pl.col("continuous_close").rolling_min(20).alias("low20"),
                pl.col("continuous_close").rolling_max(20).alias("high20"),
                pl.col("continuous_close").rolling_min(55).alias("low55"),
                pl.col("continuous_close").rolling_max(55).alias("high55"),
                pl.col("up_ret").rolling_mean(14).alias("up_mean14"),
                pl.col("down_ret").rolling_mean(14).alias("down_mean14"),
                pl.col("continuous_close").ewm_mean(span=12, adjust=False).alias("ema12"),
                pl.col("continuous_close").ewm_mean(span=26, adjust=False).alias("ema26"),
                pl.col("ret").rolling_sum(10).alias("roc10"),
                pl.col("ret").rolling_sum(20).alias("roc20"),
                pl.col("volume").rolling_mean(63).alias("volume_mean63"),
                pl.col("volume").rolling_std(63).alias("volume_std63"),
                pl.col("true_range").rolling_sum(14).alias("tr_sum14"),
                pl.col("plus_dm").rolling_sum(14).alias("plus_dm_sum14"),
                pl.col("minus_dm").rolling_sum(14).alias("minus_dm_sum14"),
                pl.col("tx_spot_basis_pct").rolling_mean(63).alias("basis_mean63"),
                pl.col("tx_spot_basis_pct").rolling_std(63).alias("basis_std63"),
                pl.col("tx_next_term_spread_pct").rolling_mean(63).alias("term_mean63"),
                pl.col("tx_next_term_spread_pct").rolling_std(63).alias("term_std63"),
                pl.col("foreign_tx_net_oi").diff().rolling_mean(5).alias("foreign_oi_chg5"),
                pl.col("foreign_tx_net_oi").rolling_mean(20).alias("foreign_oi_mean20"),
                pl.col("foreign_tx_net_oi").rolling_std(63).alias("foreign_oi_std63"),
                pl.col("cash_foreign_net").rolling_mean(20).alias("cash_foreign_mean20"),
                pl.col("cash_foreign_net").rolling_std(63).alias("cash_foreign_std63"),
                pl.col("cash_trust_net").rolling_mean(20).alias("cash_trust_mean20"),
                pl.col("cash_trust_net").rolling_std(63).alias("cash_trust_std63"),
                pl.col("cash_dealer_net").rolling_mean(20).alias("cash_dealer_mean20"),
                pl.col("cash_dealer_net").rolling_std(63).alias("cash_dealer_std63"),
                pl.col("margin_balance_sum").diff().rolling_mean(5).alias("margin_balance_chg5"),
                pl.col("margin_balance_sum").diff().rolling_std(63).alias("margin_balance_chg_std63"),
                pl.col("short_balance_sum").diff().rolling_mean(5).alias("short_balance_chg5"),
                pl.col("short_balance_sum").diff().rolling_std(63).alias("short_balance_chg_std63"),
                pl.col("sbl_balance_sum").diff().rolling_mean(5).alias("sbl_balance_chg5"),
                pl.col("sbl_balance_sum").diff().rolling_std(63).alias("sbl_balance_chg_std63"),
                pl.col("foreign_holding_ratio_mkt").diff().rolling_mean(5).alias("foreign_holding_chg5"),
                pl.col("foreign_holding_ratio_mkt").diff().rolling_std(63).alias("foreign_holding_chg_std63"),
                pl.col("rpt_5m_total_ret").rolling_mean(20).alias("rpt_5m_total_ret_mean20"),
                pl.col("rpt_5m_total_ret").rolling_std(63).alias("rpt_5m_total_ret_std63"),
                pl.col("rpt_5m_regular_ret").rolling_mean(20).alias("rpt_5m_regular_ret_mean20"),
                pl.col("rpt_5m_regular_ret").rolling_std(63).alias("rpt_5m_regular_ret_std63"),
                pl.col("rpt_5m_night_ret").rolling_mean(20).alias("rpt_5m_night_ret_mean20"),
                pl.col("rpt_5m_night_ret").rolling_std(63).alias("rpt_5m_night_ret_std63"),
                pl.col("rpt_5m_first30_ret").rolling_mean(20).alias("rpt_5m_first30_ret_mean20"),
                pl.col("rpt_5m_first30_ret").rolling_std(63).alias("rpt_5m_first30_ret_std63"),
                pl.col("rpt_5m_last60_ret").rolling_mean(20).alias("rpt_5m_last60_ret_mean20"),
                pl.col("rpt_5m_last60_ret").rolling_std(63).alias("rpt_5m_last60_ret_std63"),
                pl.col("rpt_5m_rv").rolling_mean(63).alias("rpt_5m_rv_mean63"),
                pl.col("rpt_5m_rv").rolling_std(126).alias("rpt_5m_rv_std126"),
                pl.col("rpt_5m_range_pct").rolling_mean(63).alias("rpt_5m_range_mean63"),
                pl.col("rpt_5m_range_pct").rolling_std(126).alias("rpt_5m_range_std126"),
                pl.col("rpt_15m_total_ret").rolling_mean(20).alias("rpt_15m_total_ret_mean20"),
                pl.col("rpt_15m_total_ret").rolling_std(63).alias("rpt_15m_total_ret_std63"),
                pl.col("rpt_15m_last60_ret").rolling_mean(20).alias("rpt_15m_last60_ret_mean20"),
                pl.col("rpt_15m_last60_ret").rolling_std(63).alias("rpt_15m_last60_ret_std63"),
                pl.col("rpt_60m_total_ret").rolling_mean(20).alias("rpt_60m_total_ret_mean20"),
                pl.col("rpt_60m_total_ret").rolling_std(63).alias("rpt_60m_total_ret_std63"),
            ]
        )
        .with_columns(
            [
                (
                    (pl.col("tx_spot_basis_pct") - pl.col("basis_mean63"))
                    / pl.max_horizontal(pl.col("basis_std63"), pl.lit(1e-9))
                ).alias("basis_z63"),
                (
                    (pl.col("tx_next_term_spread_pct") - pl.col("term_mean63"))
                    / pl.max_horizontal(pl.col("term_std63"), pl.lit(1e-9))
                ).alias("term_z63"),
                (
                    (pl.col("foreign_tx_net_oi") - pl.col("foreign_oi_mean20"))
                    / pl.max_horizontal(pl.col("foreign_oi_std63"), pl.lit(1.0))
                ).alias("foreign_oi_z"),
                (
                    100.0 - 100.0 / (1.0 + pl.col("up_mean14") / pl.max_horizontal(pl.col("down_mean14"), pl.lit(1e-9)))
                ).alias("rsi14"),
                (
                    (pl.col("continuous_close") - pl.col("low14"))
                    / pl.max_horizontal(pl.col("high14") - pl.col("low14"), pl.lit(1e-9))
                    * 100.0
                ).alias("stoch_k14"),
                ((pl.col("continuous_close") - pl.col("ma20")) / pl.max_horizontal(pl.col("price_std20"), pl.lit(1e-9))).alias("bb_z20"),
                (
                    (pl.col("continuous_close") - pl.col("low55"))
                    / pl.max_horizontal(pl.col("high55") - pl.col("low55"), pl.lit(1e-9))
                    * 2.0
                    - 1.0
                ).alias("donchian_pos55"),
                ((pl.col("volume") - pl.col("volume_mean63")) / pl.max_horizontal(pl.col("volume_std63"), pl.lit(1.0))).alias("volume_z63"),
                (100.0 * pl.col("plus_dm_sum14") / pl.max_horizontal(pl.col("tr_sum14"), pl.lit(1e-9))).alias("plus_di14"),
                (100.0 * pl.col("minus_dm_sum14") / pl.max_horizontal(pl.col("tr_sum14"), pl.lit(1e-9))).alias("minus_di14"),
                ((pl.col("cash_foreign_net") - pl.col("cash_foreign_mean20")) / pl.max_horizontal(pl.col("cash_foreign_std63"), pl.lit(1.0))).alias("cash_foreign_z"),
                ((pl.col("cash_trust_net") - pl.col("cash_trust_mean20")) / pl.max_horizontal(pl.col("cash_trust_std63"), pl.lit(1.0))).alias("cash_trust_z"),
                ((pl.col("cash_dealer_net") - pl.col("cash_dealer_mean20")) / pl.max_horizontal(pl.col("cash_dealer_std63"), pl.lit(1.0))).alias("cash_dealer_z"),
                (pl.col("margin_balance_chg5") / pl.max_horizontal(pl.col("margin_balance_chg_std63"), pl.lit(1.0))).alias("margin_balance_chg_z"),
                (pl.col("short_balance_chg5") / pl.max_horizontal(pl.col("short_balance_chg_std63"), pl.lit(1.0))).alias("short_balance_chg_z"),
                (pl.col("sbl_balance_chg5") / pl.max_horizontal(pl.col("sbl_balance_chg_std63"), pl.lit(1.0))).alias("sbl_balance_chg_z"),
                (
                    pl.col("foreign_holding_chg5")
                    / pl.max_horizontal(pl.col("foreign_holding_chg_std63"), pl.lit(1e-9))
                ).alias("foreign_holding_chg_z"),
                (
                    (pl.col("rpt_5m_total_ret") - pl.col("rpt_5m_total_ret_mean20"))
                    / pl.max_horizontal(pl.col("rpt_5m_total_ret_std63"), pl.lit(1e-6))
                ).alias("rpt_5m_total_ret_z"),
                (
                    (pl.col("rpt_5m_regular_ret") - pl.col("rpt_5m_regular_ret_mean20"))
                    / pl.max_horizontal(pl.col("rpt_5m_regular_ret_std63"), pl.lit(1e-6))
                ).alias("rpt_5m_regular_ret_z"),
                (
                    (pl.col("rpt_5m_night_ret") - pl.col("rpt_5m_night_ret_mean20"))
                    / pl.max_horizontal(pl.col("rpt_5m_night_ret_std63"), pl.lit(1e-6))
                ).alias("rpt_5m_night_ret_z"),
                (
                    (pl.col("rpt_5m_first30_ret") - pl.col("rpt_5m_first30_ret_mean20"))
                    / pl.max_horizontal(pl.col("rpt_5m_first30_ret_std63"), pl.lit(1e-6))
                ).alias("rpt_5m_first30_ret_z"),
                (
                    (pl.col("rpt_5m_last60_ret") - pl.col("rpt_5m_last60_ret_mean20"))
                    / pl.max_horizontal(pl.col("rpt_5m_last60_ret_std63"), pl.lit(1e-6))
                ).alias("rpt_5m_last60_ret_z"),
                (
                    (pl.col("rpt_5m_rv") - pl.col("rpt_5m_rv_mean63"))
                    / pl.max_horizontal(pl.col("rpt_5m_rv_std126"), pl.lit(1e-6))
                ).alias("rpt_5m_rv_z"),
                (
                    (pl.col("rpt_5m_range_pct") - pl.col("rpt_5m_range_mean63"))
                    / pl.max_horizontal(pl.col("rpt_5m_range_std126"), pl.lit(1e-6))
                ).alias("rpt_5m_range_z"),
                (
                    (pl.col("rpt_15m_total_ret") - pl.col("rpt_15m_total_ret_mean20"))
                    / pl.max_horizontal(pl.col("rpt_15m_total_ret_std63"), pl.lit(1e-6))
                ).alias("rpt_15m_total_ret_z"),
                (
                    (pl.col("rpt_15m_last60_ret") - pl.col("rpt_15m_last60_ret_mean20"))
                    / pl.max_horizontal(pl.col("rpt_15m_last60_ret_std63"), pl.lit(1e-6))
                ).alias("rpt_15m_last60_ret_z"),
                (
                    (pl.col("rpt_60m_total_ret") - pl.col("rpt_60m_total_ret_mean20"))
                    / pl.max_horizontal(pl.col("rpt_60m_total_ret_std63"), pl.lit(1e-6))
                ).alias("rpt_60m_total_ret_z"),
            ]
        )
        .with_columns(
            [
                (pl.col("ema12") - pl.col("ema26")).alias("macd_line"),
                (
                    100.0
                    * (pl.col("plus_di14") - pl.col("minus_di14")).abs()
                    / pl.max_horizontal((pl.col("plus_di14") + pl.col("minus_di14")).abs(), pl.lit(1e-9))
                )
                .rolling_mean(14)
                .alias("adx14"),
                (
                    0.35 * pl.col("foreign_oi_z").fill_null(0.0)
                    + 0.25 * pl.col("cash_foreign_z").fill_null(0.0)
                    + 0.15 * pl.col("cash_trust_z").fill_null(0.0)
                    + 0.10 * pl.col("cash_dealer_z").fill_null(0.0)
                    + 0.10 * pl.col("foreign_holding_chg_z").fill_null(0.0)
                    - 0.12 * pl.col("margin_balance_chg_z").fill_null(0.0)
                    - 0.08 * pl.col("sbl_balance_chg_z").fill_null(0.0)
                    + 0.05 * pl.col("short_balance_chg_z").fill_null(0.0)
                ).alias("chip_score"),
            ]
        )
        .with_columns(pl.col("macd_line").ewm_mean(span=9, adjust=False).alias("macd_signal"))
        .with_columns((pl.col("macd_line") - pl.col("macd_signal")).alias("macd_hist"))
    )
    _FEATURE_CACHE[cache_key] = featured
    return featured


def build_signal(frame: pl.DataFrame, candidate: StrategyCandidate) -> pl.DataFrame:
    f = add_common_features(frame)
    p = candidate.params
    kind = candidate.kind

    if kind == "trend":
        lb = int(p.get("lookback", 120))
        ma = int(p.get("ma", lb))
        threshold = float(p.get("threshold", 0.0))
        raw = (
            pl.when(
                ((pl.col("continuous_close") / pl.col("continuous_close").shift(lb) - 1.0) > threshold)
                & (pl.col("continuous_close") > pl.col("continuous_close").rolling_mean(ma))
            )
            .then(1.0)
            .when(
                ((pl.col("continuous_close") / pl.col("continuous_close").shift(lb) - 1.0) < -threshold)
                & (pl.col("continuous_close") < pl.col("continuous_close").rolling_mean(ma))
            )
            .then(-1.0)
            .otherwise(0.0)
        )
    elif kind == "breakout":
        lb = int(p.get("lookback", 80))
        raw = (
            pl.when(pl.col("continuous_close") > pl.col("continuous_close").rolling_max(lb).shift(1))
            .then(1.0)
            .when(pl.col("continuous_close") < pl.col("continuous_close").rolling_min(lb).shift(1))
            .then(-1.0)
            .otherwise(0.0)
        )
    elif kind == "mean_reversion":
        lb = int(p.get("lookback", 20))
        entry_z = float(p.get("entry_z", 1.5))
        z = (
            (pl.col("continuous_close") / pl.col("continuous_close").rolling_mean(lb) - 1.0)
            / pl.max_horizontal(pl.col("vol20"), pl.lit(0.005))
        )
        raw = pl.when(z < -entry_z).then(1.0).when(z > entry_z).then(-1.0).otherwise(0.0)
    elif kind == "basis":
        z = float(p.get("entry_z", 1.0))
        raw = pl.when(pl.col("basis_z63") < -z).then(1.0).when(pl.col("basis_z63") > z).then(-1.0).otherwise(0.0)
    elif kind == "term":
        z = float(p.get("entry_z", 1.0))
        raw = pl.when(pl.col("term_z63") < -z).then(1.0).when(pl.col("term_z63") > z).then(-1.0).otherwise(0.0)
    elif kind == "flow":
        z = float(p.get("entry_z", 0.5))
        raw = (
            pl.when((pl.col("foreign_oi_z") > z) & (pl.col("foreign_oi_chg5") > 0))
            .then(1.0)
            .when((pl.col("foreign_oi_z") < -z) & (pl.col("foreign_oi_chg5") < 0))
            .then(-1.0)
            .otherwise(0.0)
        )
    elif kind == "cross_market":
        lb = int(p.get("lookback", 20))
        z = (pl.col("te_tf_rel") - pl.col("te_tf_rel").rolling_mean(lb)) / pl.col("te_tf_rel").rolling_std(lb)
        entry_z = float(p.get("entry_z", 0.8))
        raw = pl.when(z > entry_z).then(1.0).when(z < -entry_z).then(-1.0).otherwise(0.0)
    elif kind == "dual_momentum":
        fast = int(p.get("fast", 40))
        slow = int(p.get("slow", 200))
        mom = int(p.get("momentum", 80))
        threshold = float(p.get("threshold", 0.0))
        fast_ma = pl.col("continuous_close").rolling_mean(fast)
        slow_ma = pl.col("continuous_close").rolling_mean(slow)
        momentum = pl.col("continuous_close") / pl.col("continuous_close").shift(mom) - 1.0
        raw = (
            pl.when((fast_ma > slow_ma) & (momentum > threshold)).then(1.0)
            .when((fast_ma < slow_ma) & (momentum < -threshold)).then(-1.0)
            .otherwise(0.0)
        )
    elif kind == "panic_rebound":
        lb = int(p.get("lookback", 5))
        trigger = float(p.get("trigger", 0.04))
        ma = int(p.get("ma", 200))
        ret = pl.col("continuous_close") / pl.col("continuous_close").shift(lb) - 1.0
        ma_filter = pl.col("continuous_close") > pl.col("continuous_close").rolling_mean(ma)
        raw = (
            pl.when((ret < -trigger) & ma_filter).then(1.0)
            .when((ret > trigger) & (~ma_filter)).then(-1.0)
            .otherwise(0.0)
        )
    elif kind == "basis_trend":
        z = float(p.get("entry_z", 1.0))
        ma = int(p.get("ma", 120))
        trend_up = pl.col("continuous_close") > pl.col("continuous_close").rolling_mean(ma)
        trend_down = pl.col("continuous_close") < pl.col("continuous_close").rolling_mean(ma)
        raw = (
            pl.when((pl.col("basis_z63") < -z) & trend_up).then(1.0)
            .when((pl.col("basis_z63") > z) & trend_down).then(-1.0)
            .otherwise(0.0)
        )
    elif kind == "h_model":
        z = float(p.get("entry_z", 0.75))
        min_volume_z = float(p.get("min_volume_z", -0.5))
        # Public H-model descriptions point to basis and volume indicators.
        # This auditable approximation uses inverse basis filtered by participation;
        # leverage remains controlled by the simulator rather than hard-coded here.
        raw = (
            pl.when((pl.col("basis_z63") < -z) & (pl.col("volume_z63") >= min_volume_z)).then(1.0)
            .when((pl.col("basis_z63") > z) & (pl.col("volume_z63") >= min_volume_z)).then(-1.0)
            .otherwise(0.0)
        )
    elif kind == "h_model_flow_filter":
        z = float(p.get("entry_z", 0.75))
        min_volume_z = float(p.get("min_volume_z", -0.5))
        min_chip = float(p.get("min_chip", -0.75))
        raw = (
            pl.when(
                (pl.col("basis_z63") < -z)
                & (pl.col("volume_z63") >= min_volume_z)
                & (pl.col("chip_score") >= min_chip)
            )
            .then(1.0)
            .when(
                (pl.col("basis_z63") > z)
                & (pl.col("volume_z63") >= min_volume_z)
                & (pl.col("chip_score") <= -min_chip)
            )
            .then(-1.0)
            .otherwise(0.0)
        )
    elif kind == "chip_composite":
        entry_z = float(p.get("entry_z", 0.5))
        raw = (
            pl.when(pl.col("chip_score") > entry_z).then(1.0)
            .when(pl.col("chip_score") < -entry_z).then(-1.0)
            .otherwise(0.0)
        )
    elif kind == "chip_breakout":
        lb = int(p.get("lookback", 55))
        entry_z = float(p.get("entry_z", 0.3))
        raw = (
            pl.when((pl.col("continuous_close") > pl.col("continuous_close").rolling_max(lb).shift(1)) & (pl.col("chip_score") > entry_z))
            .then(1.0)
            .when((pl.col("continuous_close") < pl.col("continuous_close").rolling_min(lb).shift(1)) & (pl.col("chip_score") < -entry_z))
            .then(-1.0)
            .otherwise(0.0)
        )
    elif kind == "technical_vote":
        threshold = float(p.get("threshold", 2.0))
        trend_vote = (
            pl.when((pl.col("ma50") > pl.col("ma200")) & (pl.col("continuous_close") > pl.col("ma50"))).then(1.0)
            .when((pl.col("ma50") < pl.col("ma200")) & (pl.col("continuous_close") < pl.col("ma50"))).then(-1.0)
            .otherwise(0.0)
        )
        macd_vote = pl.when(pl.col("macd_hist") > 0).then(1.0).when(pl.col("macd_hist") < 0).then(-1.0).otherwise(0.0)
        rsi_vote = pl.when(pl.col("rsi14") > 55).then(1.0).when(pl.col("rsi14") < 45).then(-1.0).otherwise(0.0)
        stoch_vote = pl.when(pl.col("stoch_k14") > 70).then(1.0).when(pl.col("stoch_k14") < 30).then(-1.0).otherwise(0.0)
        bb_vote = pl.when(pl.col("bb_z20") > 0.5).then(1.0).when(pl.col("bb_z20") < -0.5).then(-1.0).otherwise(0.0)
        dmi_vote = (
            pl.when((pl.col("plus_di14") > pl.col("minus_di14")) & (pl.col("adx14") > 18)).then(1.0)
            .when((pl.col("minus_di14") > pl.col("plus_di14")) & (pl.col("adx14") > 18)).then(-1.0)
            .otherwise(0.0)
        )
        donchian_vote = pl.when(pl.col("donchian_pos55") > 0.5).then(1.0).when(pl.col("donchian_pos55") < -0.5).then(-1.0).otherwise(0.0)
        score = trend_vote + macd_vote + rsi_vote + stoch_vote + bb_vote + dmi_vote + donchian_vote
        raw = pl.when(score >= threshold).then(1.0).when(score <= -threshold).then(-1.0).otherwise(0.0)
    elif kind == "stockstats_vote":
        threshold = float(p.get("threshold", 3.0))
        trend_vote = (
            pl.when((pl.col("ss_close_50_sma") > pl.col("ss_close_200_sma")) & (pl.col("close") > pl.col("ss_close_20_sma"))).then(1.0)
            .when((pl.col("ss_close_50_sma") < pl.col("ss_close_200_sma")) & (pl.col("close") < pl.col("ss_close_20_sma"))).then(-1.0)
            .otherwise(0.0)
        )
        ema_vote = (
            pl.when((pl.col("ss_close_10_ema") > pl.col("ss_close_50_ema")) & (pl.col("ss_close_20_ema") > pl.col("ss_close_120_ema"))).then(1.0)
            .when((pl.col("ss_close_10_ema") < pl.col("ss_close_50_ema")) & (pl.col("ss_close_20_ema") < pl.col("ss_close_120_ema"))).then(-1.0)
            .otherwise(0.0)
        )
        macd_vote = pl.when(pl.col("ss_macdh") > 0).then(1.0).when(pl.col("ss_macdh") < 0).then(-1.0).otherwise(0.0)
        rsi_vote = pl.when(pl.col("ss_rsi") > 55).then(1.0).when(pl.col("ss_rsi") < 45).then(-1.0).otherwise(0.0)
        kdj_vote = pl.when(pl.col("ss_kdjk") > pl.col("ss_kdjd")).then(1.0).when(pl.col("ss_kdjk") < pl.col("ss_kdjd")).then(-1.0).otherwise(0.0)
        adx_vote = (
            pl.when((pl.col("ss_pdi") > pl.col("ss_ndi")) & (pl.col("ss_adx") > 18)).then(1.0)
            .when((pl.col("ss_ndi") > pl.col("ss_pdi")) & (pl.col("ss_adx") > 18)).then(-1.0)
            .otherwise(0.0)
        )
        cci_vote = pl.when(pl.col("ss_cci") > 50).then(1.0).when(pl.col("ss_cci") < -50).then(-1.0).otherwise(0.0)
        wr_vote = pl.when(pl.col("ss_wr_14") > -20).then(1.0).when(pl.col("ss_wr_14") < -80).then(-1.0).otherwise(0.0)
        boll_vote = (
            pl.when(pl.col("close") > pl.col("ss_boll")).then(1.0)
            .when(pl.col("close") < pl.col("ss_boll")).then(-1.0)
            .otherwise(0.0)
        )
        mfi_vote = pl.when(pl.col("ss_mfi") > 55).then(1.0).when(pl.col("ss_mfi") < 45).then(-1.0).otherwise(0.0)
        score = trend_vote + ema_vote + macd_vote + rsi_vote + kdj_vote + adx_vote + cci_vote + wr_vote + boll_vote + mfi_vote
        raw = pl.when(score >= threshold).then(1.0).when(score <= -threshold).then(-1.0).otherwise(0.0)
    elif kind == "stockstats_chip_vote":
        threshold = float(p.get("threshold", 3.0))
        chip_z = float(p.get("chip_z", 0.2))
        trend_vote = (
            pl.when((pl.col("ss_close_50_sma") > pl.col("ss_close_200_sma")) & (pl.col("close") > pl.col("ss_close_20_sma"))).then(1.0)
            .when((pl.col("ss_close_50_sma") < pl.col("ss_close_200_sma")) & (pl.col("close") < pl.col("ss_close_20_sma"))).then(-1.0)
            .otherwise(0.0)
        )
        macd_vote = pl.when(pl.col("ss_macdh") > 0).then(1.0).when(pl.col("ss_macdh") < 0).then(-1.0).otherwise(0.0)
        adx_vote = (
            pl.when((pl.col("ss_pdi") > pl.col("ss_ndi")) & (pl.col("ss_adx") > 18)).then(1.0)
            .when((pl.col("ss_ndi") > pl.col("ss_pdi")) & (pl.col("ss_adx") > 18)).then(-1.0)
            .otherwise(0.0)
        )
        rsi_vote = pl.when(pl.col("ss_rsi") > 55).then(1.0).when(pl.col("ss_rsi") < 45).then(-1.0).otherwise(0.0)
        cci_vote = pl.when(pl.col("ss_cci") > 50).then(1.0).when(pl.col("ss_cci") < -50).then(-1.0).otherwise(0.0)
        chip_vote = pl.when(pl.col("chip_score") > chip_z).then(1.0).when(pl.col("chip_score") < -chip_z).then(-1.0).otherwise(0.0)
        basis_vote = pl.when(pl.col("basis_z63") < -0.75).then(1.0).when(pl.col("basis_z63") > 0.75).then(-1.0).otherwise(0.0)
        score = trend_vote + macd_vote + adx_vote + rsi_vote + cci_vote + chip_vote + basis_vote
        raw = pl.when(score >= threshold).then(1.0).when(score <= -threshold).then(-1.0).otherwise(0.0)
    elif kind == "technical_chip_vote":
        threshold = float(p.get("threshold", 2.0))
        chip_z = float(p.get("chip_z", 0.2))
        trend_vote = (
            pl.when((pl.col("ma50") > pl.col("ma200")) & (pl.col("continuous_close") > pl.col("ma50"))).then(1.0)
            .when((pl.col("ma50") < pl.col("ma200")) & (pl.col("continuous_close") < pl.col("ma50"))).then(-1.0)
            .otherwise(0.0)
        )
        macd_vote = pl.when(pl.col("macd_hist") > 0).then(1.0).when(pl.col("macd_hist") < 0).then(-1.0).otherwise(0.0)
        rsi_vote = pl.when(pl.col("rsi14") > 55).then(1.0).when(pl.col("rsi14") < 45).then(-1.0).otherwise(0.0)
        dmi_vote = (
            pl.when((pl.col("plus_di14") > pl.col("minus_di14")) & (pl.col("adx14") > 18)).then(1.0)
            .when((pl.col("minus_di14") > pl.col("plus_di14")) & (pl.col("adx14") > 18)).then(-1.0)
            .otherwise(0.0)
        )
        basis_vote = pl.when(pl.col("basis_z63") < -0.75).then(1.0).when(pl.col("basis_z63") > 0.75).then(-1.0).otherwise(0.0)
        chip_vote = pl.when(pl.col("chip_score") > chip_z).then(1.0).when(pl.col("chip_score") < -chip_z).then(-1.0).otherwise(0.0)
        score = trend_vote + macd_vote + rsi_vote + dmi_vote + basis_vote + chip_vote
        raw = pl.when(score >= threshold).then(1.0).when(score <= -threshold).then(-1.0).otherwise(0.0)
    elif kind == "rpt_session_momentum":
        z = float(p.get("entry_z", 0.6))
        rv_gate = float(p.get("rv_gate", -0.5))
        score = (
            0.40 * pl.col("rpt_5m_total_ret_z").fill_null(0.0)
            + 0.25 * pl.col("rpt_5m_regular_ret_z").fill_null(0.0)
            + 0.20 * pl.col("rpt_5m_last60_ret_z").fill_null(0.0)
            + 0.15 * pl.col("rpt_15m_total_ret_z").fill_null(0.0)
        )
        active = pl.col("rpt_5m_rv_z").fill_null(-9.0) >= rv_gate
        raw = pl.when((score > z) & active).then(1.0).when((score < -z) & active).then(-1.0).otherwise(0.0)
    elif kind == "rpt_overnight_reversal":
        z = float(p.get("entry_z", 0.8))
        trend_filter = float(p.get("trend_filter", 0.0))
        daily_trend = (pl.col("continuous_close") / pl.col("ma50") - 1.0).fill_null(0.0)
        raw = (
            pl.when((pl.col("rpt_5m_night_ret_z") < -z) & (daily_trend >= -trend_filter)).then(1.0)
            .when((pl.col("rpt_5m_night_ret_z") > z) & (daily_trend <= trend_filter)).then(-1.0)
            .otherwise(0.0)
        )
    elif kind == "rpt_opening_range_follow":
        z = float(p.get("entry_z", 0.7))
        min_rv_z = float(p.get("min_rv_z", -0.5))
        score = 0.70 * pl.col("rpt_5m_first30_ret_z").fill_null(0.0) + 0.30 * pl.col("rpt_5m_total_ret_z").fill_null(0.0)
        active = pl.col("rpt_5m_rv_z").fill_null(-9.0) >= min_rv_z
        raw = pl.when((score > z) & active).then(1.0).when((score < -z) & active).then(-1.0).otherwise(0.0)
    elif kind == "rpt_late_day_follow":
        z = float(p.get("entry_z", 0.6))
        range_gate = float(p.get("range_gate", -0.5))
        score = 0.65 * pl.col("rpt_5m_last60_ret_z").fill_null(0.0) + 0.35 * pl.col("rpt_15m_last60_ret_z").fill_null(0.0)
        active = pl.col("rpt_5m_range_z").fill_null(-9.0) >= range_gate
        raw = pl.when((score > z) & active).then(1.0).when((score < -z) & active).then(-1.0).otherwise(0.0)
    elif kind == "rpt_session_divergence":
        z = float(p.get("entry_z", 0.8))
        # When night and regular sessions strongly disagree, bet on the regular
        # session only if the late-day tape confirms it; otherwise mean-revert
        # the overnight excess. This is still fully lagged at execution.
        divergence = pl.col("rpt_5m_regular_ret_z").fill_null(0.0) - pl.col("rpt_5m_night_ret_z").fill_null(0.0)
        late_confirm = pl.col("rpt_5m_last60_ret_z").fill_null(0.0)
        raw = (
            pl.when((divergence > z) & (late_confirm > 0)).then(1.0)
            .when((divergence < -z) & (late_confirm < 0)).then(-1.0)
            .when((pl.col("rpt_5m_night_ret_z") > z) & (late_confirm < 0)).then(-1.0)
            .when((pl.col("rpt_5m_night_ret_z") < -z) & (late_confirm > 0)).then(1.0)
            .otherwise(0.0)
        )
    elif kind == "rpt_multiframe_vote":
        threshold = float(p.get("threshold", 2.0))
        daily_vote = (
            pl.when((pl.col("ma50") > pl.col("ma200")) & (pl.col("continuous_close") > pl.col("ma20"))).then(1.0)
            .when((pl.col("ma50") < pl.col("ma200")) & (pl.col("continuous_close") < pl.col("ma20"))).then(-1.0)
            .otherwise(0.0)
        )
        total_vote = pl.when(pl.col("rpt_5m_total_ret_z") > 0.5).then(1.0).when(pl.col("rpt_5m_total_ret_z") < -0.5).then(-1.0).otherwise(0.0)
        first_vote = pl.when(pl.col("rpt_5m_first30_ret_z") > 0.5).then(1.0).when(pl.col("rpt_5m_first30_ret_z") < -0.5).then(-1.0).otherwise(0.0)
        late_vote = pl.when(pl.col("rpt_5m_last60_ret_z") > 0.5).then(1.0).when(pl.col("rpt_5m_last60_ret_z") < -0.5).then(-1.0).otherwise(0.0)
        flow_vote = pl.when(pl.col("chip_score") > 0.25).then(1.0).when(pl.col("chip_score") < -0.25).then(-1.0).otherwise(0.0)
        score = daily_vote + total_vote + first_vote + late_vote + flow_vote
        raw = pl.when(score >= threshold).then(1.0).when(score <= -threshold).then(-1.0).otherwise(0.0)
    else:
        raise ValueError(f"unknown futures strategy kind: {kind}")

    return (
        f.with_columns(
            [
                raw.alias("raw_signal"),
                pl.col("atr14").fill_null(pl.col("close") * 0.01).alias("atr"),
            ]
        )
        .with_columns(pl.col("raw_signal").shift(1).fill_null(0.0).alias("signal"))
        .select(["date", "signal", "raw_signal", "atr"])
        .sort("date")
    )


def default_candidate_grid() -> list[StrategyCandidate]:
    candidates: list[StrategyCandidate] = []
    for product in ["TX", "MTX", "TMF"]:
        for lb in [40, 80, 120, 200]:
            candidates.append(StrategyCandidate(f"{product}_trend_lb{lb}", product, "trend", {"lookback": lb, "ma": lb, "threshold": 0.0}))
        for lb in [40, 80, 120]:
            candidates.append(StrategyCandidate(f"{product}_breakout_lb{lb}", product, "breakout", {"lookback": lb}))
        for lb in [10, 20, 40]:
            candidates.append(StrategyCandidate(f"{product}_meanrev_lb{lb}", product, "mean_reversion", {"lookback": lb, "entry_z": 1.5}))
    for z in [0.75, 1.0, 1.25, 1.5]:
        candidates.append(StrategyCandidate(f"TX_basis_z{z}", "TX", "basis", {"entry_z": z}))
        candidates.append(StrategyCandidate(f"TX_term_z{z}", "TX", "term", {"entry_z": z}))
    for z in [0.5, 0.8, 1.0]:
        candidates.append(StrategyCandidate(f"TX_flow_z{z}", "TX", "flow", {"entry_z": z}))
        candidates.append(StrategyCandidate(f"TX_cross_market_z{z}", "TX", "cross_market", {"lookback": 20, "entry_z": z}))
    for product in ["TX", "MTX"]:
        for fast, slow in [(20, 120), (40, 200), (80, 240)]:
            candidates.append(StrategyCandidate(f"{product}_dual_mom_f{fast}_s{slow}", product, "dual_momentum", {"fast": fast, "slow": slow, "momentum": fast * 2}))
        for trigger in [0.03, 0.05, 0.07]:
            candidates.append(StrategyCandidate(f"{product}_panic_rebound_{int(trigger*100)}", product, "panic_rebound", {"lookback": 5, "trigger": trigger, "ma": 200}))
    for z in [0.75, 1.0, 1.25]:
        candidates.append(StrategyCandidate(f"TX_basis_trend_z{z}", "TX", "basis_trend", {"entry_z": z, "ma": 120}))
    for z in [0.5, 0.75, 1.0, 1.25]:
        for min_volume_z in [-1.0, -0.5, 0.0]:
            candidates.append(StrategyCandidate(f"TX_h_model_z{z}_v{min_volume_z}", "TX", "h_model", {"entry_z": z, "min_volume_z": min_volume_z}))
            candidates.append(
                StrategyCandidate(
                    f"TX_h_model_flow_z{z}_v{min_volume_z}",
                    "TX",
                    "h_model_flow_filter",
                    {"entry_z": z, "min_volume_z": min_volume_z, "min_chip": -0.5},
                )
            )
    for z in [0.25, 0.5, 0.75, 1.0]:
        candidates.append(StrategyCandidate(f"TX_chip_composite_z{z}", "TX", "chip_composite", {"entry_z": z}))
    for lb in [40, 55, 80]:
        for z in [0.2, 0.5]:
            candidates.append(StrategyCandidate(f"TX_chip_breakout_lb{lb}_z{z}", "TX", "chip_breakout", {"lookback": lb, "entry_z": z}))
    for product in ["TX", "MTX"]:
        for threshold in [2.0, 3.0, 4.0]:
            candidates.append(StrategyCandidate(f"{product}_technical_vote_t{threshold:g}", product, "technical_vote", {"threshold": threshold}))
            candidates.append(StrategyCandidate(f"{product}_technical_chip_vote_t{threshold:g}", product, "technical_chip_vote", {"threshold": threshold, "chip_z": 0.2}))
        for threshold in [3.0, 4.0, 5.0, 6.0]:
            candidates.append(StrategyCandidate(f"{product}_stockstats_vote_t{threshold:g}", product, "stockstats_vote", {"threshold": threshold}))
            candidates.append(StrategyCandidate(f"{product}_stockstats_chip_vote_t{threshold:g}", product, "stockstats_chip_vote", {"threshold": threshold, "chip_z": 0.2}))
    for product in ["TX", "MTX", "TMF"]:
        for z in [0.5, 0.75, 1.0, 1.25]:
            candidates.append(StrategyCandidate(f"{product}_rpt_session_mom_z{z}", product, "rpt_session_momentum", {"entry_z": z, "rv_gate": -0.5}))
            candidates.append(StrategyCandidate(f"{product}_rpt_overnight_rev_z{z}", product, "rpt_overnight_reversal", {"entry_z": z, "trend_filter": 0.02}))
            candidates.append(StrategyCandidate(f"{product}_rpt_opening_follow_z{z}", product, "rpt_opening_range_follow", {"entry_z": z, "min_rv_z": -0.5}))
            candidates.append(StrategyCandidate(f"{product}_rpt_late_follow_z{z}", product, "rpt_late_day_follow", {"entry_z": z, "range_gate": -0.5}))
            candidates.append(StrategyCandidate(f"{product}_rpt_session_div_z{z}", product, "rpt_session_divergence", {"entry_z": z}))
        for threshold in [2.0, 3.0, 4.0]:
            candidates.append(StrategyCandidate(f"{product}_rpt_multiframe_vote_t{threshold:g}", product, "rpt_multiframe_vote", {"threshold": threshold}))
    return candidates
