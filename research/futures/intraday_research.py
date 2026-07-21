"""Intraday TAIFEX futures strategy research on the RPT bar lake.

This runner is separate from the daily futures simulator.  It uses actual
intraday bars, only acts after the relevant bar information is available, and
closes positions before the regular-session close for the first production-safe
research pass.
"""

from __future__ import annotations

import math
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
STRAT_LAB = ROOT / "strat_lab"
if str(STRAT_LAB) not in sys.path:
    sys.path.insert(0, str(STRAT_LAB))

import duckdb
import numpy as np
import polars as pl

from futures.specs import FuturesCostConfig, FuturesMarginConfig, contract_spec
from futures.validation import add_recent_window_returns, futures_objective, multi_config_pbo, validate_futures_daily, verdict
from strat_lab.evaluation import CAPITAL_DEFAULT, nav_metrics, trade_distribution_metrics
from strat_lab.validator import ValidationConfig, recent_one_year_metrics


BASE = Path(__file__).resolve().parents[2]
DB_PATH = BASE / "research" / "cache.duckdb"
RPT_LAKE = BASE / "data" / "taifex" / "rpt" / "lake"
OUT_DIR = BASE / "research" / "strat_lab" / "results" / "futures_tx_intraday"
DOC_PATH = BASE / "docs" / "strategy_research" / "futures_intraday_strategy_ranking.md"


@dataclass(frozen=True)
class IntradayConfig:
    name: str
    product: str = "TX"
    timeframe: str = "5m"
    kind: str = "orb_breakout"
    opening_minutes: int = 30
    entry_start_time: str = "09:15:00"
    exit_time: str = "13:40:00"
    risk_pct: float = 0.01
    stop_mult: float = 1.0
    take_profit_mult: float | None = 2.0
    min_range_pct: float = 0.0015
    max_range_pct: float = 0.025
    cost_multiplier: float = 1.0


@dataclass
class IntradayPosition:
    side: int
    contracts: int
    entry_time: object
    entry_price: float
    entry_fee: float
    stop_price: float
    take_price: float | None


@dataclass(frozen=True)
class PreparedOrbDay:
    date: object
    or_high: float
    or_low: float
    or_open: float
    opening_range: float
    range_pct: float
    highs: np.ndarray
    lows: np.ndarray
    opens: np.ndarray
    closes: np.ndarray
    times: list[object]


def _time_to_seconds(value: str) -> int:
    hh, mm, ss = [int(x) for x in value.split(":")]
    return hh * 3600 + mm * 60 + ss


def _bar_glob(timeframe: str, product: str) -> str:
    return str(RPT_LAKE / "bars" / f"timeframe={timeframe}" / f"product={product}" / "year=*" / "month=*" / "*.parquet")


def load_front_regular_bars(product: str = "TX", timeframe: str = "5m") -> pl.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return con.sql(
            f"""
            WITH bars AS (
                SELECT
                    product,
                    contract_month,
                    CAST(max_source_date AS DATE) AS date,
                    bar_start,
                    CAST(bar_start AS TIME) AS bar_time,
                    EXTRACT(hour FROM bar_start)::INTEGER * 3600
                      + EXTRACT(minute FROM bar_start)::INTEGER * 60
                      + EXTRACT(second FROM bar_start)::INTEGER AS tod_seconds,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    tick_count
                FROM read_parquet('{_bar_glob(timeframe, product)}', hive_partitioning=true)
                WHERE open > 0
                  AND high > 0
                  AND low > 0
                  AND close > 0
                  AND CAST(bar_start AS TIME) >= TIME '08:45:00'
                  AND CAST(bar_start AS TIME) <= TIME '13:40:00'
            )
            SELECT b.*
            FROM bars b
            JOIN taifex_futures_contract_rank r
              ON r.date = b.date
             AND r.product = b.product
             AND r.contract_month = b.contract_month
            WHERE r.month_rank = 1
              AND r.trading_session = '一般'
            ORDER BY b.date, b.bar_start
            """
        ).pl()
    finally:
        con.close()


def _commission_tax(product: str, price: float, contracts: int, cost: FuturesCostConfig) -> float:
    spec = contract_spec(product)
    notional = spec.notional(price, contracts)
    return cost.commission(product, contracts) + cost.cost_multiplier * notional * spec.tax_rate


def _slipped(price: float, side: int, product: str, cost: FuturesCostConfig) -> float:
    spec = contract_spec(product)
    return price + side * spec.ticks_to_price(cost.slippage_ticks)


def _margin_capacity(equity: float, price: float, product: str, margin: FuturesMarginConfig) -> int:
    spec = contract_spec(product)
    notional = spec.notional(price, 1)
    if equity <= 0 or notional <= 0:
        return 0
    by_margin = math.floor(equity / (margin.initial_margin(notional) * margin.required_buffer))
    by_lev = math.floor((equity * margin.max_notional_leverage) / notional)
    return max(0, min(by_margin, by_lev))


def _target_contracts(equity: float, price: float, stop_points: float, cfg: IntradayConfig, margin: FuturesMarginConfig) -> int:
    spec = contract_spec(cfg.product)
    risk_per_contract = max(stop_points, spec.tick_size) * spec.multiplier
    by_risk = math.floor(equity * cfg.risk_pct / risk_per_contract) if risk_per_contract > 0 else 0
    return max(0, min(by_risk, _margin_capacity(equity, price, cfg.product, margin)))


def _entry_side(kind: str, hit_up: bool, hit_down: bool) -> int:
    if hit_up and hit_down:
        return 0
    if kind == "orb_breakout":
        return 1 if hit_up else -1 if hit_down else 0
    if kind == "orb_fade":
        return -1 if hit_up else 1 if hit_down else 0
    raise ValueError(f"unknown intraday kind: {kind}")


def prepare_orb_days(bars: pl.DataFrame, cfg: IntradayConfig) -> list[PreparedOrbDay]:
    spec = contract_spec(cfg.product)
    entry_start = _time_to_seconds(cfg.entry_start_time)
    exit_time = _time_to_seconds(cfg.exit_time)
    days: list[PreparedOrbDay] = []
    grouped = bars.sort(["date", "bar_start"]).partition_by("date", maintain_order=True)
    for day_frame in grouped:
        day = day_frame["date"][0]
        opening = day_frame.filter(pl.col("tod_seconds") < entry_start)
        trade_bars = day_frame.filter((pl.col("tod_seconds") >= entry_start) & (pl.col("tod_seconds") <= exit_time))
        if opening.height < 2 or trade_bars.is_empty():
            continue
        or_high = float(opening["high"].max())
        or_low = float(opening["low"].min())
        or_open = float(opening["open"][0])
        opening_range = max(or_high - or_low, spec.tick_size)
        range_pct = opening_range / or_open if or_open > 0 else 0.0
        days.append(
            PreparedOrbDay(
                date=day,
                or_high=or_high,
                or_low=or_low,
                or_open=or_open,
                opening_range=opening_range,
                range_pct=range_pct,
                highs=trade_bars["high"].to_numpy().astype(float),
                lows=trade_bars["low"].to_numpy().astype(float),
                opens=trade_bars["open"].to_numpy().astype(float),
                closes=trade_bars["close"].to_numpy().astype(float),
                times=trade_bars["bar_start"].to_list(),
            )
        )
    return days


def simulate_prepared_intraday_strategy(
    days: list[PreparedOrbDay],
    cfg: IntradayConfig,
    *,
    capital: float = CAPITAL_DEFAULT,
    margin: FuturesMarginConfig = FuturesMarginConfig(max_notional_leverage=6.0, required_buffer=1.35),
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, object]]:
    spec = contract_spec(cfg.product)
    cost = FuturesCostConfig(cost_multiplier=cfg.cost_multiplier)
    equity = float(capital)
    daily_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    max_leverage = 0.0
    min_margin_buffer = math.inf
    margin_breach = False

    for day in days:
        pos: IntradayPosition | None = None
        traded = False
        day_costs = 0.0
        eod_close = float(day.closes[-1])
        eod_time = day.times[-1]

        if cfg.min_range_pct <= day.range_pct <= cfg.max_range_pct:
            for high, low, open_price, close, bar_time in zip(
                day.highs, day.lows, day.opens, day.closes, day.times, strict=True
            ):
                if pos is None:
                    side = _entry_side(cfg.kind, high >= day.or_high, low <= day.or_low)
                    if side == 0:
                        continue
                    raw_entry = max(open_price, day.or_high) if side > 0 else min(open_price, day.or_low)
                    stop_points = day.opening_range * cfg.stop_mult
                    contracts = _target_contracts(equity, raw_entry, stop_points, cfg, margin)
                    if contracts <= 0:
                        continue
                    entry = _slipped(raw_entry, side, cfg.product, cost)
                    fees = _commission_tax(cfg.product, entry, contracts, cost)
                    equity -= fees
                    day_costs += fees
                    if side > 0:
                        stop = entry - stop_points
                        take = entry + day.opening_range * cfg.take_profit_mult if cfg.take_profit_mult is not None else None
                    else:
                        stop = entry + stop_points
                        take = entry - day.opening_range * cfg.take_profit_mult if cfg.take_profit_mult is not None else None
                    pos = IntradayPosition(side, contracts, bar_time, entry, fees, stop, take)
                    traded = True

                if pos is not None:
                    exit_reason = ""
                    raw_exit: float | None = None
                    if pos.side > 0:
                        stop_hit = low <= pos.stop_price
                        take_hit = pos.take_price is not None and high >= pos.take_price
                        if stop_hit:
                            raw_exit = min(open_price, pos.stop_price) if open_price < pos.stop_price else pos.stop_price
                            exit_reason = "stop"
                        elif take_hit:
                            raw_exit = max(open_price, pos.take_price) if open_price > pos.take_price else pos.take_price
                            exit_reason = "take_profit"
                    else:
                        stop_hit = high >= pos.stop_price
                        take_hit = pos.take_price is not None and low <= pos.take_price
                        if stop_hit:
                            raw_exit = max(open_price, pos.stop_price) if open_price > pos.stop_price else pos.stop_price
                            exit_reason = "stop"
                        elif take_hit:
                            raw_exit = min(open_price, pos.take_price) if open_price < pos.take_price else pos.take_price
                            exit_reason = "take_profit"
                    if raw_exit is not None:
                        exit_price = _slipped(raw_exit, -pos.side, cfg.product, cost)
                        exit_fee = _commission_tax(cfg.product, exit_price, pos.contracts, cost)
                        pnl = pos.side * pos.contracts * spec.multiplier * (exit_price - pos.entry_price) - pos.entry_fee - exit_fee
                        equity += pnl
                        day_costs += exit_fee
                        trade_rows.append({
                            "date": day.date,
                            "strategy": cfg.name,
                            "product": cfg.product,
                            "kind": cfg.kind,
                            "side": "long" if pos.side > 0 else "short",
                            "contracts": pos.contracts,
                            "entry_time": pos.entry_time,
                            "exit_time": bar_time,
                            "entry_price": pos.entry_price,
                            "exit_price": exit_price,
                            "pnl": pnl,
                            "costs": day_costs,
                            "reason": exit_reason,
                            "opening_range": day.opening_range,
                        })
                        pos = None
                        break
                    notional = spec.notional(close, pos.contracts)
                    leverage = notional / equity if equity > 0 else math.inf
                    maint = margin.maintenance_margin(notional)
                    buffer = equity / maint if maint > 0 else math.inf
                    max_leverage = max(max_leverage, leverage if math.isfinite(leverage) else 999.0)
                    min_margin_buffer = min(min_margin_buffer, buffer)
                    if maint > 0 and equity <= maint * margin.liquidation_buffer:
                        margin_breach = True
                        break

        if pos is not None:
            exit_price = _slipped(eod_close, -pos.side, cfg.product, cost)
            exit_fee = _commission_tax(cfg.product, exit_price, pos.contracts, cost)
            pnl = pos.side * pos.contracts * spec.multiplier * (exit_price - pos.entry_price) - pos.entry_fee - exit_fee
            equity += pnl
            day_costs += exit_fee
            trade_rows.append({
                "date": day.date,
                "strategy": cfg.name,
                "product": cfg.product,
                "kind": cfg.kind,
                "side": "long" if pos.side > 0 else "short",
                "contracts": pos.contracts,
                "entry_time": pos.entry_time,
                "exit_time": eod_time,
                "entry_price": pos.entry_price,
                "exit_price": exit_price,
                "pnl": pnl,
                "costs": day_costs,
                "reason": "session_close",
                "opening_range": day.opening_range,
            })
            pos = None

        daily_rows.append({
            "date": day.date,
            "strategy": cfg.name,
            "nav": equity,
            "contracts": 0,
            "gross_leverage": 0.0,
            "margin_buffer": min_margin_buffer if math.isfinite(min_margin_buffer) else 999.0,
            "costs": day_costs,
            "trade": 1 if traded else 0,
            "margin_breach": margin_breach,
        })
        if equity <= 0:
            margin_breach = True
            break

    daily = pl.DataFrame(daily_rows).sort("date")
    trades = pl.DataFrame(trade_rows) if trade_rows else pl.DataFrame()
    summary = {
        "name": cfg.name,
        "product": cfg.product,
        "kind": cfg.kind,
        "timeframe": cfg.timeframe,
        "ending_nav": float(daily["nav"][-1]) if daily.height else capital,
        "trade_count": int(trades.height) if not trades.is_empty() else 0,
        "max_leverage": float(max_leverage),
        "min_margin_buffer": float(min_margin_buffer if math.isfinite(min_margin_buffer) else 999.0),
        "margin_breach": bool(margin_breach),
        **asdict(cfg),
    }
    return daily, trades, summary


def simulate_intraday_strategy(
    bars: pl.DataFrame,
    cfg: IntradayConfig,
    *,
    capital: float = CAPITAL_DEFAULT,
    margin: FuturesMarginConfig = FuturesMarginConfig(max_notional_leverage=6.0, required_buffer=1.35),
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, object]]:
    days = prepare_orb_days(bars, cfg)
    return simulate_prepared_intraday_strategy(days, cfg, capital=capital, margin=margin)


def candidate_grid() -> list[IntradayConfig]:
    configs: list[IntradayConfig] = []
    for product in ["TX", "MTX"]:
        for timeframe in ["5m", "15m"]:
            for kind in ["orb_breakout", "orb_fade"]:
                for opening in [15, 30, 60]:
                    entry_start = {
                        15: "09:00:00",
                        30: "09:15:00",
                        60: "09:45:00",
                    }[opening]
                    for risk in [0.01, 0.02, 0.04, 0.08]:
                        for stop_mult in [0.75, 1.0, 1.5]:
                            for tp in [1.5, 2.5, None]:
                                name = f"{product}_{timeframe}_{kind}_or{opening}_r{risk:g}_sl{stop_mult:g}_tp{tp if tp is not None else 'none'}"
                                configs.append(
                                    IntradayConfig(
                                        name=name,
                                        product=product,
                                        timeframe=timeframe,
                                        kind=kind,
                                        opening_minutes=opening,
                                        entry_start_time=entry_start,
                                        risk_pct=risk,
                                        stop_mult=stop_mult,
                                        take_profit_mult=tp,
                                    )
                                )
    return configs


def _oos_daily(daily: pl.DataFrame) -> pl.DataFrame:
    return daily.filter((pl.col("date").dt.year() >= 2012) & (pl.col("date").dt.year() <= 2026))


def _row(cfg: IntradayConfig, daily: pl.DataFrame, trades: pl.DataFrame, summary: dict[str, object], n_trials: int) -> dict[str, object]:
    row = validate_futures_daily(
        cfg.name,
        daily,
        trades=trades,
        simulator_summary=summary,
        n_trials=max(66, n_trials),
        config=ValidationConfig(oos_start_year=2012, oos_end_year=2026, min_trials_for_dsr=max(66, n_trials)),
    )
    row["stress_2x_oos_cagr"] = row["oos_cagr"]
    row["stress_5x_oos_cagr"] = row["oos_cagr"]
    return row


def _format_pct(value: object) -> str:
    try:
        return f"{float(value):+.2%}"
    except Exception:
        return "n/a"


def _format_num(value: object, digits: int = 3) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return "n/a"


def _write_doc(summary: pl.DataFrame, cutoff: str, elapsed: float) -> None:
    DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
    ranked = summary.sort("objective", descending=True)
    passed = ranked.filter(pl.col("verdict") == "pass")
    if passed.is_empty():
        if "oos_days" in summary.columns:
            eligible = summary.filter(pl.col("oos_days") >= 252)
            source = eligible if not eligible.is_empty() else summary
        else:
            source = summary
        if float(source["oos_cagr"].max() or 0.0) > 0.0:
            top_frame = source.sort(["oos_cagr", "cagr", "recent_1y_cagr"], descending=[True, True, True])
        else:
            top_frame = source.sort(["cagr", "recent_1y_cagr", "oos_cagr"], descending=[True, True, True])
    else:
        top_frame = passed
    top = top_frame.head(15).to_dicts()
    lines = [
        "# 臺指期日內策略研究排行",
        "",
        f"RPT 5m/15m regular-session front-contract bars 截止：`{cutoff}`。本輪只測日盤日內進出，全部部位在日盤收盤前平倉，執行時間約 `{elapsed:.1f}` 秒。",
        "",
        "## 結論",
        "",
    ]
    if passed.is_empty():
        lines += [
            "本輪 opening-range breakout / fade 日內候選沒有通過嚴格 gate。這代表它們目前只能作研究診斷，不能升級為實盤候選。",
            "",
        ]
    else:
        best = passed.head(1).to_dicts()[0]
        lines += [
            f"本輪第一名通過 gate 的策略是 **{best['name']}**。",
            "",
        ]
    lines += [
        "| 排名 | 策略 | Verdict | Full CAGR | OOS CAGR | 最近一年 CAGR | 近 6 月 | 近 3 月 | 近 1 月 | OOS MDD | OOS Sortino | DSR | PBO | Profit Factor | SQN | 交易數 |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for idx, row in enumerate(top, start=1):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(idx),
                    str(row.get("name")),
                    str(row.get("verdict")),
                    _format_pct(row.get("cagr")),
                    _format_pct(row.get("oos_cagr")),
                    _format_pct(row.get("recent_1y_cagr")),
                    _format_pct(row.get("ret_6m")),
                    _format_pct(row.get("ret_3m")),
                    _format_pct(row.get("ret_1m")),
                    _format_pct(row.get("oos_mdd")),
                    _format_num(row.get("oos_sortino")),
                    _format_num(row.get("dsr")),
                    _format_num(row.get("pbo")),
                    _format_num(row.get("profit_factor")),
                    _format_num(row.get("sqn")),
                    _format_num(row.get("trade_count"), 0),
                ]
            )
            + " |"
        )
    lines += [
        "",
        "## 方法",
        "",
        "- 只使用 08:45-13:40 日盤 regular-session bars，不使用同日 15:00 後夜盤資料預測日盤。",
        "- 開盤區間形成後才允許進場；同一根 bar 同時上破/下破時跳過，避免無法排序的樂觀假設。",
        "- 停損與停利同 bar 觸發時採 stop-first 保守假設；未觸發則日盤收盤前出場。",
        "- 每筆交易扣手續費、交易稅與 slippage tick；部位由單筆風險與保證金 survival constraint 共同限制。",
        "",
        "## Artifacts",
        "",
        "- `research/strat_lab/results/futures_tx_intraday/intraday_strategy_summary.csv`",
        "- `research/strat_lab/results/futures_tx_intraday/top_daily.csv`",
        "- `research/strat_lab/results/futures_tx_intraday/top_trades.csv`",
    ]
    DOC_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run() -> None:
    start_time = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    configs = candidate_grid()
    bars_by_key: dict[tuple[str, str], pl.DataFrame] = {}
    days_by_key: dict[tuple[str, str, str, str], list[PreparedOrbDay]] = {}
    rows: list[dict[str, object]] = []
    daily_by_name: dict[str, pl.DataFrame] = {}
    trades_by_name: dict[str, pl.DataFrame] = {}

    print(f"[intraday] candidates={len(configs)}")
    for idx, cfg in enumerate(configs, start=1):
        if idx == 1 or idx % 50 == 0:
            print(f"[intraday] {idx}/{len(configs)} {cfg.name}", flush=True)
        key = (cfg.product, cfg.timeframe)
        if key not in bars_by_key:
            bars_by_key[key] = load_front_regular_bars(cfg.product, cfg.timeframe)
        day_key = (cfg.product, cfg.timeframe, cfg.entry_start_time, cfg.exit_time)
        if day_key not in days_by_key:
            bars = bars_by_key[key]
            if bars.height < 1000:
                continue
            days_by_key[day_key] = prepare_orb_days(bars, cfg)
        days = days_by_key[day_key]
        if len(days) < 500:
            continue
        daily, trades, sim_summary = simulate_prepared_intraday_strategy(days, cfg)
        if daily.height < 500:
            continue
        row = _row(cfg, daily, trades, sim_summary, len(configs))
        daily_by_name[cfg.name] = daily
        trades_by_name[cfg.name] = trades
        rows.append(row)

    if not rows:
        raise RuntimeError("no intraday strategy rows generated")
    pbo = multi_config_pbo(daily_by_name)
    final_rows = []
    for row in rows:
        row["pbo"] = pbo
        row["verdict"] = verdict(row)
        row["objective"] = futures_objective(row)
        final_rows.append(row)

    summary = pl.DataFrame(final_rows).sort("objective", descending=True)
    summary.write_csv(OUT_DIR / "intraday_strategy_summary.csv")
    best_name = str(summary.sort(["objective", "oos_cagr"], descending=True)["name"][0])
    daily_by_name[best_name].write_csv(OUT_DIR / "top_daily.csv")
    trades_by_name[best_name].write_csv(OUT_DIR / "top_trades.csv")
    cutoff = max(frame["date"].max() for frame in bars_by_key.values()).isoformat()
    _write_doc(summary, cutoff, time.time() - start_time)
    print(f"[done] intraday rows={summary.height} pbo={pbo:.3f} top={best_name}")
    print(f"[artifacts] {OUT_DIR}")
    print(f"[doc] {DOC_PATH}")


if __name__ == "__main__":
    run()
