"""Multi-timeframe intraday TAIFEX futures research.

This runner uses real RPT-derived intraday bars, not daily open/close proxies.
Signals are formed only after a decision bar has closed; execution happens at
the next bar open with slippage, and all positions are flat before the regular
session close.
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
OUT_DIR = BASE / "research" / "strat_lab" / "results" / "futures_tx_intraday_mtf"
DOC_PATH = BASE / "docs" / "strategy_research" / "futures_intraday_mtf_strategy_ranking.md"
CAPITAL = 1_000_000.0


@dataclass(frozen=True)
class MtfConfig:
    name: str
    product: str
    timeframe: str
    kind: str
    decision_time: str
    exit_time: str
    threshold: float
    risk_pct: float
    stop_mult: float
    take_profit_mult: float | None
    cost_multiplier: float = 1.0


@dataclass(frozen=True)
class PreparedDay:
    date: object
    day_open: float
    prev_close: float
    trend5: float
    trend20: float
    atr20: float
    decision_close: float
    high_so_far: float
    low_so_far: float
    entry_time: object
    entry_open: float
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


def load_front_regular_bars(product: str, timeframe: str) -> pl.DataFrame:
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


def _daily_context(bars: pl.DataFrame) -> dict[object, dict[str, float]]:
    daily = (
        bars.group_by("date", maintain_order=True)
        .agg(
            [
                pl.col("open").first().alias("day_open"),
                pl.col("close").last().alias("day_close"),
                pl.col("high").max().alias("day_high"),
                pl.col("low").min().alias("day_low"),
            ]
        )
        .sort("date")
        .with_columns(
            [
                pl.col("day_close").shift(1).alias("prev_close"),
                (pl.col("day_close") / pl.col("day_close").shift(5) - 1.0).shift(1).alias("trend5"),
                (pl.col("day_close") / pl.col("day_close").shift(20) - 1.0).shift(1).alias("trend20"),
                (pl.col("day_high") - pl.col("day_low")).rolling_mean(20).shift(1).alias("atr20"),
            ]
        )
        .fill_null(0.0)
    )
    out: dict[object, dict[str, float]] = {}
    for row in daily.to_dicts():
        out[row["date"]] = {
            "day_open": float(row["day_open"]),
            "prev_close": float(row["prev_close"] or row["day_open"]),
            "trend5": float(row["trend5"] or 0.0),
            "trend20": float(row["trend20"] or 0.0),
            "atr20": float(row["atr20"] or max(row["day_open"] * 0.006, 1.0)),
        }
    return out


def prepare_days(bars: pl.DataFrame, *, decision_time: str, exit_time: str) -> list[PreparedDay]:
    decision_sec = _time_to_seconds(decision_time)
    exit_sec = _time_to_seconds(exit_time)
    context = _daily_context(bars)
    days: list[PreparedDay] = []
    for day_frame in bars.sort(["date", "bar_start"]).partition_by("date", maintain_order=True):
        date_value = day_frame["date"][0]
        ctx = context.get(date_value)
        if not ctx:
            continue
        before = day_frame.filter(pl.col("tod_seconds") <= decision_sec)
        after = day_frame.filter((pl.col("tod_seconds") > decision_sec) & (pl.col("tod_seconds") <= exit_sec))
        if before.height < 2 or after.height < 2:
            continue
        highs = after["high"].to_numpy().astype(float)
        lows = after["low"].to_numpy().astype(float)
        opens = after["open"].to_numpy().astype(float)
        closes = after["close"].to_numpy().astype(float)
        if min(len(highs), len(lows), len(opens), len(closes)) < 2:
            continue
        days.append(
            PreparedDay(
                date=date_value,
                day_open=ctx["day_open"],
                prev_close=ctx["prev_close"],
                trend5=ctx["trend5"],
                trend20=ctx["trend20"],
                atr20=max(ctx["atr20"], 1.0),
                decision_close=float(before["close"][-1]),
                high_so_far=float(before["high"].max()),
                low_so_far=float(before["low"].min()),
                entry_time=after["bar_start"][0],
                entry_open=float(opens[0]),
                highs=highs,
                lows=lows,
                opens=opens,
                closes=closes,
                times=after["bar_start"].to_list(),
            )
        )
    return days


def _side(day: PreparedDay, cfg: MtfConfig) -> int:
    morning_ret = day.decision_close / day.day_open - 1.0
    gap = day.day_open / day.prev_close - 1.0 if day.prev_close > 0 else 0.0
    span = max(day.high_so_far - day.low_so_far, 1.0)
    pos = (day.decision_close - day.low_so_far) / span
    trend = day.trend20 if abs(day.trend20) >= abs(day.trend5) else day.trend5
    th = cfg.threshold
    if cfg.kind == "morning_momo":
        return 1 if morning_ret >= th else -1 if morning_ret <= -th else 0
    if cfg.kind == "morning_fade":
        return -1 if morning_ret >= th else 1 if morning_ret <= -th else 0
    if cfg.kind == "gap_fade":
        return -1 if gap >= th and morning_ret <= gap else 1 if gap <= -th and morning_ret >= gap else 0
    if cfg.kind == "range_break":
        if span / day.day_open < th:
            return 0
        return 1 if pos >= 0.82 else -1 if pos <= 0.18 else 0
    if cfg.kind == "range_fade":
        if span / day.day_open < th:
            return 0
        return -1 if pos >= 0.82 else 1 if pos <= 0.18 else 0
    if cfg.kind == "trend_pullback":
        if trend > th and morning_ret <= -th * 0.5:
            return 1
        if trend < -th and morning_ret >= th * 0.5:
            return -1
        return 0
    raise ValueError(f"unknown kind: {cfg.kind}")


def _commission_tax(product: str, price: float, contracts: int, cost: FuturesCostConfig) -> float:
    spec = contract_spec(product)
    notional = spec.notional(price, contracts)
    return cost.commission(product, contracts) + cost.cost_multiplier * notional * spec.tax_rate


def _slipped(price: float, side: int, product: str, cost: FuturesCostConfig) -> float:
    spec = contract_spec(product)
    return price + side * spec.ticks_to_price(cost.slippage_ticks)


def _target_contracts(equity: float, price: float, stop_points: float, cfg: MtfConfig, margin: FuturesMarginConfig) -> int:
    spec = contract_spec(cfg.product)
    risk_per_contract = max(stop_points, spec.tick_size) * spec.multiplier
    by_risk = math.floor(equity * cfg.risk_pct / risk_per_contract) if risk_per_contract > 0 else 0
    notional = spec.notional(price, 1)
    by_margin = math.floor(equity / (margin.initial_margin(notional) * margin.required_buffer)) if notional > 0 else 0
    by_lev = math.floor(equity * margin.max_notional_leverage / notional) if notional > 0 else 0
    return max(0, min(by_risk, by_margin, by_lev))


def simulate(days: list[PreparedDay], cfg: MtfConfig, *, capital: float = CAPITAL) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, object]]:
    spec = contract_spec(cfg.product)
    cost = FuturesCostConfig(cost_multiplier=cfg.cost_multiplier)
    margin = FuturesMarginConfig(max_notional_leverage=6.0, required_buffer=1.35)
    equity = float(capital)
    daily_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    max_leverage = 0.0
    min_margin_buffer = math.inf
    margin_breach = False
    for day in days:
        side = _side(day, cfg)
        traded = 0
        if side != 0 and equity > 0:
            range_so_far = max(day.high_so_far - day.low_so_far, spec.tick_size)
            stop_points = max(range_so_far * cfg.stop_mult, day.atr20 * 0.15, spec.tick_size)
            raw_entry = day.entry_open
            contracts = _target_contracts(equity, raw_entry, stop_points, cfg, margin)
            if contracts > 0:
                entry_price = _slipped(raw_entry, side, cfg.product, cost)
                entry_fee = _commission_tax(cfg.product, entry_price, contracts, cost)
                if side > 0:
                    stop_price = entry_price - stop_points
                    take_price = entry_price + stop_points * cfg.take_profit_mult if cfg.take_profit_mult is not None else None
                else:
                    stop_price = entry_price + stop_points
                    take_price = entry_price - stop_points * cfg.take_profit_mult if cfg.take_profit_mult is not None else None
                raw_exit = float(day.closes[-1])
                exit_time = day.times[-1]
                reason = "session_close"
                for high, low, open_price, bar_time in zip(day.highs, day.lows, day.opens, day.times, strict=True):
                    if side > 0:
                        stop_hit = low <= stop_price
                        take_hit = take_price is not None and high >= take_price
                        if stop_hit:
                            raw_exit = min(float(open_price), stop_price) if open_price < stop_price else stop_price
                            exit_time = bar_time
                            reason = "stop"
                            break
                        if take_hit:
                            raw_exit = max(float(open_price), take_price) if open_price > take_price else float(take_price)
                            exit_time = bar_time
                            reason = "take_profit"
                            break
                    else:
                        stop_hit = high >= stop_price
                        take_hit = take_price is not None and low <= take_price
                        if stop_hit:
                            raw_exit = max(float(open_price), stop_price) if open_price > stop_price else stop_price
                            exit_time = bar_time
                            reason = "stop"
                            break
                        if take_hit:
                            raw_exit = min(float(open_price), take_price) if open_price < take_price else float(take_price)
                            exit_time = bar_time
                            reason = "take_profit"
                            break
                exit_price = _slipped(raw_exit, -side, cfg.product, cost)
                exit_fee = _commission_tax(cfg.product, exit_price, contracts, cost)
                gross = side * contracts * spec.multiplier * (exit_price - entry_price)
                pnl = gross - entry_fee - exit_fee
                equity += pnl
                notional = spec.notional(entry_price, contracts)
                max_leverage = max(max_leverage, notional / max(equity, 1e-9))
                maint = margin.maintenance_margin(notional)
                buffer = equity / maint if maint > 0 else math.inf
                min_margin_buffer = min(min_margin_buffer, buffer)
                if equity <= maint * margin.liquidation_buffer:
                    margin_breach = True
                traded = 1
                trade_rows.append(
                    {
                        "date": day.date,
                        "strategy": cfg.name,
                        "product": cfg.product,
                        "timeframe": cfg.timeframe,
                        "kind": cfg.kind,
                        "side": "long" if side > 0 else "short",
                        "contracts": contracts,
                        "entry_time": day.entry_time,
                        "exit_time": exit_time,
                        "entry_price": entry_price,
                        "exit_price": exit_price,
                        "pnl": pnl,
                        "costs": entry_fee + exit_fee,
                        "reason": reason,
                        "stop_points": stop_points,
                    }
                )
        daily_rows.append(
            {
                "date": day.date,
                "strategy": cfg.name,
                "nav": equity,
                "trade": traded,
                "margin_breach": margin_breach,
                "min_margin_buffer": min_margin_buffer if math.isfinite(min_margin_buffer) else 999.0,
            }
        )
        if equity <= 0:
            margin_breach = True
            break
    daily = pl.DataFrame(daily_rows).sort("date")
    trades = pl.DataFrame(trade_rows) if trade_rows else pl.DataFrame()
    summary = {
        **asdict(cfg),
        "ending_nav": float(daily["nav"][-1]) if daily.height else capital,
        "trade_count": int(trades.height) if not trades.is_empty() else 0,
        "max_leverage": float(max_leverage),
        "min_margin_buffer": float(min_margin_buffer if math.isfinite(min_margin_buffer) else 999.0),
        "margin_breach": bool(margin_breach),
    }
    return daily, trades, summary


def quick_screen_row(name: str, daily: pl.DataFrame, trades: pl.DataFrame, summary: dict[str, object]) -> dict[str, object]:
    row: dict[str, object] = {
        "name": name,
        **nav_metrics(daily.select(["date", "nav"])),
        **recent_one_year_metrics(daily.select(["date", "nav"])),
        **summary,
    }
    row = add_recent_window_returns(row, daily)
    oos = daily.filter((pl.col("date").dt.year() >= 2012) & (pl.col("date").dt.year() <= 2026)).select(["date", "nav"])
    row.update(nav_metrics(oos, capital=CAPITAL_DEFAULT, prefix="oos_"))
    if not trades.is_empty() and "pnl" in trades.columns:
        row.update(trade_distribution_metrics(trades["pnl"].to_list()))
    else:
        row.update({"profit_factor": 0.0, "sqn": 0.0, "trade_count": 0.0})
    return row


def candidate_grid() -> list[MtfConfig]:
    configs: list[MtfConfig] = []
    for product in ["TX", "MTX"]:
        for timeframe in ["5m", "15m", "30m", "60m"]:
            for kind in ["morning_momo", "morning_fade", "gap_fade", "range_break", "range_fade", "trend_pullback"]:
                for decision in ["09:30:00", "10:30:00", "12:00:00"]:
                    for threshold in [0.0015, 0.003, 0.005]:
                        for risk_pct in [0.005, 0.01]:
                            for take_profit in [1.5, 2.5]:
                                name = (
                                    f"{product}_{timeframe}_{kind}_d{decision.replace(':', '')}"
                                    f"_th{threshold:g}_r{risk_pct:g}_sl1_tp{take_profit:g}"
                                )
                                configs.append(
                                    MtfConfig(
                                        name=name,
                                        product=product,
                                        timeframe=timeframe,
                                        kind=kind,
                                        decision_time=decision,
                                        exit_time="13:40:00",
                                        threshold=threshold,
                                        risk_pct=risk_pct,
                                        stop_mult=1.0,
                                        take_profit_mult=take_profit,
                                    )
                                )
    return configs


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


def _write_doc(summary: pl.DataFrame, cutoff: str, elapsed: float, pbo: float) -> None:
    DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
    passed = summary.filter(pl.col("verdict") == "pass").sort("objective", descending=True)
    if passed.is_empty():
        source = summary.filter(pl.col("oos_days") >= 252) if "oos_days" in summary.columns else summary
        if source.is_empty():
            source = summary
        if float(source["oos_cagr"].max() or 0.0) > 0.0:
            ranked = source.sort(["oos_cagr", "cagr", "recent_1y_cagr"], descending=[True, True, True]).head(20)
        else:
            ranked = source.sort(["cagr", "recent_1y_cagr", "oos_cagr"], descending=[True, True, True]).head(20)
    else:
        ranked = passed.head(20)
    recent_start = ranked["recent_1y_start"][0] if "recent_1y_start" in ranked.columns else "n/a"
    recent_end = ranked["recent_1y_end"][0] if "recent_1y_end" in ranked.columns else "n/a"
    lines = [
        "# 臺指期 Multi-Timeframe 日內策略研究",
        "",
        f"RPT 1m/5m/15m/30m/60m bars 截止：`{cutoff}`。最近一年視窗：`{recent_start}` 至 `{recent_end}`。本輪測試日盤 multi-timeframe momentum / reversal / gap / range 策略，執行時間約 `{elapsed:.1f}` 秒；群組 PBO `{pbo:.3f}`。",
        "",
        "## 結論",
        "",
    ]
    if passed.is_empty():
        lines.append("本輪沒有候選通過嚴格 gate；結果只能作研究診斷，不能升級為可上線臺指期策略。")
    else:
        lines.append(f"本輪通過 gate 的第一名是 **{passed['name'][0]}**。")
    lines += [
        "",
        "| 排名 | 策略 | Verdict | Full CAGR | OOS CAGR | 最近一年 CAGR | 近 6 月 | 近 3 月 | 近 1 月 | OOS MDD | OOS Sortino | DSR | PBO | Boot CAGR LB | 2x Cost OOS | 5x Cost OOS | PF | SQN | Trades |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for idx, row in enumerate(ranked.to_dicts(), start=1):
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
                    _format_pct(row.get("boot_cagr_lb")),
                    _format_pct(row.get("stress_2x_oos_cagr")),
                    _format_pct(row.get("stress_5x_oos_cagr")),
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
        "- 訊號只在 decision bar 收完後形成；進場使用下一根 bar open，避免同根偷看。",
        "- 全部部位在日盤收盤前平倉，不持倉到夜盤。",
        "- 同一根 bar 同時觸發停利/停損時採 stop-first 保守假設。",
        "- 每筆交易扣固定手續費、交易稅與 slippage tick；2x / 5x cost stress 會重跑同一策略。",
        "",
        "## Artifacts",
        "",
        "- `research/strat_lab/results/futures_tx_intraday_mtf/intraday_mtf_summary.csv`",
        "- `research/strat_lab/results/futures_tx_intraday_mtf/top_daily.csv`",
        "- `research/strat_lab/results/futures_tx_intraday_mtf/top_trades.csv`",
    ]
    DOC_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run() -> None:
    start = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    configs = candidate_grid()
    grouped: dict[tuple[str, str, str], list[PreparedDay]] = {}
    screen_rows: list[dict[str, object]] = []
    dailies: dict[str, pl.DataFrame] = {}
    trades_by_name: dict[str, pl.DataFrame] = {}
    summaries_by_name: dict[str, dict[str, object]] = {}
    print(f"[intraday-mtf] candidates={len(configs)}", flush=True)
    for idx, cfg in enumerate(configs, start=1):
        if idx == 1 or idx % 100 == 0:
            print(f"[intraday-mtf] {idx}/{len(configs)} {cfg.name}", flush=True)
        key = (cfg.product, cfg.timeframe, cfg.decision_time)
        if key not in grouped:
            bars = load_front_regular_bars(cfg.product, cfg.timeframe)
            grouped[key] = prepare_days(bars, decision_time=cfg.decision_time, exit_time=cfg.exit_time)
        days = grouped[key]
        if len(days) < 500:
            continue
        daily, trades, sim_summary = simulate(days, cfg)
        if daily.height < 500:
            continue
        screen_rows.append(quick_screen_row(cfg.name, daily, trades, sim_summary))
        dailies[cfg.name] = daily
        trades_by_name[cfg.name] = trades
        summaries_by_name[cfg.name] = sim_summary
    screen = pl.DataFrame(screen_rows).sort(["oos_cagr", "recent_1y_cagr"], descending=[True, True])
    screen.write_csv(OUT_DIR / "intraday_mtf_fast_screen.csv")
    finalist_names = (
        screen.filter(
            (pl.col("oos_cagr") > 0.0)
            & (pl.col("oos_mdd").abs() <= 0.60)
            & (pl.col("trade_count") >= 80)
            & (pl.col("profit_factor") >= 1.03)
        )
        .sort(["oos_cagr", "recent_1y_cagr"], descending=[True, True])
        .head(120)["name"]
        .to_list()
    )
    if len(finalist_names) < 20:
        finalist_names = screen.head(120)["name"].to_list()
    finalist_set = set(str(name) for name in finalist_names)
    pbo = multi_config_pbo({name: dailies[name] for name in finalist_set})
    final = []
    config_by_name = {cfg.name: cfg for cfg in configs}
    for name in finalist_names:
        cfg = config_by_name[str(name)]
        key = (cfg.product, cfg.timeframe, cfg.decision_time)
        days = grouped[key]
        daily = dailies[cfg.name]
        trades = trades_by_name[cfg.name]
        row = validate_futures_daily(
            cfg.name,
            daily,
            trades=trades,
            simulator_summary=summaries_by_name[cfg.name],
            n_trials=len(configs),
            config=ValidationConfig(oos_start_year=2012, oos_end_year=2026, min_trials_for_dsr=len(configs)),
        )
        s2_daily, _, _ = simulate(days, MtfConfig(**{**asdict(cfg), "cost_multiplier": 2.0}))
        s5_daily, _, _ = simulate(days, MtfConfig(**{**asdict(cfg), "cost_multiplier": 5.0}))
        row["stress_2x_oos_cagr"] = nav_metrics(
            s2_daily.filter((pl.col("date").dt.year() >= 2012) & (pl.col("date").dt.year() <= 2026)).select(["date", "nav"]),
            prefix="oos_",
        )["oos_cagr"]
        row["stress_5x_oos_cagr"] = nav_metrics(
            s5_daily.filter((pl.col("date").dt.year() >= 2012) & (pl.col("date").dt.year() <= 2026)).select(["date", "nav"]),
            prefix="oos_",
        )["oos_cagr"]
        row["pbo"] = pbo
        row["verdict"] = verdict(row)
        row["objective"] = futures_objective(row)
        final.append(row)
    summary = pl.DataFrame(final).sort(["oos_cagr", "recent_1y_cagr"], descending=[True, True])
    summary.write_csv(OUT_DIR / "intraday_mtf_summary.csv")
    best_name = str(summary["name"][0])
    dailies[best_name].write_csv(OUT_DIR / "top_daily.csv")
    trades_by_name[best_name].write_csv(OUT_DIR / "top_trades.csv")
    cutoff = max(max(day.date for day in days) for days in grouped.values() if days)
    _write_doc(summary, cutoff.isoformat(), time.time() - start, pbo)
    champion = summary.filter(pl.col("verdict") == "pass").sort("objective", descending=True)
    print(f"[done] intraday-mtf rows={summary.height} pbo={pbo:.3f} champion={champion['name'][0] if not champion.is_empty() else 'NONE'}")
    print(f"[artifacts] {OUT_DIR}")
    print(f"[doc] {DOC_PATH}")


if __name__ == "__main__":
    run()
