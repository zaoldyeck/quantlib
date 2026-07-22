"""TAIFEX futures session-level strategy research.

This runner tests a separate hypothesis from daily trend and regular-session
ORB: the night session and regular session may carry different information.
It trades only after the relevant session signal is fully known:

- night -> regular: use the completed night session to trade the same day's
  regular session.
- regular -> night: use the completed regular session to trade the following
  night session, represented by the next source-date row.
"""

from __future__ import annotations

import math
import sys
import time
from dataclasses import dataclass, replace
from pathlib import Path
from research import paths

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
from futures.strategies import load_product_frame
from futures.validation import add_recent_window_returns, futures_objective, multi_config_pbo, validate_futures_daily, verdict
from strat_lab.evaluation import CAPITAL_DEFAULT, nav_metrics, trade_distribution_metrics
from strat_lab.validator import ValidationConfig


BASE = Path(__file__).resolve().parents[2]
DB_PATH = paths.CACHE_DB
OUT_DIR = paths.OUT_STRAT_LAB / "futures_tx_session"
DOC_PATH = BASE / "docs" / "strategy_research" / "futures_session_strategy_ranking.md"


@dataclass(frozen=True)
class SessionConfig:
    name: str
    product: str
    source: str
    target_session: str
    direction: str
    z: float
    risk_pct: float
    stop_mult: float
    take_profit_mult: float | None
    cost_multiplier: float = 1.0


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


def load_session_frame(product: str) -> pl.DataFrame:
    frame = load_product_frame(DB_PATH, product).sort("date")
    required = [
        "rpt_5m_regular_open",
        "rpt_5m_regular_close",
        "rpt_5m_regular_high",
        "rpt_5m_regular_low",
        "rpt_5m_regular_ret",
        "rpt_5m_night_open",
        "rpt_5m_night_close",
        "rpt_5m_night_high",
        "rpt_5m_night_low",
        "rpt_5m_night_ret",
        "rpt_5m_last60_ret",
    ]
    for col in required:
        if col not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))
    night = pl.col("rpt_5m_night_ret")
    regular = pl.col("rpt_5m_regular_ret")
    late = pl.col("rpt_5m_last60_ret")
    return (
        frame.with_columns(
            [
                ((night - night.rolling_mean(63).shift(1)) / pl.max_horizontal(night.rolling_std(63).shift(1), pl.lit(1e-6))).alias("night_z"),
                ((regular.shift(1) - regular.rolling_mean(63).shift(2)) / pl.max_horizontal(regular.rolling_std(63).shift(2), pl.lit(1e-6))).alias("regular_lag_z"),
                ((late.shift(1) - late.rolling_mean(63).shift(2)) / pl.max_horizontal(late.rolling_std(63).shift(2), pl.lit(1e-6))).alias("late_lag_z"),
            ]
        )
        .sort("date")
    )


def _signal(row: dict[str, object], cfg: SessionConfig) -> int:
    value = float(row.get("night_z") or 0.0) if cfg.source == "night" else float(row.get("regular_lag_z") or 0.0)
    if cfg.source == "late_regular":
        value = float(row.get("late_lag_z") or 0.0)
    if value > cfg.z:
        raw = 1
    elif value < -cfg.z:
        raw = -1
    else:
        raw = 0
    if cfg.direction == "reversal":
        raw *= -1
    return raw


def _session_prices(row: dict[str, object], target_session: str) -> tuple[float, float, float, float]:
    if target_session == "regular":
        return (
            float(row.get("rpt_5m_regular_open") or 0.0),
            float(row.get("rpt_5m_regular_high") or 0.0),
            float(row.get("rpt_5m_regular_low") or 0.0),
            float(row.get("rpt_5m_regular_close") or 0.0),
        )
    return (
        float(row.get("rpt_5m_night_open") or 0.0),
        float(row.get("rpt_5m_night_high") or 0.0),
        float(row.get("rpt_5m_night_low") or 0.0),
        float(row.get("rpt_5m_night_close") or 0.0),
    )


def _commission_tax(product: str, price: float, contracts: int, cost: FuturesCostConfig) -> float:
    spec = contract_spec(product)
    return cost.commission(product, contracts) + cost.cost_multiplier * spec.notional(price, contracts) * spec.tax_rate


def _slipped(price: float, side: int, product: str, cost: FuturesCostConfig) -> float:
    spec = contract_spec(product)
    return price + side * spec.ticks_to_price(cost.slippage_ticks)


def _capacity(equity: float, price: float, product: str, margin: FuturesMarginConfig) -> int:
    spec = contract_spec(product)
    notional = spec.notional(price, 1)
    if equity <= 0 or notional <= 0:
        return 0
    by_margin = math.floor(equity / (margin.initial_margin(notional) * margin.required_buffer))
    by_lev = math.floor((equity * margin.max_notional_leverage) / notional)
    return max(0, min(by_margin, by_lev))


def simulate_session_strategy(
    frame: pl.DataFrame,
    cfg: SessionConfig,
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

    for row in frame.sort("date").iter_rows(named=True):
        day = row["date"]
        side = _signal(row, cfg)
        entry_raw, high, low, exit_raw = _session_prices(row, cfg.target_session)
        traded = False
        day_costs = 0.0
        if side != 0 and entry_raw > 0 and exit_raw > 0 and high > 0 and low > 0 and equity > 0:
            session_range = max(high - low, spec.tick_size)
            stop_points = max(session_range * cfg.stop_mult, spec.tick_size)
            by_risk = math.floor(equity * cfg.risk_pct / (stop_points * spec.multiplier))
            contracts = max(0, min(by_risk, _capacity(equity, entry_raw, cfg.product, margin)))
            if contracts > 0:
                entry = _slipped(entry_raw, side, cfg.product, cost)
                fees = _commission_tax(cfg.product, entry, contracts, cost)
                equity -= fees
                day_costs += fees
                if side > 0:
                    stop = entry - stop_points
                    take = entry + session_range * cfg.take_profit_mult if cfg.take_profit_mult is not None else None
                    stop_hit = low <= stop
                    take_hit = take is not None and high >= take
                    if stop_hit:
                        raw_exit = min(entry_raw, stop) if entry_raw < stop else stop
                        reason = "stop"
                    elif take_hit:
                        raw_exit = max(entry_raw, take) if entry_raw > take else take
                        reason = "take_profit"
                    else:
                        raw_exit = exit_raw
                        reason = "session_close"
                else:
                    stop = entry + stop_points
                    take = entry - session_range * cfg.take_profit_mult if cfg.take_profit_mult is not None else None
                    stop_hit = high >= stop
                    take_hit = take is not None and low <= take
                    if stop_hit:
                        raw_exit = max(entry_raw, stop) if entry_raw > stop else stop
                        reason = "stop"
                    elif take_hit:
                        raw_exit = min(entry_raw, take) if entry_raw < take else take
                        reason = "take_profit"
                    else:
                        raw_exit = exit_raw
                        reason = "session_close"
                exit_price = _slipped(raw_exit, -side, cfg.product, cost)
                exit_fees = _commission_tax(cfg.product, exit_price, contracts, cost)
                pnl = side * contracts * spec.multiplier * (exit_price - entry) - exit_fees
                equity += pnl
                day_costs += exit_fees
                traded = True
                notional = spec.notional(entry, contracts)
                leverage = notional / equity if equity > 0 else math.inf
                maint = margin.maintenance_margin(notional)
                buffer = equity / maint if maint > 0 else math.inf
                max_leverage = max(max_leverage, leverage if math.isfinite(leverage) else 999.0)
                min_margin_buffer = min(min_margin_buffer, buffer)
                if maint > 0 and equity <= maint * margin.liquidation_buffer:
                    margin_breach = True
                trade_rows.append(
                    {
                        "date": day,
                        "strategy": cfg.name,
                        "product": cfg.product,
                        "target_session": cfg.target_session,
                        "source": cfg.source,
                        "side": "long" if side > 0 else "short",
                        "contracts": contracts,
                        "entry_price": entry,
                        "exit_price": exit_price,
                        "pnl": pnl,
                        "costs": day_costs,
                        "reason": reason,
                    }
                )
        daily_rows.append(
            {
                "date": day,
                "strategy": cfg.name,
                "nav": equity,
                "trade": 1 if traded else 0,
                "costs": day_costs,
                "gross_leverage": 0.0,
                "margin_buffer": min_margin_buffer if math.isfinite(min_margin_buffer) else 999.0,
                "margin_breach": margin_breach,
            }
        )
        if equity <= 0:
            margin_breach = True
            break

    daily = pl.DataFrame(daily_rows).sort("date")
    trades = pl.DataFrame(trade_rows) if trade_rows else pl.DataFrame()
    summary = {
        "name": cfg.name,
        "product": cfg.product,
        "target_session": cfg.target_session,
        "source": cfg.source,
        "ending_nav": float(daily["nav"][-1]) if daily.height else capital,
        "trade_count": int(trades.height) if not trades.is_empty() else 0,
        "max_leverage": float(max_leverage),
        "min_margin_buffer": float(min_margin_buffer if math.isfinite(min_margin_buffer) else 999.0),
        "margin_breach": bool(margin_breach),
    }
    return daily, trades, summary


def candidate_grid(cost_multiplier: float = 1.0) -> list[SessionConfig]:
    configs: list[SessionConfig] = []
    for product in ["TX", "MTX"]:
        specs = [
            ("night", "regular"),
            ("regular", "night"),
            ("late_regular", "night"),
        ]
        for source, target_session in specs:
            for direction in ["follow", "reversal"]:
                for z in [0.75, 1.0, 1.25]:
                    for risk_pct in [0.005, 0.01, 0.02, 0.04]:
                        for stop_mult in [1.0, 1.5]:
                            for tp in [2.0, None]:
                                name = (
                                    f"{product}_{source}_to_{target_session}_{direction}"
                                    f"_z{z:g}_r{risk_pct:g}_sl{stop_mult:g}_tp{tp if tp is not None else 'none'}"
                                )
                                configs.append(
                                    SessionConfig(
                                        name=name,
                                        product=product,
                                        source=source,
                                        target_session=target_session,
                                        direction=direction,
                                        z=z,
                                        risk_pct=risk_pct,
                                        stop_mult=stop_mult,
                                        take_profit_mult=tp,
                                        cost_multiplier=cost_multiplier,
                                    )
                                )
    return configs


def _row(cfg: SessionConfig, daily: pl.DataFrame, trades: pl.DataFrame, summary: dict[str, object], n_trials: int, pbo: float | None = None) -> dict[str, object]:
    row = validate_futures_daily(
        cfg.name,
        daily,
        trades=trades,
        simulator_summary=summary,
        n_trials=max(66, n_trials),
        group_pbo=pbo,
        config=ValidationConfig(oos_start_year=2012, oos_end_year=2026, min_trials_for_dsr=max(66, n_trials)),
    )
    return row


def _write_doc(summary: pl.DataFrame, cutoff: str, elapsed: float) -> None:
    DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
    ranked = summary.sort("objective", descending=True)
    passed = ranked.filter(pl.col("verdict") == "pass")
    top = (passed if not passed.is_empty() else summary.sort(["oos_cagr", "recent_1y_cagr"], descending=True)).head(18).to_dicts()
    lines = [
        "# 臺指期 Session-Level 策略研究排行",
        "",
        f"RPT session features 截止：`{cutoff}`。本輪測試夜盤 -> 日盤、日盤 -> 夜盤與尾盤 -> 夜盤的 momentum / reversal session 策略，執行時間約 `{elapsed:.1f}` 秒。",
        "",
        "## 結論",
        "",
    ]
    if passed.is_empty():
        lines += ["本輪沒有候選通過嚴格 gate；結果仍只能作為研究診斷。", ""]
    else:
        best = passed.head(1).to_dicts()[0]
        lines += [f"本輪第一名通過 gate 的策略是 **{best['name']}**。", ""]
    lines += [
        "| 排名 | 策略 | Verdict | Full CAGR | OOS CAGR | 最近一年 CAGR | 近 6 月 | 近 3 月 | 近 1 月 | OOS MDD | OOS Sortino | DSR | PBO | PF | SQN | Trades |",
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
        "- 日盤策略只使用已完成夜盤訊號；夜盤策略只使用已完成日盤或尾盤訊號。",
        "- 每筆交易均扣手續費、交易稅與 slippage；同一 session 內停損與停利同時觸發時採 stop-first。",
        "- 部位大小由每筆風險與保證金 survival constraint 限制，且重跑 2x/5x 成本壓力。",
        "",
        "## Artifacts",
        "",
        f"- `{paths.OUT_STRAT_LAB}/futures_tx_session/session_strategy_summary.csv`",
        f"- `{paths.OUT_STRAT_LAB}/futures_tx_session/top_daily.csv`",
        f"- `{paths.OUT_STRAT_LAB}/futures_tx_session/top_trades.csv`",
    ]
    DOC_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run() -> None:
    start_time = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    frames = {product: load_session_frame(product) for product in ["TX", "MTX"]}
    configs = candidate_grid()
    rows: list[dict[str, object]] = []
    daily_by_name: dict[str, pl.DataFrame] = {}
    trades_by_name: dict[str, pl.DataFrame] = {}
    summaries: dict[str, dict[str, object]] = {}
    stress2: dict[str, pl.DataFrame] = {}
    stress5: dict[str, pl.DataFrame] = {}
    print(f"[session] candidates={len(configs)}", flush=True)
    for idx, cfg in enumerate(configs, start=1):
        if idx == 1 or idx % 50 == 0:
            print(f"[session] {idx}/{len(configs)} {cfg.name}", flush=True)
        frame = frames[cfg.product]
        daily, trades, sim_summary = simulate_session_strategy(frame, cfg)
        if daily.height < 500:
            continue
        s2_daily, _, _ = simulate_session_strategy(frame, replace(cfg, cost_multiplier=2.0))
        s5_daily, _, _ = simulate_session_strategy(frame, replace(cfg, cost_multiplier=5.0))
        daily_by_name[cfg.name] = daily
        trades_by_name[cfg.name] = trades
        summaries[cfg.name] = sim_summary
        stress2[cfg.name] = s2_daily
        stress5[cfg.name] = s5_daily
        rows.append(_row(cfg, daily, trades, sim_summary, len(configs)))

    if not rows:
        raise RuntimeError("no session strategy rows generated")

    pbo = multi_config_pbo(daily_by_name)
    final_rows = []
    for row in rows:
        name = str(row["name"])
        row["pbo"] = pbo
        row["stress_2x_oos_cagr"] = nav_metrics(stress2[name].filter((pl.col("date").dt.year() >= 2012) & (pl.col("date").dt.year() <= 2026)).select(["date", "nav"]), prefix="oos_")["oos_cagr"]
        row["stress_5x_oos_cagr"] = nav_metrics(stress5[name].filter((pl.col("date").dt.year() >= 2012) & (pl.col("date").dt.year() <= 2026)).select(["date", "nav"]), prefix="oos_")["oos_cagr"]
        row["verdict"] = verdict(row)
        row["objective"] = futures_objective(row)
        final_rows.append(row)

    summary = pl.DataFrame(final_rows).sort("objective", descending=True)
    summary.write_csv(OUT_DIR / "session_strategy_summary.csv")
    best_name = str((summary.filter(pl.col("verdict") == "pass").sort("objective", descending=True) if (summary["verdict"] == "pass").any() else summary.sort(["oos_cagr", "recent_1y_cagr"], descending=True))["name"][0])
    daily_by_name[best_name].write_csv(OUT_DIR / "top_daily.csv")
    trades_by_name[best_name].write_csv(OUT_DIR / "top_trades.csv")
    cutoff = max(frame["date"].max() for frame in frames.values()).isoformat()
    _write_doc(summary, cutoff, time.time() - start_time)
    champion = summary.filter(pl.col("verdict") == "pass").sort("objective", descending=True)
    print(f"[done] session rows={summary.height} pbo={pbo:.3f} champion={champion['name'][0] if not champion.is_empty() else 'NONE'}")
    print(f"[artifacts] {OUT_DIR}")
    print(f"[doc] {DOC_PATH}")


if __name__ == "__main__":
    run()
