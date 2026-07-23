"""Advanced TAIFEX futures portfolio research.

This runner does not relax the production gates.  It takes the most promising
daily and intraday futures sleeves, applies lagged risk controls, then tests
multi-sleeve PM allocation with the same validation stack used by the other
futures research runners.
"""

from __future__ import annotations

import math
import sys
import time
from dataclasses import replace
from pathlib import Path
from quantlib import paths

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
STRAT_LAB = ROOT / "strat_lab"
if str(STRAT_LAB) not in sys.path:
    sys.path.insert(0, str(STRAT_LAB))

import duckdb
import numpy as np
import polars as pl

from futures.intraday_research import IntradayConfig, load_front_regular_bars, simulate_intraday_strategy
from futures.research_tx import _make_config, _normalize_benchmark, _oos_daily, _simulate_candidate
from futures.simulator import combine_sleeve_returns, simulate_single_product
from futures.specs import FuturesCostConfig
from futures.strategies import StrategyCandidate, build_signal, default_candidate_grid, load_product_frame
from futures.validation import add_recent_window_returns, futures_objective, multi_config_pbo, validate_futures_daily, verdict
from quantlib.prices import total_return_series
from strat_lab.evaluation import CAPITAL_DEFAULT, nav_metrics
from strat_lab.validator import ValidationConfig, recent_one_year_metrics


BASE = Path(__file__).resolve().parents[2]
DB_PATH = paths.CACHE_DB
DAILY_RESULTS = paths.OUT_STRAT_LAB / "futures_tx_professional" / "futures_strategy_summary.csv"
INTRADAY_RESULTS = paths.OUT_STRAT_LAB / "futures_tx_intraday" / "intraday_strategy_summary.csv"
OUT_DIR = paths.OUT_STRAT_LAB / "futures_tx_advanced"
DOC_PATH = BASE / "docs" / "strategy_research" / "futures_advanced_strategy_ranking.md"


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


def _read_summary(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing prerequisite summary: {path}")
    return pl.read_csv(path, try_parse_dates=True)


def _select_unique(df: pl.DataFrame, sort_cols: list[str], *, n: int, max_mdd: float | None = None) -> list[str]:
    source = df
    if max_mdd is not None:
        source = source.filter(pl.col("oos_mdd").abs() <= max_mdd)
    if source.is_empty():
        return []
    return source.sort(sort_cols, descending=[True] * len(sort_cols)).head(n)["name"].to_list()


def select_daily_sleeves() -> list[str]:
    df = _read_summary(DAILY_RESULTS).filter(~pl.col("name").str.starts_with("PM_lagged_"))
    names: list[str] = []
    names += _select_unique(df, ["oos_cagr", "oos_sortino"], n=10)
    names += _select_unique(df, ["oos_sortino", "oos_cagr"], n=10, max_mdd=0.45)
    names += _select_unique(df, ["recent_1y_cagr", "oos_cagr"], n=8, max_mdd=0.45)
    return list(dict.fromkeys(names))


def select_intraday_sleeves() -> list[str]:
    df = _read_summary(INTRADAY_RESULTS)
    names: list[str] = []
    names += _select_unique(df, ["oos_cagr", "oos_sortino"], n=10)
    names += _select_unique(df, ["oos_sortino", "oos_cagr"], n=8, max_mdd=0.45)
    names += _select_unique(df, ["recent_1y_cagr", "oos_cagr"], n=8)
    return list(dict.fromkeys(names))


def _parse_daily_name(name: str, candidates: list[StrategyCandidate]) -> tuple[StrategyCandidate, float, float | None, float | None]:
    if "_tv" not in name:
        raise ValueError(f"cannot parse daily candidate name: {name}")
    candidate_name, rest = name.split("_tv", 1)
    candidate = next(c for c in candidates if c.name == candidate_name)
    parts = rest.split("_")
    target_vol = float(parts[0])
    stop_token = next(p for p in parts if p.startswith("sl") or p == "nostop")
    trail_token = next(p for p in parts if p.startswith("tr") or p == "notrail")
    stop_atr = None if stop_token == "nostop" else float(stop_token.removeprefix("sl"))
    trail_atr = None if trail_token == "notrail" else float(trail_token.removeprefix("tr"))
    return candidate, target_vol, stop_atr, trail_atr


def simulate_daily_sleeves(
    names: list[str],
    *,
    cost_multiplier: float = 1.0,
    log_prefix: str = "daily-sleeve",
) -> tuple[dict[str, pl.DataFrame], dict[str, pl.DataFrame], dict[str, object]]:
    candidates = default_candidate_grid()
    needed_products = sorted({_parse_daily_name(name, candidates)[0].product for name in names})
    frames = {product: load_product_frame(DB_PATH, product) for product in needed_products}
    daily_by_name: dict[str, pl.DataFrame] = {}
    trades_by_name: dict[str, pl.DataFrame] = {}
    summaries: dict[str, object] = {}
    for idx, name in enumerate(names, start=1):
        candidate, target_vol, stop_atr, trail_atr = _parse_daily_name(name, candidates)
        print(f"[{log_prefix}] {idx}/{len(names)} {name} cost={cost_multiplier:g}", flush=True)
        _, daily, _fills, trades, summary = _simulate_candidate(
            frames,
            candidate,
            target_vol=target_vol,
            cost_multiplier=cost_multiplier,
            stop_atr=stop_atr,
            trail_atr=trail_atr,
        )
        daily_by_name[name] = daily
        trades_by_name[name] = trades
        summaries[name] = summary
    return daily_by_name, trades_by_name, summaries


def _intraday_config_from_row(row: dict[str, object]) -> IntradayConfig:
    tp = row.get("take_profit_mult")
    if tp is None or (isinstance(tp, float) and math.isnan(tp)):
        tp_value = None
    else:
        tp_value = float(tp)
    return IntradayConfig(
        name=str(row["name"]),
        product=str(row["product"]),
        timeframe=str(row["timeframe"]),
        kind=str(row["kind"]),
        opening_minutes=int(row["opening_minutes"]),
        entry_start_time=str(row["entry_start_time"]),
        exit_time=str(row["exit_time"]),
        risk_pct=float(row["risk_pct"]),
        stop_mult=float(row["stop_mult"]),
        take_profit_mult=tp_value,
        min_range_pct=float(row["min_range_pct"]),
        max_range_pct=float(row["max_range_pct"]),
        cost_multiplier=float(row.get("cost_multiplier", 1.0) or 1.0),
    )


def simulate_intraday_sleeves(
    names: list[str],
    *,
    cost_multiplier: float = 1.0,
    log_prefix: str = "intraday-sleeve",
) -> tuple[dict[str, pl.DataFrame], dict[str, pl.DataFrame], dict[str, object]]:
    summary = _read_summary(INTRADAY_RESULTS)
    rows = {str(row["name"]): row for row in summary.to_dicts()}
    configs = [_intraday_config_from_row(rows[name]) for name in names if name in rows]
    bars_by_key: dict[tuple[str, str], pl.DataFrame] = {}
    daily_by_name: dict[str, pl.DataFrame] = {}
    trades_by_name: dict[str, pl.DataFrame] = {}
    summaries: dict[str, object] = {}
    for idx, cfg in enumerate(configs, start=1):
        cfg = replace(cfg, cost_multiplier=cfg.cost_multiplier * cost_multiplier)
        print(f"[{log_prefix}] {idx}/{len(configs)} {cfg.name} cost={cost_multiplier:g}", flush=True)
        key = (cfg.product, cfg.timeframe)
        if key not in bars_by_key:
            bars_by_key[key] = load_front_regular_bars(cfg.product, cfg.timeframe)
        daily, trades, sim_summary = simulate_intraday_strategy(bars_by_key[key], cfg)
        daily_by_name[cfg.name] = daily
        trades_by_name[cfg.name] = trades
        summaries[cfg.name] = sim_summary
    return daily_by_name, trades_by_name, summaries


def _returns_from_daily(daily: pl.DataFrame) -> tuple[list[object], np.ndarray]:
    ordered = daily.select(["date", "nav"]).sort("date")
    dates = ordered["date"].to_list()
    nav = ordered["nav"].to_numpy().astype(float)
    prev = np.concatenate([[CAPITAL_DEFAULT], nav[:-1]])
    rets = np.divide(nav - prev, prev, out=np.zeros_like(nav), where=prev != 0)
    return dates, rets


def nav_from_returns(dates: list[object], rets: np.ndarray, *, capital: float = CAPITAL_DEFAULT, name: str | None = None) -> pl.DataFrame:
    nav = capital * np.cumprod(1.0 + np.asarray(rets, dtype=float))
    df = pl.DataFrame({"date": dates, "nav": nav})
    if name is not None:
        df = df.with_columns(pl.lit(name).alias("strategy"))
    return df


def apply_lagged_risk_filter(
    daily: pl.DataFrame,
    *,
    name: str,
    lookback: int,
    min_log_return: float,
    max_rolling_dd: float,
    target_vol: float,
    max_scale: float,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Apply a lagged sleeve-level risk filter.

    Decisions at date t use returns up to t-1 only.  When the sleeve's own
    recent return is weak or rolling drawdown is too deep, exposure is cut to
    zero.  Otherwise exposure is volatility-targeted with a cap.
    """

    dates, rets = _returns_from_daily(daily)
    n = len(rets)
    weights = np.zeros(n, dtype=float)
    for i in range(n):
        start = max(0, i - lookback)
        hist = rets[start:i]
        if hist.size < max(10, lookback // 3):
            continue
        log_ret = float(np.sum(np.log1p(np.clip(hist, -0.999, None))))
        wealth = np.cumprod(1.0 + hist)
        dd = wealth / np.maximum.accumulate(wealth) - 1.0
        rolling_dd = float(np.min(dd)) if dd.size else 0.0
        if log_ret < min_log_return or rolling_dd < max_rolling_dd:
            continue
        vol = float(np.std(hist, ddof=1) * math.sqrt(252.0)) if hist.size > 2 else 0.0
        scale = min(max_scale, target_vol / vol) if vol > 0 else 1.0
        weights[i] = max(0.0, scale)
    filtered_rets = rets * weights
    filtered = nav_from_returns(dates, filtered_rets, name=name)
    controls = pl.DataFrame({"date": dates, "weight": weights, "base_ret": rets, "ret": filtered_rets})
    return filtered, controls


def lagged_portfolio_allocator(
    daily_by_name: dict[str, pl.DataFrame],
    *,
    name: str,
    lookback: int,
    top_k: int,
    target_vol: float,
    max_leverage: float,
    min_score: float,
    vol_penalty: float,
    dd_penalty: float,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    names = sorted(daily_by_name)
    panel: pl.DataFrame | None = None
    for sleeve in names:
        dates, rets = _returns_from_daily(daily_by_name[sleeve])
        part = pl.DataFrame({"date": dates, sleeve: rets})
        panel = part if panel is None else panel.join(part, on="date", how="inner")
    if panel is None:
        raise ValueError("no sleeves for portfolio allocator")

    panel = panel.sort("date")
    dates = panel["date"].to_list()
    rets = panel.select(names).to_numpy().astype(float)
    n, m = rets.shape
    weights = np.zeros((n, m), dtype=float)
    for i in range(n):
        window = rets[max(0, i - lookback):i]
        if len(window) < max(21, lookback // 3):
            continue
        log_ret = np.sum(np.log1p(np.clip(window, -0.999, None)), axis=0)
        vol = np.std(window, axis=0, ddof=1) * math.sqrt(252.0)
        wealth = np.cumprod(1.0 + window, axis=0)
        dd = wealth / np.maximum.accumulate(wealth, axis=0) - 1.0
        current_dd = dd[-1]
        worst_dd = np.min(dd, axis=0)
        score = log_ret - vol_penalty * vol - dd_penalty * np.abs(current_dd) - 0.25 * np.abs(worst_dd)
        valid = np.where(np.isfinite(score) & (score >= min_score) & np.isfinite(vol) & (vol > 0))[0]
        if valid.size == 0:
            continue
        selected = valid[np.argsort(score[valid])[::-1][:top_k]]
        raw = np.zeros(m, dtype=float)
        inv_vol = 1.0 / np.maximum(vol[selected], 1e-6)
        raw[selected] = inv_vol / inv_vol.sum()
        port_window = window @ raw
        port_vol = float(np.std(port_window, ddof=1) * math.sqrt(252.0)) if len(port_window) > 2 else 0.0
        scale = min(max_leverage, target_vol / port_vol) if port_vol > 0 else 1.0
        weights[i] = raw * max(0.0, scale)

    port_rets = np.sum(rets * weights, axis=1)
    daily = nav_from_returns(dates, port_rets, name=name)
    weight_df = pl.DataFrame({"date": dates, **{f"{sleeve}__weight": weights[:, j] for j, sleeve in enumerate(names)}})
    return daily, weight_df


def validate_rows(
    daily_by_name: dict[str, pl.DataFrame],
    *,
    trades_by_name: dict[str, pl.DataFrame] | None = None,
    simulator_summary_by_name: dict[str, object] | None = None,
    cost_stress_by_name: dict[str, tuple[float, float]] | None = None,
    n_trials: int,
    pbo: float | None = None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    trades_by_name = trades_by_name or {}
    simulator_summary_by_name = simulator_summary_by_name or {}
    for name, daily in daily_by_name.items():
        row = validate_futures_daily(
            name,
            daily,
            trades=trades_by_name.get(name),
            simulator_summary=simulator_summary_by_name.get(name) if isinstance(simulator_summary_by_name.get(name), dict) else None,
            n_trials=max(66, n_trials),
            group_pbo=pbo,
            config=ValidationConfig(oos_start_year=2012, oos_end_year=2026, min_trials_for_dsr=max(66, n_trials)),
        )
        if cost_stress_by_name and name in cost_stress_by_name:
            row["stress_2x_oos_cagr"], row["stress_5x_oos_cagr"] = cost_stress_by_name[name]
        else:
            row["stress_2x_oos_cagr"] = row["oos_cagr"]
            row["stress_5x_oos_cagr"] = row["oos_cagr"]
        row["verdict"] = verdict(row)
        row["objective"] = futures_objective(row)
        rows.append(row)
    return rows


def _quick_score(daily: pl.DataFrame) -> tuple[float, float, float, float]:
    metrics = nav_metrics(_oos_daily(daily).select(["date", "nav"]), prefix="oos_")
    recent = recent_one_year_metrics(daily.select(["date", "nav"]))["recent_1y_cagr"]
    oos_cagr = float(metrics.get("oos_cagr", 0.0) or 0.0)
    oos_sortino = float(metrics.get("oos_sortino", 0.0) or 0.0)
    oos_mdd = abs(float(metrics.get("oos_mdd", 0.0) or 0.0))
    recent_1y = float(recent or 0.0)
    score = oos_cagr * max(0.05, min(oos_sortino / 0.75, 1.5)) * max(0.05, 1.0 - min(oos_mdd, 0.95)) + 0.20 * recent_1y
    return score, oos_cagr, oos_sortino, oos_mdd


def _top_by_quick_score(daily_by_name: dict[str, pl.DataFrame], *, n: int) -> list[str]:
    ranked = []
    for name, daily in daily_by_name.items():
        score, oos_cagr, oos_sortino, oos_mdd = _quick_score(daily)
        if oos_cagr > 0 and math.isfinite(score):
            ranked.append((name, score, oos_cagr, oos_sortino, oos_mdd))
    ranked.sort(key=lambda x: (x[1], x[2], x[3], -x[4]), reverse=True)
    return [name for name, *_ in ranked[:n]]


def _summary_float(summaries: dict[str, object], name: str, key: str, default: float) -> float:
    raw = summaries.get(name)
    if not isinstance(raw, dict):
        return default
    try:
        value = float(raw.get(key, default) or default)
    except (TypeError, ValueError):
        return default
    return value if math.isfinite(value) else default


def _write_doc(summary: pl.DataFrame, daily_cutoff: str, rpt_cutoff: str, elapsed: float) -> None:
    DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
    ranked = summary.sort("objective", descending=True)
    passed = ranked.filter(pl.col("verdict") == "pass")
    top = (passed if not passed.is_empty() else summary.sort(["oos_cagr", "recent_1y_cagr"], descending=True)).head(18).to_dicts()
    lines = [
        "# 臺指期進階多策略組合研究排行",
        "",
        f"日線資料截止：`{daily_cutoff}`；tick-derived / intraday 資料截止：`{rpt_cutoff}`。本輪從已成本化的 daily / intraday sleeves 出發，加入 lagged equity-curve risk filter、vol targeting 與多策略 PM allocator。執行時間約 `{elapsed:.1f}` 秒。",
        "",
        "## 結論",
        "",
    ]
    if passed.is_empty():
        lines += [
            "本輪仍沒有候選同時通過 DSR、PBO、bootstrap、成本壓力、MDD 與保證金 gate。這代表它們仍只可作研究診斷，不能列為可上線臺指期 champion。",
            "",
        ]
    else:
        best = passed.head(1).to_dicts()[0]
        lines += [
            f"本輪第一名通過 gate 的策略是 **{best['name']}**。",
            f"最近一年 CAGR：{_format_pct(best.get('recent_1y_cagr'))}，窗口 `{best.get('recent_1y_start')}` ~ `{best.get('recent_1y_end')}`。",
            "",
        ]
    lines += [
        "| 排名 | 策略 | Verdict | Full CAGR | OOS CAGR | 最近一年 CAGR | 近 6 月 | 近 3 月 | 近 1 月 | OOS MDD | OOS Sortino | DSR | PBO | PF | SQN |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
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
                ]
            )
            + " |"
        )
    lines += [
        "",
        "## 方法",
        "",
        "- 所有風控 overlay 都只使用前一日以前的 sleeve NAV，沒有用當日或未來績效。",
        "- `ECF_*` 是 equity-curve filter：rolling return 不足或 rolling drawdown 太深時停用該 sleeve，否則按已知波動調整曝險。",
        "- `PM_ADV_*` 是多策略 allocator：根據 lagged return、vol、當前 drawdown、worst drawdown 分數，選 top sleeves 並做 inverse-vol allocation。",
        "- Daily sleeves 已含期貨手續費、交易稅、滑價、roll cost、停損、追蹤停損與 time stop；intraday sleeves 已含手續費、交易稅、slippage、停損與停利。",
        "- PM 組合目前是 daily-return 層的研究 simulator；若有策略通過 gate，仍需再升級成 position-level portfolio simulator 才能進 execution-ready。",
        "",
        "## Artifacts",
        "",
        f"- `{paths.OUT_STRAT_LAB}/futures_tx_advanced/futures_advanced_summary.csv`",
        f"- `{paths.OUT_STRAT_LAB}/futures_tx_advanced/futures_advanced_base_summary.csv`",
        f"- `{paths.OUT_STRAT_LAB}/futures_tx_advanced/top_daily.csv`",
        f"- `{paths.OUT_STRAT_LAB}/futures_tx_advanced/top_weights.csv`",
    ]
    DOC_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_benchmark_chart(champion_daily: pl.DataFrame, con: duckdb.DuckDBPyConnection) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    out_path = OUT_DIR / "advanced_champion_vs_benchmarks.png"
    start = champion_daily["date"][0].isoformat()
    end = champion_daily["date"][-1].isoformat()
    tx = con.sql(
        f"""
        SELECT date, continuous_close AS adj_close
        FROM taifex_futures_continuous
        WHERE product = 'TX'
          AND date BETWEEN DATE '{start}' AND DATE '{end}'
        ORDER BY date
        """
    ).pl()
    plot = champion_daily.select(["date", pl.col("nav").alias("advanced_champion")])
    for bench_name, bench in [
        ("TX_buy_hold", _normalize_benchmark(tx, "TX_buy_hold", start, end)),
        ("0050", _normalize_benchmark(total_return_series(con, "0050", start, end), "0050", start, end)),
        ("2330", _normalize_benchmark(total_return_series(con, "2330", start, end), "2330", start, end)),
    ]:
        if not bench.is_empty():
            plot = plot.join(bench.select(["date", bench_name]), on="date", how="left")
    pdf = plot.to_pandas().set_index("date")
    fig, ax = plt.subplots(figsize=(13, 7))
    for col in pdf.columns:
        ax.plot(pdf.index, pdf[col], label=col, linewidth=1.7 if col == "advanced_champion" else 1.1)
    ax.set_yscale("log")
    ax.set_title("Advanced futures strategy vs TX / 0050 / 2330")
    ax.grid(True, alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, dpi=160)
    plt.close(fig)


def run() -> None:
    start = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        daily_cutoff = con.execute("SELECT max(date) FROM taifex_futures_continuous WHERE product='TX'").fetchone()[0].isoformat()
    finally:
        con.close()

    rpt_cutoff = "n/a"
    feature_files = list((BASE / "data" / "taifex" / "rpt" / "lake" / "features").glob("product=*/timeframe=*/daily_features.parquet"))
    if feature_files:
        rpt_cutoff = (
            pl.concat([pl.read_parquet(path).select("date") for path in feature_files])
            .select(pl.col("date").max())
            .item()
            .isoformat()
        )

    daily_names = select_daily_sleeves()
    intraday_names = select_intraday_sleeves()
    print(f"[select] daily={len(daily_names)} intraday={len(intraday_names)}", flush=True)

    daily_sleeves, daily_trades, daily_summaries = simulate_daily_sleeves(daily_names)
    intraday_sleeves, intraday_trades, intraday_summaries = simulate_intraday_sleeves(intraday_names)
    daily_sleeves_2x, _, _ = simulate_daily_sleeves(daily_names, cost_multiplier=2.0, log_prefix="daily-stress2")
    daily_sleeves_5x, _, _ = simulate_daily_sleeves(daily_names, cost_multiplier=5.0, log_prefix="daily-stress5")
    intraday_sleeves_2x, _, _ = simulate_intraday_sleeves(intraday_names, cost_multiplier=2.0, log_prefix="intraday-stress2")
    intraday_sleeves_5x, _, _ = simulate_intraday_sleeves(intraday_names, cost_multiplier=5.0, log_prefix="intraday-stress5")
    base_sleeves = {**daily_sleeves, **intraday_sleeves}
    base_sleeves_2x = {**daily_sleeves_2x, **intraday_sleeves_2x}
    base_sleeves_5x = {**daily_sleeves_5x, **intraday_sleeves_5x}
    base_trades = {**daily_trades, **intraday_trades}
    base_summaries = {**daily_summaries, **intraday_summaries}

    base_rows = validate_rows(
        base_sleeves,
        trades_by_name=base_trades,
        simulator_summary_by_name=base_summaries,
        n_trials=len(base_sleeves),
    )
    pl.DataFrame(base_rows).sort(["oos_cagr", "recent_1y_cagr"], descending=True).write_csv(OUT_DIR / "futures_advanced_base_summary.csv")

    derived_all: dict[str, pl.DataFrame] = {}
    derived_all_2x: dict[str, pl.DataFrame] = {}
    derived_all_5x: dict[str, pl.DataFrame] = {}
    derived_summaries_all: dict[str, dict[str, object]] = {}
    controls_all: dict[str, pl.DataFrame] = {}
    filter_grid = [
        # Small, pre-registered grid: fast trend confirmation and slower
        # robustness confirmation.  Wider brute-force sweeps are counter to
        # DSR/PBO discipline and too slow for interactive research.
        (42, 0.0, -0.20, 0.35, 1.25),
        (42, -0.04, -0.20, 0.50, 1.50),
        (126, 0.0, -0.25, 0.35, 1.25),
        (126, -0.04, -0.25, 0.50, 1.50),
    ]
    for sleeve_name, daily in base_sleeves.items():
        for lookback, min_log_return, max_dd, target_vol, max_scale in filter_grid:
            name = (
                f"ECF_{sleeve_name}"
                f"_lb{lookback}_min{min_log_return:g}_dd{abs(max_dd):g}_tv{target_vol:g}_cap{max_scale:g}"
            )
            filtered, control = apply_lagged_risk_filter(
                daily,
                name=name,
                lookback=lookback,
                min_log_return=min_log_return,
                max_rolling_dd=max_dd,
                target_vol=target_vol,
                max_scale=max_scale,
            )
            if filtered.height >= 500:
                derived_all[name] = filtered
                controls_all[name] = control
                base_buffer = _summary_float(base_summaries, sleeve_name, "min_margin_buffer", 999.0)
                base_leverage = _summary_float(base_summaries, sleeve_name, "max_leverage", 0.0)
                derived_summaries_all[name] = {
                    "product": "FILTERED",
                    "max_leverage": base_leverage * max_scale,
                    "min_margin_buffer": base_buffer / max(max_scale, 1e-9),
                    "margin_breach": bool(
                        isinstance(base_summaries.get(sleeve_name), dict)
                        and base_summaries[sleeve_name].get("margin_breach", False)
                    ),
                }
                if sleeve_name in base_sleeves_2x:
                    filtered_2x, _ = apply_lagged_risk_filter(
                        base_sleeves_2x[sleeve_name],
                        name=name,
                        lookback=lookback,
                        min_log_return=min_log_return,
                        max_rolling_dd=max_dd,
                        target_vol=target_vol,
                        max_scale=max_scale,
                    )
                    derived_all_2x[name] = filtered_2x
                if sleeve_name in base_sleeves_5x:
                    filtered_5x, _ = apply_lagged_risk_filter(
                        base_sleeves_5x[sleeve_name],
                        name=name,
                        lookback=lookback,
                        min_log_return=min_log_return,
                        max_rolling_dd=max_dd,
                        target_vol=target_vol,
                        max_scale=max_scale,
                    )
                    derived_all_5x[name] = filtered_5x

    selected_derived_names = _top_by_quick_score(derived_all, n=72)
    derived = {name: derived_all[name] for name in selected_derived_names}
    derived_2x = {name: derived_all_2x[name] for name in selected_derived_names if name in derived_all_2x}
    derived_5x = {name: derived_all_5x[name] for name in selected_derived_names if name in derived_all_5x}
    derived_summaries = {name: derived_summaries_all[name] for name in selected_derived_names if name in derived_summaries_all}
    controls = {name: controls_all[name] for name in selected_derived_names if name in controls_all}
    print(f"[filter] generated={len(derived_all)} selected={len(derived)}", flush=True)

    ranked_base = pl.DataFrame(base_rows).filter(~pl.col("margin_breach")).sort(["oos_cagr", "oos_sortino"], descending=True)
    core_names = [n for n in ranked_base.head(18)["name"].to_list() if n in base_sleeves]
    filtered_names = selected_derived_names[:18]

    allocator_pool = {name: base_sleeves[name] for name in core_names}
    allocator_pool.update({name: derived[name] for name in filtered_names})
    allocator_summaries: dict[str, dict[str, object]] = {}
    for name in core_names:
        if isinstance(base_summaries.get(name), dict):
            allocator_summaries[name] = base_summaries[name]  # type: ignore[assignment]
    allocator_summaries.update({name: derived_summaries[name] for name in filtered_names if name in derived_summaries})
    allocator_pool_2x = {name: base_sleeves_2x[name] for name in core_names if name in base_sleeves_2x}
    allocator_pool_2x.update({name: derived_2x[name] for name in filtered_names if name in derived_2x})
    allocator_pool_5x = {name: base_sleeves_5x[name] for name in core_names if name in base_sleeves_5x}
    allocator_pool_5x.update({name: derived_5x[name] for name in filtered_names if name in derived_5x})

    pm_all: dict[str, pl.DataFrame] = {}
    pm_all_2x: dict[str, pl.DataFrame] = {}
    pm_all_5x: dict[str, pl.DataFrame] = {}
    pm_summaries_all: dict[str, dict[str, object]] = {}
    pm_weights_all: dict[str, pl.DataFrame] = {}
    print(f"[allocator] pool={len(allocator_pool)}", flush=True)
    for lookback in [42, 126]:
        for top_k in [1, 3, 5]:
            for target_vol in [0.35, 0.50, 0.70]:
                for max_leverage in [1.0, 1.5]:
                    name = f"PM_ADV_lb{lookback}_top{top_k}_tv{target_vol:g}_lev{max_leverage:g}"
                    daily, weights = lagged_portfolio_allocator(
                        allocator_pool,
                        name=name,
                        lookback=lookback,
                        top_k=top_k,
                        target_vol=target_vol,
                        max_leverage=max_leverage,
                        min_score=-0.03,
                        vol_penalty=0.35,
                        dd_penalty=0.60,
                    )
                    pm_all[name] = daily
                    pm_weights_all[name] = weights
                    weight_cols = [col for col in weights.columns if col.endswith("__weight")]
                    max_total_weight = (
                        float(weights.select(pl.sum_horizontal([pl.col(col).abs() for col in weight_cols]).max()).item())
                        if weight_cols
                        else 0.0
                    )
                    min_component_buffer = min(
                        (
                            float(summary.get("min_margin_buffer", 999.0) or 999.0)
                            for sleeve, summary in allocator_summaries.items()
                            if sleeve in allocator_pool and isinstance(summary, dict)
                        ),
                        default=999.0,
                    )
                    max_component_leverage = max(
                        (
                            float(summary.get("max_leverage", 0.0) or 0.0)
                            for sleeve, summary in allocator_summaries.items()
                            if sleeve in allocator_pool and isinstance(summary, dict)
                        ),
                        default=0.0,
                    )
                    pm_summaries_all[name] = {
                        "product": "PM",
                        "max_leverage": max_component_leverage * max(max_total_weight, 0.0),
                        "min_margin_buffer": min_component_buffer / max(max_total_weight, 1e-9) if max_total_weight > 0 else 999.0,
                        "margin_breach": any(
                            bool(summary.get("margin_breach", False))
                            for sleeve, summary in allocator_summaries.items()
                            if sleeve in allocator_pool and isinstance(summary, dict)
                        ),
                    }
                    if allocator_pool_2x:
                        s2, _ = lagged_portfolio_allocator(
                            allocator_pool_2x,
                            name=name,
                            lookback=lookback,
                            top_k=top_k,
                            target_vol=target_vol,
                            max_leverage=max_leverage,
                            min_score=-0.03,
                            vol_penalty=0.35,
                            dd_penalty=0.60,
                        )
                        pm_all_2x[name] = s2
                    if allocator_pool_5x:
                        s5, _ = lagged_portfolio_allocator(
                            allocator_pool_5x,
                            name=name,
                            lookback=lookback,
                            top_k=top_k,
                            target_vol=target_vol,
                            max_leverage=max_leverage,
                            min_score=-0.03,
                            vol_penalty=0.35,
                            dd_penalty=0.60,
                        )
                        pm_all_5x[name] = s5

    selected_pm_names = _top_by_quick_score(pm_all, n=72)
    pm_daily = {name: pm_all[name] for name in selected_pm_names}
    pm_daily_2x = {name: pm_all_2x[name] for name in selected_pm_names if name in pm_all_2x}
    pm_daily_5x = {name: pm_all_5x[name] for name in selected_pm_names if name in pm_all_5x}
    pm_summaries = {name: pm_summaries_all[name] for name in selected_pm_names if name in pm_summaries_all}
    pm_weights = {name: pm_weights_all[name] for name in selected_pm_names if name in pm_weights_all}
    print(f"[allocator] generated={len(pm_all)} selected={len(pm_daily)}", flush=True)

    all_candidates = {**derived, **pm_daily}
    all_summaries = {**derived_summaries, **pm_summaries}
    cost_stress_by_name: dict[str, tuple[float, float]] = {}
    for name, daily in all_candidates.items():
        s2 = derived_2x.get(name)
        if s2 is None:
            s2 = pm_daily_2x.get(name)
        s5 = derived_5x.get(name)
        if s5 is None:
            s5 = pm_daily_5x.get(name)
        cost_stress_by_name[name] = (
            nav_metrics(_oos_daily(s2 if s2 is not None else daily).select(["date", "nav"]), prefix="oos_")["oos_cagr"],
            nav_metrics(_oos_daily(s5 if s5 is not None else daily).select(["date", "nav"]), prefix="oos_")["oos_cagr"],
        )
    group_pbo = multi_config_pbo(all_candidates, n_splits=500)
    print(f"[validate] derived={len(derived)} pm={len(pm_daily)} pbo={group_pbo:.3f}", flush=True)
    final_rows = validate_rows(
        all_candidates,
        simulator_summary_by_name=all_summaries,
        cost_stress_by_name=cost_stress_by_name,
        n_trials=len(all_candidates),
        pbo=group_pbo,
    )

    summary = pl.DataFrame(final_rows).sort("objective", descending=True)
    summary.write_csv(OUT_DIR / "futures_advanced_summary.csv")
    diagnostic = summary.sort(["oos_cagr", "recent_1y_cagr"], descending=True)
    best_name = str((summary.filter(pl.col("verdict") == "pass").sort("objective", descending=True) if (summary["verdict"] == "pass").any() else diagnostic)["name"][0])
    best_daily = all_candidates[best_name]
    best_daily.write_csv(OUT_DIR / "top_daily.csv")
    if best_name in pm_weights:
        pm_weights[best_name].write_csv(OUT_DIR / "top_weights.csv")
    elif best_name in controls:
        controls[best_name].write_csv(OUT_DIR / "top_weights.csv")
    else:
        pl.DataFrame().write_csv(OUT_DIR / "top_weights.csv")

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        _write_benchmark_chart(best_daily, con)
    finally:
        con.close()

    _write_doc(summary, daily_cutoff, rpt_cutoff, time.time() - start)
    champion = summary.filter(pl.col("verdict") == "pass").sort("objective", descending=True)
    print(f"[done] advanced rows={summary.height} pbo={group_pbo:.3f} champion={champion['name'][0] if not champion.is_empty() else 'NONE'}")
    print(f"[artifacts] {OUT_DIR}")
    print(f"[doc] {DOC_PATH}")


if __name__ == "__main__":
    run()
