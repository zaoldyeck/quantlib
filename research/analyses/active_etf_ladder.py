"""Stair-step same-window ranking for Taiwan-listed active ETFs.

Prerequisite: refresh PostgreSQL and DuckDB cache before running:
  sbt "runMain Main update"
  uv run --project research python research/cache_tables.py

This script intentionally reads from `research/cache.duckdb` through
`research.db.connect()` and adjusted prices through `research.prices.py`.
"""
from __future__ import annotations

import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db import connect  # noqa: E402
from prices import total_return_series  # noqa: E402


@dataclass(frozen=True)
class ActiveEtf:
    code: str
    name: str
    annual_fee: float


TAIWAN_ACTIVE_ETFS = [
    ActiveEtf("00980A", "主動野村臺灣優選", 0.00785),
    ActiveEtf("00981A", "主動統一台股增長", 0.01120),
    ActiveEtf("00982A", "主動群益台灣強棒", 0.00835),
    ActiveEtf("00984A", "主動安聯台灣高息", 0.00740),
    ActiveEtf("00985A", "主動野村台灣50", 0.00485),
    ActiveEtf("00986A", "主動台新龍頭成長", 0.01250),
    ActiveEtf("00987A", "主動台新優勢成長", 0.00785),
    ActiveEtf("00988A", "主動統一全球創新", 0.01520),
    ActiveEtf("00990A", "主動元大AI新經濟", 0.01050),
    ActiveEtf("00991A", "主動復華未來50", 0.01040),
    ActiveEtf("00992A", "主動群益科技創新", 0.01235),
    ActiveEtf("00993A", "主動安聯台灣", 0.00740),
    ActiveEtf("00994A", "主動第一金台股優", 0.00735),
    ActiveEtf("00995A", "主動中信台灣卓越", 0.00785),
    ActiveEtf("00996A", "主動兆豐台灣豐收", 0.00785),
    ActiveEtf("00400A", "主動國泰動能高息", 0.00785),
    ActiveEtf("00401A", "主動摩根台灣鑫收", 0.00785),
    ActiveEtf("00999A", "主動野村臺灣高息", 0.00785),
]

MIN_FORMAL_TRADING_DAYS = 60
LADDER_MIN_ROWS = [60, 80, 120, 190, 225]
LIQUIDITY_GATE_TWD = 100_000_000.0
OUT_DIR = Path("research/out")


def _pct_rank(values: list[float], higher_is_better: bool = True) -> list[float]:
    if len(values) == 1:
        return [1.0]
    finite = [v for v in values if math.isfinite(v)]
    if not finite:
        return [0.0 for _ in values]
    worst = min(finite) if higher_is_better else max(finite)
    safe_values = [v if math.isfinite(v) else worst for v in values]
    order = sorted(range(len(safe_values)), key=lambda i: safe_values[i], reverse=higher_is_better)
    scores = [0.0] * len(values)
    for rank, idx in enumerate(order):
        scores[idx] = 1.0 - rank / (len(values) - 1)
    return scores


def _metrics(asset: pl.DataFrame, bench: pl.DataFrame) -> dict[str, float]:
    df = (
        asset.rename({"adj_close": "asset"})
        .join(bench.rename({"adj_close": "bench"}), on="date", how="inner")
        .sort("date")
    )
    if df.height < 5:
        raise ValueError("not enough aligned rows")

    asset_px = df["asset"].to_numpy()
    bench_px = df["bench"].to_numpy()
    dates = df["date"].to_list()
    asset_ret = asset_px[1:] / asset_px[:-1] - 1.0
    bench_ret = bench_px[1:] / bench_px[:-1] - 1.0

    calendar_days = (dates[-1] - dates[0]).days
    years = calendar_days / 365.25
    cum = asset_px[-1] / asset_px[0] - 1.0
    bench_cum = bench_px[-1] / bench_px[0] - 1.0
    cagr = (1.0 + cum) ** (1.0 / years) - 1.0 if years > 0 else cum

    downside = asset_ret[asset_ret < 0]
    sortino = (
        asset_ret.mean() / downside.std(ddof=0) * math.sqrt(252)
        if len(downside) > 1 and downside.std(ddof=0) > 0
        else 0.0
    )
    vol = float(asset_ret.std(ddof=0) * math.sqrt(252)) if len(asset_ret) > 1 else 0.0
    sharpe = (
        float(asset_ret.mean() / asset_ret.std(ddof=0) * math.sqrt(252))
        if len(asset_ret) > 1 and asset_ret.std(ddof=0) > 0
        else 0.0
    )
    peak = np.maximum.accumulate(asset_px)
    mdd = float((asset_px / peak - 1.0).min())
    calmar = float(cagr / abs(mdd)) if mdd < 0 else 0.0

    active_ret = asset_ret - bench_ret
    tracking_error = float(active_ret.std(ddof=0) * math.sqrt(252)) if len(active_ret) > 1 else 0.0
    ir = (
        active_ret.mean() / active_ret.std(ddof=0) * math.sqrt(252)
        if active_ret.std(ddof=0) > 0
        else 0.0
    )
    bench_var = np.var(bench_ret)
    beta = float(np.cov(asset_ret, bench_ret, ddof=0)[0, 1] / bench_var) if bench_var > 0 else 0.0
    alpha = float((asset_ret.mean() - beta * bench_ret.mean()) * 252)
    active_cum = float(cum - bench_cum)
    hit_rate = float((active_ret > 0).mean()) if len(active_ret) else 0.0

    up = bench_ret > 0
    up_sum = float(bench_ret[up].sum()) if up.any() else 0.0
    upside_capture = float(asset_ret[up].sum() / up_sum) if abs(up_sum) > 1e-12 else 0.0

    down = bench_ret < 0
    down_sum = float(bench_ret[down].sum()) if down.any() else 0.0
    downside_capture = float(asset_ret[down].sum() / down_sum) if abs(down_sum) > 1e-12 else 0.0

    return {
        "start": dates[0],
        "end": dates[-1],
        "trading_days": df.height,
        "calendar_days": calendar_days,
        "cum": float(cum),
        "cagr": float(cagr),
        "bench_cum": float(bench_cum),
        "active_cum": active_cum,
        "vol": vol,
        "sharpe": sharpe,
        "sortino": float(sortino),
        "mdd": mdd,
        "calmar": calmar,
        "ir": float(ir),
        "tracking_error": tracking_error,
        "hit_rate": hit_rate,
        "beta": beta,
        "alpha": alpha,
        "upside_capture": upside_capture,
        "downside_capture": downside_capture,
    }


def _rank(rows: list[dict[str, object]]) -> pl.DataFrame:
    cum_scores = _pct_rank([float(r["cum"]) for r in rows])
    sortino_scores = _pct_rank([float(r["sortino"]) for r in rows])
    ir_scores = _pct_rank([float(r["ir"]) for r in rows])
    mdd_scores = _pct_rank([float(r["mdd"]) for r in rows])
    fee_scores = _pct_rank([float(r["annual_fee"]) for r in rows], higher_is_better=False)

    scored = []
    for i, row in enumerate(rows):
        liquidity_score = min(float(row["liquidity_20d"]) / 100_000_000.0, 1.0)
        score = (
            0.30 * cum_scores[i]
            + 0.25 * sortino_scores[i]
            + 0.20 * ir_scores[i]
            + 0.10 * mdd_scores[i]
            + 0.10 * fee_scores[i]
            + 0.05 * liquidity_score
        )
        scored.append({**row, "score": score})

    return (
        pl.DataFrame(scored)
        .sort("score", descending=True)
        .with_row_index("rank", offset=1)
    )


def _window_rank(
    etfs: list[ActiveEtf],
    series: dict[str, pl.DataFrame],
    bench: pl.DataFrame,
    liquidity: dict[str, float],
    min_rows: int,
) -> pl.DataFrame:
    eligible = [etf for etf in etfs if series[etf.code].height >= min_rows]
    common_start = max(series[etf.code]["date"][0] for etf in eligible)
    rows: list[dict[str, object]] = []
    for etf in eligible:
        asset = series[etf.code].filter(pl.col("date") >= common_start)
        metric = _metrics(bench=bench.filter(pl.col("date") >= common_start), asset=asset)
        rows.append(
            {
                **metric,
                "cohort_min_rows": min_rows,
                "code": etf.code,
                "name": etf.name,
                "annual_fee": etf.annual_fee,
                "liquidity_20d": liquidity[etf.code],
                "listed_rows": series[etf.code].height,
            }
        )
    return _rank(rows)


def _cohort_percentile(rank: int, count: int) -> float:
    if count <= 1:
        return 1.0
    return 1.0 - (rank - 1) / (count - 1)


def _ladder_stability(all_ladder: pl.DataFrame) -> dict[str, dict[str, float]]:
    by_code: dict[str, list[float]] = {}
    cohort_sizes = {
        int(row["cohort_min_rows"]): int(row["count"])
        for row in all_ladder.group_by("cohort_min_rows").len(name="count").iter_rows(named=True)
    }
    for row in all_ladder.iter_rows(named=True):
        cohort = int(row["cohort_min_rows"])
        percentile = _cohort_percentile(int(row["rank"]), cohort_sizes[cohort])
        by_code.setdefault(str(row["code"]), []).append(percentile)

    out: dict[str, dict[str, float]] = {}
    max_cohorts = len(LADDER_MIN_ROWS)
    for code, percentiles in by_code.items():
        arr = np.array(percentiles, dtype=float)
        rank_vol = float(arr.std(ddof=0)) if len(arr) > 1 else 0.50
        # One-window histories are not "stable"; they are simply untested.
        rank_consistency = max(0.0, 1.0 - min(rank_vol / 0.35, 1.0))
        out[code] = {
            "ladder_avg_pct": float(arr.mean()),
            "ladder_coverage": len(percentiles) / max_cohorts,
            "ladder_rank_vol": rank_vol,
            "ladder_consistency": rank_consistency,
        }
    return out


def _decision_rank(formal_rank: pl.DataFrame, all_ladder: pl.DataFrame) -> pl.DataFrame:
    """Investor-oriented active ETF ranking.

    The legacy per-cohort `score` is a useful short-window snapshot. This
    decision score is stricter: active skill and downside behavior dominate,
    short-window return is capped at 10%, and funds with only one eligible
    window must pay an evidence penalty.
    """
    rows = formal_rank.to_dicts()
    stability = _ladder_stability(all_ladder)

    skill_ir = _pct_rank([float(r["ir"]) for r in rows])
    skill_alpha = _pct_rank([float(r["alpha"]) for r in rows])
    skill_active = _pct_rank([float(r["active_cum"]) for r in rows])

    risk_sortino = _pct_rank([float(r["sortino"]) for r in rows])
    risk_mdd = _pct_rank([float(r["mdd"]) for r in rows])
    risk_downside = _pct_rank([float(r["downside_capture"]) for r in rows], higher_is_better=False)
    risk_calmar = _pct_rank([float(r["calmar"]) for r in rows])

    return_cum = _pct_rank([float(r["cum"]) for r in rows])
    return_upside = _pct_rank([float(r["upside_capture"]) for r in rows])

    fee = _pct_rank([float(r["annual_fee"]) for r in rows], higher_is_better=False)

    scored = []
    for i, row in enumerate(rows):
        code = str(row["code"])
        stab = stability[code]
        evidence_score = min(float(row["listed_rows"]) / 225.0, 1.0)
        stability_score = (
            0.45 * stab["ladder_avg_pct"]
            + 0.25 * stab["ladder_coverage"]
            + 0.20 * evidence_score
            + 0.10 * stab["ladder_consistency"]
        )
        liquidity_gate = min(float(row["liquidity_20d"]) / LIQUIDITY_GATE_TWD, 1.0)
        implementation_score = 0.65 * fee[i] + 0.35 * liquidity_gate
        active_skill_score = 0.40 * skill_ir[i] + 0.35 * skill_alpha[i] + 0.25 * skill_active[i]
        downside_score = (
            0.30 * risk_sortino[i]
            + 0.25 * risk_mdd[i]
            + 0.25 * risk_downside[i]
            + 0.20 * risk_calmar[i]
        )
        return_score = 0.70 * return_cum[i] + 0.30 * return_upside[i]
        decision_score = (
            0.30 * active_skill_score
            + 0.25 * downside_score
            + 0.25 * stability_score
            + 0.10 * return_score
            + 0.10 * implementation_score
        )
        scored.append(
            {
                **row,
                "active_skill_score": active_skill_score,
                "downside_score": downside_score,
                "stability_score": stability_score,
                "return_score": return_score,
                "implementation_score": implementation_score,
                "liquidity_gate": liquidity_gate,
                **stab,
                "decision_score": decision_score,
            }
        )

    return (
        pl.DataFrame(scored)
        .sort("decision_score", descending=True)
        .with_row_index("decision_rank", offset=1)
    )


def _fmt_pct(x: float) -> str:
    return f"{x * 100:+.2f}%"


def _print_table(title: str, df: pl.DataFrame, limit: int | None = None) -> None:
    view = df if limit is None else df.head(limit)
    print(f"\n## {title}")
    print(
        "| 排名 | 代號 | 名稱 | 區間 | 交易日 | 同期間報酬 | 0050 | Sortino | MDD | 年化 Alpha | IR | 費用 | 20D成交值 | Score |"
    )
    print("|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in view.iter_rows(named=True):
        liq_yi = float(row["liquidity_20d"]) / 100_000_000.0
        print(
            f"| {row['rank']} | {row['code']} | {row['name']} | {row['start']}~{row['end']} | "
            f"{row['trading_days']} | {_fmt_pct(float(row['cum']))} | {_fmt_pct(float(row['bench_cum']))} | "
            f"{float(row['sortino']):.2f} | {_fmt_pct(float(row['mdd']))} | {_fmt_pct(float(row['alpha']))} | "
            f"{float(row['ir']):.2f} | {float(row['annual_fee'])*100:.3f}% | {liq_yi:.2f} 億 | {float(row['score']):.3f} |"
        )


def _print_decision_table(df: pl.DataFrame, limit: int | None = None) -> None:
    view = df if limit is None else df.head(limit)
    print("\n## Professional decision rank")
    print(
        "| 排名 | 代號 | 名稱 | 同窗報酬 | Sortino | MDD | DownCap | IR | "
        "階梯覆蓋 | 穩定分 | 費用 | 20D成交值 | Decision |"
    )
    print("|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in view.iter_rows(named=True):
        liq_yi = float(row["liquidity_20d"]) / 100_000_000.0
        print(
            f"| {row['decision_rank']} | {row['code']} | {row['name']} | "
            f"{_fmt_pct(float(row['cum']))} | {float(row['sortino']):.2f} | "
            f"{_fmt_pct(float(row['mdd']))} | {float(row['downside_capture']):.2f} | "
            f"{float(row['ir']):.2f} | {float(row['ladder_coverage']):.0%} | "
            f"{float(row['stability_score']):.3f} | {float(row['annual_fee'])*100:.3f}% | "
            f"{liq_yi:.2f} 億 | {float(row['decision_score']):.3f} |"
        )


def main() -> None:
    con = connect()
    cutoff = con.sql("SELECT max(date) FROM daily_quote WHERE market='twse'").fetchone()[0]
    cache_rows = con.sql("SELECT count(*) FROM daily_quote").fetchone()[0]
    print(f"cache_cutoff={cutoff} daily_quote_rows={cache_rows}")

    codes = [etf.code for etf in TAIWAN_ACTIVE_ETFS]
    series = {
        code: total_return_series(con, code, "2025-01-01", str(cutoff), market="twse")
        for code in codes
    }
    bench = total_return_series(con, "0050", "2025-01-01", str(cutoff), market="twse")
    liquidity = {
        code: con.sql(
            f"""
            SELECT avg(trade_value)
            FROM (
                SELECT trade_value
                FROM daily_quote
                WHERE market='twse' AND company_code='{code}'
                ORDER BY date DESC
                LIMIT 20
            )
            """
        ).fetchone()[0]
        or 0.0
        for code in codes
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    listings = pl.DataFrame(
        [
            {
                "code": etf.code,
                "name": etf.name,
                "first_date": series[etf.code]["date"][0] if series[etf.code].height else None,
                "last_date": series[etf.code]["date"][-1] if series[etf.code].height else None,
                "trading_days": series[etf.code].height,
                "liquidity_20d": liquidity[etf.code],
            }
            for etf in TAIWAN_ACTIVE_ETFS
        ]
    ).sort(["trading_days", "code"], descending=[True, False])
    listings.write_csv(OUT_DIR / "active_etf_ladder_listings.csv")

    frames = []
    for min_rows in LADDER_MIN_ROWS:
        df = _window_rank(TAIWAN_ACTIVE_ETFS, series, bench, liquidity, min_rows)
        frames.append(df)
        df.write_csv(OUT_DIR / f"active_etf_ladder_min{min_rows}.csv")
        _print_table(f"Ladder min_rows={min_rows}", df)

    all_ladder = pl.concat(frames, how="vertical")
    all_ladder.write_csv(OUT_DIR / "active_etf_ladder_all.csv")

    decision = _decision_rank(frames[0], all_ladder)
    decision.write_csv(OUT_DIR / "active_etf_decision_rank.csv")
    _print_decision_table(decision)

    watchlist = listings.filter(pl.col("trading_days") < MIN_FORMAL_TRADING_DAYS)
    print("\n## Watchlist <60 trading days")
    print("| 代號 | 名稱 | 上市資料起日 | 交易日 | 20D成交值 |")
    print("|---|---|---|---:|---:|")
    for row in watchlist.iter_rows(named=True):
        print(
            f"| {row['code']} | {row['name']} | {row['first_date']} | "
            f"{row['trading_days']} | {float(row['liquidity_20d'])/100_000_000.0:.2f} 億 |"
        )


if __name__ == "__main__":
    main()
