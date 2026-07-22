"""Rank all Taiwan domestic-equity ETFs using adjusted total-return prices.

Inputs are intentionally local and reproducible:
  1. `sbt "runMain Main update"` refreshes TWSE/TPEx files and PostgreSQL.
  2. `uv run --project research python research/cache_tables.py` syncs DuckDB.
  3. This script reads TWSE ETF metadata from `data/etf/*.json` and adjusted
     prices from `research.prices.fetch_adjusted_panel`.

The output is a professional decision ranking, not a one-window return chase.
Short-history ETFs are included in the universe but receive an explicit
evidence penalty and are also listed in a watchlist when fewer than 60 trading
days are available.
"""
from __future__ import annotations

import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
from research import paths

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from research.db import connect  # noqa: E402
from research.prices import fetch_adjusted_panel  # noqa: E402


ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = paths.OUT
DOC_PATH = ROOT / "docs" / "taiwan_equity_etf_ranking.md"
DOMESTIC_JSON = ROOT / "data" / "etf" / "domestic.json"
ALL_JSON = ROOT / "data" / "etf" / "all.json"

BENCHMARK_CODE = "0050"
MIN_FORMAL_TRADING_DAYS = 60
LIQUIDITY_GATE_TWD = 100_000_000.0
SPREAD_GATE = 0.005

WINDOWS: list[tuple[str, int, float]] = [
    ("60D", 60, 0.26),
    ("120D", 120, 0.24),
    ("1Y", 252, 0.20),
    ("3Y", 756, 0.18),
    ("5Y", 1260, 0.12),
]


@dataclass(frozen=True)
class EtfMeta:
    code: str
    name: str
    management: str
    category: str
    listing_date: date | None
    issuer: str | None
    index_name: str | None

    @property
    def management_label(self) -> str:
        return "主動" if "主動" in self.management else "被動"


def _parse_listing_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y.%m.%d").date()
    except ValueError:
        return None


def _load_current_domestic_etfs() -> list[EtfMeta]:
    domestic = json.loads(DOMESTIC_JSON.read_text(encoding="utf-8"))
    all_etfs = json.loads(ALL_JSON.read_text(encoding="utf-8"))
    all_by_code = {str(row[1]): row for row in all_etfs["data"]}

    out: list[EtfMeta] = []
    for row in domestic["data"]:
        code = str(row[0]).strip()
        all_row = all_by_code.get(code)
        out.append(
            EtfMeta(
                code=code,
                name=str(row[1]).strip(),
                management=str(row[2] or "").strip(),
                category=str(row[3] or "").strip(),
                listing_date=_parse_listing_date(str(all_row[0])) if all_row else None,
                issuer=str(all_row[3]).strip() if all_row and all_row[3] else None,
                index_name=str(all_row[4]).strip() if all_row and all_row[4] else None,
            )
        )
    return out


def _sql_codes(codes: list[str]) -> str:
    return ",".join(f"'{code}'" for code in codes)


def _pct_rank(values: list[float], higher_is_better: bool = True) -> list[float]:
    if not values:
        return []
    if len(values) == 1:
        return [1.0]
    finite = [float(v) for v in values if math.isfinite(float(v))]
    if not finite:
        return [0.0 for _ in values]

    replacement = min(finite) if higher_is_better else max(finite)
    safe = [float(v) if math.isfinite(float(v)) else replacement for v in values]
    order = sorted(range(len(safe)), key=lambda i: safe[i], reverse=higher_is_better)
    scores = [0.0] * len(values)
    for rank, idx in enumerate(order):
        scores[idx] = 1.0 - rank / (len(values) - 1)
    return scores


def _clip01(x: float) -> float:
    if not math.isfinite(x):
        return 0.0
    return min(max(x, 0.0), 1.0)


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        x = float(value)
    except (TypeError, ValueError):
        return default
    return x if math.isfinite(x) else default


def _quote_stats(con: Any, codes: list[str]) -> dict[str, dict[str, Any]]:
    code_sql = _sql_codes(codes)
    rows = con.sql(
        f"""
        WITH q AS (
            SELECT
                company_code,
                date,
                closing_price,
                trade_value,
                last_best_bid_price,
                last_best_ask_price,
                row_number() OVER (
                    PARTITION BY company_code
                    ORDER BY date DESC
                ) AS rn
            FROM daily_quote
            WHERE market='twse'
              AND company_code IN ({code_sql})
              AND closing_price > 0
        )
        SELECT
            company_code,
            min(date) AS first_quote_date,
            max(date) AS last_quote_date,
            count(*) AS quote_rows,
            avg(CASE WHEN rn <= 20 THEN trade_value END) AS liquidity_20d,
            avg(CASE WHEN rn <= 60 THEN trade_value END) AS liquidity_60d,
            max(CASE WHEN rn = 1 THEN closing_price END) AS latest_close,
            max(CASE WHEN rn = 1 THEN last_best_bid_price END) AS latest_bid,
            max(CASE WHEN rn = 1 THEN last_best_ask_price END) AS latest_ask
        FROM q
        GROUP BY company_code
        """
    ).to_df().to_dict(orient="records")
    return {str(row["company_code"]): row for row in rows}


def _tdcc_stats(con: Any, codes: list[str]) -> tuple[date | None, dict[str, dict[str, Any]]]:
    code_sql = _sql_codes(codes)
    latest = con.sql("SELECT max(data_date) FROM tdcc_shareholding").fetchone()[0]
    if latest is None:
        return None, {}
    rows = con.sql(
        f"""
        SELECT
            company_code,
            sum(num_holders) AS holders,
            sum(num_shares) AS tdcc_shares
        FROM tdcc_shareholding
        WHERE data_date = DATE '{latest}'
          AND company_code IN ({code_sql})
        GROUP BY company_code
        """
    ).to_df().to_dict(orient="records")
    return latest, {str(row["company_code"]): row for row in rows}


def _series_by_code(panel: pl.DataFrame, codes: list[str]) -> dict[str, pl.DataFrame]:
    return {
        code: (
            panel.filter(pl.col("company_code") == code)
            .sort("date")
            .select(["date", "close", "raw_close"])
            .rename({"close": "adj_close"})
        )
        for code in codes
    }


def _slice_window(df: pl.DataFrame, rows: int) -> pl.DataFrame:
    if df.height < rows:
        return pl.DataFrame(schema=df.schema)
    return df.tail(rows)


def _metrics(asset: pl.DataFrame, bench: pl.DataFrame) -> dict[str, float | date | int]:
    joined = (
        asset.rename({"adj_close": "asset"})
        .join(bench.rename({"adj_close": "bench"}), on="date", how="inner")
        .sort("date")
    )
    if joined.height < 5:
        raise ValueError("not enough aligned rows")

    asset_px = joined["asset"].to_numpy()
    bench_px = joined["bench"].to_numpy()
    dates = joined["date"].to_list()
    asset_ret = asset_px[1:] / asset_px[:-1] - 1.0
    bench_ret = bench_px[1:] / bench_px[:-1] - 1.0

    calendar_days = max((dates[-1] - dates[0]).days, 1)
    years = calendar_days / 365.25
    cum = float(asset_px[-1] / asset_px[0] - 1.0)
    bench_cum = float(bench_px[-1] / bench_px[0] - 1.0)
    cagr = float((1.0 + cum) ** (1.0 / years) - 1.0) if years > 0 and cum > -1.0 else cum
    bench_cagr = (
        float((1.0 + bench_cum) ** (1.0 / years) - 1.0)
        if years > 0 and bench_cum > -1.0
        else bench_cum
    )

    asset_std = asset_ret.std(ddof=0) if len(asset_ret) > 1 else 0.0
    vol = float(asset_std * math.sqrt(252)) if asset_std > 0 else 0.0
    sharpe = float(asset_ret.mean() / asset_std * math.sqrt(252)) if asset_std > 0 else 0.0

    downside = asset_ret[asset_ret < 0]
    downside_std = downside.std(ddof=0) if len(downside) > 1 else 0.0
    sortino = float(asset_ret.mean() / downside_std * math.sqrt(252)) if downside_std > 0 else 0.0

    peak = np.maximum.accumulate(asset_px)
    mdd = float((asset_px / peak - 1.0).min())
    calmar = float(cagr / abs(mdd)) if mdd < 0 else 0.0

    active_ret = asset_ret - bench_ret
    active_std = active_ret.std(ddof=0) if len(active_ret) > 1 else 0.0
    tracking_error = float(active_std * math.sqrt(252)) if active_std > 0 else 0.0
    ir = float(active_ret.mean() / active_std * math.sqrt(252)) if active_std > 0 else 0.0
    bench_var = np.var(bench_ret)
    beta = float(np.cov(asset_ret, bench_ret, ddof=0)[0, 1] / bench_var) if bench_var > 0 else 0.0
    alpha = float((asset_ret.mean() - beta * bench_ret.mean()) * 252)
    active_cum = float(cum - bench_cum)
    excess_cagr = float(cagr - bench_cagr)
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
        "trading_days": joined.height,
        "calendar_days": calendar_days,
        "cum": cum,
        "bench_cum": bench_cum,
        "active_cum": active_cum,
        "cagr": cagr,
        "bench_cagr": bench_cagr,
        "excess_cagr": excess_cagr,
        "vol": vol,
        "sharpe": sharpe,
        "sortino": sortino,
        "mdd": mdd,
        "calmar": calmar,
        "ir": ir,
        "tracking_error": tracking_error,
        "hit_rate": hit_rate,
        "beta": beta,
        "alpha": alpha,
        "upside_capture": upside_capture,
        "downside_capture": downside_capture,
    }


def _score_window(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()

    return_cagr = _pct_rank([_safe_float(r["cagr"]) for r in rows])
    return_excess = _pct_rank([_safe_float(r["excess_cagr"]) for r in rows])
    skill_ir = _pct_rank([_safe_float(r["ir"]) for r in rows])
    skill_alpha = _pct_rank([_safe_float(r["alpha"]) for r in rows])
    skill_hit = _pct_rank([_safe_float(r["hit_rate"]) for r in rows])
    risk_sortino = _pct_rank([_safe_float(r["sortino"]) for r in rows])
    risk_calmar = _pct_rank([_safe_float(r["calmar"]) for r in rows])
    risk_mdd = _pct_rank([_safe_float(r["mdd"]) for r in rows])
    risk_down = _pct_rank([_safe_float(r["downside_capture"]) for r in rows], higher_is_better=False)
    trend_sharpe = _pct_rank([_safe_float(r["sharpe"]) for r in rows])

    scored: list[dict[str, Any]] = []
    for i, row in enumerate(rows):
        return_score = 0.60 * return_cagr[i] + 0.40 * return_excess[i]
        relative_score = 0.45 * skill_ir[i] + 0.35 * skill_alpha[i] + 0.20 * skill_hit[i]
        risk_score = (
            0.30 * risk_sortino[i]
            + 0.25 * risk_calmar[i]
            + 0.25 * risk_mdd[i]
            + 0.20 * risk_down[i]
        )
        trend_score = trend_sharpe[i]
        implementation_score = _safe_float(row["implementation_score"])
        window_score = (
            0.25 * return_score
            + 0.30 * relative_score
            + 0.25 * risk_score
            + 0.10 * trend_score
            + 0.10 * implementation_score
        )
        scored.append(
            {
                **row,
                "return_score": return_score,
                "relative_score": relative_score,
                "risk_score": risk_score,
                "trend_score": trend_score,
                "window_score": window_score,
            }
        )

    return (
        pl.DataFrame(scored)
        .sort("window_score", descending=True)
        .with_row_index("window_rank", offset=1)
    )


def _format_pct(x: Any, digits: int = 2, signed: bool = True) -> str:
    if x is None:
        return "-"
    value = _safe_float(x, default=float("nan"))
    if not math.isfinite(value):
        return "-"
    sign = "+" if signed else ""
    return f"{value * 100:{sign}.{digits}f}%"


def _format_num(x: Any, digits: int = 2) -> str:
    value = _safe_float(x, default=float("nan"))
    if not math.isfinite(value):
        return "-"
    return f"{value:.{digits}f}"


def _format_yi(x: Any) -> str:
    value = _safe_float(x)
    return f"{value / 100_000_000.0:.2f}"


def _format_holders(x: Any) -> str:
    value = _safe_float(x)
    if value <= 0:
        return "-"
    return f"{value:,.0f}"


def _window_map(df: pl.DataFrame, metric: str) -> dict[tuple[str, str], Any]:
    if df.is_empty():
        return {}
    return {
        (str(row["code"]), str(row["window"])): row[metric]
        for row in df.iter_rows(named=True)
    }


def _aggregate(
    metas: list[EtfMeta],
    quote_stats: dict[str, dict[str, Any]],
    tdcc_stats: dict[str, dict[str, Any]],
    window_metrics: pl.DataFrame,
    since_metrics: dict[str, dict[str, Any]],
) -> pl.DataFrame:
    window_rows = window_metrics.to_dicts() if not window_metrics.is_empty() else []
    weights = {name: weight for name, _, weight in WINDOWS}
    total_weight = sum(weights.values())
    by_code: dict[str, list[dict[str, Any]]] = {}
    for row in window_rows:
        by_code.setdefault(str(row["code"]), []).append(row)

    out: list[dict[str, Any]] = []
    for meta in metas:
        q = quote_stats.get(meta.code, {})
        t = tdcc_stats.get(meta.code, {})
        rows = by_code.get(meta.code, [])
        quote_rows = int(q.get("quote_rows") or 0)
        last_quote_date = q.get("last_quote_date")
        latest_missing = not rows

        if rows:
            available_weight = sum(weights[str(row["window"])] for row in rows)
            weighted_score = sum(
                _safe_float(row["window_score"]) * weights[str(row["window"])]
                for row in rows
            ) / available_weight
            rank_percentiles = []
            for row in rows:
                count = sum(1 for peer in window_rows if peer["window"] == row["window"])
                pct = 1.0 if count <= 1 else 1.0 - (int(row["window_rank"]) - 1) / (count - 1)
                rank_percentiles.append(pct)
            rank_std = float(np.std(rank_percentiles, ddof=0)) if len(rank_percentiles) > 1 else 0.50
            consistency_score = max(0.0, 1.0 - min(rank_std / 0.35, 1.0))
        else:
            available_weight = 0.0
            weighted_score = 0.0
            rank_std = 0.0
            consistency_score = 0.0

        coverage_score = available_weight / total_weight
        age_score = min(math.log1p(max(quote_rows, 0)) / math.log1p(1260), 1.0)
        evidence_score = 0.65 * coverage_score + 0.35 * age_score

        bid = _safe_float(q.get("latest_bid"))
        ask = _safe_float(q.get("latest_ask"))
        spread = (ask / bid - 1.0) if bid > 0 and ask > bid else None
        spread_score = 0.50 if spread is None else 1.0 - min(spread / SPREAD_GATE, 1.0)
        liquidity_gate = min(_safe_float(q.get("liquidity_20d")) / LIQUIDITY_GATE_TWD, 1.0)
        implementation_score = 0.75 * liquidity_gate + 0.25 * _clip01(spread_score)

        decision_score = (
            0.68 * weighted_score
            + 0.15 * evidence_score
            + 0.07 * consistency_score
            + 0.10 * implementation_score
        )

        status = "formal"
        if quote_rows < MIN_FORMAL_TRADING_DAYS:
            status = "watchlist_lt60d"
        elif latest_missing:
            status = "no_aligned_metrics"

        since = since_metrics.get(meta.code, {})
        out.append(
            {
                "code": meta.code,
                "name": meta.name,
                "management": meta.management_label,
                "category": meta.category,
                "listing_date": meta.listing_date,
                "issuer": meta.issuer,
                "index_name": meta.index_name,
                "first_quote_date": q.get("first_quote_date"),
                "last_quote_date": last_quote_date,
                "quote_rows": quote_rows,
                "liquidity_20d": _safe_float(q.get("liquidity_20d")),
                "liquidity_60d": _safe_float(q.get("liquidity_60d")),
                "latest_close": _safe_float(q.get("latest_close"), default=float("nan")),
                "latest_spread": spread,
                "holders": _safe_float(t.get("holders")),
                "tdcc_shares": _safe_float(t.get("tdcc_shares")),
                "weighted_window_score": weighted_score,
                "evidence_score": evidence_score,
                "coverage_score": coverage_score,
                "age_score": age_score,
                "consistency_score": consistency_score,
                "rank_std": rank_std,
                "liquidity_gate": liquidity_gate,
                "implementation_score": implementation_score,
                "decision_score": decision_score,
                "since_start": since.get("start"),
                "since_trading_days": since.get("trading_days"),
                "since_cum": since.get("cum"),
                "since_cagr": since.get("cagr"),
                "since_mdd": since.get("mdd"),
                "since_sortino": since.get("sortino"),
                "since_ir": since.get("ir"),
                "status": status,
            }
        )

    df = pl.DataFrame(out)
    formal = (
        df.filter(pl.col("status") == "formal")
        .sort("decision_score", descending=True)
        .with_row_index("rank", offset=1)
    )
    watch = df.filter(pl.col("status") != "formal").with_columns(pl.lit(None).alias("rank"))
    return pl.concat([formal, watch], how="diagonal_relaxed")


def _build_window_metrics(
    metas: list[EtfMeta],
    series: dict[str, pl.DataFrame],
    bench: pl.DataFrame,
    quote_stats: dict[str, dict[str, Any]],
) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    for window, rows_required, weight in WINDOWS:
        rows: list[dict[str, Any]] = []
        for meta in metas:
            asset = series.get(meta.code)
            if asset is None or asset.height < rows_required:
                continue
            asset_w = _slice_window(asset, rows_required)
            if asset_w.is_empty():
                continue
            bench_w = bench.filter(
                (pl.col("date") >= asset_w["date"][0])
                & (pl.col("date") <= asset_w["date"][-1])
            )
            try:
                metric = _metrics(asset_w, bench_w)
            except ValueError:
                continue

            q = quote_stats.get(meta.code, {})
            bid = _safe_float(q.get("latest_bid"))
            ask = _safe_float(q.get("latest_ask"))
            spread = (ask / bid - 1.0) if bid > 0 and ask > bid else None
            spread_score = 0.50 if spread is None else 1.0 - min(spread / SPREAD_GATE, 1.0)
            liquidity_gate = min(_safe_float(q.get("liquidity_20d")) / LIQUIDITY_GATE_TWD, 1.0)
            implementation_score = 0.75 * liquidity_gate + 0.25 * _clip01(spread_score)
            rows.append(
                {
                    **metric,
                    "window": window,
                    "window_rows_required": rows_required,
                    "window_weight": weight,
                    "code": meta.code,
                    "name": meta.name,
                    "management": meta.management_label,
                    "liquidity_20d": _safe_float(q.get("liquidity_20d")),
                    "latest_spread": spread,
                    "implementation_score": implementation_score,
                    "quote_rows": int(q.get("quote_rows") or 0),
                }
            )
        ranked = _score_window(rows)
        if not ranked.is_empty():
            frames.append(ranked)
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed")


def _since_inception_metrics(
    metas: list[EtfMeta],
    series: dict[str, pl.DataFrame],
    bench: pl.DataFrame,
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for meta in metas:
        asset = series.get(meta.code)
        if asset is None or asset.height < 5:
            continue
        bench_w = bench.filter(
            (pl.col("date") >= asset["date"][0])
            & (pl.col("date") <= asset["date"][-1])
        )
        try:
            out[meta.code] = _metrics(asset, bench_w)
        except ValueError:
            continue
    return out


def _write_doc(
    ranking: pl.DataFrame,
    window_metrics: pl.DataFrame,
    cutoff: date,
    cache_rows: int,
    tdcc_date: date | None,
) -> None:
    formal = ranking.filter(pl.col("status") == "formal").sort("rank")
    watch = ranking.filter(pl.col("status") != "formal").sort("listing_date")
    by_rank = _window_map(window_metrics, "window_rank")
    by_cagr = _window_map(window_metrics, "cagr")
    by_mdd = _window_map(window_metrics, "mdd")
    by_sortino = _window_map(window_metrics, "sortino")
    by_excess = _window_map(window_metrics, "excess_cagr")

    def rank_cell(code: str, window: str) -> str:
        value = by_rank.get((code, window))
        return "-" if value is None else str(int(value))

    def metric_cell(code: str, window: str, mapping: dict[tuple[str, str], Any]) -> str:
        return _format_pct(mapping.get((code, window)))

    top = formal.head(20)

    lines: list[str] = []
    lines.extend(
        [
            "# 台股 ETF 全市場排行",
            "",
            f"資料截止日：`{cutoff}`。本次已先執行標準資料更新並重建 DuckDB cache；`daily_quote` cache rows = `{cache_rows:,}`。",
            "",
            "## 結論",
            "",
        ]
    )

    if formal.height:
        winner = formal.row(0, named=True)
        lines.append(
            f"以本報告的專業綜合分數來看，第一名是 **{winner['code']} {winner['name']}**。"
            "它不是只靠單一期間累積報酬勝出，而是在近期報酬、相對 0050 的超額能力、下檔風險、流動性與樣本證據之間取得目前最好的綜合平衡。"
        )
        lines.append("")

    lines.extend(
        [
            "前 20 名如下；完整正式排行與樣本不足觀察名單在後面的附錄。",
            "",
            "| Rank | 代號 | 名稱 | 類型 | Decision | 60D Rank | 120D Rank | 1Y Rank | 3Y Rank | 5Y Rank | 近60D CAGR | 近60D超額CAGR vs 0050 | 近60D MDD | 近60D Sortino | 上市以來CAGR | 20D成交值(億) | 受益人數 |",
            "|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in top.iter_rows(named=True):
        code = str(row["code"])
        lines.append(
            f"| {int(row['rank'])} | {code} | {row['name']} | {row['management']} | "
            f"{_safe_float(row['decision_score']):.3f} | {rank_cell(code, '60D')} | {rank_cell(code, '120D')} | "
            f"{rank_cell(code, '1Y')} | {rank_cell(code, '3Y')} | {rank_cell(code, '5Y')} | "
            f"{metric_cell(code, '60D', by_cagr)} | {metric_cell(code, '60D', by_excess)} | "
            f"{metric_cell(code, '60D', by_mdd)} | {_format_num(by_sortino.get((code, '60D')))} | "
            f"{_format_pct(row['since_cagr'])} | {_format_yi(row['liquidity_20d'])} | {_format_holders(row['holders'])} |"
        )

    lines.extend(
        [
            "",
            "## 方法",
            "",
            "- **母體**：TWSE ETF 官方 `domestic` 分類中的現行國內股票型 ETF，共納入 77 檔；海外、債券、商品、期貨、槓桿與反向 ETF 不列入本次台股 ETF 排名。",
            "- **價格**：所有報酬率使用 `research/prices.py` 的 total-return-equivalent 還原股價，股息、減資與分割等事件已回補到價格序列中。",
            "- **基準**：以 `0050 元大台灣50` 作為台股機會成本 benchmark。產業型或主題型 ETF 未必以 0050 為正式追蹤基準，但長期持有決策仍需要回答是否值得放棄 0050。",
            "- **窗口**：同時評估最近 60D、120D、1Y、3Y、5Y；每個窗口只在同一窗口內互相比較，避免用上市以來累積報酬直接比較不同成立年份的 ETF。",
            "- **短期 CAGR 警語**：60D、120D 的 CAGR 是把短期漲跌年化後的數字，適合用來比較近期強弱，不代表未來一年真的會照同樣速度複利成長。",
            "- **分數**：窗口內分數由報酬 25%、相對 0050 能力 30%、下檔風險 25%、趨勢品質 10%、交易可執行性 10% 組成；總分再加入樣本證據、跨窗口穩定度與最新流動性。",
            "- **費用處理**：本機完整費率表尚未覆蓋所有 ETF，因此沒有把明示管理費做成單獨加減分；但 ETF 歷史市價與還原股價本身已是扣除基金內扣費用後的結果，歷史績效已反映費用拖累。",
            "",
            "## 指標解釋",
            "",
            "- **Decision**：本報告的總分，0 到 1 越高越好，用來做最終排名。",
            "- **CAGR**：年化報酬率，把不同長度期間轉成每年平均成長率。",
            "- **超額 CAGR vs 0050**：同期間 ETF 年化報酬率減掉 0050 年化報酬率；正數代表跑贏 0050。",
            "- **MDD**：最大回撤，代表從高點到低點最深跌幅；越接近 0 越好。",
            "- **Sortino**：只懲罰下跌波動的風險調整報酬；越高代表承受同樣下檔風險拿到更多報酬。",
            "- **IR**：Information Ratio，衡量相對 0050 的超額報酬是否穩定；越高代表不是只靠少數幾天偶然贏。",
            "- **20D成交值**：最近 20 個交易日平均成交金額，單位為億元；越高越容易大額進出。",
            "",
            "## 正式完整排行",
            "",
            "| Rank | 代號 | 名稱 | 類型 | Decision | Evidence | Consistency | 60D | 120D | 1Y | 3Y | 5Y | 上市以來CAGR | 上市以來MDD | 20D成交值(億) | 最新收盤 | 受益人數 | 上市日 |",
            "|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in formal.iter_rows(named=True):
        code = str(row["code"])
        lines.append(
            f"| {int(row['rank'])} | {code} | {row['name']} | {row['management']} | "
            f"{_safe_float(row['decision_score']):.3f} | {_safe_float(row['evidence_score']):.3f} | "
            f"{_safe_float(row['consistency_score']):.3f} | {rank_cell(code, '60D')} | {rank_cell(code, '120D')} | "
            f"{rank_cell(code, '1Y')} | {rank_cell(code, '3Y')} | {rank_cell(code, '5Y')} | "
            f"{_format_pct(row['since_cagr'])} | {_format_pct(row['since_mdd'])} | "
            f"{_format_yi(row['liquidity_20d'])} | {_format_num(row['latest_close'])} | "
            f"{_format_holders(row['holders'])} | {row['listing_date']} |"
        )

    lines.extend(
        [
            "",
            "## 樣本不足觀察名單",
            "",
            "這些 ETF 已納入母體與資料檢查，但因上市交易日少於 60 天，暫不給正式總排名。短線表現可以觀察，但不應與已經經歷多個市場窗口的 ETF 等量齊觀。",
            "",
            "| 代號 | 名稱 | 類型 | 上市日 | 交易日 | 最新收盤 | 20D成交值(億) | 受益人數 |",
            "|---|---|---|---|---:|---:|---:|---:|",
        ]
    )
    for row in watch.iter_rows(named=True):
        lines.append(
            f"| {row['code']} | {row['name']} | {row['management']} | {row['listing_date']} | "
            f"{int(row['quote_rows'])} | {_format_num(row['latest_close'])} | "
            f"{_format_yi(row['liquidity_20d'])} | {_format_holders(row['holders'])} |"
        )

    lines.extend(
        [
            "",
            "## 產出檔案",
            "",
            f"- `{paths.OUT}/taiwan_equity_etf_ranking.csv`：正式排行、觀察名單與彙總欄位。",
            f"- `{paths.OUT}/taiwan_equity_etf_window_metrics.csv`：每一檔 ETF 在每個窗口的完整 KPI。",
            "",
            "## 資料來源",
            "",
            "- 本地 PostgreSQL / DuckDB：`daily_quote`、`ex_right_dividend`、`capital_reduction`、`tdcc_shareholding`。",
            "- TWSE ETF metadata：`https://www.twse.com.tw/rwd/zh/ETF/domestic?response=json`、`https://www.twse.com.tw/rwd/zh/ETF/list?response=json`。",
            f"- TDCC 受益人資料截止日：`{tdcc_date}`。",
        ]
    )

    DOC_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    metas = _load_current_domestic_etfs()
    codes = [meta.code for meta in metas]
    con = connect(use_cache=True)
    cutoff = con.sql("SELECT max(date) FROM daily_quote WHERE market='twse'").fetchone()[0]
    cache_rows = con.sql("SELECT count(*) FROM daily_quote").fetchone()[0]

    quote_stats = _quote_stats(con, codes)
    tdcc_date, holders = _tdcc_stats(con, codes)

    start = min(meta.listing_date for meta in metas if meta.listing_date is not None)
    panel = fetch_adjusted_panel(
        con,
        start=str(start),
        end=str(cutoff),
        codes=codes,
        market="twse",
        include_extra_history_days=10,
    )
    series = _series_by_code(panel, codes)
    bench = series[BENCHMARK_CODE]

    window_metrics = _build_window_metrics(metas, series, bench, quote_stats)
    since_metrics = _since_inception_metrics(metas, series, bench)
    ranking = _aggregate(metas, quote_stats, holders, window_metrics, since_metrics)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ranking.sort(["status", "rank", "code"], nulls_last=True).write_csv(
        OUT_DIR / "taiwan_equity_etf_ranking.csv"
    )
    window_metrics.sort(["window", "window_rank"]).write_csv(
        OUT_DIR / "taiwan_equity_etf_window_metrics.csv"
    )
    _write_doc(ranking, window_metrics, cutoff, cache_rows, tdcc_date)

    formal = ranking.filter(pl.col("status") == "formal").sort("rank")
    watch = ranking.filter(pl.col("status") != "formal")
    print(f"cutoff={cutoff} cache_rows={cache_rows:,} current_domestic_etfs={len(metas)}")
    print(f"formal_ranked={formal.height} watchlist={watch.height} tdcc_date={tdcc_date}")
    print(f"wrote {OUT_DIR / 'taiwan_equity_etf_ranking.csv'}")
    print(f"wrote {OUT_DIR / 'taiwan_equity_etf_window_metrics.csv'}")
    print(f"wrote {DOC_PATH}")
    print("\nTop 10")
    cols = ["rank", "code", "name", "management", "decision_score", "since_cagr", "liquidity_20d"]
    with pl.Config(tbl_rows=12, tbl_cols=8):
        print(formal.select(cols).head(10))


if __name__ == "__main__":
    main()
