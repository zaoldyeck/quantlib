"""Survey official TW event-news types for post-event alpha.

This is a broad event-study layer, not a production strategy. It compares
official message/event families under one point-in-time protocol:

* entry date = first trading day after the public event/release date
* prices = total-return-adjusted close from the shared feature panel
* horizons = 5/20/60/120 trading days

Usage:
    uv run --project research python research/experiments/official_event_alpha_survey.py
"""

from __future__ import annotations

import math
import sys
import time
from datetime import date
from pathlib import Path

import polars as pl
from research import paths

REPO_ROOT = Path(__file__).resolve().parents[2]
RESEARCH_ROOT = REPO_ROOT / "research"
STRAT_LAB = RESEARCH_ROOT / "strat_lab"
OUT_DIR = paths.OUT_EXPERIMENTS
DOC_PATH = REPO_ROOT / "docs" / "strategy_research" / "official_event_alpha_survey.md"
sys.path.insert(0, str(RESEARCH_ROOT))
sys.path.insert(0, str(STRAT_LAB))

from research.db import connect  # noqa: E402
from experiments.spike_factor_analysis import load_panel  # noqa: E402


START = date(2012, 1, 3)
END = date(2026, 6, 16)
HORIZONS = [5, 20, 60, 120]


def next_month_release_date(year: int, month: int) -> date:
    if month == 12:
        return date(year + 1, 1, 13)
    return date(year, month + 1, 13)


def pct(v: object) -> str:
    if v is None:
        return "-"
    try:
        x = float(v)
    except Exception:
        return "-"
    if math.isnan(x):
        return "-"
    return f"{x:.2%}"


def add_event(events: list[dict[str, object]], label: str, event_date: date, company_code: str, source: str, score: float = 1.0) -> None:
    if START <= event_date <= END and company_code and company_code[:1].isdigit():
        events.append(
            {
                "label": label,
                "event_date": event_date,
                "company_code": company_code,
                "source": source,
                "score": score,
            }
        )


def load_buyback_events() -> list[dict[str, object]]:
    path = OUT_DIR / "official_event_buyback_labeled.parquet"
    if not path.exists():
        raise FileNotFoundError(f"{path} missing; run official_event_buyback_backtest.py first")
    df = pl.read_parquet(path)
    events: list[dict[str, object]] = []
    for r in df.iter_rows(named=True):
        d = r["board_date"]
        code = r["company_code"]
        add_event(events, "buyback_all", d, code, "mops_t35sc09", float(r.get("event_score") or 1.0))
        if bool(r.get("high_price_ceiling")):
            add_event(events, "buyback_high_price_ceiling", d, code, "mops_t35sc09", float(r.get("event_score") or 1.0))
        if bool(r.get("purpose_3_support")):
            add_event(events, "buyback_support_purpose", d, code, "mops_t35sc09", float(r.get("event_score") or 1.0))
        if bool(r.get("purpose_3_support")) and bool(r.get("deep_pre_drop")):
            add_event(events, "buyback_support_after_drop", d, code, "mops_t35sc09", float(r.get("event_score") or 1.0))
    return events


def load_insider_events() -> list[dict[str, object]]:
    con = connect(read_only=True)
    try:
        df = con.sql(
            """
            SELECT declare_date AS event_date, company_code, transfer_method,
                   transfer_shares, current_shares_own, current_shares_trust,
                   planned_shares_own, planned_shares_trust
            FROM insider_holding
            WHERE declare_date BETWEEN DATE '2012-01-03' AND DATE '2026-06-16'
            """
        ).pl()
    finally:
        con.close()
    events: list[dict[str, object]] = []
    for r in df.iter_rows(named=True):
        method = str(r["transfer_method"] or "").strip()
        planned = float(r["planned_shares_own"] or 0.0) + float(r["planned_shares_trust"] or 0.0)
        current = float(r["current_shares_own"] or 0.0) + float(r["current_shares_trust"] or 0.0)
        ratio = planned / current if current > 0 else 0.0
        score = min(ratio, 1.0)
        add_event(events, "insider_transfer_all", r["event_date"], r["company_code"], "mops_t56sb12", score)
        if any(k in method for k in ["一般交易", "盤後", "鉅額", "洽特定人"]):
            add_event(events, "insider_transfer_sale_like", r["event_date"], r["company_code"], "mops_t56sb12", score)
        if any(k in method for k in ["贈與", "信託"]):
            add_event(events, "insider_transfer_gift_trust", r["event_date"], r["company_code"], "mops_t56sb12", score)
        if ratio >= 0.10:
            add_event(events, "insider_transfer_large_ratio", r["event_date"], r["company_code"], "mops_t56sb12", score)
    return events


def load_capital_reduction_events() -> list[dict[str, object]]:
    con = connect(read_only=True)
    try:
        df = con.sql(
            """
            SELECT date AS event_date, company_code, reason_for_capital_reduction
            FROM capital_reduction
            WHERE date BETWEEN DATE '2012-01-03' AND DATE '2026-06-16'
            """
        ).pl()
    finally:
        con.close()
    events: list[dict[str, object]] = []
    for r in df.iter_rows(named=True):
        reason = str(r["reason_for_capital_reduction"] or "").strip()
        code = r["company_code"]
        add_event(events, "capital_reduction_all", r["event_date"], code, "twse_tpex_capital_reduction")
        if "退還股款" in reason or "現金減資" in reason:
            add_event(events, "capital_reduction_cash_return", r["event_date"], code, "twse_tpex_capital_reduction")
        if "彌補虧損" in reason:
            add_event(events, "capital_reduction_loss_cover", r["event_date"], code, "twse_tpex_capital_reduction")
    return events


def load_revenue_events() -> list[dict[str, object]]:
    con = connect(read_only=True)
    try:
        df = con.sql(
            """
            SELECT year, month, company_code, monthly_revenue_yoy
            FROM operating_revenue
            WHERE type='consolidated'
              AND year BETWEEN 2011 AND 2026
              AND monthly_revenue_yoy IS NOT NULL
              AND regexp_matches(company_code, '^[1-9][0-9]{3}$')
            """
        ).pl()
    finally:
        con.close()
    df = (
        df.sort(["company_code", "year", "month"])
        .with_columns(
            [
                pl.col("monthly_revenue_yoy").shift(1).over("company_code").alias("prev_yoy"),
                (pl.col("monthly_revenue_yoy") - pl.col("monthly_revenue_yoy").shift(1).over("company_code")).alias("yoy_delta"),
            ]
        )
    )
    events: list[dict[str, object]] = []
    for r in df.iter_rows(named=True):
        d = next_month_release_date(int(r["year"]), int(r["month"]))
        code = r["company_code"]
        yoy = float(r["monthly_revenue_yoy"])
        prev = r["prev_yoy"]
        delta = r["yoy_delta"]
        score = min(max(yoy / 100.0, -1.0), 2.0)
        if yoy >= 30 and (delta is not None and float(delta) > 0):
            add_event(events, "revenue_yoy30_accel", d, code, "mops_monthly_revenue", score)
        if yoy >= 50:
            add_event(events, "revenue_yoy50", d, code, "mops_monthly_revenue", score)
        if yoy >= 20 and prev is not None and float(prev) <= 0:
            add_event(events, "revenue_turnaround", d, code, "mops_monthly_revenue", score)
        if yoy <= -20:
            add_event(events, "revenue_yoy_neg20", d, code, "mops_monthly_revenue", score)
    return events


def attach_returns(events: pl.DataFrame, panel: pl.DataFrame, days: list[date]) -> pl.DataFrame:
    price_lookup = {
        (r["date"], r["company_code"]): float(r["close"])
        for r in panel.select(["date", "company_code", "close"]).iter_rows(named=True)
    }
    day_index = {d: i for i, d in enumerate(days)}

    def next_trading_day(d: date) -> date | None:
        # Linear scan is fine for a small day list and keeps date semantics explicit.
        for day in days:
            if day > d:
                return day
        return None

    rows = []
    for r in events.iter_rows(named=True):
        entry = next_trading_day(r["event_date"])
        if entry is None:
            continue
        idx = day_index.get(entry)
        entry_close = price_lookup.get((entry, r["company_code"]))
        if idx is None or entry_close is None or entry_close <= 0:
            continue
        out = dict(r)
        out["entry_date"] = entry
        out["entry_close"] = entry_close
        for h in HORIZONS:
            hdate = days[idx + h] if idx + h < len(days) else None
            hclose = price_lookup.get((hdate, r["company_code"])) if hdate else None
            out[f"ret_{h}d"] = hclose / entry_close - 1.0 if hclose and hclose > 0 else None
        rows.append(out)
    return pl.DataFrame(rows)


def summarize(labeled: pl.DataFrame) -> pl.DataFrame:
    exprs: list[pl.Expr] = [pl.len().alias("n"), pl.col("score").mean().alias("avg_score")]
    for h in HORIZONS:
        col = pl.col(f"ret_{h}d")
        exprs.extend(
            [
                col.count().alias(f"valid_{h}d"),
                col.mean().alias(f"mean_{h}d"),
                col.median().alias(f"median_{h}d"),
                (col > 0).mean().alias(f"win_{h}d"),
                (col > 0.10).mean().alias(f"gain10_{h}d"),
                (col < -0.10).mean().alias(f"loss10_{h}d"),
            ]
        )
    return labeled.group_by("label").agg(exprs).sort("mean_60d", descending=True, nulls_last=True)


def write_report(summary: pl.DataFrame) -> None:
    DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# 官方消息面 Alpha Survey",
        "",
        f"資料截止：`{END}`；回測起點：`{START}`；價格使用 total-return adjusted close。",
        "",
        "事件定義：事件公開後的下一個交易日收盤作為 entry reference，計算後續 5/20/60/120 個交易日報酬。",
        "",
        "## 事件類型排序",
        "",
        "| 排名 | 消息類型 | n | 60日有效n | 20日均值 | 20日勝率 | 60日均值 | 60日勝率 | 120日均值 | 120日勝率 |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for i, row in enumerate(summary.iter_rows(named=True), start=1):
        lines.append(
            f"| {i} | `{row['label']}` | {int(row['n']):,} | "
            f"{int(row['valid_60d'] or 0):,} | "
            f"{pct(row['mean_20d'])} | {pct(row['win_20d'])} | "
            f"{pct(row['mean_60d'])} | {pct(row['win_60d'])} | "
            f"{pct(row['mean_120d'])} | {pct(row['win_120d'])} |"
        )
    lines += [
        "",
        "## 初步結論",
        "",
        "這是事件研究，不是完整交易策略。正 alpha 類型可以進一步與價格動能、月營收、法人籌碼、產業分類做交互；負 alpha 類型更適合作為持股風險過濾器，而不是長多買進訊號。",
        "",
    ]
    DOC_PATH.write_text("\n".join(lines))


def main() -> None:
    t0 = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    panel, days = load_panel(START, END)
    print(f"[prices] rows={panel.height:,}, days={len(days):,}, cutoff={max(days)}")

    event_rows: list[dict[str, object]] = []
    for loader in [load_buyback_events, load_insider_events, load_capital_reduction_events, load_revenue_events]:
        rows = loader()
        event_rows.extend(rows)
        print(f"[events] {loader.__name__}: {len(rows):,}")
    events = pl.DataFrame(event_rows)
    events_path = OUT_DIR / "official_event_alpha_survey_events.parquet"
    events.write_parquet(events_path)
    print(f"[events] total labels={events.height:,} -> {events_path}")

    labeled = attach_returns(events, panel, days)
    labeled_path = OUT_DIR / "official_event_alpha_survey_labeled.parquet"
    labeled.write_parquet(labeled_path)
    print(f"[labels] usable={labeled.height:,} -> {labeled_path}")

    summary = summarize(labeled)
    summary_path = OUT_DIR / "official_event_alpha_survey_summary.csv"
    summary.write_csv(summary_path)
    write_report(summary)

    print("\n=== Official event alpha survey by 60d mean ===")
    with pl.Config(tbl_rows=40, tbl_width_chars=180):
        print(
            summary.select(
                [
                    "label",
                    "n",
                    "valid_60d",
                    pl.col("mean_20d").mul(100).round(2).alias("mean_20d_pct"),
                    pl.col("win_20d").mul(100).round(2).alias("win_20d_pct"),
                    pl.col("mean_60d").mul(100).round(2).alias("mean_60d_pct"),
                    pl.col("win_60d").mul(100).round(2).alias("win_60d_pct"),
                    pl.col("mean_120d").mul(100).round(2).alias("mean_120d_pct"),
                    pl.col("loss10_60d").mul(100).round(2).alias("loss10_60d_pct"),
                ]
            )
        )
    print(f"\n[out] {summary_path}")
    print(f"[doc] {DOC_PATH}")
    print(f"[runtime] {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
