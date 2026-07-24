"""GDELT raw-news metadata pilot for TW stock news alpha.

This uses GDELT 2.1 GKG raw files instead of the DOC search API so the sample
can be historically reproducible. The script intentionally stores metadata and
derived labels, not article bodies.

The pilot is conservative:

* only strict aliases are used to map news to TW stock codes
* generic ambiguous aliases such as "UMC", "Delta", or "Quanta" are excluded
* event date is the GDELT timestamp converted from UTC to Asia/Taipei date
* entry reference = first TW trading day after event date, close
* alpha = stock total-return forward return minus 0050 same-window return

Usage:
    uv run --project . python src/quantlib/experiments/gdelt_tw_news_alpha_survey.py \
      --sample-dates 2025-03-12,2025-04-09,2025-05-14,2025-06-11 \
      --hour-step 3
"""

from __future__ import annotations

import argparse
import hashlib
import io
import sys
import time
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
from quantlib import paths

REPO_ROOT = paths.REPO
RESEARCH_ROOT = REPO_ROOT / "src" / "quantlib"
STRAT_LAB = RESEARCH_ROOT / "strat_lab"
OUT_DIR = paths.OUT_EXPERIMENTS
RAW_DIR = OUT_DIR / "gdelt_gkg_raw"
DOC_PATH = REPO_ROOT / "docs" / "strategy_research" / "gdelt_tw_news_alpha_survey.md"
CHART_PATH = REPO_ROOT / "docs" / "strategy_research" / "gdelt_tw_news_alpha_survey.png"
sys.path.insert(0, str(RESEARCH_ROOT))
sys.path.insert(0, str(STRAT_LAB))

from quantlib.db import connect  # noqa: E402
from experiments.spike_factor_analysis import load_panel  # noqa: E402
from experiments.news_alpha_common import (  # noqa: E402
    AliasRule,
    classify_news_metadata,
    load_alias_rules,
    load_benchmark_returns,
    match_company_codes,
    num,
    pct,
    summarize_labeled_news,
)
from quantlib.prices import total_return_series  # noqa: E402

TAIPEI = ZoneInfo("Asia/Taipei")
PANEL_START = date(2012, 1, 3)
PRICE_END = date(2026, 6, 16)
HORIZONS = [5, 20, 60, 120]


@dataclass(frozen=True)
class GkgRow:
    timestamp: datetime
    event_date: date
    company_code: str
    source_domain: str
    url: str
    themes: str
    organizations: str
    tone: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sample-dates",
        default="2025-03-12,2025-04-09,2025-05-14,2025-06-11,2025-07-09,2025-08-13",
        help="Comma-separated UTC dates to sample. Use yyyy-mm-dd.",
    )
    parser.add_argument("--hour-step", type=int, default=3, help="Download every N hours per sample date.")
    parser.add_argument("--force-fetch", action="store_true")
    return parser.parse_args()


def parse_sample_dates(value: str) -> list[date]:
    return [date.fromisoformat(part.strip()) for part in value.split(",") if part.strip()]


def gdelt_timestamp(day: date, hour: int) -> str:
    return f"{day:%Y%m%d}{hour:02d}0000"


def fetch_gkg_zip(ts: str, force: bool = False) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / f"{ts}.gkg.csv.zip"
    if path.exists() and not force:
        return path
    url = f"http://data.gdeltproject.org/gdeltv2/{ts}.gkg.csv.zip"
    req = urllib.request.Request(url, headers={"User-Agent": "quantlib-research-gdelt-gkg"})
    with urllib.request.urlopen(req, timeout=45) as resp:
        data = resp.read()
    path.write_bytes(data)
    return path


def parse_tone(value: str) -> float | None:
    if not value:
        return None
    try:
        return float(value.split(",", 1)[0])
    except Exception:
        return None


def ts_to_taipei_date(ts: str) -> tuple[datetime, date]:
    dt_utc = datetime.strptime(ts, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    dt_tw = dt_utc.astimezone(TAIPEI)
    return dt_tw, dt_tw.date()


def stable_text_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def classify(row: GkgRow) -> list[str]:
    search = f"{row.source_domain} {row.url} {row.themes} {row.organizations}"
    labels = classify_news_metadata(search, "gdelt")
    if row.tone is not None and row.tone >= 2.5:
        labels.append("gdelt_positive_tone")
    if row.tone is not None and row.tone <= -2.5:
        labels.append("gdelt_negative_tone")
    return sorted(set(labels))


def iter_matching_rows(path: Path, alias_rules: list[AliasRule]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    ts = path.name.split(".", 1)[0]
    seen_dt, event_date = ts_to_taipei_date(ts)
    raw = path.read_bytes()
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        name = zf.namelist()[0]
        with zf.open(name) as handle:
            for raw_line in handle:
                line = raw_line.decode("utf-8", errors="replace").rstrip("\n")
                cols = line.split("\t")
                if len(cols) < 16:
                    continue
                source_domain = cols[3].strip()
                url = cols[4].strip()
                themes = cols[7].strip()
                organizations = cols[13].strip()
                search_text = f"{source_domain} {url} {themes} {organizations}"
                codes = match_company_codes(search_text, alias_rules)
                if not codes:
                    continue
                tone = parse_tone(cols[15])
                for code in codes:
                    base = GkgRow(
                        timestamp=seen_dt,
                        event_date=event_date,
                        company_code=code,
                        source_domain=source_domain,
                        url=url,
                        themes=themes,
                        organizations=organizations,
                        tone=tone,
                    )
                    for label in classify(base):
                        rows.append(
                            {
                                "event_date": base.event_date,
                                "seen_at_taipei": base.timestamp.replace(tzinfo=None),
                                "company_code": base.company_code,
                                "source_domain": base.source_domain,
                                "url": base.url,
                                "themes_hash": stable_text_hash(base.themes),
                                "organizations_hash": stable_text_hash(base.organizations),
                                "tone": base.tone,
                                "label": label,
                            }
                        )
    return rows


def collect_events(sample_dates: list[date], hour_step: int, force: bool) -> pl.DataFrame:
    all_rows: list[dict[str, object]] = []
    alias_rules = load_alias_rules()
    hours = list(range(0, 24, max(1, hour_step)))
    for day in sample_dates:
        for hour in hours:
            ts = gdelt_timestamp(day, hour)
            try:
                path = fetch_gkg_zip(ts, force)
            except Exception as exc:
                print(f"[gdelt] fetch failed {ts}: {exc}")
                continue
            rows = iter_matching_rows(path, alias_rules)
            all_rows.extend(rows)
        print(f"[gdelt] {day} cumulative label rows={len(all_rows):,}")
    if not all_rows:
        return pl.DataFrame(
            schema={
                "event_date": pl.Date,
                "seen_at_taipei": pl.Datetime,
                "company_code": pl.Utf8,
                "source_domain": pl.Utf8,
                "url": pl.Utf8,
                "themes_hash": pl.Utf8,
                "organizations_hash": pl.Utf8,
                "tone": pl.Float64,
                "label": pl.Utf8,
            }
        )
    return (
        pl.DataFrame(all_rows)
        .unique(["event_date", "company_code", "url", "label"], keep="first")
        .sort(["event_date", "company_code", "label"])
    )


def attach_returns(events: pl.DataFrame, panel: pl.DataFrame, days: list[date]) -> pl.DataFrame:
    price_lookup = {
        (r["date"], r["company_code"]): float(r["close"])
        for r in panel.select(["date", "company_code", "close"]).iter_rows(named=True)
    }
    bench_lookup = load_benchmark_returns(total_return_series, connect, PANEL_START, PRICE_END, "0050")
    day_index = {d: i for i, d in enumerate(days)}

    def next_trading_day(d: date) -> date | None:
        for day in days:
            if day > d:
                return day
        return None

    out_rows: list[dict[str, object]] = []
    for r in events.iter_rows(named=True):
        entry = next_trading_day(r["event_date"])
        if entry is None:
            continue
        idx = day_index.get(entry)
        entry_close = price_lookup.get((entry, r["company_code"]))
        bench_entry = bench_lookup.get(entry)
        if idx is None or entry_close is None or entry_close <= 0 or bench_entry is None or bench_entry <= 0:
            continue
        out = dict(r)
        out["entry_date"] = entry
        out["entry_close"] = entry_close
        for h in HORIZONS:
            hdate = days[idx + h] if idx + h < len(days) else None
            close = price_lookup.get((hdate, r["company_code"])) if hdate else None
            bclose = bench_lookup.get(hdate) if hdate else None
            ret = close / entry_close - 1.0 if close and close > 0 else None
            bench = bclose / bench_entry - 1.0 if bclose and bclose > 0 else None
            out[f"ret_{h}d"] = ret
            out[f"bench_0050_{h}d"] = bench
            out[f"excess_0050_{h}d"] = ret - bench if ret is not None and bench is not None else None
        out_rows.append(out)
    return pl.DataFrame(out_rows)


def summarize(labeled: pl.DataFrame) -> pl.DataFrame:
    return summarize_labeled_news(labeled, HORIZONS)


def write_chart(summary: pl.DataFrame) -> None:
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return
    data = summary.filter(pl.col("valid_60d") >= 5).sort("mean_excess_0050_60d")
    if data.height == 0:
        return
    labels = data["label"].to_list()
    values = [float(x) * 100.0 for x in data["mean_excess_0050_60d"].to_list()]
    colors = ["#168a5f" if v >= 0 else "#b23838" for v in values]
    plt.figure(figsize=(12, max(5, len(labels) * 0.42)))
    plt.barh(labels, values, color=colors)
    plt.axvline(0, color="#333333", linewidth=0.8)
    plt.xlabel("60 trading-day excess return vs 0050 (%)")
    plt.title("GDELT TW News Metadata Alpha Pilot")
    plt.tight_layout()
    CHART_PATH.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(CHART_PATH, dpi=160)
    plt.close()


def write_report(sample_dates: list[date], hour_step: int, summary: pl.DataFrame, labeled: pl.DataFrame) -> None:
    DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# GDELT 台股新聞 Metadata Alpha Pilot",
        "",
        f"樣本日期：{', '.join(str(d) for d in sample_dates)}；每 `{hour_step}` 小時抽一個 GKG 檔；價格資料截止：`{PRICE_END}`。",
        "",
        "資料來源：GDELT 2.1 GKG raw metadata。此 pilot 使用 source domain、URL、themes、organizations 與 tone，不保存新聞全文。",
        "",
        f"股票映射：使用 `{paths.RECORDS}/tw_stock_news_aliases.csv`，只啟用高可信 alias，排除容易誤判的簡稱。事件日為 GDELT timestamp 轉 Asia/Taipei 日期，下一個台股交易日收盤為 entry reference。",
        "",
    ]
    if CHART_PATH.exists():
        lines.extend(["![GDELT TW news alpha survey](gdelt_tw_news_alpha_survey.png)", ""])
    lines.extend(
        [
            "## 60 日相對 0050 排序",
            "",
            "| 排名 | 新聞類型 | label rows | articles | 股票數 | avg tone | 60日有效n | 60日均值 | 60日勝率 | 60日超額均值 | 60日超額勝率 | t-stat | 120日超額均值 |",
            "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for i, row in enumerate(summary.iter_rows(named=True), start=1):
        lines.append(
            f"| {i} | `{row['label']}` | {int(row['label_rows']):,} | {int(row['articles']):,} | "
            f"{int(row['codes']):,} | {num(row['avg_tone'])} | {int(row['valid_60d'] or 0):,} | "
            f"{pct(row['mean_60d'])} | {pct(row['win_60d'])} | {pct(row['mean_excess_0050_60d'])} | "
            f"{pct(row['excess_win_60d'])} | {num(row['excess_tstat_60d'])} | {pct(row['mean_excess_0050_120d'])} |"
        )
    by_code = (
        labeled.group_by("company_code")
        .agg(pl.col("url").n_unique().alias("articles"), pl.col("tone").mean().alias("avg_tone"))
        .sort("articles", descending=True)
    )
    lines.extend(["", "## 股票命中分布", "", "| 股票代號 | articles | avg tone |", "|---|---:|---:|"])
    for row in by_code.iter_rows(named=True):
        lines.append(f"| `{row['company_code']}` | {int(row['articles']):,} | {num(row['avg_tone'])} |")
    lines.extend(
        [
            "",
            "## 研究限制",
            "",
            "- 這是 GDELT hourly 抽樣，不是全量新聞庫；只能判斷這條資料管線是否值得擴大。",
            "- GKG 沒有完整標題或全文，本 pilot 先用 metadata 分類；若某類型有顯著 Alpha，再接 DOC/RSS/原站頁面做標題與摘要級 LLM 標籤。",
            "- 目前股票 universe 只含高可信 alias 的大型台股；擴大到全市場前，必須先建立公司別名與消歧義資料表。",
            "",
        ]
    )
    DOC_PATH.write_text("\n".join(lines))


def main() -> None:
    args = parse_args()
    t0 = time.time()
    sample_dates = parse_sample_dates(args.sample_dates)
    events = collect_events(sample_dates, args.hour_step, args.force_fetch)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    events_path = OUT_DIR / "gdelt_tw_news_events.parquet"
    events.write_parquet(events_path)
    print(f"[events] label rows={events.height:,}, articles={events.select('url').n_unique():,} -> {events_path}")

    panel, days = load_panel(PANEL_START, PRICE_END)
    print(f"[prices] rows={panel.height:,}, days={len(days):,}, cutoff={max(days)}")
    labeled = attach_returns(events, panel, days)
    labeled_path = OUT_DIR / "gdelt_tw_news_labeled.parquet"
    labeled.write_parquet(labeled_path)
    print(f"[labels] usable label rows={labeled.height:,} -> {labeled_path}")

    summary = summarize(labeled)
    summary_path = OUT_DIR / "gdelt_tw_news_summary.csv"
    summary.write_csv(summary_path)
    write_chart(summary)
    write_report(sample_dates, args.hour_step, summary, labeled)

    print("\n=== GDELT TW news metadata alpha by 60d excess vs 0050 ===")
    with pl.Config(tbl_rows=40, tbl_width_chars=190):
        print(
            summary.select(
                [
                    "label",
                    "label_rows",
                    "articles",
                    "codes",
                    pl.col("avg_tone").round(2).alias("avg_tone"),
                    "valid_60d",
                    pl.col("mean_60d").mul(100).round(2).alias("mean60_pct"),
                    pl.col("mean_excess_0050_60d").mul(100).round(2).alias("excess60_pct"),
                    pl.col("excess_win_60d").mul(100).round(2).alias("excess_win60_pct"),
                    pl.col("excess_tstat_60d").round(2).alias("tstat60"),
                    pl.col("mean_excess_0050_120d").mul(100).round(2).alias("excess120_pct"),
                ]
            )
        )
    print(f"\n[out] {summary_path}")
    print(f"[doc] {DOC_PATH}")
    if CHART_PATH.exists():
        print(f"[chart] {CHART_PATH}")
    print(f"[runtime] {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
