"""Recent Taiwan stock news RSS metadata pilot.

This runner validates the live/recent-news side of the news-alpha pipeline using
bounded Yahoo Finance Taiwan stock RSS and Google News RSS queries. RSS is not a
long-history source, so this script treats missing forward horizons as expected
when articles are too recent relative to the price cutoff.

Usage:
    uv run --project . python src/quantlib/experiments/tw_news_rss_alpha_survey.py \
      --codes 2330,2317,2454 --max-items-per-feed 5
"""

from __future__ import annotations

import argparse
import hashlib
import html
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path

import polars as pl
from quantlib import paths

REPO_ROOT = Path(__file__).resolve().parents[3]
RESEARCH_ROOT = REPO_ROOT / "src" / "quantlib"
STRAT_LAB = RESEARCH_ROOT / "strat_lab"
OUT_DIR = paths.OUT_EXPERIMENTS
RAW_DIR = OUT_DIR / "tw_news_rss_raw"
DOC_PATH = REPO_ROOT / "docs" / "strategy_research" / "tw_news_rss_alpha_survey.md"
CHART_PATH = REPO_ROOT / "docs" / "strategy_research" / "tw_news_rss_alpha_survey.png"
sys.path.insert(0, str(RESEARCH_ROOT))
sys.path.insert(0, str(STRAT_LAB))

from quantlib.db import connect  # noqa: E402
from experiments.news_alpha_common import (  # noqa: E402
    AliasRule,
    classify_news_metadata,
    load_alias_rules,
    load_benchmark_returns,
    match_company_codes,
    next_trading_day_after,
    num,
    parse_rss_datetime,
    pct,
    summarize_labeled_news,
)
from experiments.spike_factor_analysis import load_panel  # noqa: E402
from quantlib.prices import total_return_series  # noqa: E402

PANEL_START = date(2012, 1, 3)
PRICE_END = date(2026, 6, 16)
HORIZONS = [5, 20, 60, 120]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--codes", default="2330,2317,2454,2308,2382")
    parser.add_argument("--max-items-per-feed", type=int, default=5)
    parser.add_argument("--force-fetch", action="store_true")
    return parser.parse_args()


def parse_codes(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def primary_aliases_by_code(rules: list[AliasRule]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for rule in rules:
        if not rule.enabled:
            continue
        out.setdefault(rule.company_code, []).append(rule.alias)
    return out


def fetch_url(url: str, cache_key: str, force: bool) -> bytes:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / f"{cache_key}.xml"
    if path.exists() and not force:
        return path.read_bytes()
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 quantlib-research-rss"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
    path.write_bytes(raw)
    return raw


def rss_items(raw: bytes, source_family: str, source_query: str, max_items: int) -> list[dict[str, object]]:
    root = ET.fromstring(raw)
    out: list[dict[str, object]] = []
    for item in root.findall("./channel/item")[:max_items]:
        title = html.unescape(item.findtext("title") or "").strip()
        link = html.unescape(item.findtext("link") or "").strip()
        description = html.unescape(item.findtext("description") or "").strip()
        pub = parse_rss_datetime(item.findtext("pubDate") or "")
        if not title or pub is None:
            continue
        out.append(
            {
                "source_family": source_family,
                "source_query": source_query,
                "title": title,
                "description": description,
                "url": link,
                "published_at_taipei": pub.replace(tzinfo=None),
                "event_date": pub.date(),
                "article_hash": hashlib.sha256(title.encode("utf-8")).hexdigest()[:16],
            }
        )
    return out


def yahoo_feed_url(code: str) -> str:
    return f"https://tw.stock.yahoo.com/rss?s={code}.TW"


def google_feed_url(query: str) -> str:
    return "https://news.google.com/rss/search?" + urllib.parse.urlencode(
        {
            "q": query,
            "hl": "zh-TW",
            "gl": "TW",
            "ceid": "TW:zh-Hant",
        }
    )


def collect_events(codes: list[str], max_items: int, force: bool) -> pl.DataFrame:
    alias_rules = load_alias_rules()
    aliases = primary_aliases_by_code(alias_rules)
    code_set = set(codes)
    raw_articles: list[dict[str, object]] = []
    for code in codes:
        y_url = yahoo_feed_url(code)
        try:
            raw = fetch_url(y_url, f"yahoo_{code}", force)
            for item in rss_items(raw, "yahoo_tw_stock_rss", code, max_items):
                item["query_code"] = code
                raw_articles.append(item)
        except Exception as exc:
            print(f"[rss] yahoo {code} failed: {exc}")

        query_aliases = aliases.get(code, [])[:3]
        if query_aliases:
            query = " OR ".join(query_aliases)
            try:
                raw = fetch_url(google_feed_url(query), f"google_{code}", force)
                for item in rss_items(raw, "google_news_rss", query, max_items):
                    item["query_code"] = code
                    raw_articles.append(item)
            except Exception as exc:
                print(f"[rss] google {code} failed: {exc}")

    rows: list[dict[str, object]] = []
    for article in raw_articles:
        search_text = f"{article['title']} {article['description']} {article['url']}"
        matched = match_company_codes(search_text, alias_rules)
        title_matched = set(match_company_codes(str(article["title"]), alias_rules))
        if is_third_party_security_transaction(search_text):
            matched = [code for code in matched if code in title_matched]
        matched = [code for code in matched if code in code_set]
        labels = classify_news_metadata(search_text, "rss")
        for code in matched:
            for label in labels:
                row = dict(article)
                row["company_code"] = code
                row["label"] = label
                rows.append(row)
    if not rows:
        return pl.DataFrame(
            schema={
                "event_date": pl.Date,
                "published_at_taipei": pl.Datetime,
                "company_code": pl.Utf8,
                "source_family": pl.Utf8,
                "source_query": pl.Utf8,
                "title": pl.Utf8,
                "description": pl.Utf8,
                "url": pl.Utf8,
                "article_hash": pl.Utf8,
                "query_code": pl.Utf8,
                "label": pl.Utf8,
            }
        )
    return (
        pl.DataFrame(rows)
        .unique(["company_code", "article_hash", "label"], keep="first")
        .sort(["event_date", "company_code", "label"])
    )


def is_third_party_security_transaction(text: str) -> bool:
    return any(term in text for term in ["取得有價證券", "處分有價證券", "累積處分有價證券", "累積取得有價證券"])


def attach_returns(events: pl.DataFrame, panel: pl.DataFrame, days: list[date]) -> pl.DataFrame:
    price_lookup = {
        (r["date"], r["company_code"]): float(r["close"])
        for r in panel.select(["date", "company_code", "close"]).iter_rows(named=True)
    }
    bench_lookup = load_benchmark_returns(total_return_series, connect, PANEL_START, PRICE_END, "0050")
    day_index = {d: i for i, d in enumerate(days)}
    rows: list[dict[str, object]] = []
    for r in events.iter_rows(named=True):
        entry = next_trading_day_after(days, r["event_date"])
        if entry is None:
            continue
        idx = day_index.get(entry)
        entry_close = price_lookup.get((entry, r["company_code"]))
        bench_entry = bench_lookup.get(entry)
        out = dict(r)
        out["entry_date"] = entry
        out["entry_close"] = entry_close
        if idx is None or entry_close is None or entry_close <= 0 or bench_entry is None or bench_entry <= 0:
            for h in HORIZONS:
                out[f"ret_{h}d"] = None
                out[f"bench_0050_{h}d"] = None
                out[f"excess_0050_{h}d"] = None
            rows.append(out)
            continue
        for h in HORIZONS:
            hdate = days[idx + h] if idx + h < len(days) else None
            close = price_lookup.get((hdate, r["company_code"])) if hdate else None
            bclose = bench_lookup.get(hdate) if hdate else None
            ret = close / entry_close - 1.0 if close and close > 0 else None
            bench = bclose / bench_entry - 1.0 if bclose and bclose > 0 else None
            out[f"ret_{h}d"] = ret
            out[f"bench_0050_{h}d"] = bench
            out[f"excess_0050_{h}d"] = ret - bench if ret is not None and bench is not None else None
        rows.append(out)
    return pl.DataFrame(rows)


def empty_summary() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "label": pl.Utf8,
            "label_rows": pl.Int64,
            "articles": pl.Int64,
            "codes": pl.Int64,
            "valid_60d": pl.Int64,
            "mean_60d": pl.Float64,
            "win_60d": pl.Float64,
            "mean_excess_0050_60d": pl.Float64,
            "excess_win_60d": pl.Float64,
            "excess_tstat_60d": pl.Float64,
            "mean_excess_0050_120d": pl.Float64,
        }
    )


def summarize(labeled: pl.DataFrame) -> pl.DataFrame:
    if labeled.height == 0:
        return empty_summary()
    return summarize_labeled_news(labeled, HORIZONS)


def write_chart(summary: pl.DataFrame) -> None:
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return
    if summary.height == 0 or "mean_excess_0050_60d" not in summary.columns:
        return
    data = summary.filter(pl.col("valid_60d") > 0).sort("mean_excess_0050_60d")
    if data.height == 0:
        return
    labels = data["label"].to_list()
    values = [float(x) * 100.0 for x in data["mean_excess_0050_60d"].to_list()]
    colors = ["#168a5f" if v >= 0 else "#b23838" for v in values]
    plt.figure(figsize=(12, max(5, len(labels) * 0.42)))
    plt.barh(labels, values, color=colors)
    plt.axvline(0, color="#333333", linewidth=0.8)
    plt.xlabel("60 trading-day excess return vs 0050 (%)")
    plt.title("TW RSS News Metadata Alpha Pilot")
    plt.tight_layout()
    CHART_PATH.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(CHART_PATH, dpi=160)
    plt.close()


def write_report(codes: list[str], max_items: int, events: pl.DataFrame, labeled: pl.DataFrame, summary: pl.DataFrame) -> None:
    DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# 台股 RSS 新聞 Metadata Alpha Pilot",
        "",
        f"樣本股票：`{', '.join(codes)}`；每個 RSS feed 最多 `{max_items}` 筆；價格資料截止：`{PRICE_END}`。",
        "",
        "資料來源：Yahoo 個股 RSS 與 Google News RSS。此 pilot 保存標題、URL、來源與分類 metadata，不保存新聞全文。",
        "",
        "RSS 是近期資料源，主要驗證 live/news ingestion 與分類流程；若文章太新，forward return 會自然留空，不能被解讀為歷史 Alpha。",
        "",
        f"- event label rows: `{events.height:,}`",
        f"- usable labeled rows: `{labeled.height:,}`",
        f"- unique articles: `{events.select('article_hash').n_unique() if events.height else 0:,}`",
        "",
    ]
    if CHART_PATH.exists():
        lines.extend(["![TW RSS news alpha survey](tw_news_rss_alpha_survey.png)", ""])
    if summary.height:
        lines.extend(
            [
                "## 60 日相對 0050 排序",
                "",
                "| 排名 | 新聞類型 | label rows | articles | 股票數 | 60日有效n | 60日均值 | 60日勝率 | 60日超額均值 | 60日超額勝率 | t-stat | 120日超額均值 |",
                "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for i, row in enumerate(summary.iter_rows(named=True), start=1):
            lines.append(
                f"| {i} | `{row['label']}` | {int(row['label_rows']):,} | {int(row['articles']):,} | "
                f"{int(row['codes']):,} | {int(row['valid_60d'] or 0):,} | {pct(row['mean_60d'])} | "
                f"{pct(row['win_60d'])} | {pct(row['mean_excess_0050_60d'])} | "
                f"{pct(row['excess_win_60d'])} | {num(row['excess_tstat_60d'])} | {pct(row['mean_excess_0050_120d'])} |"
            )
    else:
        lines.extend(["## 60 日相對 0050 排序", "", "目前 RSS 樣本尚無可用 forward-return 統計。", ""])

    if events.height:
        samples = (
            events.unique(["company_code", "article_hash"], keep="first")
            .sort("published_at_taipei", descending=True)
            .head(20)
        )
        lines.extend(["", "## 近期文章樣本", "", "| 日期 | 股票 | 來源 | 標題 |", "|---|---|---|---|"])
        for row in samples.iter_rows(named=True):
            title = str(row["title"]).replace("|", " ").strip()
            lines.append(f"| {row['event_date']} | `{row['company_code']}` | `{row['source_family']}` | {title[:90]} |")
    DOC_PATH.write_text("\n".join(lines))


def main() -> None:
    args = parse_args()
    t0 = time.time()
    codes = parse_codes(args.codes)
    events = collect_events(codes, args.max_items_per_feed, args.force_fetch)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    events_path = OUT_DIR / "tw_news_rss_events.parquet"
    events.write_parquet(events_path)
    print(f"[rss] event label rows={events.height:,}, articles={events.select('article_hash').n_unique() if events.height else 0:,} -> {events_path}")

    panel, days = load_panel(PANEL_START, PRICE_END)
    print(f"[prices] rows={panel.height:,}, days={len(days):,}, cutoff={max(days)}")
    labeled = attach_returns(events, panel, days)
    labeled_path = OUT_DIR / "tw_news_rss_labeled.parquet"
    labeled.write_parquet(labeled_path)
    print(f"[labels] usable rows={labeled.height:,} -> {labeled_path}")

    summary = summarize(labeled)
    summary_path = OUT_DIR / "tw_news_rss_summary.csv"
    summary.write_csv(summary_path)
    write_chart(summary)
    write_report(codes, args.max_items_per_feed, events, labeled, summary)

    print("\n=== TW RSS news metadata alpha by 60d excess vs 0050 ===")
    if summary.height:
        with pl.Config(tbl_rows=40, tbl_width_chars=190):
            print(
                summary.select(
                    [
                        "label",
                        "label_rows",
                        "articles",
                        "codes",
                        "valid_60d",
                        pl.col("mean_60d").mul(100).round(2).alias("mean60_pct"),
                        pl.col("mean_excess_0050_60d").mul(100).round(2).alias("excess60_pct"),
                        pl.col("excess_win_60d").mul(100).round(2).alias("excess_win60_pct"),
                        pl.col("excess_tstat_60d").round(2).alias("tstat60"),
                        pl.col("mean_excess_0050_120d").mul(100).round(2).alias("excess120_pct"),
                    ]
                )
            )
    else:
        print("[rss] no usable summary rows")
    print(f"\n[out] {summary_path}")
    print(f"[doc] {DOC_PATH}")
    if CHART_PATH.exists():
        print(f"[chart] {CHART_PATH}")
    print(f"[runtime] {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
