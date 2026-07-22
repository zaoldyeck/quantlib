"""Shared helpers for Taiwan stock news-alpha experiments."""

from __future__ import annotations

import csv
import email.utils
import math
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
from research import paths

REPO_ROOT = Path(__file__).resolve().parents[2]
ALIAS_PATH = paths.NEWS_ALIASES
TAIPEI = ZoneInfo("Asia/Taipei")

NEWS_LABEL_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("news_semiconductor_ai", ("semiconductor", "chip", "晶片", "半導體", "ai", "artificial intelligence", "gpu", "data center", "資料中心", "wafer", "晶圓")),
    ("news_customer_nvidia_apple", ("nvidia", "輝達", "apple", "蘋果", "amd", "qualcomm", "broadcom", "tesla", "microsoft", "amazon")),
    ("news_policy_geopolitics_tariff", ("tariff", "關稅", "export control", "出口管制", "white house", "白宮", "geopolitic", "china", "中國", "beijing", "washington", "sanction", "制裁")),
    ("news_capex_expansion", ("investment", "投資", "invest", "fab", "廠", "plant", "factory", "expansion", "擴產", "arizona", "japan", "germany", "建廠")),
    ("news_earnings_revenue", ("revenue", "營收", "earnings", "獲利", "profit", "sales", "guidance", "forecast", "eps", "財測", "展望")),
    ("news_market_stock_report", ("stock", "shares", "股價", "market", "analyst", "price target", "目標價", "外資", "投信")),
    ("news_pr_wire_distribution", ("prnewswire", "businesswire", "globenewswire", "press-releases", "press release")),
    ("news_media_clarification", ("澄清", "媒體報導", "報導內容", "clarif")),
)


@dataclass(frozen=True)
class AliasRule:
    company_code: str
    company_name: str
    alias: str
    required_any: tuple[str, ...]
    exclude_any: tuple[str, ...]
    enabled: bool
    confidence: str
    notes: str


def split_terms(value: str) -> tuple[str, ...]:
    value = (value or "").strip()
    if not value:
        return ()
    return tuple(part.strip() for part in value.split("|") if part.strip())


def load_alias_rules(path: Path = ALIAS_PATH, include_disabled: bool = False) -> list[AliasRule]:
    rules: list[AliasRule] = []
    with path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            enabled = str(row.get("enabled", "")).strip().lower() == "true"
            if not enabled and not include_disabled:
                continue
            rules.append(
                AliasRule(
                    company_code=str(row["company_code"]).strip(),
                    company_name=str(row["company_name"]).strip(),
                    alias=str(row["alias"]).strip(),
                    required_any=split_terms(row.get("required_any", "")),
                    exclude_any=split_terms(row.get("exclude_any", "")),
                    enabled=enabled,
                    confidence=str(row.get("confidence", "")).strip(),
                    notes=str(row.get("notes", "")).strip(),
                )
            )
    return rules


def normalize_for_match(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").lower()


def has_term(text: str, term: str) -> bool:
    term_norm = normalize_for_match(term)
    if not term_norm:
        return False
    if re.fullmatch(r"[a-z0-9 .&-]+", term_norm):
        return re.search(rf"(?<![a-z0-9]){re.escape(term_norm)}(?![a-z0-9])", text) is not None
    return term_norm in text


def match_company_codes(text: str, rules: list[AliasRule]) -> list[str]:
    normalized = normalize_for_match(text)
    matched: set[str] = set()
    for rule in rules:
        if not has_term(normalized, rule.alias):
            continue
        if rule.exclude_any and any(has_term(normalized, term) for term in rule.exclude_any):
            continue
        if rule.required_any and not any(has_term(normalized, term) for term in rule.required_any):
            continue
        matched.add(rule.company_code)
    return sorted(matched)


def classify_news_metadata(text: str, prefix: str) -> list[str]:
    normalized = normalize_for_match(text)
    labels = [f"{prefix}_all_company_news"]
    for label, terms in NEWS_LABEL_RULES:
        if any(has_term(normalized, term) for term in terms):
            labels.append(f"{prefix}_{label.removeprefix('news_')}")
    return labels


def parse_rss_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = email.utils.parsedate_to_datetime(value)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=TAIPEI)
    return parsed.astimezone(TAIPEI)


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


def num(v: object, digits: int = 2) -> str:
    if v is None:
        return "-"
    try:
        x = float(v)
    except Exception:
        return "-"
    if math.isnan(x):
        return "-"
    return f"{x:.{digits}f}"


def next_trading_day_after(days: list[date], d: date) -> date | None:
    for day in days:
        if day > d:
            return day
    return None


def load_benchmark_returns(total_return_series_fn, connect_fn, start: date, end: date, code: str = "0050") -> dict[date, float]:
    con = connect_fn(read_only=True)
    try:
        frame = total_return_series_fn(con, code=code, start=start.isoformat(), end=end.isoformat(), market="twse")
    finally:
        con.close()
    return {r["date"]: float(r["adj_close"]) for r in frame.iter_rows(named=True)}


def summarize_labeled_news(labeled: pl.DataFrame, horizons: list[int]) -> pl.DataFrame:
    exprs: list[pl.Expr] = [
        pl.len().alias("label_rows"),
        pl.col("url").n_unique().alias("articles"),
        pl.col("company_code").n_unique().alias("codes"),
    ]
    if "tone" in labeled.columns:
        exprs.append(pl.col("tone").mean().alias("avg_tone"))
    for h in horizons:
        ret = pl.col(f"ret_{h}d")
        excess = pl.col(f"excess_0050_{h}d")
        exprs.extend(
            [
                ret.count().alias(f"valid_{h}d"),
                ret.mean().alias(f"mean_{h}d"),
                (ret > 0).mean().alias(f"win_{h}d"),
                excess.mean().alias(f"mean_excess_0050_{h}d"),
                (excess > 0).mean().alias(f"excess_win_{h}d"),
                (excess.mean() / (excess.std() / excess.count().sqrt())).alias(f"excess_tstat_{h}d"),
            ]
        )
    return labeled.group_by("label").agg(exprs).sort("mean_excess_0050_60d", descending=True, nulls_last=True)
