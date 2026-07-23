"""Survey MOPS material-information message types for alpha.

This experiment downloads the official MOPS daily material-information feed
(`t05st02`), classifies each disclosure into deterministic message families,
and measures total-return forward returns after the announcement.

It is deliberately an event-study layer, not a production strategy:

* event date/time comes from MOPS hidden fields h02/h03
* entry reference = first trading day after the announcement date, close
* prices = total-return-adjusted close from the shared research panel
* alpha = stock forward return minus same-window 0050 total-return return

Usage:
    uv run --project . python src/quantlib/experiments/mops_material_info_alpha_survey.py

Optional:
    uv run --project . python src/quantlib/experiments/mops_material_info_alpha_survey.py \
      --start 2024-01-01 --end 2025-12-31 --workers 4
"""

from __future__ import annotations

import argparse
import hashlib
import html
import math
import re
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable

import polars as pl
from quantlib import paths

REPO_ROOT = Path(__file__).resolve().parents[3]
RESEARCH_ROOT = REPO_ROOT / "src" / "quantlib"
STRAT_LAB = RESEARCH_ROOT / "strat_lab"
OUT_DIR = paths.OUT_EXPERIMENTS
RAW_DIR = OUT_DIR / "mops_t05st02_material_info"
DOC_PATH = REPO_ROOT / "docs" / "strategy_research" / "mops_material_info_alpha_survey.md"
CHART_PATH = REPO_ROOT / "docs" / "strategy_research" / "mops_material_info_alpha_survey.png"
sys.path.insert(0, str(RESEARCH_ROOT))
sys.path.insert(0, str(STRAT_LAB))

from quantlib.db import connect  # noqa: E402
from experiments.spike_factor_analysis import load_panel  # noqa: E402
from quantlib.prices import total_return_series  # noqa: E402

SOURCE_URL = "https://mopsov.twse.com.tw/mops/web/ajax_t05st02"
PANEL_START = date(2012, 1, 3)
PRICE_END = date(2026, 6, 16)
DEFAULT_EVENT_START = date(2025, 1, 1)
DEFAULT_EVENT_END = date(2025, 12, 31)
HORIZONS = [5, 20, 60, 120]


@dataclass(frozen=True)
class Rule:
    label: str
    include: tuple[str, ...]
    exclude: tuple[str, ...] = ()


RULES: tuple[Rule, ...] = (
    Rule(
        "material_order_contract",
        ("合約", "訂單", "接單", "得標", "標案", "供應", "客戶", "採購", "合作協議", "授權"),
        ("解除董事競業禁止", "股東會", "董事會"),
    ),
    Rule(
        "material_new_product_capacity",
        ("新產品", "量產", "試產", "擴產", "產能", "建廠", "廠房", "投產", "認證", "出貨", "研發"),
        ("解除董事競業禁止",),
    ),
    Rule(
        "material_biotech_regulatory",
        ("臨床", "解盲", "新藥", "FDA", "NDA", "藥證", "查驗登記", "衛福部", "TFDA", "試驗"),
    ),
    Rule(
        "material_profit_positive",
        ("轉虧為盈", "創新高", "獲利增加", "獲利成長", "每股盈餘", "EPS", "稅後純益增加", "營收成長"),
        ("減少", "衰退", "虧損", "更正", "重編"),
    ),
    Rule(
        "material_profit_negative",
        ("虧損", "轉盈為虧", "衰退", "減少", "低於", "營收減少", "獲利減少", "重大影響財務"),
        ("轉虧為盈",),
    ),
    Rule(
        "material_mna_strategic_investment",
        ("合併", "收購", "併購", "策略聯盟", "股份轉換", "取得股權", "處分股權", "投資"),
        ("解除董事競業禁止", "股東會", "董事會"),
    ),
    Rule(
        "material_asset_disposal_gain",
        ("處分不動產", "處分土地", "出售土地", "處分資產", "處分利益", "處分有價證券"),
        ("子公司",),
    ),
    Rule(
        "material_capital_raising_dilution",
        ("現金增資", "私募", "公司債", "轉換公司債", "CB", "ECB", "GDR", "募集", "增資基準日"),
    ),
    Rule(
        "material_dividend_distribution",
        ("股利", "配息", "配股", "盈餘分派", "資本公積發放", "除權息"),
    ),
    Rule(
        "material_buyback_related",
        ("買回本公司股份", "庫藏股"),
    ),
    Rule(
        "material_legal_penalty_risk",
        ("訴訟", "假扣押", "違約", "罰鍰", "裁罰", "檢調", "調查", "停工", "污染", "資安", "火災", "災害", "職災", "退票"),
    ),
    Rule(
        "material_trading_status_risk",
        ("暫停交易", "恢復交易", "停止買賣", "變更交易方法", "全額交割", "處置", "注意股票"),
    ),
    Rule(
        "material_accounting_restatement",
        ("更正財報", "重編", "追溯", "會計", "財報附註", "保留意見"),
    ),
    Rule(
        "material_shareholder_ownership",
        ("持股", "股權", "大股東", "申報轉讓", "質押", "解除質押"),
    ),
    Rule(
        "material_related_party_subsidiary",
        ("代子公司", "重要子公司", "子公司公告", "代重要子公司"),
    ),
    Rule(
        "material_routine_governance",
        ("股東會", "董事會", "董事競業禁止", "改選", "委任", "委員會", "法人董事", "獨立董事", "薪酬委員", "審計委員"),
    ),
)


class MaterialInfoParser(HTMLParser):
    """Extract h00-h08 hidden fields from the MOPS material-info HTML."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.groups: dict[str, dict[str, str]] = {}
        self.rows: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "input":
            return
        attr = {k.lower(): v or "" for k, v in attrs}
        name = attr.get("name", "")
        match = re.fullmatch(r"h(\d+)([0-8])", name)
        if match is None:
            return
        row_id, field_id = match.groups()
        current = self.groups.setdefault(row_id, {})
        current[f"h0{field_id}"] = html.unescape(attr.get("value", "")).strip()
        if field_id == "8" and {"h00", "h01", "h02", "h03", "h04"}.issubset(current):
            self.rows.append(dict(current))
            self.groups.pop(row_id, None)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=DEFAULT_EVENT_START.isoformat())
    parser.add_argument("--end", default=DEFAULT_EVENT_END.isoformat())
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--force-fetch", action="store_true")
    return parser.parse_args()


def daterange(start: date, end: date) -> Iterable[date]:
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def roc_year(d: date) -> int:
    return d.year - 1911


def fetch_one_day(query_date: date, force: bool = False) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / f"{query_date.isoformat()}.html"
    if path.exists() and not force:
        return path
    form = {
        "step": "1",
        "step00": "0",
        # The official form uses this typo; using "true" returns empty data.
        "firstin": "ture",
        "off": "1",
        "TYPEK": "all",
        "year": str(roc_year(query_date)),
        "month": f"{query_date.month:02d}",
        "day": f"{query_date.day:02d}",
    }
    data = urllib.parse.urlencode(form).encode("utf-8")
    req = urllib.request.Request(
        SOURCE_URL,
        data=data,
        headers={
            "User-Agent": "quantlib-research/mops-material-info-alpha",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    last_err: Exception | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                raw = resp.read()
            path.write_bytes(raw)
            return path
        except Exception as exc:  # pragma: no cover - network transient
            last_err = exc
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"failed to fetch {query_date}: {last_err}")


def parse_yyyymmdd(value: str) -> date | None:
    clean = "".join(ch for ch in value if ch.isdigit())
    if len(clean) != 8:
        return None
    try:
        return date(int(clean[:4]), int(clean[4:6]), int(clean[6:8]))
    except ValueError:
        return None


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", "", value or "")


def contains_any(text: str, patterns: tuple[str, ...]) -> bool:
    return any(p in text for p in patterns)


def classify(subject: str, detail: str) -> list[str]:
    text = normalize_text(subject + "\n" + detail)
    labels = ["material_all"]
    for rule in RULES:
        if contains_any(text, rule.include) and not contains_any(text, rule.exclude):
            labels.append(rule.label)
    if labels == ["material_all"]:
        labels.append("material_other")
    return labels


def parse_day_file(path: Path, event_start: date, event_end: date) -> list[dict[str, object]]:
    parser = MaterialInfoParser()
    parser.feed(path.read_text(encoding="utf-8", errors="replace"))
    rows: list[dict[str, object]] = []
    for raw in parser.rows:
        code = raw.get("h01", "").strip()
        if not re.fullmatch(r"[1-9][0-9]{3}", code):
            continue
        event_date = parse_yyyymmdd(raw.get("h02", ""))
        if event_date is None or not (event_start <= event_date <= event_end):
            continue
        subject = " ".join((raw.get("h04") or "").split())
        detail = raw.get("h08") or ""
        event_time = raw.get("h03", "").strip()
        digest = hashlib.sha256(detail.encode("utf-8")).hexdigest()[:16]
        base = {
            "event_date": event_date,
            "event_time": event_time,
            "company_code": code,
            "company_name": raw.get("h00", "").strip(),
            "subject": subject,
            "detail_hash": digest,
            "detail_len": len(detail),
            "mops_h05": raw.get("h05", "").strip(),
            "mops_h06": raw.get("h06", "").strip(),
            "fact_date": parse_yyyymmdd(raw.get("h07", "")),
            "query_file": path.name,
        }
        for label in classify(subject, detail):
            item = dict(base)
            item["label"] = label
            rows.append(item)
    return rows


def fetch_and_parse(start: date, end: date, workers: int, force: bool) -> pl.DataFrame:
    query_start = start
    query_end = end + timedelta(days=1)
    dates = list(daterange(query_start, query_end))
    local_paths: list[Path] = []
    workers = max(1, min(workers, 8))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_one_day, d, force): d for d in dates}
        for fut in as_completed(futures):
            paths.append(fut.result())
    rows: list[dict[str, object]] = []
    for path in sorted(local_paths):
        rows.extend(parse_day_file(path, start, end))
    if not rows:
        return pl.DataFrame(
            schema={
                "event_date": pl.Date,
                "event_time": pl.Utf8,
                "company_code": pl.Utf8,
                "company_name": pl.Utf8,
                "subject": pl.Utf8,
                "detail_hash": pl.Utf8,
                "detail_len": pl.Int64,
                "mops_h05": pl.Utf8,
                "mops_h06": pl.Utf8,
                "fact_date": pl.Date,
                "query_file": pl.Utf8,
                "label": pl.Utf8,
            }
        )
    return (
        pl.DataFrame(rows)
        .unique(["event_date", "event_time", "company_code", "subject", "detail_hash", "label"], keep="first")
        .sort(["event_date", "event_time", "company_code", "label"])
    )


def load_benchmark_returns(start: date, end: date, code: str = "0050") -> dict[date, float]:
    con = connect(read_only=True)
    try:
        frame = total_return_series(con, code=code, start=start.isoformat(), end=end.isoformat(), market="twse")
    finally:
        con.close()
    return {r["date"]: float(r["adj_close"]) for r in frame.iter_rows(named=True)}


def attach_returns(events: pl.DataFrame, panel: pl.DataFrame, days: list[date]) -> pl.DataFrame:
    price_lookup = {
        (r["date"], r["company_code"]): float(r["close"])
        for r in panel.select(["date", "company_code", "close"]).iter_rows(named=True)
    }
    bench_lookup = load_benchmark_returns(PANEL_START, PRICE_END, "0050")
    day_index = {d: i for i, d in enumerate(days)}

    def next_trading_day(d: date) -> date | None:
        for day in days:
            if day > d:
                return day
        return None

    rows: list[dict[str, object]] = []
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
            hclose = price_lookup.get((hdate, r["company_code"])) if hdate else None
            bclose = bench_lookup.get(hdate) if hdate else None
            ret = hclose / entry_close - 1.0 if hclose and hclose > 0 else None
            bench_ret = bclose / bench_entry - 1.0 if bclose and bclose > 0 else None
            out[f"ret_{h}d"] = ret
            out[f"bench_0050_{h}d"] = bench_ret
            out[f"excess_0050_{h}d"] = ret - bench_ret if ret is not None and bench_ret is not None else None
        rows.append(out)
    return pl.DataFrame(rows)


def summarize(labeled: pl.DataFrame) -> pl.DataFrame:
    exprs: list[pl.Expr] = [pl.len().alias("n"), pl.col("detail_hash").n_unique().alias("events")]
    for h in HORIZONS:
        ret = pl.col(f"ret_{h}d")
        excess = pl.col(f"excess_0050_{h}d")
        exprs.extend(
            [
                ret.count().alias(f"valid_{h}d"),
                ret.mean().alias(f"mean_{h}d"),
                ret.median().alias(f"median_{h}d"),
                (ret > 0).mean().alias(f"win_{h}d"),
                (ret > 0.10).mean().alias(f"gain10_{h}d"),
                (ret < -0.10).mean().alias(f"loss10_{h}d"),
                excess.mean().alias(f"mean_excess_0050_{h}d"),
                excess.median().alias(f"median_excess_0050_{h}d"),
                (excess > 0).mean().alias(f"excess_win_{h}d"),
                (excess.mean() / (excess.std() / excess.count().sqrt())).alias(f"excess_tstat_{h}d"),
            ]
        )
    return labeled.group_by("label").agg(exprs).sort("mean_excess_0050_60d", descending=True, nulls_last=True)


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


def write_chart(summary: pl.DataFrame) -> None:
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return
    data = (
        summary.filter(pl.col("valid_60d") >= 30)
        .select(["label", "mean_excess_0050_60d", "valid_60d"])
        .sort("mean_excess_0050_60d", descending=True)
    )
    if data.height == 0:
        return
    top = data.head(8)
    bottom = data.tail(8)
    plot_df = pl.concat([top, bottom]).unique("label", keep="first").sort("mean_excess_0050_60d")
    labels = plot_df["label"].to_list()
    values = [float(x) * 100.0 for x in plot_df["mean_excess_0050_60d"].to_list()]
    colors = ["#168a5f" if v >= 0 else "#b23838" for v in values]
    plt.figure(figsize=(12, max(6, len(labels) * 0.36)))
    plt.barh(labels, values, color=colors)
    plt.axvline(0, color="#333333", linewidth=0.8)
    plt.xlabel("60 trading-day excess return vs 0050 (%)")
    plt.title("MOPS Material Information: Message-Type Alpha Survey")
    plt.tight_layout()
    CHART_PATH.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(CHART_PATH, dpi=160)
    plt.close()


def sample_subjects(labeled: pl.DataFrame, label: str, limit: int = 5) -> list[str]:
    if labeled.height == 0:
        return []
    rows = (
        labeled.filter(pl.col("label") == label)
        .unique(["event_date", "company_code", "subject"], keep="first")
        .sort("event_date")
        .tail(limit)
        .select("subject")
        .to_series()
        .to_list()
    )
    return [str(x)[:70] for x in rows]


def write_report(start: date, end: date, summary: pl.DataFrame, labeled: pl.DataFrame) -> None:
    DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
    rows = summary.iter_rows(named=True)
    lines = [
        "# MOPS 重大訊息 Alpha Survey",
        "",
        f"事件樣本：`{start}` 至 `{end}` 的公開資訊觀測站 `t05st02` 重大訊息；價格資料截止：`{PRICE_END}`。",
        "",
        "回測口徑：事件日後第一個交易日收盤作為 entry reference，使用 total-return adjusted close，計算 5/20/60/120 個交易日後報酬；`excess` 是同期間相對 0050 total-return 的超額報酬。",
        "",
        "這份研究只儲存分類、主旨、hash 與統計結果；完整文字只保留在本機 raw cache 供驗證，不進入報告。",
        "",
    ]
    if CHART_PATH.exists():
        lines.extend(["![MOPS material information alpha survey](mops_material_info_alpha_survey.png)", ""])
    lines.extend(
        [
            "## 60 日相對 0050 排序",
            "",
            "| 排名 | 消息類型 | label rows | 事件數 | 60日有效n | 60日均值 | 60日勝率 | 60日超額均值 | 60日超額勝率 | t-stat | 120日超額均值 |",
            "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for i, row in enumerate(rows, start=1):
        lines.append(
            f"| {i} | `{row['label']}` | {int(row['n']):,} | {int(row['events']):,} | "
            f"{int(row['valid_60d'] or 0):,} | {pct(row['mean_60d'])} | {pct(row['win_60d'])} | "
            f"{pct(row['mean_excess_0050_60d'])} | {pct(row['excess_win_60d'])} | "
            f"{num(row['excess_tstat_60d'])} | {pct(row['mean_excess_0050_120d'])} |"
        )
    top_labels = [
        r["label"]
        for r in summary.filter(pl.col("valid_60d") >= 30).head(5).iter_rows(named=True)
        if r["label"] != "material_all"
    ]
    bottom_labels = [
        r["label"]
        for r in summary.filter(pl.col("valid_60d") >= 30).tail(5).iter_rows(named=True)
        if r["label"] != "material_all"
    ]
    lines.extend(["", "## 代表主旨樣本", ""])
    for label in top_labels + bottom_labels:
        subjects = sample_subjects(labeled, label)
        if not subjects:
            continue
        lines.append(f"### `{label}`")
        for subject in subjects:
            lines.append(f"- {subject}")
        lines.append("")
    lines.extend(
        [
            "## 初步判讀",
            "",
            "- 正向 alpha 需要同時看 `60日超額均值`、`60日超額勝率` 與 `t-stat`；樣本少但均值高的類別只能當研究線索，不能直接當交易策略。",
            "- 負向 alpha 類型同樣有價值，較適合作為持股風險過濾器或事件風險降槓桿條件。",
            "- 下一步若要進入策略層，應把強訊息類型與價格動能、月營收成長、法人籌碼、流動性、產業輪動做交互，並用 walk-forward 驗證，而不是直接買所有事件。",
            "",
        ]
    )
    DOC_PATH.write_text("\n".join(lines))


def main() -> None:
    args = parse_args()
    event_start = date.fromisoformat(args.start)
    event_end = date.fromisoformat(args.end)
    if event_end < event_start:
        raise ValueError("--end must be >= --start")
    t0 = time.time()
    print(f"[mops] fetching/parsing t05st02 {event_start} -> {event_end} workers={args.workers}")
    events = fetch_and_parse(event_start, event_end, args.workers, args.force_fetch)
    events_path = OUT_DIR / "mops_material_info_events.parquet"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    events.write_parquet(events_path)
    print(f"[events] label rows={events.height:,}; unique disclosures={events.select('detail_hash').n_unique():,} -> {events_path}")

    print(f"[prices] loading adjusted panel {PANEL_START} -> {PRICE_END}")
    panel, days = load_panel(PANEL_START, PRICE_END)
    print(f"[prices] rows={panel.height:,}, days={len(days):,}, cutoff={max(days)}")

    labeled = attach_returns(events, panel, days)
    labeled_path = OUT_DIR / "mops_material_info_labeled.parquet"
    labeled.write_parquet(labeled_path)
    print(f"[labels] usable label rows={labeled.height:,} -> {labeled_path}")

    summary = summarize(labeled)
    summary_path = OUT_DIR / "mops_material_info_summary.csv"
    summary.write_csv(summary_path)
    write_chart(summary)
    write_report(event_start, event_end, summary, labeled)

    print("\n=== MOPS material-info alpha by 60d excess vs 0050 ===")
    with pl.Config(tbl_rows=60, tbl_width_chars=190):
        print(
            summary.select(
                [
                    "label",
                    "n",
                    "events",
                    "valid_60d",
                    pl.col("mean_60d").mul(100).round(2).alias("mean_60d_pct"),
                    pl.col("win_60d").mul(100).round(2).alias("win_60d_pct"),
                    pl.col("mean_excess_0050_60d").mul(100).round(2).alias("excess60_pct"),
                    pl.col("excess_win_60d").mul(100).round(2).alias("excess_win60_pct"),
                    pl.col("excess_tstat_60d").round(2).alias("excess_tstat60"),
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
