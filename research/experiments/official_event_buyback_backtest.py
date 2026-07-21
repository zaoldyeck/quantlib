"""Official event-news pilot: MOPS treasury-stock buyback announcements.

This is the smallest verifiable news/event dataset for a message-driven
strategy test:

* Source: MOPS official t35sc09 treasury-stock buyback announcement snapshot.
* Labels: deterministic interpretation of announcement fields available on the
  board-resolution date, not future execution fields.
* Backtest: enter on the next trading day close, hold a fixed horizon, and use
  total-return-adjusted prices from the existing research panel.

Usage:
    uv run --project research python research/experiments/official_event_buyback_backtest.py
"""

from __future__ import annotations

import html
import math
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[2]
RESEARCH_ROOT = REPO_ROOT / "research"
STRAT_LAB = RESEARCH_ROOT / "strat_lab"
OUT_DIR = RESEARCH_ROOT / "experiments" / "out"
DOC_PATH = REPO_ROOT / "docs" / "strategy_research" / "official_event_buyback_study.md"
CHART_PATH = REPO_ROOT / "docs" / "strategy_research" / "official_event_buyback_nav_drawdown.png"
sys.path.insert(0, str(RESEARCH_ROOT))
sys.path.insert(0, str(STRAT_LAB))

from constants import CAPITAL  # noqa: E402
from db import connect  # noqa: E402
from evaluation import nav_metrics  # noqa: E402
from experiments.spike_factor_analysis import load_panel  # noqa: E402
from iter_32_first_principles import COMMISSION, SELL_TAX  # noqa: E402
from prices import total_return_series  # noqa: E402
from validator import recent_one_year_metrics  # noqa: E402


SOURCE_URL = "https://mopsov.twse.com.tw/mops/web/ajax_t35sc09"
START = date(2012, 1, 3)
END = date(2026, 6, 16)
HORIZONS = [5, 20, 60, 120]


class TdTableParser(HTMLParser):
    """Minimal table parser for MOPS HTML without third-party parser deps."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.in_tr = False
        self.in_td = False
        self.cell_parts: list[str] = []
        self.row: list[str] = []
        self.rows: list[list[str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "tr":
            self.in_tr = True
            self.row = []
        elif tag.lower() == "td" and self.in_tr:
            self.in_td = True
            self.cell_parts = []

    def handle_data(self, data: str) -> None:
        if self.in_td:
            self.cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag == "td" and self.in_td:
            text = " ".join("".join(self.cell_parts).split())
            self.row.append(html.unescape(text))
            self.in_td = False
        elif tag == "tr" and self.in_tr:
            if self.row:
                self.rows.append(self.row)
            self.in_tr = False


@dataclass(frozen=True)
class MopsRawSnapshot:
    market: str
    html_path: Path
    fetched_at_utc: str
    row_count: int


def fetch_mops_buybacks(market: str) -> MopsRawSnapshot:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fetched_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    path = OUT_DIR / f"mops_t35sc09_buyback_{market}_{fetched_at[:10]}.html"
    form = {
        "encodeURIComponent": "1",
        "step": "1",
        "firstin": "1",
        "off": "1",
        "TYPEK": market,
        # t35sc09 currently returns a full historical snapshot for each market.
        # Keep range params explicit to make the request auditable.
        "yearb": "101",
        "monthb": "01",
        "yeare": str(END.year - 1911),
        "monthe": f"{END.month:02d}",
    }
    data = urllib.parse.urlencode(form).encode("utf-8")
    req = urllib.request.Request(
        SOURCE_URL,
        data=data,
        headers={
            "User-Agent": "quantlib-research/official-event-study",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=45) as resp:
        raw = resp.read()
    text = raw.decode("utf-8", errors="replace")
    path.write_text(text)
    rows = parse_mops_html(text, market)
    return MopsRawSnapshot(market=market, html_path=path, fetched_at_utc=fetched_at, row_count=len(rows))


def parse_mops_html(text: str, market: str) -> list[dict[str, object]]:
    parser = TdTableParser()
    parser.feed(text)
    out: list[dict[str, object]] = []
    for cols in parser.rows:
        if len(cols) < 19:
            continue
        code = cols[1].strip()
        if not code[:1].isdigit() or len(code) < 4:
            continue
        board_date = parse_minguo_date(cols[3])
        period_start = parse_minguo_date(cols[9])
        period_end = parse_minguo_date(cols[10])
        if board_date is None or period_start is None or period_end is None:
            continue
        out.append(
            {
                "market": "twse" if market == "sii" else "tpex",
                "source_market": market,
                "company_code": code,
                "company_name": cols[2].strip(),
                "board_date": board_date,
                "purpose_code": cols[4].strip(),
                "legal_limit_amount": parse_number(cols[5]),
                "planned_shares": parse_number(cols[6]),
                "price_low": parse_float(cols[7]),
                "price_high": parse_float(cols[8]),
                "period_start": period_start,
                "period_end": period_end,
                "completed_flag": cols[11].strip(),
                # Future execution fields are stored for audit only and are not
                # used in pre-announcement labels or signals.
                "executed_shares_final": parse_number(cols[13]) if len(cols) > 13 else None,
                "execution_pct_final": parse_float(cols[15]) if len(cols) > 15 else None,
                "avg_buyback_price_final": parse_float(cols[17]) if len(cols) > 17 else None,
                "pct_outstanding_final": parse_float(cols[18]) if len(cols) > 18 else None,
                "unfinished_reason_final": cols[19].strip() if len(cols) > 19 else "",
            }
        )
    return out


def parse_minguo_date(value: str) -> date | None:
    value = value.strip()
    if not value or "/" not in value:
        return None
    try:
        y, m, d = [int(x) for x in value.split("/")[:3]]
        return date(y + 1911, m, d)
    except Exception:
        return None


def parse_number(value: str) -> float | None:
    clean = value.replace(",", "").strip()
    if not clean:
        return None
    try:
        return float(clean)
    except ValueError:
        return None


def parse_float(value: str) -> float | None:
    return parse_number(value)


def load_events() -> tuple[pl.DataFrame, list[MopsRawSnapshot]]:
    snapshots: list[MopsRawSnapshot] = []
    rows: list[dict[str, object]] = []
    for market in ["sii", "otc"]:
        snap = fetch_mops_buybacks(market)
        snapshots.append(snap)
        rows.extend(parse_mops_html(snap.html_path.read_text(), market))
        time.sleep(0.5)
    events = pl.DataFrame(rows)
    return events, snapshots


def add_event_labels(events: pl.DataFrame, panel: pl.DataFrame, days: list[date]) -> pl.DataFrame:
    day_index = {d: i for i, d in enumerate(days)}
    price_lookup = {
        (r["date"], r["company_code"]): float(r["close"])
        for r in panel.select(["date", "company_code", "close"]).iter_rows(named=True)
    }

    def next_day(d: date) -> date | None:
        for day in days:
            if day > d:
                return day
        return None

    records: list[dict[str, object]] = []
    for row in events.iter_rows(named=True):
        bd = row["board_date"]
        if bd < START or bd > END:
            continue
        entry = next_day(bd)
        if entry is None:
            continue
        code = row["company_code"]
        entry_close = price_lookup.get((entry, code))
        if entry_close is None or entry_close <= 0:
            continue
        planned_shares = float(row["planned_shares"] or 0.0)
        price_high = float(row["price_high"] or 0.0)
        price_low = float(row["price_low"] or 0.0)
        legal_limit = float(row["legal_limit_amount"] or 0.0)
        planned_budget_high = planned_shares * price_high
        budget_to_limit = planned_budget_high / legal_limit if legal_limit > 0 else None
        price_high_premium = price_high / entry_close - 1.0 if price_high > 0 else None
        price_low_discount = price_low / entry_close - 1.0 if price_low > 0 else None
        pre20_idx = day_index.get(entry, 0) - 20
        pre20_date = days[pre20_idx] if pre20_idx >= 0 else None
        pre20_close = price_lookup.get((pre20_date, code)) if pre20_date else None
        pre20_ret = entry_close / pre20_close - 1.0 if pre20_close and pre20_close > 0 else None
        enriched = dict(row)
        enriched.update(
            {
                "entry_date": entry,
                "entry_close": entry_close,
                "planned_budget_high": planned_budget_high,
                "budget_to_limit": budget_to_limit,
                "price_high_premium": price_high_premium,
                "price_low_discount": price_low_discount,
                "pre20_ret": pre20_ret,
                "purpose_3_support": row["purpose_code"] == "3",
                "high_price_ceiling": price_high_premium is not None and price_high_premium >= 0.20,
                "deep_pre_drop": pre20_ret is not None and pre20_ret <= -0.05,
                "large_authorization": budget_to_limit is not None and budget_to_limit >= 0.03,
            }
        )
        score = 0.0
        score += 1.5 if enriched["purpose_3_support"] else 0.0
        score += min(max((price_high_premium or 0.0), 0.0), 1.0)
        score += min(max((budget_to_limit or 0.0) * 10.0, 0.0), 1.0)
        score += 0.4 if enriched["deep_pre_drop"] else 0.0
        enriched["event_score"] = score
        records.append(enriched)
    return pl.DataFrame(records).unique(["market", "company_code", "board_date", "purpose_code"], keep="last")


def attach_forward_returns(events: pl.DataFrame, panel: pl.DataFrame, days: list[date]) -> pl.DataFrame:
    day_index = {d: i for i, d in enumerate(days)}
    price_lookup = {
        (r["date"], r["company_code"]): float(r["close"])
        for r in panel.select(["date", "company_code", "close"]).iter_rows(named=True)
    }

    rows = []
    for row in events.iter_rows(named=True):
        entry = row["entry_date"]
        idx = day_index.get(entry)
        entry_close = row["entry_close"]
        out = dict(row)
        for h in HORIZONS:
            hdate = days[idx + h] if idx is not None and idx + h < len(days) else None
            hclose = price_lookup.get((hdate, row["company_code"])) if hdate else None
            out[f"ret_{h}d"] = hclose / entry_close - 1.0 if hclose and entry_close else None
        rows.append(out)
    return pl.DataFrame(rows)


def summarize_events(events: pl.DataFrame) -> pl.DataFrame:
    def metric_expr(h: int) -> list[pl.Expr]:
        col = pl.col(f"ret_{h}d")
        return [
            col.mean().alias(f"mean_{h}d"),
            col.median().alias(f"median_{h}d"),
            (col > 0).mean().alias(f"win_{h}d"),
            (col > 0.10).mean().alias(f"gain10_{h}d"),
        ]

    groups = {
        "all_buybacks": pl.lit(True),
        "purpose_1": pl.col("purpose_code") == "1",
        "purpose_2": pl.col("purpose_code") == "2",
        "purpose_3_support": pl.col("purpose_3_support"),
        "high_price_ceiling": pl.col("high_price_ceiling"),
        "large_authorization": pl.col("large_authorization"),
        "purpose3_high_ceiling": pl.col("purpose_3_support") & pl.col("high_price_ceiling"),
        "purpose3_after_drop": pl.col("purpose_3_support") & pl.col("deep_pre_drop"),
        "purpose3_large_high": pl.col("purpose_3_support") & pl.col("large_authorization") & pl.col("high_price_ceiling"),
    }
    rows = []
    for name, expr in groups.items():
        subset = events.filter(expr)
        if subset.height == 0:
            continue
        agg_exprs: list[pl.Expr] = [pl.len().alias("n")]
        for h in HORIZONS:
            agg_exprs.extend(metric_expr(h))
        row = subset.select(agg_exprs).to_dicts()[0]
        row["label"] = name
        rows.append(row)
    return pl.DataFrame(rows).select(["label", "n"] + [c for c in rows[0].keys() if c not in {"label", "n"}])


def signal_filter(name: str) -> pl.Expr:
    if name == "all_buybacks":
        return pl.lit(True)
    if name == "purpose_3_support":
        return pl.col("purpose_3_support")
    if name == "high_price_ceiling":
        return pl.col("high_price_ceiling")
    if name == "purpose3_high_ceiling":
        return pl.col("purpose_3_support") & pl.col("high_price_ceiling")
    if name == "purpose3_after_drop":
        return pl.col("purpose_3_support") & pl.col("deep_pre_drop")
    if name == "purpose3_large_high":
        return pl.col("purpose_3_support") & pl.col("large_authorization") & pl.col("high_price_ceiling")
    raise ValueError(name)


def simulate_event_strategy(
    panel: pl.DataFrame,
    days: list[date],
    events: pl.DataFrame,
    signal_name: str,
    hold_days: int,
    max_positions: int = 10,
) -> tuple[dict[str, object], pl.DataFrame]:
    signals = (
        events.filter(signal_filter(signal_name))
        .filter(pl.col("entry_date").is_not_null())
        .sort(["entry_date", "event_score"], descending=[False, True])
        .group_by("entry_date", maintain_order=True)
        .head(max_positions)
        .select(["entry_date", "company_code", "event_score"])
    )
    starts_by_day: dict[date, list[tuple[str, float]]] = {}
    for key, g in signals.group_by("entry_date", maintain_order=True):
        day = key[0] if isinstance(key, tuple) else key
        starts_by_day[day] = [(r["company_code"], float(r["event_score"])) for r in g.iter_rows(named=True)]

    daily_rets = (
        panel.sort(["company_code", "date"])
        .with_columns((pl.col("close") / pl.col("close").shift(1).over("company_code") - 1.0).fill_null(0.0).alias("ret"))
        .select(["date", "company_code", "ret"])
    )
    ret_lookup = {
        (r["date"], r["company_code"]): float(r["ret"] or 0.0)
        for r in daily_rets.iter_rows(named=True)
    }
    day_index = {d: i for i, d in enumerate(days)}
    nav = CAPITAL
    active: dict[str, int] = {}
    weights: dict[str, float] = {}
    rows = []
    turnover_sum = 0.0
    rebalance_count = 0

    for day in days:
        nav *= 1.0 + sum(w * ret_lookup.get((day, code), 0.0) for code, w in weights.items())
        idx = day_index[day]
        active = {code: exit_idx for code, exit_idx in active.items() if exit_idx > idx}
        for code, _score in starts_by_day.get(day, []):
            if code not in active:
                active[code] = idx + hold_days
        if len(active) > max_positions:
            # Keep the most recent active names when event count exceeds slots.
            active = dict(sorted(active.items(), key=lambda kv: kv[1], reverse=True)[:max_positions])
        next_weights = {code: 1.0 / len(active) for code in active} if active else {}
        all_codes = set(weights) | set(next_weights)
        buy_turnover = sum(max(next_weights.get(code, 0.0) - weights.get(code, 0.0), 0.0) for code in all_codes)
        sell_turnover = sum(max(weights.get(code, 0.0) - next_weights.get(code, 0.0), 0.0) for code in all_codes)
        turnover = buy_turnover + sell_turnover
        if turnover > 0:
            nav -= nav * (buy_turnover * COMMISSION + sell_turnover * (COMMISSION + SELL_TAX))
            turnover_sum += turnover
            rebalance_count += 1
            weights = next_weights
        rows.append({"date": day, "nav": nav, "position_count": len(weights), "turnover": turnover})

    daily = pl.DataFrame(rows)
    row = nav_metrics(daily.select(["date", "nav"]))
    row.update(recent_one_year_metrics(daily.select(["date", "nav"]), capital=CAPITAL))
    row.update(
        {
            "name": f"{signal_name}_hold{hold_days}",
            "signals": int(signals.height),
            "avg_positions": float(daily["position_count"].mean()),
            "avg_turnover": turnover_sum / max(rebalance_count, 1),
            "final_nav": float(nav),
        }
    )
    return row, daily


def benchmark(code: str, label: str, start: date, end: date) -> tuple[dict[str, object], pl.DataFrame]:
    con = connect(read_only=True)
    try:
        s = total_return_series(con, code, start.isoformat(), end.isoformat(), market="twse").sort("date")
    finally:
        con.close()
    daily = (
        s.select(["date", "adj_close"])
        .with_columns((CAPITAL * pl.col("adj_close") / pl.col("adj_close").first()).alias("nav"))
        .select(["date", "nav"])
    )
    row = nav_metrics(daily)
    row.update(recent_one_year_metrics(daily, capital=CAPITAL))
    row.update({"name": label, "final_nav": float(daily["nav"][-1])})
    return row, daily


def write_nav_chart() -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    series = {
        "Buyback high ceiling hold60": OUT_DIR / "official_event_buyback_high_price_ceiling_hold60_daily.csv",
        "All buybacks hold60": OUT_DIR / "official_event_buyback_all_buybacks_hold60_daily.csv",
        "0050 TR": OUT_DIR / "official_event_buyback_0050_benchmark_daily.csv",
        "2330 TR": OUT_DIR / "official_event_buyback_2330_benchmark_daily.csv",
    }
    fig, axes = plt.subplots(2, 1, figsize=(13, 8), sharex=True, gridspec_kw={"height_ratios": [2.0, 1.0]})
    for label, path in series.items():
        if not path.exists():
            continue
        df = pl.read_csv(path, try_parse_dates=True).sort("date")
        nav = df["nav"].to_numpy()
        norm = nav / nav[0]
        peak = np.maximum.accumulate(norm)
        dd = norm / peak - 1.0
        axes[0].plot(df["date"].to_list(), norm, label=label, linewidth=1.7)
        axes[1].plot(df["date"].to_list(), dd * 100.0, label=label, linewidth=1.2)
    axes[0].set_title("Official Buyback Event Strategies vs Benchmarks")
    axes[0].set_ylabel("NAV multiple")
    axes[0].grid(True, alpha=0.25)
    axes[0].legend(ncols=2, fontsize=9)
    axes[1].set_title("Drawdown")
    axes[1].set_ylabel("Drawdown (%)")
    axes[1].grid(True, alpha=0.25)
    axes[1].axhline(0, color="black", linewidth=0.7)
    fig.tight_layout()
    CHART_PATH.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(CHART_PATH, dpi=160)


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


def write_report(snapshots: list[MopsRawSnapshot], event_summary: pl.DataFrame, strategy_summary: pl.DataFrame) -> None:
    DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# 官方消息面最小實驗：庫藏股公告事件研究",
        "",
        f"資料下載時間：`{datetime.now(timezone.utc).replace(microsecond=0).isoformat()}` UTC。",
        f"資料來源：MOPS `t35sc09` 公司買回自己公司股份彙總統計表：`{SOURCE_URL}`。",
        f"價格資料截止：`{END}`；回測起點：`{START}`；價格使用 total-return adjusted close。",
        "",
        "## 下載驗證",
        "",
        "| 市場 | 原始檔 | 解析事件數 |",
        "|---|---|---:|",
    ]
    for snap in snapshots:
        rel = snap.html_path.relative_to(REPO_ROOT)
        lines.append(f"| {snap.market} | `{rel}` | {snap.row_count:,} |")
    lines += [
        "",
        "## 事件標籤",
        "",
        "本實驗只使用公告當下可見欄位，不使用後續實際買回股數、執行比例、未執行原因等未來資訊。",
        "",
        "- `purpose_3_support`：買回目的代碼為 3，通常對應維護公司信用與股東權益。",
        "- `high_price_ceiling`：公告買回最高價比進場日收盤價高 20% 以上。",
        "- `large_authorization`：預定最高買回金額 / 法定買回上限 >= 3%。",
        "- `deep_pre_drop`：公告前約 20 個交易日跌幅 <= -5%。",
        "",
        "## 事件後報酬",
        "",
        "| 標籤 | n | 20日均值 | 20日勝率 | 60日均值 | 60日勝率 | 120日均值 | 120日勝率 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in event_summary.sort("mean_60d", descending=True).iter_rows(named=True):
        lines.append(
            f"| `{row['label']}` | {int(row['n']):,} | {pct(row['mean_20d'])} | {pct(row['win_20d'])} | "
            f"{pct(row['mean_60d'])} | {pct(row['win_60d'])} | {pct(row['mean_120d'])} | {pct(row['win_120d'])} |"
        )
    lines += [
        "",
        "## 固定持有策略回測",
        "",
        "策略規則：公告後下一個交易日收盤建立部位，最多同時持有 10 檔，等權，固定持有 20 或 60 個交易日；含手續費與賣出交易稅。",
        "",
        "![Official Buyback Event Strategies vs Benchmarks](official_event_buyback_nav_drawdown.png)",
        "",
        "| 策略 | CAGR | 最近一年 CAGR | Sortino | MDD | 訊號數 | 平均持股 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in strategy_summary.sort("cagr", descending=True).iter_rows(named=True):
        sortino = row.get("sortino")
        sortino_s = "-" if sortino is None or (isinstance(sortino, float) and math.isnan(sortino)) else f"{float(sortino):.3f}"
        signals = row.get("signals")
        signals_s = "-" if signals is None or (isinstance(signals, float) and math.isnan(signals)) else str(int(signals))
        avg_pos = row.get("avg_positions")
        avg_pos_s = "-" if avg_pos is None or (isinstance(avg_pos, float) and math.isnan(avg_pos)) else f"{float(avg_pos):.2f}"
        lines.append(
            f"| `{row['name']}` | {pct(row['cagr'])} | {pct(row['recent_1y_cagr'])} | {sortino_s} | "
            f"{pct(row['mdd'])} | {signals_s} | {avg_pos_s} |"
        )
    lines += [
        "",
        "## 初步結論",
        "",
        "庫藏股公告本身有可量化的事件訊號，但第一輪最小回測尚未形成足以取代既有策略或 2330/0050 benchmark 的強策略。它比較適合成為消息面特徵之一，下一步應與營收動能、技術突破、法人籌碼、產業催化一起做交互條件，而不是單獨交易。",
        "",
    ]
    DOC_PATH.write_text("\n".join(lines))


def main() -> None:
    t0 = time.time()
    events, snapshots = load_events()
    events_path = OUT_DIR / "official_event_buyback_raw.parquet"
    events.write_parquet(events_path)
    print(f"[download] parsed {events.height:,} raw buyback events -> {events_path}")
    for snap in snapshots:
        print(f"  {snap.market}: rows={snap.row_count:,} file={snap.html_path}")

    panel, days = load_panel(START, END)
    print(f"[prices] panel rows={panel.height:,}, days={len(days):,}, cutoff={max(days)}")

    labeled = add_event_labels(events, panel, days)
    labeled = attach_forward_returns(labeled, panel, days)
    labeled_path = OUT_DIR / "official_event_buyback_labeled.parquet"
    labeled.write_parquet(labeled_path)
    print(f"[labels] usable events={labeled.height:,} -> {labeled_path}")

    event_summary = summarize_events(labeled)
    event_summary_path = OUT_DIR / "official_event_buyback_event_summary.csv"
    event_summary.write_csv(event_summary_path)
    print(f"[event] wrote {event_summary_path}")

    strategy_rows: list[dict[str, object]] = []
    for signal_name in [
        "all_buybacks",
        "purpose_3_support",
        "high_price_ceiling",
        "purpose3_high_ceiling",
        "purpose3_after_drop",
        "purpose3_large_high",
    ]:
        for hold_days in [20, 60]:
            row, daily = simulate_event_strategy(panel, days, labeled, signal_name, hold_days)
            daily_path = OUT_DIR / f"official_event_buyback_{signal_name}_hold{hold_days}_daily.csv"
            daily.write_csv(daily_path)
            row["daily_path"] = str(daily_path)
            strategy_rows.append(row)
            print(
                f"  {row['name']}: CAGR={float(row['cagr']):+.2%} "
                f"recent1Y={float(row['recent_1y_cagr']):+.2%} MDD={float(row['mdd']):+.2%} "
                f"signals={row['signals']}"
            )

    for code, label in [("0050", "0050 TR"), ("2330", "2330 TR")]:
        row, daily = benchmark(code, label, START, END)
        daily_path = OUT_DIR / f"official_event_buyback_{code}_benchmark_daily.csv"
        daily.write_csv(daily_path)
        row["daily_path"] = str(daily_path)
        strategy_rows.append(row)

    strategy_summary = pl.DataFrame(strategy_rows).sort("cagr", descending=True)
    strategy_summary_path = OUT_DIR / "official_event_buyback_strategy_summary.csv"
    strategy_summary.write_csv(strategy_summary_path)
    write_nav_chart()
    write_report(snapshots, event_summary, strategy_summary)

    print("\n=== event summary by 60d mean ===")
    with pl.Config(tbl_rows=20, tbl_width_chars=180):
        print(
            event_summary.sort("mean_60d", descending=True).select(
                [
                    "label",
                    "n",
                    pl.col("mean_20d").mul(100).round(2).alias("mean_20d_pct"),
                    pl.col("win_20d").mul(100).round(2).alias("win_20d_pct"),
                    pl.col("mean_60d").mul(100).round(2).alias("mean_60d_pct"),
                    pl.col("win_60d").mul(100).round(2).alias("win_60d_pct"),
                    pl.col("mean_120d").mul(100).round(2).alias("mean_120d_pct"),
                ]
            )
        )

    print("\n=== strategy summary ===")
    with pl.Config(tbl_rows=20, tbl_width_chars=180):
        print(
            strategy_summary.select(
                [
                    "name",
                    pl.col("cagr").mul(100).round(2).alias("cagr_pct"),
                    pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
                    pl.col("sortino").round(3),
                    pl.col("mdd").mul(100).round(2).alias("mdd_pct"),
                    "signals",
                    pl.col("avg_positions").round(2),
                ]
            )
        )
    print(f"\n[out] {strategy_summary_path}")
    print(f"[doc] {DOC_PATH}")
    print(f"[runtime] {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
