"""Cross-sectional spike factor study.

This turns the spike-event inventory into a practical factor question:

    If we see a signal today, does the stock have a higher chance of gaining
    >=80% over the next 60 trading days?

The study uses monthly snapshots to reduce overlapping labels.  It reuses the
canonical point-in-time feature panel, then ranks features cross-sectionally
within each snapshot date and measures future 60-trading-day spike probability.
"""

from __future__ import annotations

import math
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[3]
RESEARCH_ROOT = REPO_ROOT / "src" / "quantlib"
STRAT_LAB = RESEARCH_ROOT / "strat_lab"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(RESEARCH_ROOT))
sys.path.insert(0, str(STRAT_LAB))

from quantlib.db import connect  # noqa: E402
from iter_33_pm_first_principles import load_or_build_panel  # noqa: E402


START = date(2012, 1, 3)
OUT_DIR = REPO_ROOT / "src/quantlib/experiments"
DOC_DIR = REPO_ROOT / "docs/strategy_research"


@dataclass(frozen=True)
class FactorSpec:
    name: str
    direction: str
    label: str


def pct(value: float | None, digits: int = 2) -> str:
    if value is None or not math.isfinite(float(value)):
        return "n/a"
    return f"{float(value) * 100:.{digits}f}%"


def num(value: float | None, digits: int = 2) -> str:
    if value is None or not math.isfinite(float(value)):
        return "n/a"
    return f"{float(value):.{digits}f}"


def latest_0050_day() -> date:
    con = connect(read_only=True)
    try:
        return con.sql(
            """
            SELECT MAX(date)
            FROM daily_quote
            WHERE market='twse' AND company_code='0050'
            """
        ).fetchone()[0]
    finally:
        con.close()


def first_trading_day_by_month(days: list[date]) -> list[date]:
    out: list[date] = []
    seen: set[tuple[int, int]] = set()
    for day in days:
        key = (day.year, day.month)
        if key not in seen:
            out.append(day)
            seen.add(key)
    return out


def load_panel(start: date, end: date) -> tuple[pl.DataFrame, list[date]]:
    con = connect(read_only=True)
    try:
        panel, days = load_or_build_panel(con, start, end, use_cache=True)
        value = con.sql(
            f"""
            SELECT q.date, q.company_code,
                   q.closing_price AS raw_close,
                   p.price_to_earning_ratio AS pe,
                   p.price_book_ratio AS pb,
                   p.dividend_yield
            FROM daily_quote q
            LEFT JOIN stock_per_pbr p
              ON p.market = q.market
             AND p.date = q.date
             AND p.company_code = q.company_code
            WHERE q.date BETWEEN DATE '{start}' AND DATE '{end}'
              AND regexp_matches(q.company_code, '^[1-9][0-9]{{3}}$')
            """
        ).pl()
    finally:
        con.close()

    panel = panel.join(value.unique(["date", "company_code"], keep="last"), on=["date", "company_code"], how="left")
    return panel, [d for d in days if start <= d <= end]


def build_labeled_monthly(panel: pl.DataFrame, days: list[date]) -> pl.DataFrame:
    monthly = first_trading_day_by_month(days)
    base = (
        panel.sort(["company_code", "date"])
        .with_columns(
            [
                (pl.col("close") / pl.col("close").shift(20).over("company_code") - 1.0).alias("ret20"),
                (pl.col("close") / pl.col("close").shift(60).over("company_code") - 1.0).alias("ret60"),
                (pl.col("close") / pl.col("close").shift(120).over("company_code") - 1.0).alias("ret120_check"),
                (pl.col("close").shift(-60).over("company_code") / pl.col("close") - 1.0).alias("future_ret_60d"),
                pl.col("close").rolling_min(60).over("company_code").alias("low60"),
                pl.col("close").rolling_max(60).over("company_code").alias("high60"),
                pl.col("close").rolling_mean(60).over("company_code").alias("avg60"),
                pl.col("close").rolling_max(252).over("company_code").alias("high252"),
                (pl.col("vol") / pl.col("vol_avg60")).alias("volume_surge_60d"),
            ]
        )
        .with_columns(
            [
                ((pl.col("high60") - pl.col("low60")) / pl.col("avg60")).alias("pre_breakout_consolidation"),
                ((pl.col("close") - pl.col("low60")) / (pl.col("high60") - pl.col("low60"))).alias("rsv_60d"),
                (pl.col("close") / pl.col("high252")).alias("near_52w_high"),
                (pl.col("future_ret_60d") >= 0.80).fill_null(False).alias("spike60"),
                (pl.col("future_ret_60d") >= 0.20).fill_null(False).alias("gain20_60d"),
            ]
        )
        .filter(pl.col("date").is_in(monthly))
        .filter(
            (~pl.col("is_etf"))
            & (~pl.col("is_finance"))
            & (pl.col("listed_days") >= 252)
            & (pl.col("adv60") >= 30_000_000)
            & (pl.col("close") >= 10)
            & pl.col("future_ret_60d").is_not_null()
        )
    )

    rank_cols = [
        "latest_yoy",
        "yoy_delta",
        "volume_surge_60d",
        "ret20",
        "ret60",
        "ret120",
        "near_52w_high",
        "rsv_60d",
        "pre_breakout_consolidation",
        "inst_flow20",
        "roa_ttm",
        "gross_margin_ttm",
        "f_score_raw",
        "pe",
    ]
    exprs = []
    for col in rank_cols:
        exprs.append((pl.col(col).rank("average").over("date") / pl.col(col).count().over("date")).alias(f"{col}_pct"))
    return base.with_columns(exprs)


def build_current_snapshot(panel: pl.DataFrame) -> pl.DataFrame:
    latest = panel["date"].max()
    base = (
        panel.sort(["company_code", "date"])
        .with_columns(
            [
                (pl.col("close") / pl.col("close").shift(20).over("company_code") - 1.0).alias("ret20"),
                (pl.col("close") / pl.col("close").shift(60).over("company_code") - 1.0).alias("ret60"),
                (pl.col("close") / pl.col("close").shift(120).over("company_code") - 1.0).alias("ret120_check"),
                pl.col("close").rolling_min(60).over("company_code").alias("low60"),
                pl.col("close").rolling_max(60).over("company_code").alias("high60"),
                pl.col("close").rolling_mean(60).over("company_code").alias("avg60"),
                pl.col("close").rolling_max(252).over("company_code").alias("high252"),
                (pl.col("vol") / pl.col("vol_avg60")).alias("volume_surge_60d"),
            ]
        )
        .with_columns(
            [
                ((pl.col("high60") - pl.col("low60")) / pl.col("avg60")).alias("pre_breakout_consolidation"),
                ((pl.col("close") - pl.col("low60")) / (pl.col("high60") - pl.col("low60"))).alias("rsv_60d"),
                (pl.col("close") / pl.col("high252")).alias("near_52w_high"),
            ]
        )
        .filter(pl.col("date") == latest)
        .filter(
            (~pl.col("is_etf"))
            & (~pl.col("is_finance"))
            & (pl.col("listed_days") >= 252)
            & (pl.col("adv60") >= 30_000_000)
            & (pl.col("close") >= 10)
        )
    )

    rank_cols = [
        "latest_yoy",
        "yoy_delta",
        "volume_surge_60d",
        "ret20",
        "ret60",
        "ret120",
        "near_52w_high",
        "rsv_60d",
        "pre_breakout_consolidation",
        "inst_flow20",
        "roa_ttm",
        "gross_margin_ttm",
        "f_score_raw",
        "pe",
    ]
    exprs = []
    for col in rank_cols:
        exprs.append((pl.col(col).rank("average") / pl.col(col).count()).alias(f"{col}_pct"))
    return base.with_columns(exprs)


def stats_for(frame: pl.DataFrame, base_rate: float) -> dict[str, float | int]:
    n = frame.height
    if n == 0:
        return {
            "n": 0,
            "spike_rate": 0.0,
            "lift": 0.0,
            "mean_future_ret_60d": 0.0,
            "median_future_ret_60d": 0.0,
            "gain20_rate": 0.0,
        }
    spike_rate = float(frame["spike60"].mean())
    return {
        "n": n,
        "spike_rate": spike_rate,
        "lift": spike_rate / base_rate if base_rate > 0 else 0.0,
        "mean_future_ret_60d": float(frame["future_ret_60d"].mean()),
        "median_future_ret_60d": float(frame["future_ret_60d"].median()),
        "gain20_rate": float(frame["gain20_60d"].mean()),
    }


def single_factor_table(labeled: pl.DataFrame, base_rate: float) -> pl.DataFrame:
    specs = [
        FactorSpec("latest_yoy", "high", "月營收 YoY 高"),
        FactorSpec("yoy_delta", "high", "月營收加速度高"),
        FactorSpec("volume_surge_60d", "high", "成交量突然放大"),
        FactorSpec("ret20", "high", "20 日動能強"),
        FactorSpec("ret60", "high", "60 日動能強"),
        FactorSpec("ret120", "high", "120 日動能強"),
        FactorSpec("near_52w_high", "high", "接近 52 週高點"),
        FactorSpec("rsv_60d", "high", "站在 60 日區間高位"),
        FactorSpec("pre_breakout_consolidation", "low", "60 日整理區間窄"),
        FactorSpec("inst_flow20", "high", "法人 20 日買超強"),
        FactorSpec("roa_ttm", "high", "ROA 高"),
        FactorSpec("gross_margin_ttm", "high", "毛利率高"),
        FactorSpec("f_score_raw", "high", "Piotroski F-score 高"),
        FactorSpec("pe", "low", "PE 低"),
    ]
    rows: list[dict[str, object]] = []
    for spec in specs:
        pct_col = f"{spec.name}_pct"
        if pct_col not in labeled.columns:
            continue
        if spec.direction == "high":
            top10 = labeled.filter(pl.col(pct_col) >= 0.90)
            top20 = labeled.filter(pl.col(pct_col) >= 0.80)
        else:
            top10 = labeled.filter(pl.col(pct_col) <= 0.10)
            top20 = labeled.filter(pl.col(pct_col) <= 0.20)
        row = {
            "factor": spec.name,
            "label": spec.label,
            "direction": spec.direction,
            **{f"top10_{k}": v for k, v in stats_for(top10, base_rate).items()},
            **{f"top20_{k}": v for k, v in stats_for(top20, base_rate).items()},
        }
        rows.append(row)
    return pl.DataFrame(rows).sort(["top10_lift", "top20_lift"], descending=[True, True])


def combo_table(labeled: pl.DataFrame, base_rate: float) -> pl.DataFrame:
    combo_exprs: list[tuple[str, str, pl.Expr]] = [
        (
            "momentum_new_high",
            "近高點 + 60日動能前20% + 60日RSV高",
            (pl.col("near_52w_high") >= 0.90) & (pl.col("ret60_pct") >= 0.80) & (pl.col("rsv_60d") >= 0.80),
        ),
        (
            "revenue_momentum",
            "月營收YoY>30 + 加速度正 + 60日動能前20% + 接近高點",
            (pl.col("latest_yoy") >= 30)
            & (pl.col("yoy_delta").fill_null(-999) > 0)
            & (pl.col("ret60_pct") >= 0.80)
            & (pl.col("near_52w_high") >= 0.80),
        ),
        (
            "volume_breakout",
            "量能>1.5倍 + 20日漲幅>10% + 接近高點",
            (pl.col("volume_surge_60d") >= 1.50) & (pl.col("ret20") >= 0.10) & (pl.col("near_52w_high") >= 0.90),
        ),
        (
            "quality_growth_breakout",
            "月營收YoY>20 + ROA>8% + F-score>=4 + 120日動能前20%",
            (pl.col("latest_yoy") >= 20)
            & (pl.col("roa_ttm") >= 0.08)
            & (pl.col("f_score_raw") >= 4)
            & (pl.col("ret120_pct") >= 0.80),
        ),
        (
            "cheap_growth_momentum",
            "PE<20 + 月營收YoY>20 + 60日動能>10%",
            (pl.col("pe") > 0) & (pl.col("pe") < 20) & (pl.col("latest_yoy") >= 20) & (pl.col("ret60") >= 0.10),
        ),
        (
            "institutional_breakout",
            "法人流前20% + 接近高點 + 60日動能前20%",
            (pl.col("inst_flow20_pct") >= 0.80) & (pl.col("near_52w_high") >= 0.90) & (pl.col("ret60_pct") >= 0.80),
        ),
    ]
    rows: list[dict[str, object]] = []
    for name, description, expr in combo_exprs:
        selected = labeled.filter(expr)
        rows.append({"combo": name, "description": description, **stats_for(selected, base_rate)})
    return pl.DataFrame(rows).sort(["lift", "spike_rate", "mean_future_ret_60d"], descending=[True, True, True])


def latest_candidates(labeled: pl.DataFrame) -> pl.DataFrame:
    latest = labeled["date"].max()
    if latest is None:
        return pl.DataFrame()
    frame = labeled.filter(pl.col("date") == latest).with_columns(
        (
            0.28 * pl.col("near_52w_high_pct").fill_null(0.0)
            + 0.24 * pl.col("ret60_pct").fill_null(0.0)
            + 0.18 * pl.col("volume_surge_60d_pct").fill_null(0.0)
            + 0.14 * pl.col("latest_yoy_pct").fill_null(0.0)
            + 0.10 * pl.col("inst_flow20_pct").fill_null(0.0)
            + 0.06 * pl.col("roa_ttm_pct").fill_null(0.0)
        ).alias("spike_setup_score")
    )
    return (
        frame.sort("spike_setup_score", descending=True)
        .select(
            [
                "date",
                "company_code",
                "close",
                "adv60",
                "latest_yoy",
                "yoy_delta",
                "ret20",
                "ret60",
                "near_52w_high",
                "volume_surge_60d",
                "inst_flow20",
                "roa_ttm",
                "pe",
                "spike_setup_score",
            ]
        )
        .head(30)
    )


def markdown_report(
    *,
    start: date,
    end: date,
    labeled: pl.DataFrame,
    single: pl.DataFrame,
    combos: pl.DataFrame,
    latest: pl.DataFrame,
) -> str:
    base_rate = float(labeled["spike60"].mean())
    lines = [
        "# 暴漲股因子研究：60 日 +80% 事件",
        "",
        f"資料截止：價格 / 籌碼至 `{end}`；月營收至本地 cache 最新可用月份。",
        f"研究窗口：`{start}` - `{end}`。",
        "",
        "## 方法",
        "",
        "- Spike 定義：未來 60 個交易日 total-return adjusted close 漲幅 >= 80%。",
        "- 測試頻率：每月第一個交易日做一次全市場截面觀察，避免每日樣本高度重疊。",
        "- Universe：排除 ETF / 金融股 / 掛牌未滿 252 交易日 / 60 日均成交值低於 3,000 萬 / 股價低於 10 元。",
        "- 因子全部使用當下可見資料，不使用未來資訊。",
        "",
        "## 基準機率",
        "",
        f"- 月頻有效樣本數：`{labeled.height:,}`。",
        f"- 基準 spike 機率：`{pct(base_rate, 3)}`。",
        "",
        "## 單因子排序",
        "",
        "| 因子 | Top 10% spike率 | Lift | Top 10% 60日均報酬 | Top 20% spike率 | Top 20% Lift |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in single.head(10).iter_rows(named=True):
        lines.append(
            f"| {row['label']} | {pct(row['top10_spike_rate'], 3)} | {num(row['top10_lift'], 2)}x | "
            f"{pct(row['top10_mean_future_ret_60d'], 2)} | {pct(row['top20_spike_rate'], 3)} | {num(row['top20_lift'], 2)}x |"
        )
    lines.extend(
        [
            "",
            "## 組合訊號",
            "",
            "| 組合 | 條件 | 樣本數 | spike率 | Lift | 60日均報酬 | 60日 +20% 機率 |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in combos.iter_rows(named=True):
        lines.append(
            f"| `{row['combo']}` | {row['description']} | {int(row['n']):,} | {pct(row['spike_rate'], 3)} | "
            f"{num(row['lift'], 2)}x | {pct(row['mean_future_ret_60d'], 2)} | {pct(row['gain20_rate'], 2)} |"
        )
    lines.extend(
        [
            "",
            "## 最新截面高分名單",
            "",
            "這不是買進建議，只是把歷史 spike setup score 最高的股票列出，供後續人工研究新聞、產業與籌碼原因。",
            "",
            "| 日期 | 股票 | 收盤 | 月營收YoY | YoY加速度 | 20日報酬 | 60日報酬 | 近52週高點 | 量能倍率 | PE | score |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in latest.head(15).iter_rows(named=True):
        lines.append(
            f"| {row['date']} | {row['company_code']} | {num(row['close'], 2)} | {pct((row['latest_yoy'] or 0)/100, 2)} | "
            f"{pct((row['yoy_delta'] or 0)/100, 2)} | {pct(row['ret20'], 2)} | {pct(row['ret60'], 2)} | "
            f"{pct(row['near_52w_high'], 2)} | {num(row['volume_surge_60d'], 2)} | {num(row['pe'], 1)} | {num(row['spike_setup_score'], 3)} |"
        )
    lines.extend(
        [
            "",
            "## 初步結論",
            "",
            "大漲股不是單一因子可以解釋。最有用的方向通常是「已經接近新高 / 中短期動能強 / 量能放大 / 基本面或月營收沒有背離」。",
            "這類訊號比較適合做候選池與人工研究入口；是否能直接買進，仍需要用下一階段 target-book 回測驗證進出場、停損與持有規則。",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    t0 = time.time()
    end = latest_0050_day()
    start = START
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DOC_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[spike-factor] load panel {start} -> {end}", flush=True)
    panel, days = load_panel(start, end)
    print(f"[spike-factor] panel rows={panel.height:,} days={len(days):,}", flush=True)
    labeled = build_labeled_monthly(panel, days)
    current = build_current_snapshot(panel)
    base_rate = float(labeled["spike60"].mean())
    print(f"[spike-factor] labeled monthly rows={labeled.height:,} base_spike_rate={base_rate:.4%}", flush=True)

    single = single_factor_table(labeled, base_rate)
    combos = combo_table(labeled, base_rate)
    latest = latest_candidates(current)

    labeled_path = OUT_DIR / "spike_factor_monthly_labeled.parquet"
    single_path = OUT_DIR / "spike_factor_single_factors.csv"
    combo_path = OUT_DIR / "spike_factor_combos.csv"
    latest_path = OUT_DIR / "spike_factor_latest_candidates.csv"
    report_path = DOC_DIR / "spike_factor_study.md"

    labeled.write_parquet(labeled_path)
    single.write_csv(single_path)
    combos.write_csv(combo_path)
    latest.write_csv(latest_path)
    report_path.write_text(
        markdown_report(start=start, end=end, labeled=labeled, single=single, combos=combos, latest=latest)
    )

    print("\n=== Single factors ===")
    with pl.Config(tbl_rows=20, tbl_width_chars=160):
        print(single.select([
            "label",
            pl.col("top10_spike_rate").mul(100).round(3).alias("top10_spike_pct"),
            pl.col("top10_lift").round(2).alias("top10_lift"),
            pl.col("top10_mean_future_ret_60d").mul(100).round(2).alias("top10_mean_ret_pct"),
            pl.col("top20_spike_rate").mul(100).round(3).alias("top20_spike_pct"),
            pl.col("top20_lift").round(2).alias("top20_lift"),
        ]))
    print("\n=== Combos ===")
    with pl.Config(tbl_rows=20, tbl_width_chars=180):
        print(combos.select([
            "combo",
            "description",
            "n",
            pl.col("spike_rate").mul(100).round(3).alias("spike_pct"),
            pl.col("lift").round(2),
            pl.col("mean_future_ret_60d").mul(100).round(2).alias("mean_ret_pct"),
            pl.col("gain20_rate").mul(100).round(2).alias("gain20_pct"),
        ]))
    print("\n=== Latest candidates ===")
    with pl.Config(tbl_rows=15, tbl_width_chars=180):
        print(latest.head(15))
    print(f"\n[spike-factor] wrote {single_path}, {combo_path}, {latest_path}, {report_path}")
    print(f"[spike-factor] runtime {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
