"""Industry-first Serenity-style thesis replay from 2025 onward.

This is the backtestable version of the qualitative SOP:

1. Human / industry research defines a point-in-time thesis registry.
2. The backtest may only pick from registry rows active on each signal date.
3. The program acts as a double-check layer: revenue, momentum, liquidity,
   valuation, fund flow, and drawdown controls.

This is intentionally different from `serenity_style_replay_2025.py`, which
scans the full market first and then asks whether the winners look like
structural bottlenecks.
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl
from quantlib import paths

REPO_ROOT = Path(__file__).resolve().parents[3]
RESEARCH_ROOT = REPO_ROOT / "src" / "quantlib"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(RESEARCH_ROOT))

import empyrical as ep  # noqa: E402  Sharpe/Sortino 學理正解基準(2026-07-23 稽核)

from quantlib.constants import CAPITAL, COMMISSION, SELL_TAX, TDPY  # noqa: E402
from quantlib.db import connect  # noqa: E402
from quantlib.prices import fetch_adjusted_panel, total_return_series  # noqa: E402


RESULTS = paths.OUT_STRAT_LAB
DOCS = REPO_ROOT / "docs" / "serenity"
REGISTRY = Path(__file__).parent.joinpath("registry", "thesis_registry_2025.csv")
OUT_PREFIX = "serenity_industry_first_replay_2025"


@dataclass(frozen=True)
class Variant:
    name: str
    top_n: int
    weighting: str
    strict: bool = False
    theme_cap: int | None = None


VARIANTS = (
    Variant("sop_top10_equal", 10, "equal"),
    Variant("sop_top10_score_weighted", 10, "score"),
    Variant("sop_top15_equal", 15, "equal"),
    Variant("sop_top15_theme3_equal", 15, "equal", theme_cap=3),
    Variant("sop_top10_strict_equal", 10, "equal", strict=True),
)


def month_add(year: int, month: int, delta: int = 1) -> tuple[int, int]:
    month0 = year * 12 + (month - 1) + delta
    return month0 // 12, month0 % 12 + 1


def revenue_report_date(year: int, month: int) -> date:
    report_year, report_month = month_add(year, month, 1)
    return date(report_year, report_month, 10)


def first_trading_day_on_or_after(days: list[date], target: date) -> date | None:
    for day in days:
        if day >= target:
            return day
    return None


def build_rebalance_pairs(days: list[date], start: date, end: date) -> list[tuple[date, date]]:
    pairs: list[tuple[date, date]] = []
    year, month = start.year, start.month
    while date(year, month, 1) <= end:
        signal = first_trading_day_on_or_after(days, date(year, month, 11))
        if signal is not None and signal >= start and signal < end:
            idx = days.index(signal)
            if idx + 1 < len(days):
                pairs.append((signal, days[idx + 1]))
        year, month = month_add(year, month, 1)
    return pairs


def load_registry(path: Path = REGISTRY) -> pd.DataFrame:
    registry = pd.read_csv(path, dtype={"company_code": str})
    registry["company_code"] = registry["company_code"].astype(str).str.zfill(4)
    registry["active_from"] = pd.to_datetime(registry["active_from"]).map(lambda value: value.date())
    registry["active_until"] = pd.to_datetime(registry["active_until"], errors="coerce").map(
        lambda value: value.date() if pd.notna(value) else None
    )
    registry["conviction"] = pd.to_numeric(registry["conviction"], errors="coerce").fillna(3.0)
    if "role" not in registry:
        registry["role"] = ""  # battle 18: role axis(僅 wf registry 帶 role 欄)
    return registry


def active_registry_for_day(registry: pd.DataFrame, day: date) -> pd.DataFrame:
    active = registry[
        (registry["active_from"] <= day)
        & (registry["active_until"].isna() | (registry["active_until"] >= day))
    ].copy()
    if active.empty:
        return active

    grouped = (
        active.sort_values(["company_code", "conviction"], ascending=[True, False])
        .groupby("company_code", as_index=False)
        .agg(
            theme_id=("theme_id", "first"),
            theme_name=("theme_name", "first"),
            bottleneck_layer=("bottleneck_layer", "first"),
            conviction=("conviction", "max"),
            theme_count=("theme_id", "nunique"),
            first_active_from=("active_from", "min"),
            role=("role", "first"),
        )
    )
    return grouped


def load_universe(con, codes: list[str]) -> pd.DataFrame:
    codes_sql = ",".join(f"'{code}'" for code in sorted(set(codes)))
    frame = con.sql(
        f"""
        WITH latest_tax AS (
            SELECT market, company_code, company_name, industry, broad_sector,
                   is_financial, is_special_category
            FROM industry_taxonomy_pit
            WHERE company_code IN ({codes_sql})
            QUALIFY row_number() OVER (
                PARTITION BY company_code
                ORDER BY effective_date DESC NULLS LAST, source_ym DESC NULLS LAST
            ) = 1
        ),
        latest_px AS (
            SELECT market, company_code, closing_price
            FROM daily_quote
            WHERE date = (SELECT max(date) FROM daily_quote)
              AND company_code IN ({codes_sql})
              AND closing_price > 0
        )
        SELECT t.*
        FROM latest_tax t
        JOIN latest_px p USING (market, company_code)
        WHERE t.is_financial = false
          AND t.is_special_category = false
        """
    ).pl()
    return frame.with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4)).to_pandas()


def load_price_features(con, universe: pd.DataFrame, start: date, end: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    panels: list[pl.DataFrame] = []
    for market in ("twse", "tpex"):
        codes = universe.loc[universe["market"] == market, "company_code"].tolist()
        if not codes:
            continue
        panels.append(
            fetch_adjusted_panel(
                con,
                start.isoformat(),
                end.isoformat(),
                codes=codes,
                market=market,
                include_extra_history_days=320,
            )
        )
    if not panels:
        raise RuntimeError("No price panel for thesis registry universe.")
    panel = (
        pl.concat(panels, how="diagonal")
        .with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4))
        .sort(["company_code", "date"])
    )
    featured = (
        panel.with_columns(
            [
                (pl.col("close") / pl.col("close").shift(1).over("company_code") - 1.0).alias("ret_1d"),
                (pl.col("close") / pl.col("close").shift(20).over("company_code") - 1.0).alias("ret_20d"),
                (pl.col("close") / pl.col("close").shift(60).over("company_code") - 1.0).alias("ret_60d"),
                (pl.col("close") / pl.col("close").shift(120).over("company_code") - 1.0).alias("ret_120d"),
                (pl.col("close") / pl.col("close").shift(252).over("company_code") - 1.0).alias("ret_252d"),
                pl.col("trade_value").rolling_mean(20).over("company_code").shift(1).alias("adv20"),
                (pl.col("close") / pl.col("close").rolling_max(252).over("company_code") - 1.0).alias("drawdown_252"),
                # 命名誠實(2026-07-23 稽核 D-technical):這是 **20 日均幅/收盤**,不是
                # Wilder ATR——缺 True Range 的 |H−C_prev|/|L−C_prev| 跳空兩項、非 Wilder
                # 平滑、期數 20 非 14,有隔夜跳空的股票會低估真實波動。僅非 champion 變體
                # ev_v2_watr(weight_mode=inv_atr)用它做反波動加權;現役 ev_v3_wf=equal
                # 完全不觸及。要真 ATR 應接 True Range + Wilder 平滑;此處作均幅代理故留。
                (
                    (pl.col("high") - pl.col("low")).rolling_mean(20).over("company_code").shift(1)
                    / pl.col("close")
                ).alias("atr20_pct"),
            ]
        )
        .filter(pl.col("date") >= pl.lit(start).cast(pl.Date))
        .select(
            [
                "date",
                "company_code",
                "close",
                "raw_close",
                "ret_1d",
                "ret_20d",
                "ret_60d",
                "ret_120d",
                "ret_252d",
                "adv20",
                "drawdown_252",
                "atr20_pct",
            ]
        )
    )
    daily_returns = featured.select(["date", "company_code", "ret_1d"]).to_pandas()
    return featured.to_pandas(), daily_returns


def load_revenue_features(con, codes: list[str] | None = None) -> pd.DataFrame:
    # 效能(2026-07-17):可按池過濾;None = 全市場(mechanical mode 用)
    code_filter = ""
    if codes:
        codes_sql = ",".join(f"'{c}'" for c in sorted(set(codes)))
        code_filter = f" AND company_code IN ({codes_sql})"
    rev = (
        con.sql(
            f"""
            SELECT company_code, year, month, type, monthly_revenue_yoy
            FROM operating_revenue
            WHERE regexp_matches(company_code, '^[0-9]{{4}}$'){code_filter}
            """
        )
        .pl()
        .with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4))
        .to_pandas()
    )
    rev["_type_priority"] = np.where(rev["type"] == "consolidated", 0, 1)
    rev = (
        rev.sort_values(["company_code", "year", "month", "_type_priority"])
        .drop_duplicates(["company_code", "year", "month"], keep="first")
        .drop(columns=["_type_priority"])
    )
    rev = rev.sort_values(["company_code", "year", "month"]).copy()
    rev["report_date"] = [revenue_report_date(int(y), int(m)) for y, m in zip(rev["year"], rev["month"])]
    rev["yoy_3m"] = rev.groupby("company_code")["monthly_revenue_yoy"].transform(
        lambda s: s.rolling(3, min_periods=2).mean()
    )
    prev3 = rev.groupby("company_code")["monthly_revenue_yoy"].transform(
        lambda s: s.shift(3).rolling(3, min_periods=2).mean()
    )
    rev["yoy_accel"] = rev["yoy_3m"] - prev3
    return rev


def load_point_in_time_table(con, table: str, fields: list[str], date_field: str = "date",
                             codes: list[str] | None = None) -> pd.DataFrame:
    select_fields = ", ".join(["company_code", date_field, *fields])
    where = ""
    if codes:
        codes_sql = ",".join(f"'{c}'" for c in sorted(set(codes)))
        where = f" WHERE company_code IN ({codes_sql})"
    frame = con.sql(f"SELECT {select_fields} FROM {table}{where}").pl()
    return frame.with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4)).to_pandas()


def load_taxonomy(con, codes: list[str]) -> pd.DataFrame:
    codes_sql = ",".join(f"'{code}'" for code in sorted(set(codes)))
    frame = con.sql(
        f"""
        SELECT company_code, company_name, effective_date, industry, broad_sector,
               is_financial, is_special_category
        FROM industry_taxonomy_pit
        WHERE company_code IN ({codes_sql})
        """
    ).pl()
    return frame.with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4)).to_pandas()


_RLB_CACHE: dict[int, tuple] = {}


def row_latest_before(df: pd.DataFrame, day: date, date_col: str) -> pd.DataFrame:
    """As-of 篩選:每 company_code 取 date_col ≤ day 的最新一列。

    效能(2026-07-17):原版每呼叫全表 copy+轉型+排序(refresh 迴圈 × 多表 = 主要
    熱點);現以 id(df) 快取「已轉型、按日全域穩定排序」版本,每呼叫僅
    searchsorted 切片 + drop_duplicates(keep='last')——語義等價(穩定排序保證
    組內日序;同日多列取原順序最後,與原版 tie 行為一致級)。"""
    key = (id(df), date_col)
    cached = _RLB_CACHE.get(key)
    if cached is None or cached[2] is not df:
        work = df.copy()
        work[date_col] = pd.to_datetime(work[date_col]).dt.date
        work = work.sort_values(date_col, kind="stable").reset_index(drop=True)
        _RLB_CACHE[key] = (work, work[date_col].to_numpy(), df)
        cached = _RLB_CACHE[key]
    work, dates, _ = cached
    hi = int(np.searchsorted(dates, day, side="right"))
    return work.iloc[:hi].drop_duplicates("company_code", keep="last")


_ABLATE: set = set()
_PE_PEN_MODE: str = "full"


def set_ablate(components) -> None:
    """Battle 11: disable individual score components / soft filters for ablation.

    Valid keys: conviction | theme_count | revenue | momentum | adv | inst |
    pe_pen | pb_pen | dd_pen | filters. Empty set = production scoring (baseline).
    """
    global _ABLATE
    _ABLATE = {c.strip() for c in (components or []) if c and c.strip()}


def set_pe_pen_mode(mode: str) -> None:
    """Battle 13: PE-penalty schedule. 'full' (legacy) | 'extreme' (only PE>180
    penalty + PE<25 bonus, drops the mid >80/>120 penalties that suppressed
    high-PE momentum winners) | 'off'."""
    global _PE_PEN_MODE
    _PE_PEN_MODE = mode or "full"


# Battle 18: new score axes (default 0 = production behaviour unchanged).
_ROLE_BONUS: float = 0.0
_FRESH_BONUS: float = 0.0
_FRESH_MONTHS: int = 6


def set_role_bonus(value: float) -> None:
    """Battle 18: score bonus for chokepoint_owner members (role axis)."""
    global _ROLE_BONUS
    _ROLE_BONUS = float(value or 0.0)


def set_fresh(bonus: float, months: int) -> None:
    """Battle 18: theme-freshness bonus for members whose theme was admitted
    within `months` of the refresh day (theme lifecycle axis, from backfill)."""
    global _FRESH_BONUS, _FRESH_MONTHS
    _FRESH_BONUS = float(bonus or 0.0)
    _FRESH_MONTHS = int(months or 6)


_CONV_W: float = 8.0


def set_conv_weight(value: float) -> None:
    """Battle 18 round 2: conviction score weight (production default 8.0)."""
    global _CONV_W
    _CONV_W = float(value if value is not None else 8.0)


def score_candidates(frame: pd.DataFrame) -> pd.DataFrame:
    data = frame.copy()
    for col in (
        "conviction",
        "theme_count",
        "monthly_revenue_yoy",
        "yoy_3m",
        "yoy_accel",
        "ret_20d",
        "ret_60d",
        "ret_252d",
        "drawdown_252",
        "adv20",
        "price_to_earning_ratio",
        "price_book_ratio",
        "inst_20d",
    ):
        if col not in data:
            data[col] = np.nan
        data[col] = pd.to_numeric(data[col], errors="coerce")

    # Basic tradability floor is always on (else NAV sim breaks on illiquid/penny).
    keep = (data["adv20"] >= 50_000_000) & (data["raw_close"] >= 20)
    if "filters" not in _ABLATE:
        # Soft guardrail filters (momentum floors + valuation caps) — ablatable.
        keep &= (
            (data["ret_60d"] >= -0.35)
            & (data["ret_252d"] >= -0.35)
            & (data["drawdown_252"] >= -0.55)
            & (data["price_to_earning_ratio"].isna() | (data["price_to_earning_ratio"] <= 250))
            & (data["price_book_ratio"].isna() | (data["price_book_ratio"] <= 45))
        )
    data = data[keep].copy()
    if data.empty:
        return data.assign(score=[])

    score = pd.Series(0.0, index=data.index)
    if "conviction" not in _ABLATE:
        score += data["conviction"].fillna(3.0) * _CONV_W
    # battle 12 (2026-07-07): theme_count removed — dead weight (ablation 0/3,
    # identical picks; most names are single-theme so the term never breaks ties).
    if "revenue" not in _ABLATE:
        score += data["monthly_revenue_yoy"].clip(-40, 160).fillna(0.0) * 0.12
        score += data["yoy_3m"].clip(-40, 130).fillna(0.0) * 0.08
        score += data["yoy_accel"].clip(-60, 90).fillna(0.0) * 0.05
    if "momentum" not in _ABLATE:
        score += data["ret_60d"].clip(-0.5, 1.8).fillna(0.0) * 14.0
        score += data["ret_20d"].clip(-0.35, 0.9).fillna(0.0) * 5.0
        score += data["ret_252d"].clip(-0.8, 3.2).fillna(0.0) * 3.0
    if "adv" not in _ABLATE:
        score += (np.log10(data["adv20"].clip(lower=1.0)) - 8.0).clip(upper=3.0) * 3.0
    if "inst" not in _ABLATE:
        score += (data["inst_20d"].fillna(0.0) / 1_000 / 10_000).clip(-3.0, 3.0) * 0.9

    if "pe_pen" not in _ABLATE and _PE_PEN_MODE != "off":
        pe = data["price_to_earning_ratio"]
        if _PE_PEN_MODE == "extreme":
            # Battle 13: keep only the loss-making flag, extreme-PE guard, and
            # the low-PE value bonus; drop mid-range >80/>120 penalties.
            score += np.select(
                [pe.isna() | (pe <= 0), pe > 180, pe < 25],
                [-4.0, -6.0, 2.0],
                default=0.0,
            )
        else:  # "full" (legacy)
            score += np.select(
                [pe.isna() | (pe <= 0), pe > 180, pe > 120, pe > 80, pe < 25],
                [-4.0, -9.0, -6.0, -3.0, 2.0],
                default=0.0,
            )
    if "pb_pen" not in _ABLATE:
        pb = data["price_book_ratio"]
        score += np.select([pb > 35, pb > 25], [-6.0, -3.0], default=0.0)
    # battle 12 (2026-07-07): dd_pen removed — dead weight (ablation 0/3, lag0
    # even improved 248→252.5); the -35%/-55% soft filters already exclude
    # deep-drawdown names, so the extra penalty never bit on survivors.
    # battle 18: new axes — default 0 keeps production scoring bit-identical.
    if _ROLE_BONUS and "role" in data:
        score += (data["role"] == "chokepoint_owner").astype(float) * _ROLE_BONUS
    if _FRESH_BONUS and "theme_age_days" in data:
        fresh = pd.to_numeric(data["theme_age_days"], errors="coerce")
        score += (fresh <= _FRESH_MONTHS * 30).fillna(False).astype(float) * _FRESH_BONUS
    data["score"] = score
    # battle 10 (iter_87): expectations-gap unit in [-1, 1] — rank(yoy_3m) - rank(PE)
    # within surviving candidates. Pure extra column; base score is unchanged.
    data["gap_unit"] = (
        data["yoy_3m"].rank(pct=True).fillna(0.5)
        - data["price_to_earning_ratio"].rank(pct=True).fillna(0.5)
    )
    return data.sort_values("score", ascending=False)


def select_variant_picks(scored: pd.DataFrame, variant: Variant) -> pd.DataFrame:
    picks = scored.copy()
    if variant.strict:
        picks = picks[
            (picks[["monthly_revenue_yoy", "yoy_3m"]].max(axis=1) >= 15)
            & (picks["ret_60d"] >= 0.0)
            & (picks["adv20"] >= 80_000_000)
            & (picks["price_to_earning_ratio"].isna() | (picks["price_to_earning_ratio"] <= 180))
        ].copy()
    if variant.theme_cap is not None and not picks.empty:
        picks = (
            picks.sort_values("score", ascending=False)
            .groupby("theme_id", as_index=False, group_keys=False)
            .head(variant.theme_cap)
            .sort_values("score", ascending=False)
        )
    return picks.head(variant.top_n).copy()


def target_weights(scored: pd.DataFrame, variant: Variant) -> dict[str, float]:
    picks = select_variant_picks(scored, variant)
    if picks.empty:
        return {}
    if variant.weighting == "equal":
        return {code: 1.0 / len(picks) for code in picks["company_code"]}
    raw = picks["score"].clip(lower=0.0)
    if float(raw.sum()) <= 0:
        return {code: 1.0 / len(picks) for code in picks["company_code"]}
    weights = raw / raw.sum()
    weights = weights.clip(upper=0.20)
    weights = weights / weights.sum()
    return {code: float(weight) for code, weight in zip(picks["company_code"], weights)}


def cagr(start_nav: float, end_nav: float, days: int) -> float:
    if start_nav <= 0 or end_nav <= 0 or days <= 0:
        return float("nan")
    return (end_nav / start_nav) ** (365.25 / days) - 1.0


def max_drawdown(nav: pd.Series) -> float:
    peak = nav.cummax()
    return float((nav / peak - 1.0).min())


def sortino_ratio(returns: pd.Series) -> float:
    # empyrical 學理正解(2026-07-23 稽核 D-metrics 修):舊版下行差用「只取負報酬對
    # 自身均值 ddof=1 std」,而 Sortino & Price (1994) 是 sqrt(mean(min(r−MAR,0)²)) 對
    # **全期 N** 取平均、以 MAR 為錨——舊法系統性把 live Sortino 灌高。rf=0(CLAUDE.md
    # 定調台股 rf≈0,與本檔 summarize_nav 的 Sharpe 同慣例)。
    val = ep.sortino_ratio(returns, required_return=0.0, annualization=TDPY)
    return float(val) if np.isfinite(val) else float("nan")


def recent_cagr(daily: pd.DataFrame, days_back: int) -> tuple[float, str]:
    end = pd.to_datetime(daily["date"].iloc[-1]).date()
    anchor = end - timedelta(days=days_back)
    start_rows = daily[pd.to_datetime(daily["date"]).dt.date <= anchor]
    if start_rows.empty:
        start = pd.to_datetime(daily["date"].iloc[0]).date()
        return cagr(CAPITAL, float(daily["nav"].iloc[-1]), (end - start).days), f"{start}~{end}"
    start_row = start_rows.tail(1).iloc[0]
    start = pd.to_datetime(start_row["date"]).date()
    return cagr(float(start_row["nav"]), float(daily["nav"].iloc[-1]), (end - start).days), f"{start}~{end}"


def summarize_nav(name: str, daily: pd.DataFrame, total_turnover: float, rebalances: int) -> dict[str, object]:
    ordered = daily.sort_values("date").reset_index(drop=True)
    start = pd.to_datetime(ordered["date"].iloc[0]).date()
    end = pd.to_datetime(ordered["date"].iloc[-1]).date()
    returns = ordered["nav"].pct_change().fillna(0.0)
    sharpe = float(np.sqrt(TDPY) * returns.mean() / returns.std()) if returns.std() > 0 else float("nan")
    r1y, r1y_window = recent_cagr(ordered, 365)
    return {
        "name": name,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "days": len(ordered),
        "cagr": cagr(float(ordered["nav"].iloc[0]), float(ordered["nav"].iloc[-1]), (end - start).days),
        "recent_1y_cagr": r1y,
        "recent_1y_window": r1y_window,
        "sharpe": sharpe,
        "sortino": sortino_ratio(returns),
        "mdd": max_drawdown(ordered["nav"]),
        "calmar": cagr(float(ordered["nav"].iloc[0]), float(ordered["nav"].iloc[-1]), (end - start).days)
        / abs(max_drawdown(ordered["nav"])),
        "final_nav": float(ordered["nav"].iloc[-1]),
        "total_turnover": total_turnover,
        "avg_rebalance_turnover": total_turnover / max(rebalances, 1),
        "rebalances": rebalances,
        "avg_active": float(ordered.get("active", pd.Series(dtype=float)).mean())
        if "active" in ordered
        else float("nan"),
        "max_active": int(ordered["active"].max()) if "active" in ordered else 1,
    }


def simulate_variant(
    variant: Variant,
    trading_days: list[date],
    rebalance_targets: dict[date, dict[str, float]],
    returns_by_day: dict[date, dict[str, float]],
) -> tuple[pd.DataFrame, float]:
    nav = CAPITAL
    current: dict[str, float] = {}
    rows: list[dict[str, object]] = []
    total_turnover = 0.0
    fee_buy = COMMISSION + 0.0005
    fee_sell = COMMISSION + SELL_TAX + 0.0005

    for day in trading_days:
        if day in rebalance_targets:
            target = rebalance_targets[day]
            keys = set(current) | set(target)
            buys = sum(max(target.get(code, 0.0) - current.get(code, 0.0), 0.0) for code in keys)
            sells = sum(max(current.get(code, 0.0) - target.get(code, 0.0), 0.0) for code in keys)
            turnover = buys + sells
            nav *= 1.0 - (buys * fee_buy + sells * fee_sell)
            total_turnover += turnover
            current = dict(target)
        day_rets = returns_by_day.get(day, {})
        port_ret = sum(weight * day_rets.get(code, 0.0) for code, weight in current.items())
        nav *= 1.0 + port_ret
        rows.append({"date": day, "nav": nav, "active": len(current)})
    return pd.DataFrame(rows), total_turnover


def benchmark_nav(con, code: str, market: str, start: date, end: date, name: str) -> pd.DataFrame:
    series = total_return_series(con, code, start.isoformat(), end.isoformat(), market=market).to_pandas()
    series = series.sort_values("date").reset_index(drop=True)
    series["nav"] = CAPITAL * series["adj_close"] / float(series["adj_close"].iloc[0])
    return series[["date", "nav"]].assign(name=name)


def plot_outputs(summary: pd.DataFrame, daily_paths: dict[str, Path], out_prefix: str) -> Path:
    top_names = summary.head(5)["name"].tolist()
    fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    for name in top_names:
        daily = pd.read_csv(daily_paths[name], parse_dates=["date"])
        axes[0].plot(daily["date"], daily["nav"] / daily["nav"].iloc[0], label=name)
        dd = daily["nav"] / daily["nav"].cummax() - 1.0
        axes[1].plot(daily["date"], dd, label=name)
    axes[0].set_title("Serenity Industry-First SOP Replay: NAV")
    axes[0].set_ylabel("Growth of NT$1")
    axes[0].grid(True, alpha=0.3)
    axes[0].legend(loc="best", fontsize=8)
    axes[1].set_title("Drawdown")
    axes[1].set_ylabel("Drawdown")
    axes[1].grid(True, alpha=0.3)
    axes[1].yaxis.set_major_formatter(lambda x, _pos: f"{x:.0%}")
    fig.tight_layout()
    out = RESULTS / f"{out_prefix}_overview.png"
    fig.savefig(out, dpi=160)
    plt.close(fig)
    return out


def write_report(
    summary: pd.DataFrame,
    picks: pd.DataFrame,
    chart_path: Path,
    cutoff: date,
    pairs: int,
    out_prefix: str,
    activation_lag_days: int,
) -> Path:
    report = DOCS / f"{out_prefix}.md"
    chart_rel = os.path.relpath(chart_path, report.parent)
    latest_signal = picks["signal_date"].max() if not picks.empty else None
    latest = picks[picks["signal_date"] == latest_signal].head(20) if latest_signal is not None else pd.DataFrame()

    display = summary.copy()
    for col in ("cagr", "recent_1y_cagr", "sharpe", "sortino", "mdd", "calmar"):
        display[col] = pd.to_numeric(display[col], errors="coerce")

    lines = [
        "# Serenity Industry-First SOP 回測 2025-至今",
        "",
        f"- 資料 cutoff：`{cutoff}`",
        f"- 回測重平衡次數：`{pairs}`",
        f"- Thesis 啟用日延後壓力測試：`{activation_lag_days}` 天",
        "- 方法：先使用 `serenity_industry_thesis_registry_2025.csv` 定義產業 thesis 與候選股，再由程式做 point-in-time 營收、動能、估值、流動性與籌碼 double-check。",
        "- 交易假設：月營收公布後第一個可交易 signal day，隔一交易日執行；使用 total-return adjusted price；含手續費、證交稅與 5 bps 買賣滑價。",
        "- 重要限制：這是 thesis-registry replay，不是完整人工新聞逐日 replay；若要升級，registry 每筆 thesis 都要補 source URL / evidence date / analyst note。",
        "",
        f"![NAV and drawdown]({chart_rel})",
        "",
        "## KPI",
        "",
        "| 名稱 | CAGR | 最近 1 年 CAGR | 1Y 窗口 | Sharpe | Sortino | MDD | Calmar | Final NAV | Turnover | 平均持股 | 最大持股 |",
        "|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in display.itertuples(index=False):
        lines.append(
            "| {name} | {cagr:.2%} | {recent_1y_cagr:.2%} | {recent_1y_window} | "
            "{sharpe:.3f} | {sortino:.3f} | {mdd:.2%} | {calmar:.2f} | "
            "{final_nav:,.0f} | {total_turnover:.2f}x | {avg_active:.1f} | {max_active} |".format(
                **row._asdict()
            )
        )
    if not latest.empty:
        lines += [
            "",
            f"## 最新 signal top 20（{latest_signal}）",
            "",
            "| Rank | 代號 | 公司 | Thesis | 產業 | Score | 60D | 月營收 YoY | PE |",
            "|---:|---|---|---|---|---:|---:|---:|---:|",
        ]
        for row in latest.itertuples(index=False):
            lines.append(
                "| {rank} | {company_code} | {company_name} | {theme_id} | {industry} | "
                "{score:.1f} | {ret_60d:.1%} | {monthly_revenue_yoy:.1f} | {price_to_earning_ratio:.1f} |".format(
                    **row._asdict()
                )
            )
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--capital", type=float, default=CAPITAL)
    parser.add_argument("--registry", default=str(REGISTRY))
    parser.add_argument("--activation-lag-days", type=int, default=0)
    parser.add_argument("--label", default=None)
    args = parser.parse_args()
    if args.capital != CAPITAL:
        raise ValueError("This script currently uses quantlib.constants.CAPITAL for comparable outputs.")

    registry = load_registry(Path(args.registry))
    if args.activation_lag_days:
        registry["active_from"] = registry["active_from"].map(
            lambda value: value + timedelta(days=args.activation_lag_days)
        )
    out_prefix = args.label or (
        OUT_PREFIX if args.activation_lag_days == 0 else f"{OUT_PREFIX}_lag{args.activation_lag_days}"
    )
    con = connect(read_only=True)
    try:
        cutoff = con.sql("select max(date) from daily_quote").fetchone()[0]
        start = date.fromisoformat(args.start)
        load_start = start - timedelta(days=420)
        universe = load_universe(con, registry["company_code"].tolist())
        missing = sorted(set(registry["company_code"]) - set(universe["company_code"]))
        if missing:
            print(f"warning: registry codes missing from tradable non-special universe: {','.join(missing)}")
        taxonomy = load_taxonomy(con, universe["company_code"].tolist())
        price_features, daily_returns = load_price_features(con, universe, load_start, cutoff)
        revenue = load_revenue_features(con)
        per = load_point_in_time_table(con, "stock_per_pbr", ["price_to_earning_ratio", "price_book_ratio"])
        flows = (
            con.sql(
                """
                SELECT date, company_code, total_difference AS inst_diff
                FROM daily_trading_details
                """
            )
            .pl()
            .with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4))
            .to_pandas()
        )
        flows = flows.sort_values(["company_code", "date"]).copy()
        flows["inst_20d"] = flows.groupby("company_code")["inst_diff"].transform(
            lambda s: s.rolling(20, min_periods=5).sum()
        )

        trading_days = sorted(pd.to_datetime(price_features["date"]).dt.date.unique())
        trading_days = [day for day in trading_days if day >= start and day <= cutoff]
        pairs = build_rebalance_pairs(trading_days, start, cutoff)

        dr = daily_returns[pd.to_datetime(daily_returns["date"]).dt.date.isin(trading_days)].copy()
        dr["_date"] = pd.to_datetime(dr["date"]).dt.date
        daily_returns_map = {
            day: group.set_index("company_code")["ret_1d"].fillna(0.0).to_dict()
            for day, group in dr.groupby("_date")
        }

        targets_by_variant: dict[str, dict[date, dict[str, float]]] = {variant.name: {} for variant in VARIANTS}
        pick_rows: list[dict[str, object]] = []
        target_rows: list[dict[str, object]] = []

        universe = universe.assign(company_code=universe["company_code"].astype(str).str.zfill(4))
        for signal_day, exec_day in pairs:
            active = active_registry_for_day(registry, signal_day)
            if active.empty:
                continue
            px_day = price_features[pd.to_datetime(price_features["date"]).dt.date == signal_day].copy()
            tax_day = row_latest_before(taxonomy, signal_day, "effective_date")
            tax_day = tax_day[(tax_day["is_financial"] == False) & (tax_day["is_special_category"] == False)].copy()
            rev_day = row_latest_before(revenue, signal_day, "report_date")
            per_day = row_latest_before(per, signal_day, "date")
            flow_day = row_latest_before(flows[["date", "company_code", "inst_20d"]], signal_day, "date")
            joined = (
                active.merge(tax_day, on="company_code", how="inner")
                .merge(px_day, on="company_code", how="inner")
                .merge(rev_day, on="company_code", how="left", suffixes=("", "_rev"))
                .merge(per_day, on="company_code", how="left")
                .merge(flow_day, on="company_code", how="left")
            )
            scored = score_candidates(joined)
            if scored.empty:
                continue
            for rank, row in enumerate(scored.head(30).itertuples(index=False), 1):
                pick_rows.append(
                    {
                        "signal_date": signal_day,
                        "execution_date": exec_day,
                        "rank": rank,
                        "company_code": row.company_code,
                        "company_name": row.company_name,
                        "theme_id": row.theme_id,
                        "theme_name": row.theme_name,
                        "industry": row.industry,
                        "score": float(row.score),
                        "conviction": float(row.conviction),
                        "ret_20d": float(row.ret_20d),
                        "ret_60d": float(row.ret_60d),
                        "ret_252d": float(row.ret_252d),
                        "monthly_revenue_yoy": float(row.monthly_revenue_yoy)
                        if not pd.isna(row.monthly_revenue_yoy)
                        else np.nan,
                        "yoy_3m": float(row.yoy_3m) if not pd.isna(row.yoy_3m) else np.nan,
                        "price_to_earning_ratio": float(row.price_to_earning_ratio)
                        if not pd.isna(row.price_to_earning_ratio)
                        else np.nan,
                        "price_book_ratio": float(row.price_book_ratio)
                        if not pd.isna(row.price_book_ratio)
                        else np.nan,
                    }
                )
            for variant in VARIANTS:
                targets = target_weights(scored, variant)
                targets_by_variant[variant.name][exec_day] = targets
                for code, weight in targets.items():
                    row = scored.loc[scored["company_code"] == code].iloc[0]
                    target_rows.append(
                        {
                            "variant": variant.name,
                            "signal_date": signal_day,
                            "execution_date": exec_day,
                            "company_code": code,
                            "company_name": row["company_name"],
                            "theme_id": row["theme_id"],
                            "weight": weight,
                            "score": row["score"],
                        }
                    )

        summaries: list[dict[str, object]] = []
        daily_paths: dict[str, Path] = {}
        for variant in VARIANTS:
            daily, turnover = simulate_variant(
                variant, trading_days, targets_by_variant[variant.name], daily_returns_map
            )
            path = RESULTS / f"{out_prefix}_{variant.name}_daily.csv"
            daily.to_csv(path, index=False)
            daily_paths[variant.name] = path
            summaries.append(summarize_nav(variant.name, daily, turnover, len(targets_by_variant[variant.name])))

        bench_start = trading_days[0]
        bench_end = trading_days[-1]
        for code, market, name in (("0050", "twse", "hold_0050"), ("2330", "twse", "hold_2330")):
            daily = benchmark_nav(con, code, market, bench_start, bench_end, name)
            path = RESULTS / f"{out_prefix}_{name}_daily.csv"
            daily.to_csv(path, index=False)
            daily_paths[name] = path
            summaries.append(summarize_nav(name, daily, 0.0, 0))

        picks = pd.DataFrame(pick_rows)
        targets = pd.DataFrame(target_rows)
        picks_path = RESULTS / f"{out_prefix}_picks.csv"
        targets_path = RESULTS / f"{out_prefix}_target_weights.csv"
        summary_path = RESULTS / f"{out_prefix}_summary.csv"
        picks.to_csv(picks_path, index=False)
        targets.to_csv(targets_path, index=False)
        summary = pd.DataFrame(summaries).sort_values("cagr", ascending=False)
        summary.to_csv(summary_path, index=False)
        chart_path = plot_outputs(summary, daily_paths, out_prefix)
        report_path = write_report(summary, picks, chart_path, cutoff, len(pairs), out_prefix, args.activation_lag_days)

        print(f"data_cutoff={cutoff}")
        print(f"trading_window={trading_days[0]}~{trading_days[-1]} rebalances={len(pairs)}")
        print(f"registry={Path(args.registry)} active_rows={len(registry)} unique_codes={registry['company_code'].nunique()}")
        print("outputs:")
        for path in (summary_path, picks_path, targets_path, chart_path, report_path):
            print(f"  {path}")
        display = summary.copy()
        for col in (
            "cagr",
            "recent_1y_cagr",
            "sharpe",
            "sortino",
            "mdd",
            "calmar",
            "total_turnover",
            "avg_rebalance_turnover",
        ):
            display[col] = display[col].astype(float)
        print(
            display[
                [
                    "name",
                    "cagr",
                    "recent_1y_cagr",
                    "recent_1y_window",
                    "sharpe",
                    "sortino",
                    "mdd",
                    "calmar",
                    "final_nav",
                    "total_turnover",
                    "avg_active",
                    "max_active",
                    "rebalances",
                ]
            ].to_string(
                index=False,
                formatters={
                    "cagr": "{:.2%}".format,
                    "recent_1y_cagr": "{:.2%}".format,
                    "sharpe": "{:.3f}".format,
                    "sortino": "{:.3f}".format,
                    "mdd": "{:.2%}".format,
                    "calmar": "{:.2f}".format,
                    "final_nav": "{:,.0f}".format,
                    "total_turnover": "{:.2f}x".format,
                    "avg_active": "{:.1f}".format,
                },
            )
        )
        latest_signal = max((row["signal_date"] for row in pick_rows), default=None)
        if latest_signal is not None:
            latest_picks = picks[picks["signal_date"] == latest_signal].head(20)
            print("\nlatest_signal_top20")
            print(
                latest_picks[
                    [
                        "rank",
                        "company_code",
                        "company_name",
                        "theme_id",
                        "industry",
                        "score",
                        "ret_60d",
                        "monthly_revenue_yoy",
                        "price_to_earning_ratio",
                    ]
                ].to_string(
                    index=False,
                    formatters={
                        "score": "{:.1f}".format,
                        "ret_60d": "{:.1%}".format,
                        "monthly_revenue_yoy": "{:.1f}".format,
                        "price_to_earning_ratio": "{:.1f}".format,
                    },
                )
            )
    finally:
        con.close()


if __name__ == "__main__":
    main()
