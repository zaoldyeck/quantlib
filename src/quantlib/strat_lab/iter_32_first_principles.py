"""iter_32 - first-principles event-driven TW stock strategy research.

Goal
====
Design from the portfolio mandate instead of tuning the existing 5+5 hybrid:

* Long-only Taiwan-listed common stocks.
* At most 10 simultaneous stock positions.
* Event-driven entries and exits; no calendar rebalance requirement.
* Use total-return-equivalent adjusted OHLCV from quantlib.prices.
* Trade next-day open after signals are known at today's close.
* Unused capital stays cash, not 0050, so performance is not padded by ETF beta.

Hypothesis
==========
In Taiwan equities, the durable edge should require all of:

1. Fundamental acceleration: recently published monthly revenue is strong.
2. Price confirmation: adjusted price is in an uptrend / near breakout.
3. Participation: volume expansion and/or institutional accumulation.
4. Quality floor: avoid fragile balance sheets and low-quality cyclicals.
5. Risk control: ATR-based trailing exit and trend/fundamental fade exits.

This file intentionally does not call sweep_hybrid.py. It builds features once,
then runs a small family of simple monotonic rule sets.
"""
from __future__ import annotations

import argparse
import math
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from quantlib import paths

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from quantlib.db import connect
from quantlib.industry_taxonomy import attach_industry_asof
from quantlib.prices import fetch_adjusted_panel, total_return_series


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
CAPITAL = 1_000_000.0
COMMISSION = 0.000285
SELL_TAX = 0.003
RF = 0.01
TDPY = 252

COMMON_CODE = r"^[1-9][0-9]{3}$"
FINANCE_MARKERS = ("金融", "證券", "保險")


@dataclass(frozen=True)
class Config:
    name: str
    max_positions: int = 10
    min_adv: float = 50_000_000.0
    min_yoy: float = 30.0
    min_yoy_delta: float = -10.0
    breakout_lkb: int = 90
    breakout_ratio: float = 1.00
    vol_mult: float = 1.5
    min_inst_flow: float = -0.02
    min_roa: float = 0.08
    min_gm: float = 0.20
    min_fscore: int = 4
    min_score: float = 1.05
    exit_yoy: float = 0.0
    atr_mult: float = 3.0
    trail_min: float = 0.10
    trail_max: float = 0.25
    use_ma100_exit: bool = True


def metrics(nav: np.ndarray, days: list[date], capital: float = CAPITAL) -> dict[str, float]:
    rets = np.diff(np.concatenate([[capital], nav])) / np.concatenate([[capital], nav[:-1]])
    years = max((days[-1] - days[0]).days / 365.25, 1e-9)
    cagr = (nav[-1] / capital) ** (1 / years) - 1
    vol = float(rets.std(ddof=1) * math.sqrt(TDPY))
    downside = rets[rets < 0]
    downvol = float(downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 1e-9
    sharpe = (cagr - RF) / vol if vol > 0 else 0.0
    sortino = (cagr - RF) / downvol if downvol > 0 else 0.0
    peak = capital
    mdd = 0.0
    for v in nav:
        peak = max(peak, v)
        mdd = min(mdd, (v - peak) / peak)
    return {
        "cagr": float(cagr),
        "sortino": float(sortino),
        "sharpe": float(sharpe),
        "mdd": float(mdd),
        "vol": vol,
        "downvol": downvol,
        "final_nav": float(nav[-1]),
    }


def _clip_expr(expr: pl.Expr, lo: float, hi: float) -> pl.Expr:
    return expr.clip(lo, hi)


def _quarter_effective_date(year: int, quarter: int) -> date:
    if quarter == 1:
        return date(year, 5, 16)
    if quarter == 2:
        return date(year, 8, 15)
    if quarter == 3:
        return date(year, 11, 15)
    return date(year + 1, 4, 8)


def build_feature_panel(con, start: date, end: date) -> tuple[pl.DataFrame, list[date]]:
    t0 = time.time()
    print("[iter32] build adjusted OHLCV panel (twse+tpex) ...")
    panels = []
    for market in ("twse", "tpex"):
        p = fetch_adjusted_panel(
            con,
            start.isoformat(),
            end.isoformat(),
            market=market,
            include_extra_history_days=320,
        )
        if not p.is_empty():
            panels.append(p)
    if not panels:
        raise RuntimeError("no adjusted OHLCV data")

    px = (
        pl.concat(panels)
        .filter(
            (pl.col("open") > 0)
            & (pl.col("high") > 0)
            & (pl.col("low") > 0)
            & (pl.col("close") > 0)
            & (pl.col("volume") > 0)
        )
        .filter(pl.col("company_code").str.contains(COMMON_CODE))
        .sort(["company_code", "date"])
        .rename({"volume": "vol"})
    )
    print(f"  price rows: {px.height:,}, codes: {px['company_code'].n_unique():,}")

    px = px.with_columns(
        pl.col("close").shift(1).over("company_code").alias("prev_close")
    )
    px = px.with_columns(
        pl.max_horizontal(
            [
                pl.col("high") - pl.col("low"),
                (pl.col("high") - pl.col("prev_close")).abs(),
                (pl.col("low") - pl.col("prev_close")).abs(),
            ]
        ).alias("tr")
    )
    px = px.with_columns(
        [
            pl.col("close").shift(1).rolling_max(60).over("company_code").alias("hi60"),
            pl.col("close").shift(1).rolling_max(90).over("company_code").alias("hi90"),
            pl.col("close").shift(1).rolling_max(120).over("company_code").alias("hi120"),
            pl.col("vol").shift(1).rolling_mean(60).over("company_code").alias("vol_avg60"),
            pl.col("trade_value").rolling_mean(60).over("company_code").alias("adv60"),
            pl.col("close").rolling_mean(50).over("company_code").alias("ma50"),
            pl.col("close").rolling_mean(100).over("company_code").alias("ma100"),
            pl.col("close").rolling_mean(200).over("company_code").alias("ma200"),
            pl.col("tr").rolling_mean(14).over("company_code").alias("atr14"),
            (pl.col("close") / pl.col("close").shift(20).over("company_code") - 1).alias("ret20"),
            (pl.col("close") / pl.col("close").shift(60).over("company_code") - 1).alias("ret60"),
            (pl.col("close") / pl.col("close").shift(120).over("company_code") - 1).alias("ret120"),
            pl.col("open").shift(-1).over("company_code").alias("next_open"),
            pl.col("date").shift(-1).over("company_code").alias("next_date"),
        ]
    ).with_columns((pl.col("atr14") / pl.col("close")).alias("atr_pct"))

    print("[iter32] join monthly revenue surprise/acceleration ...")
    rev = con.sql(
        """
        SELECT company_code, year, month, monthly_revenue_yoy AS yoy
        FROM operating_revenue
        WHERE monthly_revenue_yoy IS NOT NULL
          AND regexp_matches(company_code, '^[1-9][0-9]{3}$')
        """
    ).pl()
    rev = (
        rev.with_columns(
            pl.date(pl.col("year"), pl.col("month"), 1)
            .dt.offset_by("1mo")
            .dt.offset_by("12d")
            .alias("publish_d")
        )
        .sort(["company_code", "publish_d"])
        .with_columns(pl.col("yoy").shift(1).over("company_code").alias("prev_yoy"))
        .with_columns((pl.col("yoy") - pl.col("prev_yoy")).alias("yoy_delta"))
        .select(["company_code", "publish_d", "yoy", "yoy_delta"])
    )
    px = px.sort(["company_code", "date"]).join_asof(
        rev,
        left_on="date",
        right_on="publish_d",
        by="company_code",
        strategy="backward",
    ).rename({"yoy": "latest_yoy"})

    print("[iter32] join institutional flow ...")
    flow = con.sql(
        """
        SELECT market, date, company_code,
               COALESCE(foreign_investors_difference, 0)
             + COALESCE(trust_difference, 0)
             + COALESCE(dealers_difference, 0) AS inst_net_shares
        FROM daily_trading_details
        WHERE regexp_matches(company_code, '^[1-9][0-9]{3}$')
        """
    ).pl()
    px = (
        px.join(flow, on=["market", "date", "company_code"], how="left")
        .with_columns(pl.col("inst_net_shares").fill_null(0))
        .with_columns(
            [
                (pl.col("inst_net_shares") * pl.col("raw_close")).alias("inst_net_value"),
                pl.col("trade_value").rolling_sum(20).over("company_code").alias("trade_value20"),
                (pl.col("inst_net_shares") * pl.col("raw_close"))
                .rolling_sum(20)
                .over("company_code")
                .alias("inst_net_value20"),
            ]
        )
        .with_columns((pl.col("inst_net_value20") / pl.col("trade_value20")).alias("inst_flow20"))
    )

    print("[iter32] join PIT quarterly quality ...")
    q = con.sql(
        """
        SELECT company_code, year, quarter, roa_ttm, gross_margin_ttm, f_score_raw
        FROM raw_quarterly
        WHERE regexp_matches(company_code, '^[1-9][0-9]{3}$')
        """
    ).pl()
    q_rows = []
    for r in q.iter_rows(named=True):
        q_rows.append(
            {
                "company_code": r["company_code"],
                "q_effective_d": _quarter_effective_date(r["year"], r["quarter"]),
                "roa_ttm": r["roa_ttm"],
                "gross_margin_ttm": r["gross_margin_ttm"],
                "f_score_raw": r["f_score_raw"],
            }
        )
    q_pit = pl.DataFrame(q_rows).sort(["company_code", "q_effective_d"])
    px = px.sort(["company_code", "date"]).join_asof(
        q_pit,
        left_on="date",
        right_on="q_effective_d",
        by="company_code",
        strategy="backward",
    )

    print("[iter32] join static filters ...")
    etf = con.sql("SELECT DISTINCT company_code FROM etf").pl().with_columns(pl.lit(True).alias("is_etf"))
    first_day = con.sql(
        """
        SELECT company_code, MIN(date) AS first_date
        FROM daily_quote
        WHERE closing_price > 0
        GROUP BY company_code
        """
    ).pl()
    px = (
        px.join(etf, on="company_code", how="left")
        .join(first_day, on="company_code", how="left")
        .with_columns(
            [
                pl.col("is_etf").fill_null(False),
                (pl.col("date") - pl.col("first_date")).dt.total_days().alias("listed_days"),
            ]
        )
    )
    px = attach_industry_asof(px, con)

    # Score is deliberately simple and monotonic. It is not a fitted model.
    px = px.with_columns(
        [
            _clip_expr(pl.col("latest_yoy") / 50.0, -1.0, 3.0).alias("s_rev"),
            _clip_expr(pl.col("yoy_delta") / 40.0, -1.0, 2.0).alias("s_accel"),
            _clip_expr(pl.col("ret120") / 0.50, -1.0, 2.0).alias("s_mom120"),
            _clip_expr(pl.col("ret20") / 0.15, -1.0, 2.0).alias("s_mom20"),
            _clip_expr(pl.col("inst_flow20") / 0.08, -1.0, 2.0).alias("s_inst"),
            _clip_expr((0.06 - pl.col("atr_pct")) / 0.03, -1.0, 1.0).alias("s_risk"),
        ]
    ).with_columns(
        (
            0.30 * pl.col("s_rev")
            + 0.15 * pl.col("s_accel")
            + 0.20 * pl.col("s_mom120")
            + 0.15 * pl.col("s_mom20")
            + 0.10 * pl.col("s_inst")
            + 0.10 * pl.col("s_risk")
        ).alias("score")
    )

    days = [
        r[0]
        for r in con.sql(
            f"""
            SELECT date FROM daily_quote
            WHERE market='twse' AND company_code='0050'
              AND date BETWEEN DATE '{start}' AND DATE '{end}'
            ORDER BY date
            """
        ).fetchall()
    ]
    px = px.filter((pl.col("date") >= start) & (pl.col("date") <= end))
    print(f"[iter32] feature panel ready: {px.height:,} rows in {time.time() - t0:.1f}s")
    return px, days


def _breakout_col(lkb: int) -> str:
    if lkb == 60:
        return "hi60"
    if lkb == 90:
        return "hi90"
    if lkb == 120:
        return "hi120"
    raise ValueError(f"unsupported breakout_lkb={lkb}")


def build_signal_groups(panel: pl.DataFrame, cfg: Config) -> dict[date, list[dict]]:
    hi_col = _breakout_col(cfg.breakout_lkb)
    finance_expr = (
        pl.col("industry").str.contains("金融").fill_null(False)
        | pl.col("industry").str.contains("證券").fill_null(False)
        | pl.col("industry").str.contains("保險").fill_null(False)
    )
    sig = (
        panel.filter(
            (~pl.col("is_etf"))
            & (~finance_expr)
            & (pl.col("listed_days") >= 180)
            & (pl.col("adv60") >= cfg.min_adv)
            & (pl.col("latest_yoy") >= cfg.min_yoy)
            & (pl.col("yoy_delta").fill_null(0) >= cfg.min_yoy_delta)
            & (pl.col("close") >= pl.col(hi_col) * cfg.breakout_ratio)
            & (pl.col("vol") >= pl.col("vol_avg60") * cfg.vol_mult)
            & (pl.col("close") > pl.col("ma200"))
            & (pl.col("ma50") > pl.col("ma200"))
            & (pl.col("inst_flow20").fill_null(0) >= cfg.min_inst_flow)
            & (pl.col("roa_ttm").fill_null(-999) >= cfg.min_roa)
            & (pl.col("gross_margin_ttm").fill_null(-999) >= cfg.min_gm)
            & (pl.col("f_score_raw").fill_null(0) >= cfg.min_fscore)
            & (pl.col("atr_pct").is_between(0.01, 0.09))
            & (pl.col("score") >= cfg.min_score)
        )
        .select(
            [
                "date",
                "company_code",
                "score",
                "close",
                "atr_pct",
                "latest_yoy",
                "yoy_delta",
                "ret120",
                "inst_flow20",
            ]
        )
        .sort(["date", "score"], descending=[False, True])
    )
    return {
        key[0] if isinstance(key, tuple) else key: g.to_dicts()
        for key, g in sig.group_by("date", maintain_order=True)
    }


def build_row_lookup(panel: pl.DataFrame) -> dict[tuple[date, str], dict]:
    cols = [
        "date",
        "company_code",
        "open",
        "close",
        "ma50",
        "ma100",
        "ma200",
        "latest_yoy",
        "atr_pct",
        "score",
    ]
    return {(r["date"], r["company_code"]): r for r in panel.select(cols).iter_rows(named=True)}


def run_config(
    cfg: Config,
    panel: pl.DataFrame,
    days: list[date],
    row_lookup: dict[tuple[date, str], dict],
) -> tuple[dict, pl.DataFrame, pl.DataFrame]:
    signal_groups = build_signal_groups(panel, cfg)
    cash = CAPITAL
    positions: dict[str, dict] = {}
    pending_exits: set[str] = set()
    pending_entries: list[dict] = []
    nav_hist = []
    trades = []
    max_active = 0

    def current_nav(d: date, use_open: bool = False) -> float:
        total = cash
        px_col = "open" if use_open else "close"
        for code, pos in positions.items():
            row = row_lookup.get((d, code))
            px = row.get(px_col) if row else None
            if px is None or px <= 0:
                px = pos["last_close"]
            total += pos["shares"] * px
        return total

    for d in days:
        # Execute exits signaled after prior close at today's open.
        for code in list(pending_exits):
            pos = positions.pop(code, None)
            if pos is None:
                continue
            row = row_lookup.get((d, code))
            sell_px = row.get("open") if row else None
            if sell_px is None or sell_px <= 0:
                sell_px = pos["last_close"]
            proceeds = pos["shares"] * sell_px * (1 - SELL_TAX - COMMISSION)
            cash += proceeds
            trades.append(
                {
                    "date": d,
                    "code": code,
                    "action": "exit",
                    "price": sell_px,
                    "ret": sell_px / pos["entry_px"] - 1,
                    "reason": pos.get("pending_reason", ""),
                }
            )
        pending_exits.clear()

        # Execute entries signaled after prior close at today's open.
        if pending_entries:
            nav_open = current_nav(d, use_open=True)
            target_dollar = nav_open / cfg.max_positions
            for sig in pending_entries:
                if len(positions) >= cfg.max_positions:
                    break
                code = sig["company_code"]
                if code in positions:
                    continue
                row = row_lookup.get((d, code))
                buy_px = row.get("open") if row else None
                if buy_px is None or buy_px <= 0:
                    continue
                spend = min(cash, target_dollar)
                if spend <= 0:
                    break
                shares = spend / buy_px / (1 + COMMISSION)
                cost = shares * buy_px * (1 + COMMISSION)
                if shares <= 0 or cost > cash + 1e-6:
                    continue
                cash -= cost
                atr_pct = row.get("atr_pct") or sig.get("atr_pct") or 0.05
                positions[code] = {
                    "shares": shares,
                    "entry_px": buy_px,
                    "entry_date": d,
                    "entry_atr_pct": atr_pct,
                    "trail_pct": max(cfg.trail_min, min(cfg.trail_max, atr_pct * cfg.atr_mult)),
                    "peak_close": buy_px,
                    "last_close": buy_px,
                    "entry_score": sig.get("score"),
                }
                trades.append(
                    {
                        "date": d,
                        "code": code,
                        "action": "entry",
                        "price": buy_px,
                        "ret": None,
                        "reason": f"score={sig.get('score', 0):.3f}",
                    }
                )
            pending_entries = []

        # Mark to close and update peaks.
        for code, pos in list(positions.items()):
            row = row_lookup.get((d, code))
            if row is None:
                continue
            close = row.get("close")
            if close is None or close <= 0:
                continue
            pos["last_close"] = close
            pos["peak_close"] = max(pos["peak_close"], close)

        nav = current_nav(d, use_open=False)
        max_active = max(max_active, len(positions))
        nav_hist.append((d, nav, len(positions), cash))

        # Signal exits for next open.
        pending_exits = set()
        for code, pos in positions.items():
            row = row_lookup.get((d, code))
            if row is None:
                continue
            close = row.get("close")
            ma100 = row.get("ma100")
            latest_yoy = row.get("latest_yoy")
            reason = None
            if pos["peak_close"] > 0 and close / pos["peak_close"] - 1 <= -pos["trail_pct"]:
                reason = "atr_trailing"
            elif cfg.use_ma100_exit and ma100 is not None and close < ma100:
                reason = "ma100_break"
            elif latest_yoy is not None and latest_yoy < cfg.exit_yoy:
                reason = "yoy_fade"
            if reason:
                pos["pending_reason"] = reason
                pending_exits.add(code)

        # Signal entries for next open.
        available = cfg.max_positions - (len(positions) - len(pending_exits))
        pending_entries = []
        if available > 0:
            for sig in signal_groups.get(d, []):
                code = sig["company_code"]
                if code in positions or code in pending_exits:
                    continue
                pending_entries.append(sig)
                if len(pending_entries) >= available:
                    break

    nav_df = pl.DataFrame(
        {
            "date": [x[0] for x in nav_hist],
            "nav": [x[1] for x in nav_hist],
            "n_active": [x[2] for x in nav_hist],
            "cash": [x[3] for x in nav_hist],
        }
    )
    trades_df = pl.DataFrame(trades) if trades else pl.DataFrame(
        schema={"date": pl.Date, "code": pl.Utf8, "action": pl.Utf8, "price": pl.Float64}
    )
    m = metrics(nav_df["nav"].to_numpy(), nav_df["date"].to_list())
    m.update(
        {
            "name": cfg.name,
            "max_active": max_active,
            "n_entries": int((trades_df["action"] == "entry").sum()) if trades_df.height else 0,
            "n_exits": int((trades_df["action"] == "exit").sum()) if trades_df.height else 0,
            "avg_active": float(nav_df["n_active"].mean()),
            "cash_avg": float((nav_df["cash"] / nav_df["nav"]).mean()),
        }
    )
    return m, nav_df, trades_df


def default_configs() -> list[Config]:
    return [
        Config("fp_balanced_y30_b90_v15_s105", min_yoy=30, breakout_lkb=90, vol_mult=1.5, min_score=1.05),
        Config("fp_strict_y30_b90_v20_s115", min_yoy=30, breakout_lkb=90, vol_mult=2.0, min_score=1.15),
        Config("fp_growth_y20_b90_v20_s110", min_yoy=20, breakout_lkb=90, vol_mult=2.0, min_score=1.10),
        Config("fp_quality_y30_b120_v15_s105", min_yoy=30, breakout_lkb=120, vol_mult=1.5, min_score=1.05, min_roa=0.10),
        Config("fp_momentum_y30_b60_v20_s120", min_yoy=30, breakout_lkb=60, vol_mult=2.0, min_score=1.20),
        Config("fp_inst_y20_b90_v15_s100", min_yoy=20, breakout_lkb=90, vol_mult=1.5, min_score=1.00, min_inst_flow=0.02),
        Config("fp_aggressive_y20_b60_v15_s095", min_yoy=20, breakout_lkb=60, vol_mult=1.5, min_score=0.95, min_roa=0.06, min_gm=0.15),
        Config("fp_high_quality_y40_b90_v15_s110", min_yoy=40, breakout_lkb=90, vol_mult=1.5, min_score=1.10, min_roa=0.12, min_gm=0.25),
    ]


def benchmark_rows(con, start: date, end: date) -> list[dict]:
    rows = []
    for code in ("2330", "0050"):
        s = total_return_series(con, code, start.isoformat(), end.isoformat(), market="twse").sort("date")
        nav = CAPITAL * (s["adj_close"] / s["adj_close"][0]).to_numpy()
        m = metrics(nav, s["date"].to_list())
        m.update(
            {
                "name": f"hold_{code}",
                "max_active": 1,
                "n_entries": 1,
                "n_exits": 0,
                "avg_active": 1.0,
                "cash_avg": 0.0,
            }
        )
        rows.append(m)
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2005-01-03")
    ap.add_argument("--end", default="2026-05-08")
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    RESULTS.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    con = connect()
    panel, days = build_feature_panel(con, start, end)
    row_lookup = build_row_lookup(panel)
    print(f"[iter32] row lookup: {len(row_lookup):,} rows")

    rows = []
    for cfg in default_configs():
        print(f"\n[iter32] run {cfg.name}")
        t = time.time()
        result, nav_df, trades_df = run_config(cfg, panel, days, row_lookup)
        nav_path = RESULTS / f"iter_32_{cfg.name}_daily.csv"
        trades_path = RESULTS / f"iter_32_{cfg.name}_trades.csv"
        nav_df.write_csv(nav_path)
        trades_df.write_csv(trades_path)
        result.update(asdict(cfg))
        result["nav_path"] = str(nav_path)
        rows.append(result)
        print(
            f"  CAGR {result['cagr'] * 100:+.2f}% Sortino {result['sortino']:.3f} "
            f"Sharpe {result['sharpe']:.3f} MDD {result['mdd'] * 100:.2f}% "
            f"entries {result['n_entries']} max_active {result['max_active']} "
            f"cash_avg {result['cash_avg']:.1%} ({time.time() - t:.1f}s)"
        )

    rows.extend(benchmark_rows(con, start, end))
    out = pl.DataFrame(rows).sort("sortino", descending=True)
    out_path = RESULTS / "iter_32_first_principles_summary.csv"
    out.write_csv(out_path)

    print("\n" + "=" * 90)
    print(f"iter_32 first-principles summary ({start} -> {end})")
    print("=" * 90)
    print(
        out.select(
            [
                "name",
                pl.col("cagr").mul(100).round(2).alias("cagr_pct"),
                pl.col("sortino").round(3),
                pl.col("sharpe").round(3),
                pl.col("mdd").mul(100).round(2).alias("mdd_pct"),
                "max_active",
                pl.col("avg_active").round(2),
                pl.col("cash_avg").mul(100).round(1).alias("cash_avg_pct"),
                "n_entries",
            ]
        )
        .to_pandas()
        .to_string(index=False)
    )
    print(f"\nSaved: {out_path}")
    print(f"Total runtime: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
