"""iter_51 - dynamic switch between defensive and aggressive validated sleeves.

Previous rounds found:
  - iter42: cleaner risk-adjusted champion.
  - iter45/46/49: higher return or Sortino in some regimes, but weaker DSR.

This iteration tests a production-feasible switch: each day hold one complete
strategy sleeve, selected only from prior-day information. Because we switch
whole portfolios instead of layering them, the live max-holding count remains
the max of the chosen sleeve rather than the union of all sleeves.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, os.path.dirname(__file__))
from iter_40_research_campaign import CAPITAL, validate_daily
from iter_45_fallback_gate_sweep import load_base


RESULTS = Path("research/strat_lab/results")
N_TRIALS_EFFECTIVE = 1800

DEFENSES = [
    ("iter42_w61_cash", RESULTS / "iter42_q3_risk_breakout_top3_w61_daily.csv", 6.0),
    ("iter42_w72_cash", RESULTS / "iter42_q3_risk_breakout_top3_w72_daily.csv", 6.0),
]
ATTACKS = [
    ("iter45_mkt_mom21_w61", RESULTS / "iter45_q3_risk_breakout_top3_w61_gate_mkt_mom21_daily.csv", 6.0),
    ("iter45_mkt_mom21_w62", RESULTS / "iter45_q3_risk_breakout_top3_w62_gate_mkt_mom21_daily.csv", 6.0),
    (
        "iter46_mom21_or_q3_ma50_w62",
        RESULTS / "iter46_q3_risk_breakout_top3_w62_gate_mkt_mom21_or_q3_ma50_daily.csv",
        6.0,
    ),
    (
        "iter49_iter45_w62_q3_ma50_off75",
        RESULTS / "iter49_iter45_w62_mkt_mom21_gate_q3_ma50_off75_daily.csv",
        6.0,
    ),
    ("claude_nextopen_5q5c_w85", RESULTS / "iter_38_nextopen_5q_5c_w85_daily.csv", 10.0),
]


def nav_to_ret(name: str, path: Path) -> pl.DataFrame:
    return (
        pl.read_csv(path, try_parse_dates=True)
        .sort("date")
        .select(["date", "nav"])
        .with_columns(
            [
                pl.col("nav").pct_change().fill_null(0.0).alias(f"ret_{name}"),
                pl.col("nav").rolling_mean(50).alias(f"{name}_ma50"),
                pl.col("nav").rolling_mean(100).alias(f"{name}_ma100"),
            ]
        )
        .with_columns(
            [
                (pl.col("nav") > pl.col(f"{name}_ma50")).fill_null(True).shift(1).fill_null(True).alias(f"gate_{name}_ma50"),
                (pl.col("nav") > pl.col(f"{name}_ma100")).fill_null(True).shift(1).fill_null(True).alias(f"gate_{name}_ma100"),
                (pl.col("nav").pct_change(21) > 0).fill_null(True).shift(1).fill_null(True).alias(f"gate_{name}_mom21"),
                (pl.col("nav").pct_change(63) > 0).fill_null(True).shift(1).fill_null(True).alias(f"gate_{name}_mom63"),
                pl.col("nav").pct_change(63).shift(1).fill_null(0.0).alias(f"score_{name}_mom63"),
            ]
        )
        .select(
            [
                "date",
                f"ret_{name}",
                f"gate_{name}_ma50",
                f"gate_{name}_ma100",
                f"gate_{name}_mom21",
                f"gate_{name}_mom63",
                f"score_{name}_mom63",
            ]
        )
    )


def load_switch_base(defense: tuple[str, Path, float]) -> pl.DataFrame:
    defense_name, defense_path, _ = defense
    df = nav_to_ret(defense_name, defense_path)
    for attack_name, attack_path, _ in ATTACKS:
        df = df.join(nav_to_ret(attack_name, attack_path), on="date", how="inner")

    gates = load_base().select(
        [
            "date",
            "gate_mkt_ma50",
            "gate_mkt_ma100",
            "gate_mkt_ma150",
            "gate_mkt_ma200",
            "gate_mkt_mom21",
            "gate_mkt_mom63",
            "gate_mkt_mom126",
            "gate_mkt_dd10",
            "gate_mkt_dd15",
            "gate_q3_ma50",
            "gate_q3_ma100",
            "gate_q3_mom21",
            "gate_q3_mom63",
        ]
    )
    df = df.join(gates, on="date", how="left")
    gate_cols = [c for c in df.columns if c.startswith("gate_")]
    return df.with_columns([pl.col(c).fill_null(True) for c in gate_cols]).with_columns(
        [
            (pl.col("gate_mkt_mom21") & pl.col("gate_q3_ma50")).alias("gate_mkt_mom21_and_q3_ma50"),
            (pl.col("gate_mkt_ma50") & pl.col("gate_q3_ma50")).alias("gate_mkt_ma50_and_q3_ma50"),
            (pl.col("gate_mkt_mom63") | pl.col("gate_q3_mom63")).alias("gate_mkt_mom63_or_q3_mom63"),
            (pl.col("gate_mkt_ma100") | pl.col("gate_q3_ma100")).alias("gate_mkt_ma100_or_q3_ma100"),
        ]
    )


def daily_from_returns(dates: list, rets: np.ndarray) -> pl.DataFrame:
    nav = CAPITAL * np.cumprod(1 + rets)
    return pl.DataFrame({"date": dates, "nav": nav})


def switch_one_attack(base: pl.DataFrame, defense_name: str, attack: str, gate: str) -> pl.DataFrame:
    ret = (
        base.with_columns(
            pl.when(pl.col(gate)).then(pl.col(f"ret_{attack}")).otherwise(pl.col(f"ret_{defense_name}")).alias("ret")
        )
        .select("ret")
        .to_numpy()
        .reshape(-1)
    )
    return daily_from_returns(base["date"].to_list(), ret)


def switch_best_prior_mom(base: pl.DataFrame, defense_name: str, gate: str) -> pl.DataFrame:
    attack_names = [x[0] for x in ATTACKS]
    score_cols = [f"score_{name}_mom63" for name in attack_names]
    ret_cols = [f"ret_{name}" for name in attack_names]
    scores = base.select(score_cols).to_numpy()
    attack_rets = base.select(ret_cols).to_numpy()
    best_idx = np.argmax(scores, axis=1)
    best_rets = attack_rets[np.arange(len(best_idx)), best_idx]
    defense_rets = base[f"ret_{defense_name}"].to_numpy()
    gate_arr = base[gate].to_numpy()
    rets = np.where(gate_arr, best_rets, defense_rets)
    return daily_from_returns(base["date"].to_list(), rets)


def one_year_cagr(daily: pl.DataFrame) -> float:
    daily = daily.sort("date")
    latest = daily["date"][-1]
    start_target = latest.replace(year=latest.year - 1)
    window = daily.filter(pl.col("date") >= start_target)
    days = (window["date"][-1] - window["date"][0]).days
    return float((float(window["nav"][-1]) / float(window["nav"][0])) ** (365.25 / days) - 1)


def main() -> None:
    gates = [
        "gate_mkt_mom21",
        "gate_mkt_mom63",
        "gate_mkt_ma50",
        "gate_mkt_ma100",
        "gate_mkt_ma150",
        "gate_mkt_ma200",
        "gate_mkt_dd10",
        "gate_mkt_dd15",
        "gate_q3_ma50",
        "gate_q3_ma100",
        "gate_q3_mom21",
        "gate_q3_mom63",
        "gate_mkt_mom21_and_q3_ma50",
        "gate_mkt_ma50_and_q3_ma50",
        "gate_mkt_mom63_or_q3_mom63",
        "gate_mkt_ma100_or_q3_ma100",
    ]
    n_candidates = len(DEFENSES) * len(gates) * (len(ATTACKS) + 1)
    print(
        f"[iter51] defenses={len(DEFENSES)} attacks={len(ATTACKS)} gates={len(gates)} "
        f"candidates={n_candidates} dsr_trials={N_TRIALS_EFFECTIVE}",
        flush=True,
    )
    rows = []
    for defense_name, defense_path, defense_slots in DEFENSES:
        base = load_switch_base((defense_name, defense_path, defense_slots))
        print(f"[iter51] defense={defense_name} rows={base.height}", flush=True)
        for gate in gates:
            print(f"[iter51] gate={gate}", flush=True)
            for attack_name, _, slots in ATTACKS:
                name = f"iter51_switch_{defense_name}_{attack_name}_when_{gate}"
                daily = switch_one_attack(base, defense_name, attack_name, gate)
                out_path = RESULTS / f"{name}_daily.csv"
                daily.write_csv(out_path)
                row = validate_daily(
                    name,
                    daily,
                    N_TRIALS_EFFECTIVE,
                    {"max_active": max(slots, defense_slots), "trade_days": 0.0, "avg_turnover_trade_day": 0.0},
                )
                row["defense"] = defense_name
                row["selector"] = "single_attack"
                row["attack"] = attack_name
                row["gate"] = gate
                row["recent_1y_cagr"] = one_year_cagr(daily)
                row["promotable"] = (
                    row["dsr"] >= 0.95
                    and row["pbo"] < 0.50
                    and row["boot_cagr_lb"] > 0.10
                    and row["oos_mdd"] > -0.45
                )
                rows.append(row)

            name = f"iter51_switch_{defense_name}_best_prior63_when_{gate}"
            daily = switch_best_prior_mom(base, defense_name, gate)
            out_path = RESULTS / f"{name}_daily.csv"
            daily.write_csv(out_path)
            row = validate_daily(
                name,
                daily,
                N_TRIALS_EFFECTIVE,
                {
                    "max_active": max(defense_slots, max(slots for _, _, slots in ATTACKS)),
                    "trade_days": 0.0,
                    "avg_turnover_trade_day": 0.0,
                },
            )
            row["defense"] = defense_name
            row["selector"] = "best_prior63"
            row["attack"] = "best_prior63"
            row["gate"] = gate
            row["recent_1y_cagr"] = one_year_cagr(daily)
            row["promotable"] = (
                row["dsr"] >= 0.95
                and row["pbo"] < 0.50
                and row["boot_cagr_lb"] > 0.10
                and row["oos_mdd"] > -0.45
            )
            rows.append(row)

    summary = pl.DataFrame(rows).sort(["promotable", "oos_sortino", "oos_cagr"], descending=[True, True, True])
    out = RESULTS / "iter_51_dynamic_champion_switch_summary.csv"
    summary.write_csv(out)
    view_cols = [
        "name",
        "promotable",
        "defense",
        "selector",
        "attack",
        "gate",
        pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
        pl.col("sortino").round(3).alias("full_sortino"),
        pl.col("mdd").mul(100).round(2).alias("full_mdd_pct"),
        pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
        pl.col("oos_sortino").round(3),
        pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
        pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
        pl.col("boot_cagr_lb").mul(100).round(2).alias("boot_cagr_lb_pct"),
        pl.col("dsr").round(3),
        pl.col("pbo").round(3),
    ]
    print("=" * 120)
    print("iter_51 dynamic champion switch")
    print("=" * 120)
    print(summary.select(view_cols).head(30).to_pandas().to_string(index=False))
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
