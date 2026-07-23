"""iter_68 - position-level bridge productionization.

Iter67 was strong at the NAV-sleeve level, but a partial blend of two complete
strategies can violate the <=10 holding mandate once converted to actual
orders. This pass rebuilds the strongest bridge idea at the target-position
layer:

  - reconstruct the production Iter63 target book;
  - reconstruct the Iter64 high-firepower target book;
  - switch between production core and a capped partial attack blend;
  - merge duplicate names, cap the final book at <=10 holdings, then simulate
    with next-open execution and stock-level transaction costs.

Only candidates that pass this position-level test are eligible for production
documentation.
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from quantlib import paths

sys.path.insert(0, os.path.dirname(__file__))

from iter_40_research_campaign import (  # noqa: E402
    CAPITAL,
    CampaignConfig,
    build_price_lookup,
    build_targets as build_campaign_targets,
    fetch_market_calendar,
    load_panel,
    risk_multipliers,
    simulate,
    validate_daily,
)
from iter_44_idle_fallback import load_market_0050  # noqa: E402
from iter_52_ownership_flow_alpha import (  # noqa: E402
    FlowConfig,
    add_flow_scores,
    build_targets as build_flow_targets,
    fetch_extra_features,
)
from iter_57_cost_aware_switch import SwitchSpec, simulate_switch  # noqa: E402
from iter_62_sector_leadership_meta import MetaSpec, Sleeve, load_base as load_meta_base, simulate_meta  # noqa: E402
from iter_63_sector_meta_risk_overlay import exposure_path, load_gates  # noqa: E402
from iter_64_active_etf_beater_confirm import compare_active_etfs, load_active_etfs, strict_dsr, window_metrics  # noqa: E402
from iter_66_core_bridge import Sleeve as BridgeSleeve  # noqa: E402
from iter_66_core_bridge import load_base as load_bridge_base  # noqa: E402
from iter_67_partial_bridge import CUMULATIVE_TRIALS as ITER67_CUMULATIVE_TRIALS  # noqa: E402
from iter_67_partial_bridge import PartialSpec, simulate_partial  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_68_position_level_bridge"
SWITCH_COST = 0.00357


Book = dict[str, float]
BookByDate = dict[date, Book]


@dataclass(frozen=True)
class PositionBridgeSpec:
    name: str
    lookback: int
    margin: float
    min_hold_days: int
    confirm_days: int
    attack_weight: float
    max_positions: int
    renormalize_after_cap: bool


def merge_books(parts: list[tuple[float, Book]]) -> Book:
    out: Book = {}
    for scale, book in parts:
        if scale <= 0:
            continue
        for code, weight in book.items():
            if weight <= 0:
                continue
            out[code] = out.get(code, 0.0) + scale * weight
    return {code: weight for code, weight in out.items() if weight > 1e-12}


def cap_book(book: Book, max_positions: int, renormalize: bool) -> Book:
    if len(book) <= max_positions:
        return book
    kept = dict(sorted(book.items(), key=lambda kv: (-kv[1], kv[0]))[:max_positions])
    if renormalize:
        old_total = sum(book.values())
        kept_total = sum(kept.values())
        if kept_total > 0 and old_total > 0:
            scale = min(old_total, 1.0) / kept_total
            kept = {code: weight * scale for code, weight in kept.items()}
    return kept


def load_pick_targets(path: Path) -> BookByDate:
    if not path.exists():
        raise FileNotFoundError(path)
    df = pl.read_csv(path, try_parse_dates=True, schema_overrides={"company_code": pl.Utf8}).rename({"rebal_d": "date"})
    out: BookByDate = {}
    for d, sub in df.group_by("date", maintain_order=True):
        key = d[0] if isinstance(d, tuple) else d
        out[key] = {
            str(row["company_code"]): float(row["weight"])
            for row in sub.iter_rows(named=True)
            if float(row["weight"]) > 0
        }
    return out


def expand_targets(days: list[date], raw: BookByDate, persist: bool, risk: dict[date, float] | None = None) -> BookByDate:
    active: Book = {}
    out: BookByDate = {}
    last_mult = 1.0
    for d in days:
        mult = 1.0 if risk is None else risk.get(d, 1.0)
        if d in raw:
            active = raw[d]
        elif not persist:
            active = {}
        elif abs(mult - last_mult) > 1e-12:
            pass
        last_mult = mult
        out[d] = {code: weight * mult for code, weight in active.items() if weight * mult > 1e-12}
    return out


def state_by_date(frame: pl.DataFrame) -> dict[date, str]:
    return dict(zip(frame["date"].to_list(), frame["selected"].to_list(), strict=True))


def exposure_by_date(days: list[date], gate_name: str, off_mult: float, confirm_days: int, min_hold_days: int) -> dict[date, float]:
    gates = load_gates().filter(pl.col("date").is_in(days)).sort("date")
    gate = gates[gate_name].to_numpy().astype(bool)
    expo = exposure_path(gate, off_mult, confirm_days, min_hold_days)
    return dict(zip(gates["date"].to_list(), expo.tolist(), strict=True))


def build_position_books() -> tuple[list[date], pl.DataFrame, BookByDate, BookByDate, BookByDate, BookByDate, BookByDate]:
    panel, days, market = load_panel()

    q3_raw = load_pick_targets(RESULTS / "iter_13_iter50_q3_mcap_monthly_picks.csv")
    q3 = expand_targets(days, q3_raw, persist=True)

    breakout_cfg = CampaignConfig(
        name="breakout_risk_ma200_cash_top3",
        family="breakout_risk",
        score_kind="rev_accel",
        topn=3,
        risk_mode="ma200_cash",
        min_yoy=30.0,
        breakout_lkb=90,
        breakout_ratio=0.98,
        vol_mult=1.5,
        min_roa=0.02,
        min_gm=0.10,
        min_fscore=3,
        max_atr=0.10,
    )
    breakout_raw = build_campaign_targets(panel, days, breakout_cfg)
    breakout = expand_targets(days, breakout_raw, persist=False, risk=risk_multipliers(days, market, "ma200_cash"))

    extra = fetch_extra_features()
    flow_panel = (
        panel.join(extra, on=["date", "company_code"], how="left")
        .with_columns(
            [
                pl.col("outstanding_shares").fill_null(0),
                pl.col("foreign_held_ratio").fill_null(0.0),
                pl.col("foreign_chg20").fill_null(0.0),
                pl.col("foreign_chg60").fill_null(0.0),
                pl.col("margin_balance").fill_null(0),
                pl.col("short_balance").fill_null(0),
                pl.col("sbl_balance").fill_null(0),
                pl.col("margin_ratio").fill_null(0.0),
                pl.col("short_ratio").fill_null(0.0),
                pl.col("sbl_ratio").fill_null(0.0),
                pl.col("pbr").fill_null(999.0),
                pl.col("dividend_yield").fill_null(0.0),
                pl.col("pe").fill_null(999.0),
                pl.col("buyback_pct").fill_null(0.0),
                pl.col("buyback_executed_shares").fill_null(0),
            ]
        )
        .pipe(add_flow_scores)
        .rechunk()
    )
    squeeze_cfg = FlowConfig(
        name="squeeze_top5_monthly",
        score_kind="squeeze_score",
        topn=5,
        rebalance="monthly",
        min_adv=80_000_000.0,
        min_roa=0.00,
        min_gm=0.05,
        min_fscore=2,
        require_short_pressure=True,
        require_trend=True,
    )
    squeeze_raw = build_flow_targets(flow_panel, days, squeeze_cfg)
    squeeze = expand_targets(days, squeeze_raw, persist=True)

    q3_trend = {
        row["date"]: bool(row["mkt_up"])
        for row in load_market_0050().select(["date", "mkt_up"]).iter_rows(named=True)
    }

    iter42 = {
        d: merge_books([(0.59, q3.get(d, {})), (0.41, breakout.get(d, {}))])
        for d in days
    }
    iter44 = {}
    for d in days:
        sat = breakout.get(d, {})
        if not sat and q3_trend.get(d, True):
            sat = q3.get(d, {})
        iter44[d] = merge_books([(0.74, q3.get(d, {})), (0.26, sat)])

    iter57_spec = SwitchSpec(
        name="iter57_position_rebuild",
        defense="iter44_w74_q3_trend",
        attack="iter52_squeeze_top5",
        entry_gate="gate_mkt_mom63_q3_ma50_sq_ma50",
        exit_gate="gate_mkt_mom63_q3_ma50_sq_ma50",
        schedule="monthly",
        min_hold_days=20,
        confirm_days=3,
    )
    from iter_54_cross_family_switch import load_switch_base  # noqa: WPS433
    switch_base = load_switch_base({"iter44_w74_q3_trend", "iter52_squeeze_top5"}).with_columns(
        (pl.col("gate_mkt_mom63") & pl.col("gate_q3_ma50") & pl.col("gate_iter52_squeeze_top5_ma50")).alias(
            "gate_mkt_mom63_q3_ma50_sq_ma50"
        )
    )
    iter57_state = state_by_date(simulate_switch(switch_base, iter57_spec))
    iter57 = {
        d: squeeze.get(d, {}) if iter57_state.get(d) == "iter52_squeeze_top5" else iter44.get(d, {})
        for d in days
    }

    return days, panel, iter42, iter57, q3, iter44, squeeze


def build_iter64_targets(days: list[date], iter42: BookByDate, iter57: BookByDate) -> BookByDate:
    core = Sleeve("iter42", "Corrected Iter42 w59 core", RESULTS / "iter42_q3_risk_breakout_top3_w59_daily.csv", 6.0)
    attack = Sleeve(
        "iter57",
        "Iter57 cost-aware monthly switch",
        RESULTS
        / (
            "iter57_iter44_w74_q3_trend_iter52_squeeze_top5_gate_mkt_mom63_q3_ma50_sq_ma50"
            "_exit_gate_mkt_mom63_q3_ma50_sq_ma50_monthly_hold20_confirm3_daily.csv"
        ),
        6.0,
    )
    spec = MetaSpec(
        name="iter64_position_state",
        core=core,
        attack=attack,
        gate="gate_tech_rs_mom21_abs21",
        schedule="monthly",
        lookback=63,
        margin=-0.05,
        min_hold_days=40,
        confirm_days=2,
    )
    state = state_by_date(simulate_meta(load_meta_base(core, attack), spec))
    return {d: iter57.get(d, {}) if state.get(d) == "attack" else iter42.get(d, {}) for d in days}


def build_iter63_targets(days: list[date], iter42: BookByDate, iter57: BookByDate) -> BookByDate:
    core = Sleeve("iter42", "Corrected Iter42 w59 core", RESULTS / "iter42_q3_risk_breakout_top3_w59_daily.csv", 6.0)
    attack = Sleeve(
        "iter57",
        "Iter57 cost-aware monthly switch",
        RESULTS
        / (
            "iter57_iter44_w74_q3_trend_iter52_squeeze_top5_gate_mkt_mom63_q3_ma50_sq_ma50"
            "_exit_gate_mkt_mom63_q3_ma50_sq_ma50_monthly_hold20_confirm3_daily.csv"
        ),
        6.0,
    )
    meta = MetaSpec(
        name="iter63_active_position_state",
        core=core,
        attack=attack,
        gate="gate_tech_rs_mom21_abs21",
        schedule="monthly",
        lookback=21,
        margin=-0.05,
        min_hold_days=40,
        confirm_days=2,
    )
    state = state_by_date(simulate_meta(load_meta_base(core, attack), meta))
    expo = exposure_by_date(days, "gate_mkt_mom21", 0.75, 2, 10)
    out = {}
    for d in days:
        base = iter57.get(d, {}) if state.get(d) == "attack" else iter42.get(d, {})
        out[d] = {code: weight * expo.get(d, 1.0) for code, weight in base.items() if weight * expo.get(d, 1.0) > 1e-12}
    return out


def build_bridge_state(base: pl.DataFrame, attack: BridgeSleeve, spec: PositionBridgeSpec) -> dict[date, str]:
    partial = PartialSpec(
        name=spec.name,
        attack=attack,
        gate="gate_tech_rs_mom21_abs21",
        schedule="monthly",
        lookback=spec.lookback,
        margin=spec.margin,
        min_hold_days=spec.min_hold_days,
        confirm_days=spec.confirm_days,
        attack_weight=spec.attack_weight,
    )
    return state_by_date(simulate_partial(base, partial))


def build_bridge_targets(
    days: list[date],
    core: BookByDate,
    attack: BookByDate,
    state: dict[date, str],
    spec: PositionBridgeSpec,
) -> tuple[BookByDate, dict[str, float]]:
    out = {}
    uncapped_max = 0
    capped_days = 0
    avg_kept_weight = []
    for d in days:
        if state.get(d) == "attack":
            raw = merge_books([(1.0 - spec.attack_weight, core.get(d, {})), (spec.attack_weight, attack.get(d, {}))])
        else:
            raw = core.get(d, {})
        uncapped_max = max(uncapped_max, len(raw))
        capped = cap_book(raw, spec.max_positions, spec.renormalize_after_cap)
        if len(raw) > len(capped):
            capped_days += 1
            avg_kept_weight.append(sum(capped.values()) / max(sum(raw.values()), 1e-12))
        out[d] = capped
    return out, {
        "uncapped_max_positions": float(uncapped_max),
        "capped_days": float(capped_days),
        "avg_kept_weight_when_capped": float(np.mean(avg_kept_weight)) if avg_kept_weight else 1.0,
    }


def collect_codes(*books_by_date: BookByDate) -> set[str]:
    return {code for books in books_by_date for book in books.values() for code in book}


def simulate_books(days: list[date], price_lookup: dict, books: BookByDate) -> tuple[pl.DataFrame, dict[str, float]]:
    return simulate(days, price_lookup, books, {d: 1.0 for d in days}, persist=True)


def build_specs() -> list[PositionBridgeSpec]:
    specs = []
    for lookback in (21, 42, 63):
        for margin in (-0.05, 0.0, 0.05):
            for hold in (40, 60):
                for confirm in (1, 2):
                    for weight in (0.50, 0.75, 1.00):
                        for renorm in (False, True):
                            name = (
                                f"iter68_prodcore_iter64_pos_gate_tech_rs_mom21_abs21_monthly"
                                f"_lb{lookback}_m{int(margin * 100)}_hold{hold}_confirm{confirm}"
                                f"_w{int(weight * 100)}_cap10_{'renorm' if renorm else 'cash'}"
                            )
                            specs.append(PositionBridgeSpec(name, lookback, margin, hold, confirm, weight, 10, renorm))
    return specs


def main() -> None:
    specs = build_specs()
    n_trials = ITER67_CUMULATIVE_TRIALS + len(specs)
    print(f"[iter68] build position books specs={len(specs)} cumulative_trials={n_trials}", flush=True)
    days, panel, iter42, iter57, _q3, _iter44, _squeeze = build_position_books()
    iter63 = build_iter63_targets(days, iter42, iter57)
    iter64 = build_iter64_targets(days, iter42, iter57)
    price_lookup = build_price_lookup(panel, collect_codes(iter63, iter64))

    # Validate the reconstructed production core first; this is the anchor for all bridge variants.
    core_daily, core_stats = simulate_books(days, price_lookup, iter63)
    core_daily.write_csv(RESULTS / "iter68_rebuilt_iter63_active_position_daily.csv")
    core_val = validate_daily("iter68_rebuilt_iter63_active_position", core_daily, n_trials, core_stats)
    print(
        "[iter68 core] "
        f"OOS={core_val['oos_cagr']:+.2%} Sortino={core_val['oos_sortino']:.3f} "
        f"MDD={core_val['oos_mdd']:.2%} DSR={core_val['dsr']:.3f} PBO={core_val['pbo']:.3f} "
        f"max_active={core_stats['max_active']:.0f}",
        flush=True,
    )

    core_path = RESULTS / "iter68_rebuilt_iter63_active_position_daily.csv"
    attack_daily, _attack_stats = simulate_books(days, price_lookup, iter64)
    attack_path = RESULTS / "iter68_rebuilt_iter64_no_overlay_position_daily.csv"
    attack_daily.write_csv(attack_path)
    bridge_base = load_bridge_base(BridgeSleeve("prod_iter63", core_path), BridgeSleeve("iter64_no_overlay", attack_path))
    bridge_attack = BridgeSleeve("iter64_no_overlay", attack_path)
    state_cache: dict[tuple[int, float, int, int], dict[date, str]] = {}

    etfs = load_active_etfs(days[0], days[-1])
    rows = []
    compare_rows = []
    for i, spec in enumerate(specs, 1):
        state_key = (spec.lookback, spec.margin, spec.min_hold_days, spec.confirm_days)
        if state_key not in state_cache:
            state_cache[state_key] = build_bridge_state(bridge_base, bridge_attack, spec)
        state = state_cache[state_key]
        books, cap_stats = build_bridge_targets(days, iter63, iter64, state, spec)
        daily, stats = simulate_books(days, price_lookup, books)
        active_summary, active_rows = compare_active_etfs(spec.name, daily, etfs)
        focused = validate_daily(spec.name, daily, len(specs), stats)
        row = {
            "name": spec.name,
            "lookback": spec.lookback,
            "margin": spec.margin,
            "min_hold_days": spec.min_hold_days,
            "confirm_days": spec.confirm_days,
            "attack_weight": spec.attack_weight,
            "renormalize_after_cap": spec.renormalize_after_cap,
            **focused,
            "cumulative_dsr": strict_dsr(daily, n_trials),
            **window_metrics(daily, 365),
            **active_summary,
            **cap_stats,
        }
        row["strict_promotable"] = (
            row["cumulative_dsr"] >= 0.95
            and row["pbo"] < 0.50
            and row["boot_cagr_lb"] > 0.10
            and row["oos_mdd"] > -0.45
            and row["max_active"] <= 10.0
        )
        rows.append(row)
        compare_rows.extend(active_rows)
        if i % 20 == 0 or row["strict_promotable"]:
            print(
                f"[iter68] {i:03d}/{len(specs)} wins={row['active_etf_wins']:.0f}/{row['active_etf_count']:.0f} "
                f"OOS={row['oos_cagr']:+.2%} Sortino={row['oos_sortino']:.3f} "
                f"1Y={row['recent_1y_cagr']:+.2%} cumDSR={row['cumulative_dsr']:.3f} "
                f"PBO={row['pbo']:.3f} capDays={row['capped_days']:.0f}",
                flush=True,
            )
        if row["strict_promotable"] or (row["active_etf_wins"] >= 16 and row["cumulative_dsr"] >= 0.90):
            out_path = RESULTS / f"{spec.name}_daily.csv"
            daily.write_csv(out_path)
            row["path"] = str(out_path)
        else:
            row["path"] = ""

    summary = pl.DataFrame(rows).sort(
        ["strict_promotable", "active_etf_wins", "cumulative_dsr", "oos_sortino", "recent_1y_cagr"],
        descending=[True, True, True, True, True],
    )
    summary_path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    compare_path = RESULTS / f"{OUT_PREFIX}_active_etf_comparison.csv"
    summary.write_csv(summary_path)
    pl.DataFrame(compare_rows).write_csv(compare_path)
    view = summary.select(
        [
            "name",
            "strict_promotable",
            "active_etf_wins",
            "active_etf_count",
            pl.col("active_etf_min_gap").mul(100).round(2).alias("min_gap_pct"),
            pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
            pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
            pl.col("oos_sortino").round(3),
            pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
            pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
            pl.col("boot_cagr_lb").mul(100).round(2).alias("boot_cagr_lb_pct"),
            pl.col("dsr").round(3).alias("focused_dsr"),
            pl.col("cumulative_dsr").round(3),
            pl.col("pbo").round(3),
            "max_active",
            "uncapped_max_positions",
            "capped_days",
            pl.col("avg_kept_weight_when_capped").mul(100).round(1).alias("kept_weight_pct"),
        ]
    )
    print("=" * 140)
    print("iter_68 position-level bridge productionization")
    print("=" * 140)
    print(view.head(25).to_pandas().to_string(index=False))
    print(f"\nSaved: {summary_path}")
    print(f"Saved: {compare_path}")


if __name__ == "__main__":
    main()
