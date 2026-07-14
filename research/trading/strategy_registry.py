"""Trading strategy registry with explicit deployment stages.

The project separates evidence quality from capital-allocation readiness:

- BACKTEST_VALIDATED: strict professional backtest passed.
- EXECUTION_READY: target-book/order-level implementation is reproducible.
- LIVE_PILOT: approved for small-capital live trading.
- PRODUCTION_SCALED: approved for unattended large-capital deployment.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class StrategyStage(str, Enum):
    RESEARCH_CANDIDATE = "research_candidate"
    BACKTEST_VALIDATED = "backtest_validated"
    EXECUTION_READY = "execution_ready"
    LIVE_PILOT = "live_pilot"
    PRODUCTION_SCALED = "production_scaled"


STAGE_RANK = {
    StrategyStage.RESEARCH_CANDIDATE: 0,
    StrategyStage.BACKTEST_VALIDATED: 1,
    StrategyStage.EXECUTION_READY: 2,
    StrategyStage.LIVE_PILOT: 3,
    StrategyStage.PRODUCTION_SCALED: 4,
}


@dataclass(frozen=True)
class StrategyRegistration:
    strategy_id: str
    name: str
    stage: StrategyStage
    validation_doc: Path
    max_positions: int | None = None
    target_weights_path: Path | None = None
    blocker: str | None = None


STRATEGIES: dict[str, StrategyRegistration] = {
    "serenity_ev_v2_thesis_inst": StrategyRegistration(
        strategy_id="serenity_ev_v2_thesis_inst",
        name="Serenity Event Engine v2 thesis-inst 262.12 / 649.50 (registry-curated)",
        stage=StrategyStage.EXECUTION_READY,
        validation_doc=Path("docs/serenity/serenity_event_engine_v1.md"),
        max_positions=10,
        target_weights_path=Path(
            "research/strat_lab/results/serenity_event_engine_v1_ev_v2_thesis_inst_target_weights.csv"
        ),
        blocker=(
            "Thesis-registry curated event engine, user-selected single "
            "strategy; champion since battle 8 (2026-07-07) replaced the "
            "revenue-only thesis stop with the institutional-distribution "
            "exit (inst_20d<0 while below entry). 2025-26 registry window "
            "CAGR 262.12% (lag90 202.06%, lag180 189.48%), Sortino 5.07, MDD "
            "-17.85%; Fubon realistic road test CAGR 282.4%, MDD -17.1%, "
            "fill 96.9%; permutation p=0.000, bootstrap CAGR 5% LB +107%, "
            "DSR 1.00 at 52 pre-registered trials; 2020-23 non-AI backcast "
            "beats 0050 at every activation lag (trials ledger). LIVE BOOK "
            "architecture: broker inventory is the source of truth; "
            "pre-existing holdings are ADOPTED (anchor = adoption close, "
            "clocks restart, same five exit rules) — never sold merely for "
            "being off-list. OPERATING REQUIREMENTS: (1) daily loop via "
            "`python -m research.trading.serenity_daily run` (regenerates "
            "engine book + live target weights at the daily_quote cutoff); "
            "(2) maintain the thesis registry per serenity_curation_sop.md — "
            "the registry IS the alpha source; (3) live submission stays "
            "blocked until a user-approved QL_STRATEGY_CAPITAL_TWD is set, "
            "the broker accounting smoke test passes, and FUBON_DRY_RUN is "
            "explicitly disabled by the user for a live run."
        ),
    ),
    "iter95_global_exit_time50_r_minus1": StrategyRegistration(
        strategy_id="iter95_global_exit_time50_r_minus1",
        name="Iter95 Global Exit-Aware Time50 r-1 39.14 / 295.53",
        stage=StrategyStage.EXECUTION_READY,
        validation_doc=Path("docs/strategy_ranking.md"),
        max_positions=10,
        target_weights_path=Path(
            "research/strat_lab/results/"
            "iter_95_global_exit_aware_search_iter92_unconstrained_meta_switch__"
            "time50_r-1_target_weights.csv"
        ),
        blocker=(
            "Current realistic-execution champion as of the 2026-05-18 "
            "daily_quote cutoff after global exit-aware re-search. The base "
            "target book is Iter92, with a strategy-level time exit: after "
            "50 trading days, sell positions still below -1% from entry. "
            "Fubon realistic OOS CAGR is 39.14%, recent 1Y CAGR is 295.53%, "
            "OOS Sortino is 2.470, OOS MDD is -22.09%, DSR is 0.993, and "
            "PBO is 0.032. Broker order-plan generation, strategy-level "
            "time-exit mapping, and dry-run order translation are implemented "
            "for live-pilot use. It is not live_pilot or production_scaled "
            "until a user-approved capital ceiling is configured, the latest "
            "broker accounting smoke test passes, and FUBON_DRY_RUN is "
            "explicitly disabled for a live submit run."
        ),
    ),
    "iter92_unconstrained_execution_meta_switch": StrategyRegistration(
        strategy_id="iter92_unconstrained_execution_meta_switch",
        name="Iter92 Unconstrained Execution Meta Switch 38.11 / 260.55",
        stage=StrategyStage.BACKTEST_VALIDATED,
        validation_doc=Path("docs/strategy_ranking.md"),
        max_positions=11,
        target_weights_path=Path(
            "research/strat_lab/results/iter_92_execution_meta_switch_target_weights.csv"
        ),
        blocker=(
            "Former realistic-execution champion as of the 2026-05-18 "
            "daily_quote cutoff after the max-position constraint was removed "
            "as a rejection gate. Fubon realistic OOS CAGR is 38.11%, recent "
            "1Y CAGR is 260.55%, OOS Sortino is 2.397, OOS MDD is -26.55%, "
            "DSR is 0.986, and PBO is 0.032. It is not execution-ready until "
            "broker order-plan generation and dry-run reconciliation are "
            "implemented."
        ),
    ),
    "iter89_robust_execution": StrategyRegistration(
        strategy_id="iter89_robust_execution",
        name="Iter89 Robust Execution Champion 35.80 / 253.84",
        stage=StrategyStage.BACKTEST_VALIDATED,
        validation_doc=Path("docs/strategy_ranking.md"),
        max_positions=10,
        target_weights_path=Path(
            "research/strat_lab/results/"
            "iter_89_execution_champion_search_iter86_b20_b08_weekly_lb5_m2_hold40_c1_rw0_100_d75_target_weights.csv"
        ),
        blocker=(
            "Former realistic-execution champion as of the 2026-05-18 daily_quote "
            "cutoff. Fubon realistic OOS CAGR is 35.80%, recent 1Y CAGR is "
            "253.84%, OOS Sortino is 2.234, and OOS MDD is -26.22%. Iter90/91 "
            "found active-ETF all-win challengers, but none passed the strict "
            "DSR/MDD validation gate, so this strategy remains the registered "
            "champion. It is not execution-ready until broker order-plan "
            "generation and dry-run reconciliation are implemented."
        ),
    ),
    "iter86_dual_4244": StrategyRegistration(
        strategy_id="iter86_dual_4244",
        name="Iter86 Dual Max 42.44 / 287.01",
        stage=StrategyStage.BACKTEST_VALIDATED,
        validation_doc=Path("docs/strategy_ranking.md"),
        max_positions=6,
        target_weights_path=Path(
            "research/strat_lab/results/iter_87_iter86_execution_validation_iter86_dual_target_weights_daily.csv"
        ),
        blocker=(
            "Backtest-validated champion as of the 2026-05-15 data cutoff. "
            "Corrected Iter87 broker-aware validation covers both TWSE and TPEx "
            "and the best realistic Fubon case has OOS CAGR 36.61%, above 2330 "
            "total return. It is not execution-ready until broker order-plan "
            "generation and dry-run reconciliation are implemented."
        ),
    ),
    "iter84_conservative_3720": StrategyRegistration(
        strategy_id="iter84_conservative_3720",
        name="Iter84 Conservative 37.20",
        stage=StrategyStage.BACKTEST_VALIDATED,
        validation_doc=Path("docs/strategy_ranking.md"),
        max_positions=6,
        blocker=(
            "Backtest-validated champion as of the 2026-05-15 data cutoff, but "
            "target-book/order-level implementation has not yet reconciled to "
            "the source NAV-lineage, so it is not execution-ready."
        ),
    ),
    "iter67_full_switch": StrategyRegistration(
        strategy_id="iter67_full_switch",
        name="Iter67 Full-Switch",
        stage=StrategyStage.BACKTEST_VALIDATED,
        validation_doc=Path("docs/strategy_ranking.md"),
        max_positions=6,
        blocker=(
            "Target-book/order-level implementation has not yet reconciled to the "
            "source NAV-lineage, so it is not execution-ready."
        ),
    )
}


def strategies_at_or_above(stage: StrategyStage) -> list[StrategyRegistration]:
    min_rank = STAGE_RANK[stage]
    return [strategy for strategy in STRATEGIES.values() if STAGE_RANK[strategy.stage] >= min_rank]


def best_strategy_at_or_above(stage: StrategyStage) -> StrategyRegistration:
    candidates = strategies_at_or_above(stage)
    if not candidates:
        raise RuntimeError(f"No strategy is currently registered at stage >= {stage.value}.")
    return sorted(candidates, key=lambda item: (STAGE_RANK[item.stage], item.strategy_id), reverse=True)[0]


def best_backtest_validated_strategy() -> StrategyRegistration:
    return best_strategy_at_or_above(StrategyStage.BACKTEST_VALIDATED)


def best_execution_ready_strategy() -> StrategyRegistration:
    return best_strategy_at_or_above(StrategyStage.EXECUTION_READY)
