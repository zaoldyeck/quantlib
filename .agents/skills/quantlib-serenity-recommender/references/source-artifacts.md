# Serenity Source Artifacts

## Primary Inputs

- Thesis registry:
  `research/experiments/serenity_industry_thesis_registry_2025.csv`
- Industry-first replay:
  `research/experiments/serenity_industry_first_replay_2025.py`
- Valuation-method replay:
  `research/experiments/serenity_valuation_methods_replay_2025.py`

## Generated Reports

- `docs/strategy_research/serenity_industry_first_replay_2025.md`
- `docs/strategy_research/serenity_industry_first_replay_2025_lag90.md`
- `docs/strategy_research/serenity_industry_first_replay_2025_lag180.md`
- `docs/strategy_research/serenity_valuation_methods_replay_2025.md`

## Generated Result Files

- `var/out/strat_lab/serenity_industry_first_replay_2025_picks.csv`
- `var/out/strat_lab/serenity_industry_first_replay_2025_summary.csv`
- `var/out/strat_lab/serenity_industry_first_replay_2025_target_weights.csv`
- `var/out/strat_lab/serenity_valuation_methods_replay_2025_scored_candidates.csv`
- `var/out/strat_lab/serenity_valuation_methods_replay_2025_summary.csv`
- `var/out/strat_lab/serenity_valuation_methods_replay_2025_target_weights.csv`

## Helper

Read the latest valuation-aware candidate artifact:

```bash
.agents/skills/quantlib-serenity-recommender/scripts/serenity_latest_snapshot.py --top 10
```

This helper is read-only. It prints latest signal date, raw current price fields,
valuation scores, and core growth/momentum columns when available.

## Refresh Commands

Use only when artifacts are stale, missing, or the user asks for a fresh run:

```bash
uv run --project research python research/experiments/serenity_industry_first_replay_2025.py --start 2025-01-01
uv run --project research python research/experiments/serenity_valuation_methods_replay_2025.py --start 2025-01-01
```

These commands are research artifact refreshes. They are not live trading or
broker actions.
