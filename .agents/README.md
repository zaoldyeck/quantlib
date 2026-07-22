# Codex Skills

This directory contains repository-local Codex skills. Each skill must defer to
`AGENTS.md` for canonical rules on data freshness, adjusted prices, broker
secrets, strategy stages, performance, and reporting.

Skills should not write external memory files. Durable results belong in
repository artifacts such as `docs/`, `research/experiments/`,
`research/trading/strategy_registry.py`, and `var/out/strat_lab/`.
