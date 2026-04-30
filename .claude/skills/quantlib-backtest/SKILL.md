---
name: quantlib-backtest
description: Use this skill when the user wants to run a strategy backtest, compare variants, or parameter-sweep (e.g. "跑 regime_aware", "test threshold 3% vs 5% vs 7%", "backtest my new strategy idea", "比較 A vs B"). Always uses Python (research/strat_lab/v4.py or vectorbt) — never Scala. Interprets against memory baseline, flags deviations, produces actionable next-step recommendation.
---

# Backtest workflow (Python-only)

**Python is canonical**. Never run Scala strategies — that package is frozen historical reference.

## Step 1: Parse request

Classify:
- **Single run**: specific strategy + specific window → use `research/strat_lab/v4.py` directly or fork it
- **Parameter sweep**: multiple config of same strategy (threshold / top-N / lookback) → use `vectorbt` grid
- **Variant comparison**: 2-5 strategies side-by-side → run each + tabulate

## Step 2: Freshness check

`research/cache.duckdb` mtime > 24h old → advise user to run `quantlib-data-refresh` first (don't auto-refresh).

## Step 3: Run

### Single run

```bash
cd /Users/zaoldyeck/Documents/scala/quantlib && \
  uv run --project research python research/strat_lab/v4.py \
    --start 2018-01-02 --end 2026-04-17 --capital 1000000
```

For a variant: copy `research/strat_lab/v4.py` to `research/experiments/<variant-name>.py`, modify one parameter, run the copy.

### Parameter sweep (vectorbt)

Write a one-shot script at `research/experiments/sweep_<name>.py`:
```python
import vectorbt as vbt
import polars as pl
from research.v4 import backtest  # reuse core engine

results = []
for threshold in [0.03, 0.05, 0.07, 0.10]:
    r = backtest(start="2018-01-02", end="2026-04-17", min_day=1,
                 capital=1_000_000, regime_threshold=threshold)
    results.append({"threshold": threshold, **r})
print(pl.DataFrame(results).sort("CAGR", descending=True))
```

## Step 4: Interpretation

Compare against memory `project_v4_baseline.md`:

| ΔCAGR vs baseline | Interpretation |
|---|---|
| within ±1pp | Similar — note as "on baseline" |
| 1pp < delta <= 3pp | Material — investigate which component changed |
| delta > 3pp | Either breakthrough or bug — triple-check inputs |

Sanity checks:
- Sharpe < 0.8 with CAGR > 20% → high-beta ride warning
- MDD worse than -45% → reject as production-unsuitable
- IC t-stat < 2.0 (if applicable) → no selection skill

## Step 5: Output (Traditional Chinese)

- **Runtime**: X 秒
- **結果 table** (each strategy): CAGR / Sharpe / MDD / Turnover
- **vs v4 baseline**: ΔCAGR / ΔSharpe
- **關鍵觀察**: 3-5 句中立解讀
- **建議 next step**: 探索變體 / adopt 為新 baseline / 丟棄
- **Commit 建議**: 若值得 commit，草稿訊息

## Anti-patterns

- **Never run Scala** (`sbt "runMain Main strategy ..."`)
- Don't silently run — show the exact command being executed
- Don't paraphrase numbers — paste exact values from stdout
- Don't compare across different rebalance timings — flag timing difference first
- Don't chase bit-exactness — 5s iteration + 1pp approximation noise beats 10-min high-precision
- If variant Sharpe improved but CAGR dropped > 3pp, highlight this tradeoff explicitly
