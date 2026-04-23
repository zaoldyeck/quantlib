---
name: quantlib-factor-test
description: Use this skill when the user describes a factor hypothesis to test (e.g. "試試 momentum 加速度這因子", "看 F-Score 在 TW 有沒有 IC", "test whether 52-week-high distance predicts returns"). Runs a full research cycle in Python (memory check → implement → alphalens IC → accept/reject). Never modifies Scala Signals.scala.
---

# Factor research cycle (Python-only)

## Step 1: Prior-work check (mandatory)

Read memory `project_strategy_research_findings.md`. If the factor (or close variant) is already tested:
- **Already accepted** → report prior IC + decision, ask user why re-testing
- **Already rejected** → report prior IC + rejection reason, confirm user wants to override

Don't waste compute on settled factors.

## Step 2: Hypothesis

State in 1-2 sentences:
- What does this factor capture economically?
- Why should it predict forward TW returns?

Reject if the hypothesis is vague ("因為應該會漲"). Require specific mechanism.

## Step 3: Python implementation

Write to `research/experiments/factor_<name>.py`:
```python
"""Factor: <name>. Hypothesis: <1-line>."""
import polars as pl
from db import connect  # reads research/cache.duckdb

def factor_<name>(asof_dates: list) -> pl.DataFrame:
    """Returns (asof, company_code, score). PIT-safe."""
    con = connect()
    q = f"""
    -- SQL here. Rules:
    -- * Only data <= asof
    -- * Quarterly: Q1 from 5/22, Q2 from 8/21, Q3 from 11/21, Q4 from next-year 4/7
    -- * Monthly revenue: usable from 13th of next month
    """
    return con.sql(q).pl()

if __name__ == "__main__":
    from datetime import date
    # Read rebal dates same way research/v4.py does
    ...
```

Do NOT modify `src/main/scala/strategy/Signals.scala` — Scala package is frozen reference.

## Step 4: IC via alphalens

```python
import alphalens as al
factor_series = ...  # from factor_<name>() — pandas Series with MultiIndex (date, asset)
prices_wide = ...    # close price pivot (date × asset)
data = al.utils.get_clean_factor_and_forward_returns(
    factor_series, prices_wide, periods=(1, 5, 21))
al.tears.create_information_tear_sheet(data)
```

Capture the output IC / t-stat / hit-rate from the tear-sheet.

## Step 5: Decision

Strict criteria:
- **Accept**: mean IC >= 0.04 AND |t-stat| >= 2.0 AND hit rate >= 55%
- **Reject**: t-stat < 1.5 OR mean IC < 0.02
- **Borderline (1.5 <= t-stat < 2.0)**: run OOS split
  - Train 2018-2022, OOS 2023-2026
  - Accept only if OOS IC retention > 50%

## Step 6: Integration check (only if Accepted)

Before adding to production strategies:

1. **Redundancy**: Spearman correlation with surviving factors (yield, pbBand, dropScore, fcfYield, revenueYoYLatest). If |ρ| > 0.7 with any, pick the higher-IC one (usually existing survivor).

2. **Factor dilution test**: Fork `research/v4.py` to add factor as composite member. If composite CAGR < pbBand-only CAGR, the new factor dilutes the signal even though its solo IC is valid → reject despite IC.

## Step 7: Memory update

Append one line to `~/.claude/projects/-Users-zaoldyeck-Documents-scala-quantlib/memory/project_strategy_research_findings.md`:

```markdown
- **<factor_name>** (tested YYYY-MM-DD): IC +0.0XX, t=+X.XX, hit X%% → Accept/Reject. <reason>
```

## Output (Traditional Chinese)

- **假設**: 因子經濟邏輯
- **IC table**: mean IC / t-stat / hit rate / months
- **配對相關性**: 與 5 個既存倖存因子的 |ρ|
- **決策**: Accept / Reject / Borderline→OOS / Factor-dilution veto
- **Python snippet**: 可直接 run 的實作
- **Memory 更新**: 建議 append 的行

## Anti-patterns

- Never edit Scala Signals.scala
- Don't re-test factors already in memory without user confirmation
- Don't accept a factor with only one strong pillar (IC high but correlated with winner)
- Don't skip OOS split for borderline
- Don't add to v4 without factor-dilution test
