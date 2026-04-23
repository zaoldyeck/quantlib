---
name: quantlib-stock-deepdive
description: Use this skill when the user wants a comprehensive multi-angle analysis of a single TWSE/TPEx stock (e.g. "分析 2330", "深入看 6488", "研究一下 XXXX", "should I watch YYYY"). Orchestrates 4 analyst agents in parallel, then bull/bear debate, then trader/risk synthesis. Produces an integrated report WITHOUT giving investment advice.
---

# Stock deep-dive orchestration

Synthesize evidence from 6 specialist subagents into one integrated report. User provides a TWSE/TPEx ticker; this skill runs the full pipeline.

## Preconditions

- `research/cache.duckdb` exists (suggest `quantlib-data-refresh` if > 3 days old)
- Ticker is valid TWSE/TPEx 4-digit code

## Step 1: Analyst phase (parallel)

Dispatch 4 agents concurrently (single message with 4 Agent tool calls):

1. `twstock-fundamental-analyst` — financials / ROIC / margin / health
2. `twstock-technical-analyst` — price action / volume / fund flow
3. `twstock-news-analyst` — last-30-day news + MOPS disclosures + macro
4. `twstock-sentiment-analyst` — PTT / retail crowding / foreign-vs-retail divergence

Pass the ticker to all four.

## Step 2: Research phase (parallel)

After analyst phase completes, dispatch both researchers with analyst outputs as context:

5. `twstock-bull-researcher` — strongest bullish thesis + conviction 0-100
6. `twstock-bear-researcher` — strongest bearish thesis + conviction 0-100

## Step 3: Synthesis phase (sequential)

7. `twstock-trader` — integrate bull/bear convictions → decision (Enter/Hold/Trim/Exit/Avoid) + sizing + triggers
8. `twstock-risk-manager` — review trader's output against concentration/liquidity/tail limits → Green/Yellow/Red

Stop here unless user explicitly asks for portfolio-level approval (then invoke `twstock-portfolio-manager`).

## Step 4: Integrated report

Produce ONE final document in **Traditional Chinese** containing:

- **一行結論**: net conviction (bull% − bear%) + trader decision + risk rating
- **基本面快照**（fundamental analyst 摘要）
- **技術面快照**（technical analyst 摘要）
- **新聞與事件**（news analyst 摘要 + 風險級事件）
- **社群氛圍**（sentiment analyst 摘要 + 法散分歧）
- **Bull vs Bear 對比表**: 三大支柱對三大風險
- **Trader 建議**: Enter/Hold/Trim/Exit/Avoid + 建倉方案（若 Enter）
- **Risk 評等**: Green/Yellow/Red + 主要警訊
- **Falsification triggers**: 哪些事件會讓論點失效
- **Review cadence**: 建議下次複檢時點

## Disclaimer (mandatory)

Every report MUST end with:

> 本報告為分析整理，不構成投資建議；任何實際交易決策應自行承擔責任。

## Anti-patterns

- Don't run agents sequentially when they can be parallel — wastes wall-clock time
- Don't hide bull or bear — present both sides
- Don't let risk-manager's Red be silently overridden — if trader says Enter but risk says Red, output requires user confirmation
- Don't give target prices — none of the analyst agents produce valuations
- Don't skip sentiment — even a "no discussion found on PTT" finding is informative
