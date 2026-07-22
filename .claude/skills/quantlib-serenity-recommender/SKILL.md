---
name: quantlib-serenity-recommender
description: >-
  Use this skill when the user asks for Serenity-style stock recommendations in
  any market (Taiwan TWSE/TPEx or the global IB-tradable universe: US, EU, JP,
  KR, HK), structural-bottleneck stock picks, ranked watchlists,
  valuation-aware growth recommendations, 10-stock recommendation reports with
  current price, target price, stop-loss price, and pre-trade plans,
  "Serenity 選股", "結構性瓶頸股", "全球瓶頸股", "用 Serenity 系統選股",
  "現在推薦哪幾檔", or industry-thesis stock selection. This skill is for stock
  recommendation and pre-trade planning only; it must not run live trading,
  broker order execution, or portfolio inventory management.
---

# Serenity Stock Recommender

This skill produces stock recommendations using the Serenity framework: start
from structural industry bottlenecks, then double-check candidates with market
data, valuation discipline, momentum, revenue, liquidity, and risk. It may
produce a pre-trade plan, but not a live order plan.

It runs in two market modes (state the mode in the report):

- **Taiwan mode** (TWSE/TPEx): full local-data double-check via PostgreSQL /
  DuckDB (monthly revenue, institutional flow, adjusted prices).
- **Global mode** (IB-tradable universe: US, EU, JP, KR, HK, others): same
  framework; evidence comes from the companion skill, company filings, and
  timestamped web quotes. See `references/recommendation-workflow.md` →
  "Global Mode (IB-Tradable Universe)".

**REQUIRED COMPANION SKILL:** `serenity-aleabitoreddit` (auto-updating vendored
copy at `.claude/skills/serenity-aleabitoreddit`). Refresh it first
(`npx -y skills update serenity-aleabitoreddit -y`), then use its
`references/methodology.md` (vetting checklist), `theses.md` (per-ticker
stances), and `track-record.md` (how much to weight a call).

**GUARDRAILS:** before echoing any Serenity-style sizing, margin, holding, or
exit advice, read `references/replication-guardrails.md` — parts of his system
(no stop-loss, ~1.4x margin, 30-50% single-name concentration, never selling)
must NOT be copied into recommendations. The daily supply-chain information
pipeline that feeds candidate discovery is documented in
`references/info-sources.md`.

## Scope Boundary

Use this skill for:

- Ranked stock recommendations or watchlists.
- Default 10-stock Serenity recommendation reports.
- Serenity structural-bottleneck stock selection.
- Comparing candidate stocks inside or across industries.
- Deciding whether a candidate is attractive, watch-only, too expensive, or
  invalidated.
- Research-level trade plans: current price, valuation target price, stop-loss
  price, entry condition, position role, and risk/reward.

Do not use this skill for:

- Backtest research as the main task. Use `quantlib-backtest`,
  `quantlib-factor-test`, or research scripts directly.
- Live broker order placement, cancellation, or account mutation.
- Current-position inventory management, rebalance execution, or order sizing
  from an account ledger.

If the user asks to trade after reviewing recommendations, stop and hand off to
a separate trading/execution workflow. If the user asks to manage existing
holdings, hand off to a separate portfolio or inventory-management workflow.
Pre-trade weights or sizing bands are allowed only as research guidance, not as
broker instructions.

## Mandatory Preconditions

- Taiwan mode: follow repository `AGENTS.md` data freshness rules before using
  market data, and state the exact PostgreSQL and DuckDB data cutoff used.
- Global mode: state the quote source and timestamp for every price, and the
  filing/transcript/source date for every fundamental claim (these names have
  no local DB coverage).
- Use total-return-adjusted prices for performance context. Use tradable raw
  prices for current price, target price, stop-loss price, and entry plan; state
  the quote timestamp/date.
- Keep broker credentials, certificate files, passwords, and account details out
  of this skill and out of recommendation reports.
- Never present a recommendation as guaranteed profit.

## Recommendation Workflow

1. Clarify the decision frame only when necessary: time horizon, capital size,
   risk tolerance, max names, and whether the output is a watchlist or buy list.
   If missing, default to a ranked watchlist for discussion.
2. Verify data freshness. Reuse same Taiwan-day verified data if PostgreSQL and
   DuckDB cutoffs are already current; otherwise refresh through the project
   data workflow before producing conclusions.
3. Gather source evidence for each structural bottleneck thesis. Prefer
   official filings, MOPS, company investor relations, earnings-call materials,
   exchange data, reputable industry research, and primary supply-chain sources.
   Provide links and dates when web sources are used.
4. Build the candidate pool. Taiwan mode: from the Serenity thesis registry and
   latest industry-first research artifacts. Global mode: from the companion
   skill's `theses.md` (his live universe) plus fresh names vetted with the
   methodology checklist. Prefer industry-thesis candidates over purely numeric
   screen results.
5. Apply the valuation overlay in `references/valuation-overlay.md`.
6. Cross-check candidates with momentum, revenue growth, institutional flow,
   liquidity, drawdown, industry concentration, and thesis invalidation risks.
7. Produce a ranked 10-name recommendation table with current price, valuation
   target price, stop-loss price, entry condition, risk/reward, thesis evidence,
   and whether each name is buy-ready or watch-only.

Load `references/recommendation-workflow.md` for the detailed operating flow
(including Global Mode), `references/output-contract.md` for the report shape,
and `references/valuation-overlay.md` when target-price logic is needed. Load
`references/source-research.md` when the task requires current news, filings,
industry evidence, or source-quality rules. Load `references/source-artifacts.md`
when artifact paths or refresh commands are needed. Load
`references/info-sources.md` to run or automate the daily supply-chain
information sweep, and `references/replication-guardrails.md` before giving any
sizing/exit/holding guidance.

## Output Contract

Final answers must be in Taiwan Traditional Chinese unless the user asks
otherwise. Include:

- Data cutoff and whether the data was refreshed or reused.
- A clear 10-stock ranking table with ticker, company name, theme, current price,
  target price, stop-loss price, risk/reward, thesis, valuation status, major
  risk, and recommendation status.
- Plain-language reasoning for the top picks.
- Evidence explaining why each recommended stock is a structural bottleneck
  beneficiary, with source names, links, and dates when available.
- Watch-only or avoid flags for expensive, crowded, illiquid, or invalidated
  names.
- The most important next verification item for each high-conviction pick.

Avoid burying the ranking behind methodology. The user should understand which
stocks are recommended first, and why.
