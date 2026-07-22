# Serenity Recommendation Workflow

## Intent

This workflow is for recommending Taiwan stocks and producing a research-level
pre-trade plan, not for developing a trading strategy or executing broker
orders. The Serenity logic starts from an industry or supply-chain thesis, then
uses project data as a double-check layer.

## Candidate Construction

1. Start from structural bottleneck themes:
   - scarce capacity, materials, equipment, certification, or distribution;
   - demand that can persist beyond one quarter;
   - pricing power or volume growth that can flow into revenue and margin.
2. Map each theme to TWSE/TPEx tickers with a point-in-time industry taxonomy
   and explicit thesis notes.
3. Prefer candidates that are selected by the theme first and validated by
   data second. A pure numeric screen is not enough for Serenity.
4. Penalize names where the theme is stale, the company mapping is weak, or the
   business is only indirectly exposed.
5. Unless the user asks for a different count, produce 10 ranked names. If fewer
   than 10 are buy-ready, keep the table at 10 but label lower-conviction names
   as `觀察` or `避免追高` rather than forcing a buy recommendation.

## Theme Eligibility

Serenity Core must exclude pure commodity-cycle or inventory-revaluation themes.
`memory_cycle` is a tactical cycle overlay, not a Serenity Core bottleneck,
unless a specific stock has separate evidence of a durable bottleneck such as
qualification lock-in, proprietary controller/IP, constrained certified
industrial storage capacity, or customer switching costs.

Do not include `memory_cycle` names in a default Serenity Core top-10 report.
If they are useful, present them in a separate `Tactical Cycle Overlay` section
with explicit caveats that the return driver is cycle beta, inventory value, or
cost pass-through rather than durable structural bottleneck power.

## Data Checks

Before ranking:

- Verify PostgreSQL latest dates for relevant source tables.
- Verify `var/cache/cache.duckdb` latest dates if Python research artifacts are
  used.
- Check the latest available adjusted-price date.
- Check monthly revenue freshness, valuation data freshness, and liquidity.

If the same Taiwan calendar day already has verified fresh cutoffs, reuse the
verified data instead of rerunning the full refresh.

## Ranking Logic

Use a multi-layer score:

1. Structural bottleneck quality: theme strength, conviction, scarcity, and
   company exposure.
2. Growth confirmation: monthly revenue YoY, 3-month YoY trend, acceleration,
   and margin/fundamental support.
3. Price behavior: 20/60/120/252-day trend, drawdown from high, and whether the
   stock is extended.
4. Valuation discipline: DCF + PEG blend as the formal overlay; reverse DCF as
   an overpay guardrail.
5. Tradability: liquidity, special category flags, extreme concentration, and
   practical slippage risk.
6. Plan quality: target upside, stop distance, risk/reward, entry condition, and
   the clarity of thesis invalidation.

The final recommendation is not the highest single score. It should reflect the
best balance of thesis quality, valuation, confirmation, and risk.

## Pre-Trade Plan

For each ranked name, include:

- current price: latest tradable raw close, or live/near-live quote if the user
  explicitly asks for current-market analysis;
- target price: valuation-derived base target and, when useful, a bear/base/bull
  range;
- stop-loss price: a concrete price on the same raw-price scale;
- invalidation stop: the business or thesis event that would override the price
  plan;
- entry plan: buy-now, buy-on-pullback, breakout confirmation, or watch-only;
- risk/reward: `(target price - current price) / (current price - stop price)`.

If risk/reward is below 2:1, downgrade the recommendation unless there is a
clear catalyst and the report explicitly explains why the setup remains
acceptable.

## Recommendation Status

Use these labels:

- `推薦`: thesis is strong, valuation is acceptable, and data confirmation is
  present.
- `分批`: attractive but entry timing or volatility risk argues against one-shot
  buying.
- `觀察`: thesis is promising but valuation, confirmation, or timing is not yet
  sufficient.
- `避免追高`: business may be good, but price or valuation risk dominates.
- `排除`: thesis mapping, liquidity, data quality, or risk fails.

## Global Mode (IB-Tradable Universe)

Use this mode when the user asks for non-Taiwan names or a cross-market report.
The account executes through Interactive Brokers, so the tradable universe is
US, EU (incl. Nordics/UK), JP, KR, HK, and other IB-listed markets.

Differences from Taiwan mode — everything not listed here stays the same
(ranking logic, valuation overlay, pre-trade plan contract, status labels):

1. **Candidate pool**: start from the companion skill
   (`serenity-aleabitoreddit`) — `references/theses.md` for his live universe
   with conviction tiers, `references/track-record.md` for weighting. For names
   he never covered, run the checklist in its `references/methodology.md`.
   Always classify the triggering post type first (new bottleneck thesis /
   reaffirmation / supplier map / victory lap) per that skill's workflow (d) —
   victory laps and supplier lists are research inputs, not entries.
2. **Data checks substitution**: local PostgreSQL/DuckDB only covers TWSE/TPEx.
   For global names use timestamped web quotes, company filings (EDGAR 8-K/10-Q,
   TDnet, KIND, RNS, MFN/Cision for Nordics), and earnings-call transcripts.
   Growth confirmation uses quarterly reports and guidance instead of Taiwan
   monthly revenue — note this weakens confirmation cadence and say so.
3. **Valuation overlay**: same DCF + PEG blend discipline, but compute forward
   inputs yourself from ramp assumptions and call transcripts — do not trust
   stale screener forward-P/E fields for hypergrowth hardware names. The replay
   IC evidence behind the overlay was validated on Taiwan 2025 data; treat it
   as a discipline heuristic elsewhere, not proven edge.
4. **Reflexivity discount**: for any name Serenity has already posted about,
   assume the initial pop is in the price. Check days-since-his-post and
   whether institutions already validated (13F/13G, Nordic short registers).
   Fresh names vetted by checklist deserve more weight than chased echoes.
   Full rules in `references/replication-guardrails.md`.
5. **Execution notes**: state exchange, quote currency, FX exposure, market
   session, and liquidity (many of his names are micro-caps that move 20%+ a
   day; some EU/Nordic lines have thin books). Position-risk math must use the
   quote currency and mention FX where material.
6. **Report shape**: same output contract; add columns/fields for market,
   currency, and days-since-Serenity-post (if applicable).

## Escalation Boundary

If the user decides to place orders, switch to the separate trading/execution
workflow. This skill may summarize recommended tickers and rationale, but it
must not call broker APIs or create live orders.
