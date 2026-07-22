# Serenity Valuation Overlay

## Preferred Framework

Use `DCF + PEG blend` as the formal Serenity valuation overlay.

Reason:

- PEG was the best single valuation factor in the 2025 replay.
- Reverse DCF gap worked better as an overpay guardrail than as the only score.
- The DCF + PEG blend kept strong performance while being less dependent on one
  fragile valuation lens.

## Verified Research Snapshot

Latest validated Serenity valuation replay artifacts showed:

| Method | Role | Result Interpretation |
|---|---|---|
| `dcf_peg_blend_top10` | Preferred formal overlay | Best balance of growth valuation and overpay control. |
| `peg_top10` | Best single factor | Strongest simple valuation factor, but more single-factor fragile. |
| `reverse_dcf_gap_top10` | Risk guardrail | Useful for detecting over-optimistic prices. |
| `dcf_upside_top10` | Supporting factor | Helpful, but depends heavily on DCF assumptions. |
| `gross_profit_yield_top10` | Diagnostic only | Strong replay return but negative IC, so do not use alone as primary ranking logic. |
| `pe_band_top10` | Avoid as primary | Weak or negative predictive support in this Serenity context. |

Key IC evidence from the replay:

- `valuation_peg_score`: positive 63d and 126d IC.
- `valuation_dcf_peg_blend`: positive 63d and 126d IC.
- `valuation_reverse_dcf_gap`: positive 126d IC.
- `valuation_pe_band_score`: not suitable as the main decision rule.

## Practical Rules

- Do not buy only because a stock is cheap; Serenity requires structural demand.
- Do not buy only because a stock has strong growth; price can already discount
  the thesis.
- Treat extreme valuation as a position-sizing or wait-for-pullback warning.
- For high-growth bottleneck stocks, prefer "reasonable enough" valuation over
  classical low PE/PB cheapness.
- If valuation and thesis disagree, explain the conflict and downgrade to
  `觀察` or `避免追高` unless the user explicitly wants high-risk momentum.

## Target Price Method

Target prices must be on the tradable raw-price scale, not adjusted-price NAV
scale.

Use a scenario range:

- Bear target: conservative fair value if growth slows or valuation contracts.
- Base target: the main target used in the ranking table.
- Bull target: upside if the structural bottleneck thesis strengthens.

Default base target construction when replay fields are available:

1. `DCF fair price = raw_close * (1 + dcf_upside)`.
2. `PEG fair price = raw_close * ((supported_growth * 100) / PE)`, using a
   target PEG near 1.0 unless the report explicitly justifies another value.
3. Base target = weighted blend of DCF fair price and PEG fair price, with
   reverse-DCF gap as an overpay guardrail.
4. If reverse DCF says the market already implies growth above supported growth,
   cap the target, downgrade the status, or mark the stock as `避免追高`.

When inputs are missing or unstable, state assumptions directly and use a range
instead of pretending the target is precise.

## Stop-Loss Method

Every recommendation must include both:

- price stop: a concrete stop-loss price;
- thesis stop: the event or evidence that invalidates the structural bottleneck.

Default price-stop construction:

1. Estimate a volatility stop from recent ATR or realized volatility.
2. Compare with nearby support such as recent swing low or moving-average break.
3. Apply a maximum-loss guardrail appropriate to the stock's volatility.
4. Use the stop that best represents thesis invalidation without making the
   setup mechanically too tight.

If the stop distance is so wide that position risk becomes unacceptable,
downgrade the stock to `觀察` even if the upside target is attractive.
