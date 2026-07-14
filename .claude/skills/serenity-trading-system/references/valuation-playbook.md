# Valuation Playbook — how Serenity prices a name

Distilled 2026-07-03 from his own posts (418 valuation-related tweets in the
full corpus). He never uses DCF-to-terminal-value or trailing multiples. The
stack has four layers — use the highest layer the data allows, and show the
arithmetic in any report.

## Layer 1 — Capacity × ASP forward revenue build

For companies with a published capacity ramp. Take the company's stated
capacity timeline, multiply by third-party ASP data (LightCounting, Dell'Oro,
Yole for optics; TrendForce for memory), apply GAAP gross margin, and compare
the resulting forward run-rate to market cap.

Worked example (AAOI, 2026-03-18): capacity plan → Q2 2026 ~$312M, Q4 2026
~$1.41B, Q4 2027 ~$1.97B quarterly revenue at 34-40% GM vs $6.49B MC →
"very undervalued". He flags it himself: contract prices are unknown, so this
is speculative — state the same caveat.

## Layer 2 — BOM unit economics × downstream volume

For component suppliers. Price-per-unit × units-per-system × downstream system
forecast → revenue → forward P/E at the target year.

Worked example (Nextronics 8147, 2026-05-13): CPO connector $15-25 + thermal
cage ~$50, 18 cages + 72 optical engines per switch ≈ $2,000 content/switch ×
NVDA Spectrum-X volumes → ~2x 2028 forward P/E → "10x re-rating room".
Variant (VPG): $-content per humanoid × unit targets vs current MC.

## Layer 3 — Contract ARR / deserved-multiple re-rating

For names with signed contracts or consensus forward numbers.

- Contract conversion (NBIS): $17B/5y MSFT contract → 2026 revenue $5-6B at
  ~70% GM; sum-of-parts with a 40% haircut on non-core → base value vs MC.
- Deserved multiple (MU): fwd P/E 11.6 with +133% rev / +319% EPS growth =
  "priced like a slow-growth commodity" → assign a deserved multiple (20x) →
  target. For Korea/Japan he lifts sell-side forward P/E tables directly.
- Rule: for hypergrowth hardware, NEVER trust screener forward-P/E fields —
  compute forward earnings yourself from ramp assumptions and call
  transcripts. Read sell-side reports only to harvest non-public nuances,
  then "derive your own MC projection" (their PTs are noise).

## Layer 4 — Cross-chain comps, historical templates, strategic value

The chokepoint-specific layer; least rigorous, biggest driver of his targets.

- Cross-chain comps: upstream maker's MC vs downstream repackagers' MC
  (SIVE $200M while companies that buy-and-repackage its lasers trade $1-4B;
  AAOI one-stop US fab at $5.6B vs FN $20B assembly-only at 12% GM).
- Historical template anchors: "the next LITE" ($3B → $65B through the EML
  bottleneck), AXTI ≈ NAND-style bottleneck re-rating, FLY priced off RKLB
  multiples. Pick the last-cycle company that occupied the same chokepoint.
- M&A anchor: MRVL paid $5.5B for Celestial with $0 revenue until 2028 —
  strategic buyers set the floor for scarce assets.
- Game-theory cap (his own words on AXTI at $4B): "Is it overvalued by
  traditional metrics? Yes. Would hyperscalers pay $10B to secure their AI
  buildout? Yes. These bottlenecks can't be valued with traditional metrics."
  The ceiling is what the trapped downstream would pay, not an earnings
  multiple. Use as an upside scenario, never as the base case.

## Target-price discipline

- Every PT carries a timeframe and gets re-rated on new information ("re-rating
  it weekly on new news"). Historical format: NBIS $92 → PT $400/1Y; RKLB $43
  → $500/5Y; CRCL $72 → $150/8M.
- Maintain buy/hold/sell price bands per name (NBIS: <$145 strong buy /
  $145-170 hold / $170+ hold-sell — and he sells covered calls at the band
  ceiling instead of dumping shares).
- Present bear / base / bull targets when inputs are uncertain; say which
  layer produced each number.

## Overvaluation calls (his bear side)

- >NAV closed-end / ETF premiums = "greater fools" (VCX at ~20x NAV) — avoid,
  never short (low float).
- Cult stocks (PLTR interest-income earnings, TSLA at stretch) — avoid, never
  short ("regular people put paychecks in without looking at fundamentals").
- Earnings quality traps: SBC-funded buybacks, non-GAAP margin
  cherry-picking, carry-forward-loss distortions (RDDT true margin ~28% after
  stripping) — re-rank on GAAP before comparing.
