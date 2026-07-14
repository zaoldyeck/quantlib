# TW Event Engine — the system's single executable strategy (ev_v2_thesis_inst)

> Champion since battle 8 (2026-07-07): the revenue-only thesis stop was
> replaced by the **institutional-distribution exit** (20-day 三大法人 net flow
> negative while the position is below entry). Validated numbers below;
> ev_full_tp60_v2 rows kept for lineage. Live operation runs the LIVE BOOK
> architecture (`references/daily-ops.md`): broker inventory is truth,
> pre-existing holdings are adopted and managed to their own exits.

The backtested, validated Taiwan implementation of this skill's philosophy.
One strategy, no allocator. Full research trail:
`docs/serenity/serenity_event_engine_v1.md` (campaign),
`serenity_engine_trials_ledger.md` (all 44 pre-registered trials),
`serenity_curation_sop.md` (registry production line).

## Architecture (curation × discipline)

- **Curation layer (the alpha)**: human/LLM-maintained thesis registry
  (`research/serenity/registry/thesis_registry_2025.csv`, v2 schema
  with evidence_date / evidence_url / invalidation_criteria / review_by).
  Admission = bottleneck signature, theme-agnostic, bottleneck duration ≥ 12
  months (see curation SOP). Proven: pool-level alpha ~140% CAGR in 2025-26
  even with random in-pool picks; mechanical screening has NO alpha.
- **Discipline layer (the portability)**: monthly-revenue refresh + daily
  monitored exits — take-profit +60% recycle / trailing -20% / absolute -15% /
  time-stop 50d below entry / **institutional-distribution exit (inst_20d < 0
  while below entry; battle-8 champion rule — the fastest structured proxy for
  fact-level bad news)** / revenue thesis stop (3M YoY < 0) — plus regime
  guards: pool median drawdown_252 ≤ -30% → halve new-entry slots and tighten
  trail to 15%; 0050 < MA120 → halt new entries. Entry throttle 3/day,
  position ≤ 20% × ADV20, 10 slots, T+1.

## Run

```bash
# backtest / signal check (registry mode)
uv run --project research python research/serenity/engine.py \
    --start 2025-01-01
# realistic-execution road test
uv run --project research python research/serenity/engine.py \
    --start 2025-01-01 --emit-book ev_v2_thesis_inst --live-revenue
uv run --project research python research/serenity/execution_test.py \
    --variant ev_v2_thesis_inst
# validation battery
uv run --project research python research/serenity/validate.py \
    --variant ev_v2_thesis_inst
# diagnostics (battle 11-13): ablate a score component / retune pe_pen
uv run --project research python research/serenity/engine.py \
    --start 2025-01-01 --ablate inst          # leave-one-out a component
uv run --project research python research/serenity/engine.py \
    --start 2025-01-01 --pe-pen-mode extreme  # pe_pen schedule variant
```

## Validated numbers (data cutoff 2026-07-06, post battle 11-13 scoring)

Scoring = 8 components, each leave-one-out validated (battles 11-13). Numbers
from `validate.py` battery on `ev_v2_thesis_inst`:

| Window | CAGR | Sortino | MDD | DSR | PBO |
|---|---:|---:|---:|---:|---:|
| registry lag0 | **253.3%** | 8.97 | **-18.0%** | 1.00 | 0.48 |
| registry lag90 | 197.7% | 7.29 | -11.8% | 1.00 | 0.64 |
| registry lag180 | 180.0% | 6.29 | -13.3% | 1.00 | 0.78 |
| Realistic (Fubon sim) | **271.6%** | 9.82 | -17.2% (fill 96.9%) | — | — |
| mech_2018 (no curation) | 21.1% | — | -36.2% | 0.46 | — |
| Backcast 2020-23 (9 non-AI themes) | 18.6-26.5% | — | -21.5~-26.3% (0050: 12.4%, -34.0%) | — | — |

Permutation p=0.000 (200 in-pool random draws, median 127.9%), bootstrap CAGR
5% LB +102.5%, Lo-t 4.29, DSR 1.00 @**75 trials**. **PBO caveat**: <0.5 at lag0
(healthy) but 0.64/0.78 at lag90/180 — quarterly folds are sparse; treat long-lag
overfit probability as an open caveat.

**Battle-15 pool (44 names, beneficiaries expelled) revalidation @2026-07-14
cutoff**: lag0 288.8% / Sortino 5.16 / MDD −16.9%; permutation p=0.000 (median
146.7% vs actual 289.7%), Lo-t 4.59, DSR 1.00 @82 trials, bootstrap CAGR 5% LB
+111.4%. **PBO honestly worsened: lag0 0.526 (crossed 0.5), lag90/180
0.594/0.778** — 6 quarterly folds are too sparse for PBO to stabilize; window
depth is the validation's structural weak point (backfill-labeling pilot
pending user approval). Full report:
`docs/serenity/serenity_event_engine_v1_validation_ev_v2_thesis_inst.md`. Expectation setting: 2025-26 magnitude is
the AI supercycle; normal-cycle expectation ≈ beat 0050 by 6-14pp/yr with
shallower MDD. 2020 covid-type short-window themes FAILED under lag stress —
hence the ≥12-month duration admission rule.

## Frozen decisions (do not re-search; ledger battles 2-11)

Exit price params frozen; theme_cap rejected (0/3); full-market buyback channel
rejected (0/3); SBL/foreign-trend score add-ons rejected (0/3); valuation-based
take-profit rejected (PEG-target 0/3, PEG-exit 0/3 — cyclical low-PE trap);
weighting: equal frozen (score 1/3, inverse-ATR 0/3); thesis-stop family:
institutional-distribution exit ADOPTED (2/3, battle 8), stricter revenue
thresholds rejected (lt10 0/3, single-month 1/3, decel40 1/3); slot count 10
frozen (1-3 and 12-30 all rejected, battle 9 — curve peaks at 10);
expectations-gap score tilt rejected (0/3, battle 10 — full-market factor,
not pool-ranking); entry-consistency veto rejected (battle 14, 0/3 —
"institutions selling while price rises" is a winner pattern in this pool;
exit-gate post-pruning beats entry-prior veto); **member-level role purity
ADOPTED (battle 15, 2026-07-14)** — beneficiary members (module makers / ODM
assemblers / theme misfits, 14 names) expelled via registry
`active_until=2026-07-13`, zero engine-code change (pool is defined by the
registry); all-window CAGR gain (lag0 +63.0pp / lag90 +31.7pp / lag180 +4.9pp),
monotonic attribution gradient (owner win-rate 62-69% > enabler 54-62% >
beneficiary 41-56%); owner-only over-purity rejected (0/3 vs no-beneficiary —
enablers are positive contributors); admission now requires the SOP §1.5
member-level three-test + role label in
`research/serenity/registry/member_roles.csv`. **Scoring is now
component-validated (battles 11-13):
leave-one-out proved 8 components earn their place (momentum/conviction/revenue/
adv/inst/pe_pen/pb_pen + the hard filters — filters ARE backtest-supported,
lag0 248→168 without them); theme_count + dd_pen REMOVED as dead weight
(battle 12, no-loss simplification, lag0 248→252.5); pe_pen retune tested and
REJECTED (battle 13 — `extreme` worse on all 3 windows, `off` only helps the
stale-info lag windows at the cost of the live lag0 case; valuation discipline
is a cross-regime prior, kept full). Scoring formula: 8 components, all
individually backtest-backed.** Any new engine change MUST be pre-registered in
`docs/serenity/serenity_engine_trials_ledger.md` (79 trials to date).

## Operating loop (live)

1. Daily sweep (workflow d) feeds the registry per `serenity_curation_sop.md`;
   registry changes are git-committed with evidence dates (the forward audit
   trail that proves curation alpha over time).
2. Monthly (revenue publication ~11th): run engine for the fresh candidate
   list; place/adjust positions per its book, respecting guards.
3. Daily: check exits vs the five rules + guards; execute T+1.
4. Quarterly: registry full review (`review_by` enforcement); regime
   kill-switch fundamentals check (hyperscaler capex, TSM outlook, memory
   pricing, optical backlog).
