# Daily Supply-Chain Information Pipeline

Serenity's "edge" inputs are ~80% public. This file lists the sources by layer,
with cadence and automation notes, so the sweep can be run manually or wired
into a scheduled agent (see Automation at the bottom). What is NOT replicable:
his synthesis speed, domain fluency, and DM network — compensate with the
checklist and slower validation windows (5-60 trading days), not with haste.

## Layer 0 — Serenity feed itself (half-hourly to daily)

| Source | What | Access |
|---|---|---|
| `npx -y skills update serenity-aleabitoreddit -y` | Refresh vendored theses/track-record distilled from his live feed (upstream regenerates ~every 30 min) | Free, CLI |
| https://x.com/aleabitoreddit | Raw feed incl. replies (position disclosures often live in replies) | Browser (logged-in) |
| https://github.com/yan-labs/serenity-aleabitoreddit | Full tweet archive `data/aleabitoreddit_tweets.json` for text-mining / event studies | Free, git pull |

## Layer 1 — Company primary sources (event-driven, same-day)

- **US**: SEC EDGAR full-text search + 8-K/6-K RSS per ticker (free,
  real-time). Read for: contracts, prepayments, ATMs/dilution, lockups.
- **Earnings/AGM**: call transcripts (company IR, Motley Fool free transcripts,
  Seeking Alpha), AGM decks, capital-market-day slides. He mines OTHER
  companies' calls for supplier read-throughs (e.g. POET AGM → LITE sold out).
- **Non-US filings**: MOPS (TW — already in local DB), TDnet (JP), KIND (KR),
  RNS (UK), MFN/Cision (Nordics — Sivers announcements), HKEX news, 巨潮 (CN).

## Layer 2 — Trade press & pricing data (daily)

- **Digitimes** (TW; his most-cited supply-chain source; paid but headlines
  free), 工商時報/經濟日報/UDN 產業版, Commercial Times.
- **TrendForce / DRAMeXchange**: DRAM/NAND spot & contract pricing (memory
  regime input).
- **SMM 上海有色網**: indium, gallium, high-purity materials pricing (his AXTI
  input; partial free tier).
- **LightCounting / Dell'Oro / Yole**: optical ASP & volume forecasts (paid;
  press releases free — enough for ASP sanity checks).

## Layer 3 — OSINT diffs (weekly; the highest-alpha layer)

- **Partner-page diffs**: startup/vendor "partners" and "customers" pages via
  Wayback Machine snapshots — additions AND removals (Ayar removing
  LITE/MTSI → SIVE inference). Fully automatable (fetch + diff).
- **LinkedIn**: job postings (capacity ramps, new fab roles), exec moves,
  engineer post slips. Manual browsing; do not scrape.
- **US import records**: bills of lading (ImportGenius/Panjiva paid; free
  partial mirrors) — he found LPK↔SpaceX this way.
- **Conference materials**: OFC/GTC/Computex/CES/SEMICON exhibitor lists,
  session titles, slide photos posted on X.
- **Gov/policy**: BIS export-control actions, CHIPS/IPCEI award lists, MOFCOM
  export-license announcements (China chokepoint triggers).

## Layer 4 — Flow & positioning (lagging validation, weekly)

- 13F/13G/SC filings (WhaleWisdom), Nordic short registers
  (FI blankningsregistret — SIVE shorts are public), TW 三大法人 (local DB),
  borrow rates/utilization. Use to confirm "institutions arriving", never as
  the discovery signal — by 13F time the mispricing is mostly gone.

## Automation in this repo

- **Scheduled sweep**: a daily cron/scheduled agent can (1) refresh the
  companion skill, (2) run WebSearch over Layer-2 headlines for the active
  theme keywords (CPO, InP, HBM4, glass substrate, humanoid actuators…),
  (3) fetch EDGAR RSS for held/watch tickers, (4) diff tracked partner pages,
  and append findings to a watch log. Integrates naturally with the
  `quantlib-daily-briefing` skill.
- **Event studies**: clone the GitHub archive into
  `research/external/serenity-archive/` (gitignored) and join tweet timestamps
  against `daily_quote` to measure post-tweet drift/reversal for TW names
  before trusting any copy-trade impulse.
- **Honest limits**: paid research (Digitimes full text, LightCounting,
  import records) and expert-network knowledge stay out of reach; treat
  conclusions needing them as lower-confidence and say so in reports.
