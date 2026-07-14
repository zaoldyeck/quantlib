---
name: serenity-trading-system
description: >-
  Use when the user wants Serenity-style (@aleabitoreddit) supply-chain
  bottleneck investing in any IB-tradable market (US, EU, JP, KR, HK, TW) —
  「跑每日 loop」「每日管理/檢查持倉」「產生下單計畫」「Serenity 選股」
  「Serenity 交易系統/觀點」「結構性瓶頸股」「找下一個 AXTI」「這是不是真瓶頸」
  "what would Serenity think"; running the daily ops loop (data refresh, live
  book, order-plan generation, brief review); vetting a ticker as a chokepoint;
  ranked buy/watch reports with price/target/stop; adopting or reviewing
  existing holdings; reacting to a fresh Serenity post; or the daily
  supply-chain news sweep. Research and pre-trade planning only — order
  submission is always the user's own human-gated step.
---

# Serenity Trading System (Claude Code edition)

The complete trading system of Serenity (@aleabitoreddit, ex-Reddit u/AleaBito,
~920k followers) — **Chokepoint Theory**: trace hyperscaler AI capex upstream
through the bill of materials to the small-cap sole-source node the buildout
cannot route around, enter at qualification stage before institutions price it
in, and hold through the re-rating. Self-contained in this directory: vendored
distillations of his 5,969-tweet corpus plus local playbooks and guardrails
from an independent 2026-07-03 full-corpus study. Attribution: vendored files
derive from github.com/yan-labs/serenity-aleabitoreddit (regenerated from his
live feed ~every 30 min).

His returns are self-reported and unverified; independent re-scoring of his
dated public calls ≈ 61% 30-day directional accuracy (~75-85% for mature
bottleneck theses). This skill copies his *method*, and deliberately does NOT
copy his risk posture — see Guardrails.

## Step 0 — freshness

Theses and track record decay. Before relying on them:

```bash
.claude/skills/serenity-trading-system/scripts/update_from_upstream.sh
```

If offline, use the cached copies and state that they may be stale. Taiwan
names may additionally use the project PostgreSQL/DuckDB stack (state the data
cutoff); all other markets use timestamped web quotes and filings.

## System map

| Stage | What happens | File |
|---|---|---|
| 1. Discover | Daily/weekly supply-chain sweep, 5 source layers | `references/info-sources.md` |
| 2. Vet | 14-principle lens + universal checklist on any name | `references/methodology.md` |
| 3. Look up | His live per-ticker stances, conviction tiers, universe stats | `references/theses.md`, `references/ticker-stats.txt` |
| 4. Weight | Dated calls, outcomes, calibration bands | `references/track-record.md`, `references/articles.md` |
| 5. Price | 4-layer valuation stack, PT bands, bear/base/bull | `references/valuation-playbook.md` |
| 6. Enter / size / exit | Forward clock, sleeves/blocks, six sell triggers + mandatory stops | `references/entry-exit-sizing.md` |
| 7. Overlays | Reflexivity discount, regime kill-switch, do-not-copy list | `references/replication-guardrails.md` |
| 8. Execute (TW) | The validated single-strategy engine (ev_v2_thesis_inst): registry curation × event exits × regime guards, 8-component score (battles 11-13 ablation-validated), run/validate commands, live operating loop | `references/tw-event-engine.md` |
| 9. Daily ops | The complete daily management loop: mechanical stage (`research/serenity/daily.py`), Claude judgment checklist (news → thesis health → overrides → plan review), user trigger stage, fail-closed fuses, weekly/quarterly chores | `references/daily-ops.md` |

Provenance for any claim: `analysis/*.md` (period-by-period distillations).
For Taiwan the philosophy is OPERATIONAL: registry curation follows
`docs/serenity/serenity_curation_sop.md` (bottleneck-signature
admission, theme-agnostic — AI is the current instance, not the definition),
and the engine reference above is the executable strategy.

## Architecture sovereignty(血統原則,2026-07-07 使用者敕令)

本系統有自己的血統。當 Serenity 的需求與 quantlib 舊慣例衝突(calendar 排程、
「只存量化資料」教條、月頻節奏、目錄習慣),**一律改造專案去服務 Serenity,
絕不反向把 Serenity 折彎去遷就舊框架**。已執行的先例:Scala 爬蟲為公布窗
營收改造成事件驅動;`research/serenity/` 獨立成系統自己的家;live book 以
券商庫存為真相收養既有持股。文字/質性資訊採 **fetch-on-demand**:決策當下
抓、當下判斷(WebFetch 優先,登入牆/JS 頁改用使用者已登入的 Chrome via
claude-in-chrome——使用者已授權);永久儲存只有首見時間戳 parquet、策展
蒸餾(註冊表 evidence/論點註記/watch log)與量化資料,**不建原文語料庫**
(詳見 `references/info-sources.md` 文字資料政策)。

## Non-negotiable guardrails

Read `references/replication-guardrails.md` before giving sizing, margin,
holding, or exit advice. Hard rules, no exceptions:

1. Every recommendation carries a **price stop AND a thesis stop**; his
   no-stop-loss style is not reproducible without his information flow.
2. **No leverage** in recommendations; single-name and theme caps per the
   user's risk budget (his 30-50% overrides and 1.4x margin are do-not-copy).
3. **Reflexivity discount**: for any name he already posted about, record
   days-since-post, classify the post type (new thesis / reaffirmation /
   supplier map / victory lap), and check whether institutions already
   arrived. Same-week chases default to 避免追高.
4. **Regime kill-switch**: if 2+ AI-capex leading indicators deteriorate
   (hyperscaler capex guidance, TSM outlook, memory pricing, optical
   backlog), freeze new theme adds and tighten stops.
5. Risk/reward at entry ≥ 2:1 against the base target, else watch-only.
6. Active large ATM / serial dilution / SBC-alongside-raise = disqualified.

## Workflows

### (a) Vet one ticker

1. Refresh (Step 0). Look it up in `references/theses.md`; note stance,
   conviction tier, evolution, reversals (IREN, CRWV, POET all flipped).
2. Not covered → run the checklist at the bottom of
   `references/methodology.md` on fresh evidence (filings, transcripts,
   partner pages).
3. Price it with `references/valuation-playbook.md` (state which layer
   produced each number); weight his view via `references/track-record.md`.
4. Apply guardrails 3-6. Output: supply-chain map, bull/bear, bear/base/bull
   targets, price + thesis stop, R:R, verdict (推薦/分批/觀察/避免追高/排除).

### (b) Ranked recommendation report (default 20 names — 2026-07-09 使用者指令)

**固定輸出 20 檔**:第 1–10 名 = 等權執行簿(實際買進;戰役九凍結),第 11–20 名 =
補位板凳(席位空出時依當下分數遞補)。一覽總表 + 每檔詳細區塊都要涵蓋全部 20 檔;
既有名字重用上一版敘事(只更新量化表格與名次),**只有新進榜的名字才做完整
資料蒐集與敘事**(fetch-and-extract 用低階模型,判斷用預設模型)。

1. Refresh; confirm market scope with the user only if genuinely ambiguous
   (default: global IB universe, TW included).
2. Candidate pool: current high-conviction names from `references/theses.md`
   + fresh checklist-passed names from the latest sweep. Prefer
   thesis-first candidates over numeric screens.
3. Score: bottleneck quality → growth confirmation (quarterly guidance;
   monthly revenue for TW) → valuation (playbook) → tradability (liquidity,
   float, currency) → plan quality (R:R, stop clarity).
4. **Fund-manager grade(2026-07-07 使用者指令):報告讀者是基金經理人,
   必須讓他讀完充分知道「為什麼買這檔」。** 結構:(i) 組合層敘事——這十檔
   合起來買的是什麼故事、共用失效條件、集中度誠實聲明;(ii) 每檔:論點敘事
   (供應鏈位置、為什麼是瓶頸)→ 為什麼是現在(營收/法人/動能證據數字)→
   風險與失效條件(具體、可觀察)→ 完整交易計畫(進場價、止盈/止損/trailing
   ／time／法人／營收六道門的規則價位）。量化表格輔佐敘事，不取代敘事。
   每檔標明**上市／上櫃**（依 daily_quote 最新交易日的 market 判定，轉板股
   以最新為準）。報告開頭必附「如何使用本報告」：語氣梯度＝風險地圖（每檔
   最可能先觸發哪道出場門），**不是買入權重**；執行＝等權前十（戰役七／九
   凍結決定；歷史第 2、3、6 大金主正是敘事警示最重的名字，單筆勝率僅約 54%，
   alpha 在不對稱出場 × 廣度）。中文排版遵循全域 CLAUDE.md 0.5 節。
   Every price carries a source and timestamp.
5. **超連結 reference 必附(2026-07-07 使用者指令)**:凡質性/敘事段落引用的
   外部事實(瓶頸地位、法說催化、營收數字、風險事件),必須在報告附上**可點擊
   的來源 URL**(markdown 超連結),每檔至少 2–3 條(一條佐證瓶頸地位、一條佐證
   近期催化)。做法:報告產製時對 11+ 名(或全部)跑來源收集 workflow(每檔一
   agent WebSearch 回傳 `{title,url,verifies}`),把連結置於各檔區塊末的「資料
   來源」列或報告末的參考清單。**嚴禁虛構 URL**——只放搜尋結果實際出現的網址,
   無法確認寧可少放並註明。量化數字(價/量/PE/法人)註明來自本地 PIT DB + cutoff 日期。
6. Label sub-2:1 or chased names 觀察/避免追高 rather than forcing 10 buys.

### (c) React to a fresh Serenity post

Classify first (guardrail 3), then: new bottleneck thesis → run workflow (a)
promptly (validation lag is typically 5-60 trading days — thorough beats
fast); reaffirmation → re-verify his mechanism against externals before
averaging; supplier map / no-position → watchlist input only; victory lap →
no action, the mispricing is gone.

### (d) Daily supply-chain sweep

Run the Layer 0-4 pipeline in `references/info-sources.md`; append dated
findings to a watch log; promote candidates into workflow (a). Pairs with the
`quantlib-daily-briefing` skill for scheduled mornings.

### (e) Archive mining / event study

`scripts/update_from_upstream.sh --with-archive` drops the full tweet JSON
into `research/external/serenity-archive/` (self-gitignored). Join tweet
timestamps with local `daily_quote` (TW) or web price history to measure
post-tweet drift/reversal before trusting any copy-trade pattern.

## Output contract

Respond in Taiwan Traditional Chinese unless asked otherwise. Ranking/verdict
first, methodology second. Every price carries a source and timestamp; every
fundamental claim carries a filing/transcript date; every vendored-thesis
citation notes its as-of date. State market mode, currency, and FX exposure
where material. Never present a recommendation as guaranteed profit.

## Boundaries

Research and pre-trade planning only: no broker API calls, no order
placement/modification/cancellation, no portfolio-ledger mutation. If the
user decides to trade, hand off to their own execution process. This is
decision support, not financial advice; his self-reported performance is
unverified and survivorship-biased.
