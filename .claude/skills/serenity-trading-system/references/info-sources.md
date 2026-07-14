# Daily Supply-Chain Information Pipeline

Serenity's input edge is ~80% public. This file lists the sources by layer with
cadence and automation notes. What is NOT replicable: his synthesis speed,
domain fluency, and DM network — compensate with the methodology checklist and
the 5-60 trading-day validation window, not with haste.

## Layer 0 — Serenity feed itself (half-hourly to daily)

| Source | What | Access |
|---|---|---|
| `scripts/update_from_upstream.sh` | Refresh this skill's vendored theses / track-record / methodology (upstream regenerates from his live feed ~every 30 min) | Free, one command |
| https://x.com/aleabitoreddit | Raw feed incl. replies — position disclosures often live in replies | Browser (logged-in session) |
| https://github.com/yan-labs/serenity-aleabitoreddit | Full tweet archive (`data/aleabitoreddit_tweets.json`, ~7.8MB) for text mining and event studies — fetch via `update_from_upstream.sh --with-archive` | Free |

## Layer 1 — Company primary sources (event-driven, same-day)

- **US**: SEC EDGAR full-text search + per-ticker 8-K/6-K RSS (free,
  real-time). Read for contracts, prepayments, ATMs/dilution, lockups, SBC.
- **Earnings/AGM**: call transcripts (company IR pages, Motley Fool free
  transcripts, Seeking Alpha), AGM decks, capital-market-day slides. Mine
  OTHER companies' calls for supplier read-throughs — e.g. POET's AGM revealed
  Lumentum sold out into 2029.
- **TW 法說會全內容鏈(2026-07-07 定案)**:行事曆+簡報 PDF 已自動進每日 brief
  (`research/serenity/daily.py`,MOPS t100sb02_1)。簡報之外的內容層:
  **AlphaMemo**(alphamemo.ai/free-transcripts — 台股法說逐字稿+摘要;登入牆
  → 用使用者的 Chrome)與 **富果法說會備忘錄**(blog.fugle.tw/topic/earnings-call-memo
  — 圖解財報+營運展望+法說 QA 筆記,旺季近乎日更,**覆蓋中小型股**——正是
  read-through alpha 所在),再加公司 IR 回放與隔日工商/經濟記者稿交叉。
  讀法一律挖 **supplier read-through**,不只看發表公司本身。
- **Non-US filings**: MOPS (TW — already in the local DB), TDnet (JP), KIND
  (KR), RNS (UK), MFN/Cision (Nordics — Sivers announcements), HKEX news,
  巨潮資訊 (CN).

## Layer 2 — Trade press & pricing data (daily)

- **Digitimes** (his most-cited supply-chain source — a Taiwanese outlet;
  headlines free, full text paid), 工商時報 / 經濟日報 / UDN 產業版,
  Commercial Times.
- **富果研究部(blog.fugle.tw,免費)**:`topic/industry-analysis` — 台股
  供應鏈深度報告(實例:CoPoS 供應鏈解析、FPC 漲價 38%、記憶體格局變化、
  AI 電力瓶頸)——**直接的註冊表策展養料**,每篇都用瓶頸署名檢核一遍;
  `topic/stock-analysis` — 中小型股深研(金居、南俊國際、新應材…)——
  候選發掘 + 論點驗證素材。
- **TrendForce / DRAMeXchange**: DRAM/NAND spot & contract prices (memory
  regime input).
- **SMM 上海有色網**: indium, gallium, high-purity materials pricing (his
  AXTI input; partial free tier).
- **LightCounting / Dell'Oro / Yole**: optical ASP and volume forecasts
  (paid; press releases free — enough for ASP sanity checks).

## Layer 3 — OSINT diffs (weekly; the highest-alpha layer)

- **Partner-page diffs**: vendor/startup "partners" and "customers" pages via
  Wayback Machine snapshots — additions AND removals (Ayar removing LITE and
  MTSI, leaving only SIVE, preceded any press release). Fully automatable
  (fetch + diff).
- **LinkedIn**: job postings (capacity ramps, new-fab roles), exec moves,
  engineers' post slips. Manual browsing; do not scrape.
- **US import records**: bills of lading (ImportGenius / Panjiva, paid; free
  partial mirrors exist) — how he linked LPK to SpaceX.
- **Conference material**: OFC / GTC / Computex / CES / SEMICON exhibitor
  lists, session programs, slide photos posted on X.
- **Government/policy**: BIS export-control actions, CHIPS / EU IPCEI award
  lists, MOFCOM export-license announcements (China chokepoint triggers).

## Layer 4 — Flow & positioning (lagging validation only, weekly)

13F/13G/SC filings (WhaleWisdom), Nordic short registers (Finansinspektionen
blankningsregistret — SIVE shorts are public), TW 三大法人 (local DB), borrow
rates/utilization. Use to CONFIRM "institutions arriving" — never as the
discovery signal; by 13F time the mispricing has mostly compressed.

## 文字資料政策 — fetch-on-demand(2026-07-07 定案)

質性/文字資訊一律**當下抓、當下判斷**,不建原文語料庫。永久儲存只有三類:

1. **首見時間戳**(`confcall_events.parquet`、`revenue_first_seen.parquet`)——
   未來 event study 的 PIT 骨架;是量化 metadata,不是文字。
2. **策展蒸餾**(註冊表 evidence_date/evidence_url/引句、live book 論點註記、
   watch log、override 理由)——git commit 即時間戳。**這才是 Serenity 式的
   資產:他的 edge 是論點筆記,不是文字存檔。**
3. 量化資料(現行 pg/DuckDB 管線,維持不變)。

理由:逐字稿 NLP 無截面排名價值(已測,舊 Task #86/87 結論仍成立),原文庫
沒有排名因子就沒有回測用途;判斷時新鮮抓取永遠優於陳舊存檔;MOPS/來源站
自己保存原文。唯一例外:作為 live 部位失效證據的關鍵文件,把引句+URL 寫進
論點註記即可(幾乎不需要存原檔)。

**抓取工具鏈**:WebFetch 優先;登入牆/JS 重的頁面 → 使用者已登入的 Chrome
(claude-in-chrome,使用者已授權此用途)。付費牆內容不翻牆,結論標註
lower-confidence。
**模型分級(token 經濟,2026-07-09)**:純量化資料一律直接 DuckDB/Bash,
**不開 LLM agent**(零 token);網頁抓取+萃取型子任務(fetch-and-extract,無判斷)
用最低階模型(haiku / effort low),不影響品質;**判斷型**(瓶頸檢核、論點撰寫、
read-through)維持預設高階模型。能用 1–2 次 inline WebSearch 解決的,不開 workflow。

## Automation in this project

- **Daily sweep** (manual or scheduled agent): (1) run
  `scripts/update_from_upstream.sh`; (2) WebSearch Layer-2 headlines for the
  active theme keywords (CPO, InP, HBM4, glass substrate, humanoid actuators,
  WF6, MLCC…); (3) fetch EDGAR RSS for held/watch tickers; (4) diff tracked
  partner pages; (5) append findings to a dated watch log. Pairs naturally
  with the `quantlib-daily-briefing` skill.
- **Event studies**: `update_from_upstream.sh --with-archive` drops the full
  tweet archive into `research/external/serenity-archive/` (self-gitignored);
  join tweet timestamps against local `daily_quote` for TW names to measure
  post-tweet drift/reversal before trusting any copy-trade impulse.
- **Honest limits**: paid research (Digitimes full text, LightCounting,
  import-record databases) and expert-network knowledge stay out of reach.
  Mark conclusions that would need them as lower-confidence.
