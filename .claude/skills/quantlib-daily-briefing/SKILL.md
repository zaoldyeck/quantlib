---
name: quantlib-daily-briefing
description: Use this skill when user requests a daily/morning market briefing (e.g. "daily briefing", "morning report", "今日重點", "盤前掃描", "早報", "今天該注意什麼"). Auto-generates: macro market summary + holdings signals + watchlist changes + key events + news classifier output. Designed to be the user's morning starting point before market opens.
---

# Daily Market Briefing — TWSE/TPEx morning report

A pre-market briefing that pulls together everything a TW investor needs to start their day. Should run < 2 minutes, deliver before 9:00 AM.

## Preconditions

- `research/cache.duckdb` updated within last 24h (if not → suggest `quantlib-data-refresh`)
- User has provided **holdings list** (in chat memory or in `~/portfolio.json`) — if not, ask once
- User has provided **watchlist** (5-20 tickers being tracked) — if not, ask once

## Workflow

### Step 1: Macro snapshot（市場概況）— 5 分鐘
Run in parallel:

1. **Index status**: query `daily_quote` for 加權 / TPEX / 0050 / 0052
   - 昨日收盤、今日 (預測 / 已開盤) 漲跌幅
   - 5d / 20d / 60d trend
2. **External cues**: WebFetch SOX / NDX / SPX 收盤
3. **Currency**: USD/TWD 動向
4. **Big-cap movers**: 加權前 20 大股票漲跌

### Step 2: 持倉訊號（Holdings Signals）— 5 分鐘
For each holding, check (parallel via Bash):

- 昨日漲跌 ± Z%
- 跌破 200d MA？
- 從持倉高點 trailing -X%（如 < -15% 標 ⚠️）
- 月營收 YoY 最近公告（從 `operating_revenue`）
- 重大事件（從 `mops` 或 web search）

For each finding, state:
- ✅ 正常持有
- ⚠️ 觀察（給 1 句具體理由 + 建議下一步）
- 🚨 警示（建議立即 review，invoke `twstock-position-reviewer` 細查）

### Step 3: Watchlist 異動 — 3 分鐘
For each watchlist ticker:

- 是否觸發 entry signal（iter_24 catalyst breakout）？
- 是否有重大 news catalyst 提到該股？
- 量價是否異常（成交量 > 60d avg × 2）？

如有 trigger → 標記 🟢 並建議 invoke `quantlib-stock-deepdive` 細看

### Step 4: 今日重大事件
- 法說會 schedule（從 web 抓）
- 月營收公告日（每月 10 日附近）
- 季報公告 deadline（5/22, 8/21, 11/21, 4/7 next year）
- 除息 / 除權公告（從 `ex_right_dividend`）

### Step 5: News digest
- 用 WebSearch 抓最新台股新聞 ≤ 24h（重大標題）
- 簡短分類：
  - 📈 利多（升評等 / 業績超預期 / 大單）
  - 📉 利空（降評等 / 業績下修 / 重大事件）
  - 🌐 macro（央行、國際情勢、產業政策）

## Output format (Traditional Chinese)

```markdown
# 🌅 Daily Briefing — 2026-XX-XX (XXX 開盤)

## 📊 市場概況
- 加權指數 收盤 XXX (昨 ±Z%)、5d ±X%、20d ±Y%
- 0050 收盤 NT$XXX (昨 ±Z%)
- 國際對標：SOX 昨 ±X%、NDX ±Y%、SPX ±Z%
- 匯率：USD/TWD ±X.X%
- 加權成分前 20 大昨日表現：[...]

---

## 💼 你的持倉狀態（共 N 檔）

### ✅ 正常持有
- **2330** 收 NT$XXX (持倉成本 NT$XXX, +X%)，昨成交量正常，無異常事件
- ...

### ⚠️ 需注意
- **6488** 從持倉高點回跌 -8%（trailing -10% 預警），建議觀察今日收盤
  - 建議下一步：若再跌 -2% 觸發 -10% trailing → trim 50%

### 🚨 警示（建議立即 review）
- **3008** 跌破 200d MA + 月營收 YoY 轉負 → 建議呼叫 `twstock-position-reviewer 3008`

---

## 👀 Watchlist 異動

### 🟢 觸發 entry signal
- **6488** 今日突破 60d 高點 + 量增 1.8 倍 + 月營收 YoY +35%
  - 建議：呼叫 `/quantlib-stock-deepdive 6488` 細看

### 🟡 量價異常（值得追蹤）
- **2454** 昨成交量 = 60d avg × 2.5（無重大新聞，可能法人 rotation）

---

## 📅 今日大事
- 09:00 AM — 月營收公告：[ticker list]
- 14:00 — XXX 法說會
- 收盤後 — XXX 季報公告

---

## 📰 News Digest（最近 24h）

### 📈 利多
- [標題 + 1 句摘要 + 影響股票]
- ...

### 📉 利空
- [標題 + 1 句摘要 + 影響股票]
- ...

### 🌐 Macro / 政策
- [標題 + 1 句摘要]
- ...

---

## 🎯 今日操作建議

基於以上資訊，建議：
1. [優先級 1：必做]
2. [優先級 2：可做]
3. [觀察名單]

---

⚠️ 本 briefing 為自動產出，最終決定權在你。重大決策建議 invoke 對應 agent 細查（`twstock-position-reviewer` / `quantlib-stock-deepdive`）。
```

## 嚴格要求

- **絕對不可** 推薦「all-in」單一持股
- **絕對不可** 在沒查 DB 的情況下報數字
- 持倉建議要附「**為什麼**」（哪個訊號 + 數字）
- News digest 必須附來源（WebSearch 結果）
- 若使用者持倉 / watchlist 為空 → 問一次再執行（不要強製跑空 briefing）

## 自動化 hint

若使用者要每日 8:30 自動執行此 skill：
- 建議用 Claude Code 的 schedule（CronCreate）trigger 每工作日早上 8:30
- Output 寄到 user 偏好的 channel（chat / email / Slack）

## Output language

繁體中文，使用 markdown 格式 + emoji（briefing 場景下 emoji 可幫助快速 scan）
