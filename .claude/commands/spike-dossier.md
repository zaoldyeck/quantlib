---
description: Produce a full dossier on a single historical price spike — why it happened (news/fundamentals/flow), what pre-event features flagged it, post-peak behavior, and what cluster of past spikes it resembles. Arguments: <ticker> <approx_spike_date>
---

Generate a deep-dive dossier on a specific TWSE/TPEx stock spike event. This is meant for reverse-engineering alpha — "why did THIS one spike?" — using all available data sources (quant + news).

User arguments:

$ARGUMENTS

Expected format: `<4-digit ticker> <YYYY-MM-DD start date>` (e.g. `3138 2020-12-11`).

## Workflow

### Step 1: Locate the event

1. Query `research/cache.duckdb` for this ticker around the given date
2. Compute actual 60-trading-day return from the given start date → identify peak_date
3. Verify this qualifies as a spike (gain ≥ 50% over ≤ 90 trading days)
4. If not, suggest the nearest spike event for this ticker (search ±30 trading days)

### Step 2: Pre-event snapshot (T-60 to T-1 from start_date)

Pull from cache in parallel:
- Price: cumulative return, trading range, consolidation metric
- Volume: 5d vs 60d surge ratio, trade_value range
- Institutional flow: foreign + trust net buy, daily and 20d cumulative
- Margin / short: 融資餘額 / 融券餘額 change trajectory
- Fundamental: latest PIT-safe 月營收 YoY, YoY 加速度, 3-month avg
- Valuation: PE / PB / dividend yield at T-1 vs 3-year percentile rank
- Peer: 同產業 60-day return percentile

### Step 3: News / event trace (T-60 to peak_date)

Use WebSearch and WebFetch to find:
- TWSE MOPS 重大訊息 in the window (company official disclosures)
- Financial media coverage: 鉅亨網 / 經濟日報 / 工商時報
- Industry context: 同業同時期是否也漲（題材 vs 個股）

Classify found news into event types:
- M&A / 併購 / 借殼
- 重大訂單 / 客戶突破
- 產品發表 / 技術突破 / 專利
- 財報超預期 / 月營收大增
- 法說會上修 / 下修
- 大股東增持 / 庫藏股 / 申報轉讓
- 產業政策利多 / 補貼 / 關稅
- 純籌碼面 (無對應公告)

### Step 4: Post-peak behavior

- Compute post_peak_ret_{5,21,63}d from peak_date
- 標記 peak 後是「續漲」(+> 10%)、「橫盤」(-10% ~ +10%)、「反轉」(< -10%)

### Step 5: Cluster with similar past spikes

Query `research/experiments/spike_dataset.parquet`:
- 過濾同產業的其他 spikes in last 5 years
- 找 10 個 pre-event features 最相似的 case（歐氏距離或 cosine）
- 這些類比案例 peak 後的平均報酬 = 本案的 reference 參考

### Step 6: Dossier output (Traditional Chinese)

```
# Spike Dossier: <公司名> (<ticker>) @ <start_date>

## Summary
- Gain: +X% in N 交易日 (<start_date> → <peak_date>)
- 事前 60 日 return: +Y%（若 Y > 20% 表示已在上升段）
- Post-peak: 續漲 / 橫盤 / 反轉

## Why did it spike (event-type verdict)
- Primary driver: <M&A / 訂單 / 產業題材 / 純籌碼...>
- Secondary factors: <...>
- 關鍵新聞時間線 (bullet list with dates)

## Quantitative pre-event snapshot (vs universe distribution)
- 月營收 YoY: X% (universe 同期 percentile)
- 法人 20d 流向: ±$X (強度 percentile)
- 量能 5d/60d: X (universe 同期 percentile)
- 融資變化 20d: X%
- RSV 60d: X
- 距 52w 高: X%
- 同業 relative strength: X percentile

## Classification
- Event-type cluster: <M&A / 財報爆發 / 產業輪動 / 籌碼集中 / 純投機>
- 10 個最相似歷史案例 → 平均 post-peak 21d/63d return

## Alpha take-away
- 若本案屬 <某類型>，該類型歷史上:
  - 發生頻率: X 次 / 年
  - Pre-event 可辨識訊號: <...>
  - Post-peak 續漲機率: X%
  - 操作建議: <predictable / chase-only / avoid>
```

## Anti-patterns

- 禁止只做 quant 分析跳過 news trace — 真 alpha 藏在「為什麼漲」的解釋裡
- 禁止對搜尋不到新聞的案例直接標「純籌碼」—— 必須先確認搜尋涵蓋了 MOPS + 主流財經媒體
- 禁止用訓練資料記憶的新聞當證據 — 所有新聞必須有可驗證的 URL + 日期

若找不到 spike 事件，告知使用者並建議最近的真實 spike；不要硬編造。

Respond in Traditional Chinese.
