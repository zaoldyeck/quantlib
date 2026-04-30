---
name: quantlib-spike-study
description: Use this skill when the user wants to study historical price spikes / 暴漲股 / 飆股 for alpha research (e.g. "暴漲股研究", "找出這幾年漲最多的股票", "分析事前徵兆", "spike event study", "為什麼這支股票會噴"). Builds the full reverse-engineering pipeline — find spikes → extract pre-event features → run post-peak continuation analysis → optionally trace news — and interprets against memory of past spike findings. Meant to seed new alpha discovery, not backtesting.
---

# Spike event study workflow (reverse-engineering暴漲股)

Purpose: start from "which stocks 暴漲 in the past?" and work backward to find pre-event predictors. Hypothesis track: if predictable → build signal; if not predictable → build chase + trailing-stop.

## Preconditions

- `research/cache.duckdb` fresh (< 24h). If stale, advise `quantlib-data-refresh`.
- Cache must have `daily_quote`, `ex_right_dividend`, `capital_reduction`, and ideally `operating_revenue`, `daily_trading_details`, `margin_transactions` (for pre-event features).

## Memory-first reference

Read FIRST:
- `project_strategy_research_findings.md` — known factor IC table; avoid re-testing dead factors
- `project_data_bug_history.md` — if top spikes include a fixed-bug signature (e.g. 2024-10-22 partial publish), drop them
- `project_data_real_edge_cases.md` — e.g. financial sector negatives are real, not bugs

## Step 1: Run spike detector

```bash
cd /Users/zaoldyeck/Documents/scala/quantlib && \
  uv run --project research python research/audits/01_find_spikes.py \
    --min-gain 0.80 --window 60 --start 2005-01-01 --end 2026-12-31
```

Parameters:
- Default `--min-gain 0.80 --window 60` = "gained ≥ 80% in 60 trading days" (~1000 events for TW 2015-2026)
- Adjust per user intent: `--min-gain 1.0 --window 90` for stricter spikes

**Sanity check top 20 gains**: anything > 5× or unusual (e.g. 27x, 36x) usually = data bug. Cross-ref `project_data_bug_history.md`; if new anomaly, invoke `quantlib-data-auditor`.

## Step 2: Build dataset with pre-window features + post-peak returns

```bash
cd /Users/zaoldyeck/Documents/scala/quantlib && \
  uv run --project research python research/experiments/build_spike_dataset.py
```

Output: `research/experiments/spike_dataset.parquet` with dual anchors:
- `start_date` (rally entry) + pre-window features (for predictive analysis)
- `peak_date` (= start + 60 trading days) + post-peak returns (for continuation analysis)

Features currently included (anchored at start_date T, all PIT-safe ≤ T-1):
- revenue_yoy_latest / revenue_yoy_3m_avg / revenue_accel (月營收加速度)
- institutional_flow_20d (外資+投信 20 日累積淨買)
- volume_surge_60d (5d 均量 / 60d 均量)
- margin_change_20d (融資餘額 20 日變化)
- short_squeeze_proxy (融券餘額 60 日 percentile rank)
- pre_breakout_consolidation ((max-min)/mean over 60d)
- rsv_60d, near_52w_high
- peer_relative_strength (同產業 60 日報酬 percentile)

## Step 3: Interpret

### Path A: Look for pre-event predictability

- Eyeball top 20 spikes' pre-window features — any obvious common pattern?
- If 4+ features show same skew (e.g. most spikes have institutional_flow > 0 AND near_52w_high > 0.9) → predictable pattern, proceed to ML classifier (Phase 2)
- If features look random → **no pre-event signal**, pivot to Path B

### Path B: Post-peak chase study

Compute post_peak_ret_{5,21,63}d stats:
- Mean, median, win_rate, quantiles
- If mean ≥ 5% AND win_rate ≥ 55% for any horizon → build chase strategy
- If all horizons have mean < 3% → mean reversion dominates, drop this direction
- If mean > 0 but win_rate < 50% → fat-tail pattern (classic for trend-following), **chase + trailing stop is the design pattern**

## Step 4 (optional): News trace for top N events

If user wants to know WHY each spike happened:
- Invoke `twstock-news-analyst` per event with a 60-day pre-event window
- Classify each spike by event type: M&A / 重大訂單 / 財報爆發 / 產業政策 / 純籌碼
- Aggregate: which event type has highest rally-triggering rate?

## Step 5: Output report

Respond in **Traditional Chinese**:

1. **Spike summary**: N 件 / M 家公司 / 年度分佈
2. **Top 20 with pre-features table**: 看得出共通 pattern 嗎？
3. **Pre-event verdict**: 可預測 / 訊號太弱 / 需更多資料
4. **Post-peak verdict**: 追漲 alpha 是否存在（依 mean/win_rate 判斷）
5. **Proposed next**:
   - (a) 若找到 pattern → 建 ML classifier (Phase 2)
   - (b) 若 post-peak 有 alpha → 跑 chase backtest (`research/experiments/chase_trailing_stop.py`)
   - (c) 若都沒 → 建議補資料（MOPS 公告、集保大戶、TPEx 擴展）

**Append**:
- `project_strategy_research_findings.md` 新增本次結論，避免未來重跑

## Anti-patterns

- **禁止** 在 top spikes 未清乾淨前就開始建 dataset — 先 audit
- **禁止** 用 single anchor（T=start 或 T=peak）做 Phase 2 + Phase 3b — 必須 dual anchor
- **禁止** fwd_ret_63d 從 start_date 算起當作「post-peak 訊號」—— 它被 spike 定義污染（win_rate 必然 100%）

Respond in Traditional Chinese.
