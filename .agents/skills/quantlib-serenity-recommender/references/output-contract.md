# Serenity Output Contract

## Required Sections

Use this order unless the user requests a different format:

1. `結論`: one paragraph naming the best recommendations first.
2. `資料截止`: PostgreSQL and DuckDB cutoffs, plus whether data was refreshed or
   reused.
3. `10 檔推薦排名`: table of 10 names unless the user requested another count.
4. `結構性瓶頸證據`: why these stocks are bottleneck beneficiaries, with sources.
5. `交易計劃`: entry condition, target, stop, risk/reward, and invalidation.
6. `前三名理由`: plain-language explanation for the highest-ranked picks.
7. `主要風險`: what could break the thesis.
8. `下一步驗證`: concrete checks to run next.

## Ranking Table Columns

Use these columns when available:

- 排名
- 股票
- 公司
- Serenity 主題
- 推薦狀態
- 現價
- 目標價
- 停損價
- 風險報酬比
- 進場計劃
- 估值判斷
- 成長/動能確認
- 結構性瓶頸證據
- 主要來源
- 主要風險
- 下一步驗證

If the user asks for portfolio weights, add `建議比例`, but keep it clearly
separate from broker execution. Weight suggestions are research guidance, not a
live order plan. If the user asks for "現在可買哪 10 檔", include all table
columns above by default.

## Price Semantics

- `現價`: latest tradable raw close or live/near-live quote if explicitly used.
- `目標價`: valuation-derived raw-price target, preferably with a target range in
  the detailed section.
- `停損價`: raw-price stop-loss level, plus a thesis invalidation event.
- Performance and CAGR context, if included, must use adjusted total-return
  prices.

## Writing Style

- Write for an investor who has not read prior project context.
- Explain technical terms briefly when they materially affect the decision.
- Be direct about uncertainty, stale thesis risk, crowded trade risk, and
  valuation risk.
- Explain why each stock is structurally important. Do not merely say it belongs
  to a hot industry.
- Cite high-quality sources and dates for material news, filings, industry
  evidence, and company claims.
- Avoid saying "一定", "保證", or "穩賺".
- Do not include the history of how the report was developed unless asked.

## Handoff Language

If the user asks to trade:

> 這已經超出 Serenity 推薦 Skill 的範圍；下一步要切到交易執行 workflow，先產生 dry-run order plan，再確認是否送出真實委託。

If the user asks to manage holdings:

> 這屬於庫存/投資組合管理，不應由 Serenity 推薦 Skill 直接處理；需要切到持倉管理 workflow，先查庫存與 ledger，再做調整建議。
