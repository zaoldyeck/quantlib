# 台股自動交易 Live Pilot Runbook

最後更新：2026-05-19

本文件定義 Iter95 Global Exit-Aware Time50 r-1 的小額實盤驗證流程。目標是先驗證資料更新、訊號產生、訂單計畫、券商送單與成交回報能否穩定閉環，而不是直接投入大資金。

## 架構

流程分成兩段：

1. 收盤後：更新資料、同步 DuckDB cache、用目前最強 `execution_ready` 策略產生下一交易日訂單計畫。
2. 隔天盤前：讀取已凍結的訂單計畫，送出 dry-run 或 live 委託。

不把「策略判斷」和「券商送單」綁在同一支長跑程式內。這樣任一階段失敗時都能保留明確 artifact，方便人工檢查與重跑。

## 資金設定

`research/.env` 必須設定：

```bash
QL_STRATEGY_CAPITAL_TWD=50000
QL_CASH_BUFFER_PCT=0.03
QL_ORDER_PRICE_POLICY=limit_up_down
QL_BUY_PRICE_BUFFER_PCT=0.10
FUBON_DRY_RUN=true
```

`QL_STRATEGY_CAPITAL_TWD` 是策略可動用資金上限。即使富邦 Neo API 可用 `accounting.bank_remain` 查詢銀行可用餘額，系統仍不能假設整個帳戶餘額都屬於這個策略。

## 持倉來源

預設使用本地 managed-position ledger：

```text
var/state/trading/managed_positions.json
```

原因是券商庫存只能告訴我們帳戶總庫存，無法區分這些股票是手動買入，還是策略買入。小額實盤應該從空 ledger 開始，或由使用者明確匯入要交給策略管理的既有持股。

## 每日命令

收盤後：

```bash
uv run --project . python -m quantlib.trading.auto_trader run-after-close
```

如果同一個台灣日已經完成資料刷新且 cutoff 已驗證，可用：

```bash
uv run --project . python -m quantlib.trading.auto_trader run-after-close --skip-refresh
```

隔天盤前 dry-run：

```bash
uv run --project . python -m quantlib.trading.auto_trader submit-plan var/out/trading/plans/<plan>.json
```

成交後 reconcile：

```bash
uv run --project . python -m quantlib.trading.auto_trader reconcile-plan var/out/trading/plans/<plan>.json --write
```

隔天盤前真實送單：

```bash
uv run --project . python -m quantlib.trading.auto_trader submit-plan var/out/trading/plans/<plan>.json --live
```

真實送單還必須在 `research/.env` 設定：

```bash
FUBON_DRY_RUN=false
```

## 富邦第一次連線測試

富邦新一代 API 的開通流程中，第一次登入可能回傳：

```text
無簽署完成API使用風險暨聲明書帳號，請與營業員聯絡；若正進行簽署流程並測試連線中，此訊息表連線測試成功，使用權限將應於次日開通
```

這不是程式錯誤；它代表連線測試已到達富邦，帳號權限通常要等次日開通。`smoke-test --accounting` 會把這個狀態標為 `connection_test_success_pending_activation`，仍然記錄 `placed_order=false`。

次日再跑：

```bash
uv run --project . python -m quantlib.trading.auto_trader smoke-test --accounting
```

若成功，才會繼續查委託、銀行餘額與庫存。

若要完全對齊富邦文件中的 `sdk.login(身分證字號, 登入密碼, 憑證路徑, 憑證密碼)` 測試步驟，使用：

```bash
uv run --project . python -m quantlib.trading.auto_trader smoke-test --login-method password
```

API key 模式開通後，再使用預設的 `--login-method apikey`。

## 安全規則

- 沒有 `execution_ready` 策略時，不產生 live order。
- 沒有 `QL_STRATEGY_CAPITAL_TWD` 時，不產生 order plan。
- `submit-plan --live` 但 `FUBON_DRY_RUN=true` 時，直接失敗。
- 策略只使用 registry 中最高 stage 的單一最強策略。
- order plan 使用 buy `LimitUp` / sell `LimitDown`，讓委託具備市場性但仍受每日漲跌停限制。
- 真實送單前先跑 `smoke-test --accounting`，確認登入、銀行餘額與庫存查詢正常。
- 真實送單前系統會再次查詢富邦庫存，並比對 planned symbols 的券商庫存與本地 managed ledger；若不一致，直接停止，不送單。
- 真實送單後必須跑 `reconcile-plan --write`，用富邦成交回報更新 managed ledger；否則下一次產生計畫會被禁止進入 production-scaled 流程。

## 最低資金判斷

技術上，只要能買到每個目標權重至少一股，就可以做連線與下單驗證。但這種金額太小，會被零股四捨五入和最低手續費嚴重扭曲。

正式建議：

- 連線 / 下單流程測試：可用更小金額，但只代表 API 管線成功，不代表策略暴露完整。
- 最小策略驗證門檻：NT$50,000。
- 完整覆蓋目前 target basket 每檔至少 1 股：依 `capital-check` 當期估算。
- 接近回測假設：NT$1,000,000。
- 放大前：至少連續完成多個換倉週期，並核對實際成交、滑價、未成交、部分成交與本地 ledger。
