"""盤中執行器(intraday execution)— 買低賣高的紀律化實作。

兩支 CLI:
    uv run --project . python -m quantlib.trading.execution.buy  ...
    uv run --project . python -m quantlib.trading.execution.sell ...

哲學(誠實版):沒有人能買在最低、賣在最高。可工程化的目標是——
被動優先(掛在自己這一側撈價差)→ 隨時間升級(避免錯過;本系統回測證明
「等回檔」會系統性錯過贏家)→ 死線前必成交(升級到可成交限價)→
永不超過價格護欄(cap/floor)。成效以「到達價 (arrival) 滑價」衡量,
每筆執行寫 TCA 日誌供事後評估。

安全鐵律:預設 dry-run(僅訂閱行情、模擬成交);`--live` 需 FUBON_DRY_RUN=false
+ QL_STRATEGY_CAPITAL_TWD 齊備——武裝與啟動永遠是使用者的動作。
"""
