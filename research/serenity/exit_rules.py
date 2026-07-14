"""Serenity 六道出場門——單一真相來源。

live 執行系統(`research.serenity.daily`)與唯讀決策支援(`research.tri.daily`)
都 import 本模組,確保兩邊對同一個 lot 永遠給出同一判決。2026-07-13 教訓:
tri 曾以「引擎模擬簿成員資格」當 KEEP/SELL 判準,與 live 的 lot 錨定評估相左
(模擬簿裡的舊 lot 浮盈不出場、帳上的新 lot 浮虧觸發法人門)。

規則與參數 = champion `ev_v2_thesis_inst`(戰役八採納法人門、戰役十一消融
背書、戰役十四否決進場端否決;見 docs/serenity/serenity_engine_trials_ledger.md)。
出場門是 **lot 錨定**:同一支股票、不同進場錨,判決可以不同——設計如此。
"""

from __future__ import annotations

TRAIL, ABS_STOP, TAKE_PROFIT = 0.20, 0.15, 0.60
TIME_DAYS, TIME_RET = 50, -0.01


def evaluate_exit(
    *,
    px: float,
    anchor: float,
    peak: float,
    days_held: int,
    inst20: float | None = None,
    yoy3: float | None = None,
) -> str | None:
    """六道門逐一評估(順序即優先序);全綠回 None。

    px=最新收盤、anchor=該 lot 的止盈/止損錨(成交價或收養價)、
    peak=持有期收盤峰值、days_held=交易日、inst20=20 日法人淨買賣(股)、
    yoy3=近三月營收 YoY 均值(%)。override(人工事實級出場)屬 live-ops
    層,不在本函式。
    """
    if px <= anchor * (1 - ABS_STOP):
        return "abs_stop"
    if px <= peak * (1 - TRAIL):
        return "trail"
    if px >= anchor * (1 + TAKE_PROFIT):
        return "take_profit"
    if days_held >= TIME_DAYS and px <= anchor * (1 + TIME_RET):
        return "time_stop"
    if inst20 is not None and inst20 < 0 and px < anchor:
        return "thesis(inst_20d<0 且虧損)"  # battle 8 champion rule
    if yoy3 is not None and yoy3 < 0:
        return "thesis(yoy_3m<0)"
    return None
