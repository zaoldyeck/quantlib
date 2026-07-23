"""融資槓桿 NAV-overlay 模擬(常數槓桿買進持有 + 動態波動率目標槓桿)。

制度常數(money-path,第一手查證 2026-07-21,EV54 預註冊):
- 融資成數:0~6 成**逐股訂定**(上市上限 6 成 → L≤2.5、上櫃 5 成 → L≤2.0、
  注意/處置股可降到 0;國泰證券信用交易頁)。
- 整戶擔保維持率 130%:低於即追繳(補回 166% 撤令),T+2 未補 T+3 斷頭
  (國泰/永豐期貨)。本模型**當日即斷**=時點保守、收盤價成交=價格樂觀。
- 融資利率:券商牌告 ~6.4-6.5%/年(2026)→ 取 6.5%,按交易日 /252 日計。

一級近似(NAV 層,整簿視為單一擔保品)。**未建模**(使結果偏樂觀):個股
融資限額、處置股停融資/降成數(S 微型股池高頻踩中)、跌停鎖死賣不掉、
T+2 補繳緩衝。泛化 EV53c vt_overlay(cap 開放 >1 + 融資利息 + 非對稱摩擦)。

Run(自測): uv run --project . python -m quantlib.trading.tests.test_margin_sim
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import polars as pl

FIN_RATE = 0.065     # 融資年利率(牌告 6.4-6.5% 取 6.5%,日計 /252)
MAINT = 1.30         # 整戶擔保維持率追繳線(本模型觸線當日即斷,不給 T+2)
BUY_COST = 0.0013    # 加碼摩擦:手續費 0.0285% + 滑價 0.1%
SELL_COST = 0.0043   # 減碼摩擦:手續費 + 證交稅 0.3% + 滑價 0.1%
FORCED_COST = 0.01   # 斷頭強賣額外摩擦(弱市市價殺出;假設值,偏保守)
L_MAX_TWSE = 2.5     # 上市成數 6 成 → 1/0.4
L_MAX_TPEX = 2.0     # 上櫃成數 5 成 → 1/0.5


def constant_leverage(nav: pl.DataFrame, L: float, *, relever_every: int = 21
                      ) -> tuple[pl.DataFrame, int]:
    """買進持有式融資:每 relever_every 交易日把曝險調回 L×淨值,期間債務只長
    利息(6.5%/252/日)。維持率 P/D < MAINT → 當日賣掉融資部位清償債務(斷頭,
    加 FORCED_COST),空手融資至下個錨點再槓桿。回 (nav_df, 斷頭次數)。"""
    v = nav["nav"].to_numpy()
    r = np.concatenate([[0.0], v[1:] / v[:-1] - 1.0])
    n = len(v)
    eq = np.empty(n)
    eq[0] = 1.0 - (L - 1.0) * BUY_COST          # 初始加槓桿的買進摩擦
    P = L * eq[0]                                # 部位市值
    D = P - eq[0]                                # 融資債務
    forced = 0
    for t in range(1, n):
        P *= (1.0 + r[t])
        D *= (1.0 + FIN_RATE / 252.0)
        e_now = P - D
        if D > 1e-12 and P / D < MAINT:          # 斷頭:賣融資部位、清償債務
            sold = P - e_now
            e_now -= sold * (SELL_COST + FORCED_COST)
            P, D = e_now, 0.0
            forced += 1
        elif t % relever_every == 0 and e_now > 0:
            delta = L * e_now - P                # 調回目標曝險的買/賣摩擦
            e_now -= abs(delta) * (BUY_COST if delta > 0 else SELL_COST)
            P = L * e_now
            D = P - e_now
        eq[t] = e_now
        if e_now <= 0:                           # 淨值歸零保險絲(理論上先斷頭)
            eq[t:] = max(e_now, 0.0)
            break
    return pl.DataFrame({"date": nav["date"], "nav": eq}), forced


def vol_target_leverage(nav: pl.DataFrame, sigma_tgt: float, lookback: int,
                        cap: float) -> pl.DataFrame:
    """預應式動態槓桿:e_t = min(cap, σ_tgt/σ_realized(昨日止)),cap 可 >1。
    e>1 部分按日計融資利息;曝險變動收非對稱摩擦(加碼 BUY_COST/減碼
    SELL_COST)。日頻調整下維持率觸不到 130%(單日組合跌幅受 10% 漲跌停限
    制,e≤2 時斷頭需單日 −35%),故不模擬斷頭——EV54 預註冊揭露。"""
    v = nav["nav"].to_numpy()
    r = np.concatenate([[0.0], v[1:] / v[:-1] - 1.0])
    rv = (pd.Series(r).rolling(lookback, min_periods=5).std(ddof=1)
          .shift(1).to_numpy())
    daily_tgt = sigma_tgt / np.sqrt(252.0)
    e = np.where(np.isnan(rv), 1.0,
                 np.minimum(cap, daily_tgt / np.maximum(rv, 1e-6)))
    eq = np.empty_like(v)
    eq[0] = 1.0
    prev = 1.0
    for t in range(1, len(v)):
        de = e[t] - prev
        cost = de * BUY_COST if de > 0 else -de * SELL_COST
        fin = max(e[t] - 1.0, 0.0) * FIN_RATE / 252.0
        eq[t] = eq[t - 1] * (1.0 + e[t] * r[t] - fin - cost)
        prev = e[t]
    return pl.DataFrame({"date": nav["date"], "nav": eq})
