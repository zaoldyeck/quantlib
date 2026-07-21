# 臺指期專業量化交易策略排行

日線資料截止：`2026-05-21`；RPT tick-derived intraday features 截止：`2026-05-20`。本次研究使用 TAIFEX 官方日線、最後結算價、近三年法人期貨部位、RPT 5m/15m/60m 多時間框架特徵與 DuckDB/Parquet cache。執行時間約 `451.5` 秒。

## 結論

本輪沒有策略同時通過 DSR、PBO、bootstrap、成本壓力與保證金 survival gate。這代表目前不能把任何臺指期候選升級成正式可上線 champion。
下方表格是未通過策略的診斷排行，排序改用 OOS CAGR，目的是看哪一類訊號有研究價值；它不是可上線排行。

## 策略排行

| 排名 | 策略 | Verdict | Full CAGR | OOS CAGR | 最近一年 CAGR | 近 6 月 | 近 3 月 | 近 1 月 | OOS MDD | OOS Sortino | DSR | PBO | Profit Factor | SQN | 最大槓桿 | 最低 Margin Buffer |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | TX_cross_market_z1.0_tv0.45_nostop_tr4 | reject_mdd | +5.83% | +8.40% | -7.72% | -13.57% | +0.00% | +0.00% | -51.69% | 0.366 | 0.016 | 0.908 | 1.006 | 0.051 | 5.369 | 1.774 |
| 2 | TX_cross_market_z0.8_tv0.45_nostop_tr4 | reject_mdd | +6.13% | +8.30% | -3.60% | -14.68% | +0.00% | +0.00% | -52.34% | 0.359 | 0.013 | 0.908 | 0.962 | -0.322 | 4.896 | 1.945 |
| 3 | TX_h_model_z1.25_v-1.0_tv0.45_nostop_tr4 | reject_dsr | +4.68% | +6.98% | -1.23% | -1.34% | +0.00% | +0.00% | -28.70% | 0.368 | 0.073 | 0.908 | 1.281 | 1.644 | 4.870 | 1.956 |
| 4 | TX_h_model_z0.5_v-1.0_tv0.45_sl2.5_tr4 | reject_mdd | +3.99% | +6.77% | -7.45% | -5.41% | +0.00% | +0.00% | -59.70% | 0.322 | 0.010 | 0.908 | 0.965 | -0.334 | 5.507 | 1.729 |
| 5 | PM_lagged_risk_budget_lb126_top3_tv0.7 | reject_dsr | +4.21% | +6.74% | +0.04% | +2.20% | +0.00% | +0.00% | -31.53% | 0.371 | 0.246 | 0.908 | 0.000 | 0.000 | 0.000 | 1.729 |
| 6 | TX_h_model_z0.5_v-0.5_tv0.45_nostop_tr4 | reject_mdd | +4.06% | +6.27% | -5.71% | -3.04% | +0.00% | +0.00% | -47.87% | 0.313 | 0.012 | 0.908 | 1.100 | 0.842 | 5.459 | 1.745 |
| 7 | TX_basis_trend_z1.0_tv0.45_nostop_tr4 | reject_dsr | +3.75% | +5.93% | +6.55% | +2.18% | +0.00% | +0.00% | -18.03% | 0.291 | 0.078 | 0.908 | 1.657 | 2.753 | 4.012 | 2.374 |
| 8 | TX_h_model_z1.25_v-1.0_tv0.45_sl2.5_tr4 | reject_dsr | +3.75% | +5.83% | -4.20% | -2.19% | +0.00% | +0.00% | -23.79% | 0.292 | 0.037 | 0.908 | 1.206 | 1.224 | 4.400 | 2.164 |
| 9 | PM_lagged_risk_budget_lb63_top3_tv0.35 | reject_dsr | +3.59% | +5.76% | +5.32% | +2.20% | +0.00% | +0.00% | -24.83% | 0.422 | 0.316 | 0.908 | 0.000 | 0.000 | 0.000 | 1.729 |
| 10 | PM_lagged_risk_budget_lb126_top2_tv0.35 | reject_dsr | +4.07% | +5.75% | -5.72% | +2.20% | +0.00% | +0.00% | -35.16% | 0.272 | 0.131 | 0.908 | 0.000 | 0.000 | 0.000 | 1.729 |
| 11 | TX_basis_trend_z0.75_tv0.45_nostop_tr4 | reject_dsr | +3.81% | +5.61% | +6.01% | +2.15% | +0.00% | +0.00% | -31.46% | 0.264 | 0.027 | 0.908 | 1.418 | 2.206 | 5.199 | 1.832 |
| 12 | TX_basis_trend_z1.0_tv0.45_sl2.5_tr4 | reject_dsr | +3.57% | +5.61% | +6.91% | +2.29% | +0.00% | +0.00% | -18.03% | 0.272 | 0.064 | 0.908 | 1.548 | 2.362 | 4.172 | 2.283 |

## 驗證方法

- 訊號在產生後全部 shift 一天，預設下一個交易日開盤成交，避免 look-ahead。
- 每筆交易扣除固定手續費、股價指數期貨交易稅、滑價 tick 與 roll 額外滑價。
- 部位大小由目標波動與保證金 survival constraint 同時限制；歷史或壓力情境爆倉者淘汰。
- 籌碼候選納入現貨三大法人買賣超、融資融券、借券、外資持股比例變化，以及期貨三大法人未平倉與成交淨額。
- 技術指標候選使用 `stockstats` 批次產生 RSI、MACD、Bollinger、KDJ、ADX/DMI、CCI、ATR、WR、MFI、TRIX、TEMA 與多組 SMA/EMA，再與手寫透明特徵交叉驗證。
- H 模型候選不是未授權課程的完整公式，而是依公開資訊實作的可稽核 approximation：價差指標為主、量指標為濾網、槓桿交由 simulator 的 survival constraint 控制。
- 每個候選都跑 2x 與 5x 成本壓力；正式通過至少需要 2x 成本 OOS CAGR 仍為正。
- DSR 使用候選數作多重試驗修正；PBO 使用 multi-config CSCV，避免只看單一策略美化曲線。
- 本輪 multi-config PBO：`0.908`。

## 重要限制

- 本輪已使用長歷史 RPT tick 轉出的 5m/15m/60m 日內特徵，但 simulator 仍是日線開盤成交模型；真正日內進出場策略需要另建 intraday order simulator。
- 保證金使用保守名目比例 proxy；若要進入 live pilot，必須接入券商或 TAIFEX point-in-time margin table。
- Flow sleeve 只可作近三年 overlay，不能當成長期主模型，因為官方免費法人期貨資料只有 rolling 三年。

## 資料來源

- TAIFEX 期貨每日交易行情下載：https://www.taifex.com.tw/cht/3/dlFutDailyMarketView
- TAIFEX 三大法人期貨契約：https://www.taifex.com.tw/cht/3/futContractsDateView?menuid1=03
- TAIFEX 指數期貨最後結算價：https://www.taifex.com.tw/cht/5/futIndxFSP
- TAIFEX 交易歷史資料申請：https://www.taifex.com.tw/cht/3/hisAppForm
- TAIFEX E-Data Shop：https://edatashop.taifex.com.tw/zh/product/list/28
- H 模型公開說明：https://axhuang.com/courses/%E7%AC%AC%E4%B8%80%E5%A1%8A%E9%87%91%E7%A3%9A%EF%BC%9Ah%E6%A8%A1%E5%9E%8B%E5%8E%9F%E7%90%86%E8%88%87%E8%A8%AD%E8%A8%88/
- H 模型歷史資料說明：https://axhuang.com/product/%E5%BB%BA%E6%A7%8Bh%E6%A8%A1%E5%9E%8B%E7%9A%84%E6%AD%B7%E5%8F%B2%E8%B3%87%E6%96%99/
- 台指期價差交易公開說明：https://futuresinvest90223.com/%E5%8F%B0%E6%8C%87%E6%9C%9F%E7%8F%BE%E8%B2%A8%E5%83%B9%E5%B7%AE/
- 台指期基差與三大法人研究摘要：https://www.airitilibrary.com/Article/Detail/19937571-202003-202006240007-202006240007-39-62

## Artifacts

- `research/strat_lab/results/futures_tx_professional/futures_strategy_summary.csv`
- `research/strat_lab/results/futures_tx_professional/futures_fast_screen.csv`
- 本輪無通過 gate 的 champion，因此未輸出 champion 交易 artifacts；未通過候選只保留在 summary 與 fast screen 診斷檔。
