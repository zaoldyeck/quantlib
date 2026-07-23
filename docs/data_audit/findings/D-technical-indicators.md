# D-technical-indicators — 技術指標學理稽核

範圍:research 端所有技術指標實作(stockstats 使用、iter_100 ICT/SMC/TPO overlay、
ATR/布林/KD/MACD/RSI 手寫處、intraday)。逐一對照學理定義。

## 白話總結

**沒有會讓「已上線策略」或「已報告結論」失真的硬 BUG。** 現役策略(Serenity `ev_v3_wf`
等權)不吃任何技術指標;純量化冠軍 Iter95 也不靠這些。這批技術指標全部集中在
「因子廣篩(apex t01/f01/f03)」「Iter100 結構 overlay(已 bounded 實驗)」「期貨研究
(多數未過驗證)」三個**研究階段**管線。

指標公式本身大多正確或屬**已聲明的近似**(SMA 版 RSI、除以 close 做截面正規化等),
對「排序型 IC 檢定」不影響結論。真正值得記的偏差有兩處,但都不在上線路徑:

1. **Serenity `atr20_pct` 名不符實**:叫 ATR 卻只用「當日高−低」的 20 日均,漏掉真實
   波幅(True Range)的跳空項。台股有 10% 漲跌停、常跳空,漏跳空會**低估波動**。只用於
   非冠軍的 `inv_atr` 加權變體,冠軍是等權,故不影響現行下單。
2. **期貨 stockstats 指標算在「未還原轉倉」的近月原始價上**:MACD/RSI/BOLL/KDJ 等
   趨勢指標吃的是每月轉倉會跳價的原始近月序列,手寫版卻用還原連續價 —— 兩者基準
   不同,文件宣稱「手寫 vs stockstats 交叉驗證」其實比的是兩條不同價格序列。實測 TX
   轉倉日中位跳幅僅 0.95%(平日 0.67%),對「有界震盪指標的投票」擾動很小,故列 SUSPECT
   而非 BUG;但方法學上不乾淨。

其餘為 OK / 已聲明近似,詳列如下。

---

## SUSPECT(定義偏差,均非上線路徑)

### S1. Serenity `atr20_pct` 是「高−低 range」不是 ATR(漏 True Range 跳空項)
- 檔案:`research/serenity/replay_2025.py:196-199`
- 學理:Wilder(1978)ATR。True Range `TR = max(H−L, |H−C_prev|, |L−C_prev|)`,ATR = TR 的
  Wilder 平滑(SMMA,α=1/N)。TR 的**跳空兩項**專門捕捉隔夜/漲跌停跳空。
- 實作:`((high-low).rolling_mean(20).shift(1) / close)` —— 只有 `H−L`,完全沒有
  `|H−C_prev|`、`|L−C_prev|`。且用 SMA 非 Wilder。
- 證據:台股日漲跌停 ±10%,跳停日 `H−L` 可能=0(一字板)但真實 TR=|C−C_prev|≈10%;此式
  對一字板/跳空股回傳近 0 波動 → inv_atr 加權會**過度加碼**這些其實最猛的股票。
- 影響界定:僅 `weight_mode="inv_atr"`(變體 `ev_v2_watr`)使用;現役冠軍
  `live_config.json` `weighting="equal"`,**不吃此欄**。
- 修法:改成真 TR:`tr = max_horizontal(high-low, (high-close.shift(1)).abs(),
  (low-close.shift(1)).abs())`,再 `tr.rolling_mean(20).shift(1)/close`(或 Wilder
  `ewm(alpha=1/20)`)。若刻意要用 range 當波動 proxy,欄位改名 `range20_pct` 別叫 ATR。

### S2. 期貨 stockstats 趨勢指標算在「未轉倉還原」的原始近月價
- 檔案:`research/futures/strategies.py:120-148`(stockstats 用 `frame` 的 `close`=原始
  近月價,見 `load_product_frame` SELECT 的 `r.close`,line 201)、對照手寫趨勢用
  `continuous_close`(line 299-313)。
- 學理:期貨連續合約做趨勢指標(MACD/RSI/EMA/BOLL)須用**還原(back-adjusted)連續價**,
  否則每月轉倉的合約價差會被當成真實價格變動注入指標。
- 實作:`ss_macd/ss_rsi/ss_boll/ss_kdjk/ss_adx/ss_cci/ss_atr/ss_wr/ss_mfi` 全在
  `open/high/low/close`=原始近月價上算;手寫 `rsi14/stoch_k14/bb_z20/macd_line` 卻在
  `continuous_close` 上算。
- 證據:實測 `continuous_close≈16439` vs 原始近月 `raw_close≈41473`(2026-05-21,~2.5x
  背離);轉倉日 332 次,轉倉日中位 |日報酬| 0.95% vs 平日 0.67%(每次轉倉注入約 +0.3%
  假跳動)。因指標多為有界震盪(RSI/KDJ/CCI/WR/MFI)或只取正負號(MACD hist),對投票
  擾動小 → 列 SUSPECT;但 `研究方法「手寫 vs stockstats 交叉驗證」比的是兩條不同序列`
  的宣稱不成立。
- 修法:stockstats 也餵 `continuous_close`(或已還原的還原 OHLC);或在文件明說 stockstats
  版刻意用原始近月價、並移除「交叉驗證」宣稱。

### S3. 期貨手寫 rsi14 用「報酬的 SMA」而非「價格變動的 Wilder 平滑」
- 檔案:`research/futures/strategies.py:378-380`(+ up_ret/down_ret 定義 273-274,
  rolling_mean(14) 於 310-311)
- 學理:Wilder RSI:`RS = SMMA_14(gain)/SMMA_14(loss)`,gain/loss 為**價格變動**;
  `RSI = 100 − 100/(1+RS)`。
- 實作:`RSI 轉換式正確`,但(a)輸入用**報酬** up_ret/down_ret 非價格變動,(b)平滑用
  `rolling_mean(14)`(SMA)非 Wilder SMMA(α=1/14)。屬 Cutler's RSI 變體 + 報酬版。
- 證據:同一序列 SMA-RSI 與 Wilder-RSI 截面排序不同(平滑核不同 → 非單調轉換);故非
  「與 ss_rsi 等價」的交叉驗證。台指 14 日內價位變動小,報酬 vs 點數差異次要。
- 修法:要對齊教科書就用價格變動 + Wilder(`(H... )`);或明確聲明是 Cutler 報酬版,別
  宣稱與 ss_rsi 交叉驗證。

### S4. 期貨 stoch_k14 用收盤價的 min/max,非期間 high/low
- 檔案:`research/futures/strategies.py:381-385`(low14/high14 = `continuous_close`
  的 rolling_min/max,line 304-305)
- 學理:Stochastic %K = `100×(C − LL_n)/(HH_n − LL_n)`,`HH_n`/`LL_n` 為期間**最高高價/
  最低低價**。
- 實作:`(close − rolling_min_14(close))/(rolling_max_14(close) − rolling_min_14(close))`
  —— 分母用收盤價極值,非 high/low。屬「收盤價 stochastic」。
- 證據:高波動期 high/low 較收盤極值更寬,兩者 %K 可差數十點;此處餵 `>70/<30` 投票,
  邊界樣本會誤判。
- 修法:`LL=rolling_min(low)`、`HH=rolling_max(high)`。

### S5. 期貨 DMI/ADX 用 SMA 累加平滑且吃原始 high/low
- 檔案:`research/futures/strategies.py:394-395`(+DI/−DI),`452-457`(ADX);DM 定義
  278-279,rolling_sum(14) 於 318-320。
- 學理:Wilder DMI:+DM/−DM 定義正確✓;但 +DI = `100×SMMA_14(+DM)/SMMA_14(TR)`、
  ADX = `SMMA_14(DX)`,皆 Wilder 平滑。
- 實作:用 `rolling_sum(14)`(等價 SMA)取代 Wilder;且 +DM/−DM 由**原始近月** high/low
  算(同 S2 轉倉汙染)。DX 公式正確,ADX 用 `rolling_mean(14)`(SMA)非 Wilder。
- 證據:SMA vs Wilder 使 ADX 反應快、峰值不同;`adx14>18` 門檻投票在邊界會不同。
- 修法:改 Wilder 平滑(`ewm(alpha=1/14, adjust=False)`);high/low 用還原連續序列。

### S6.（低)iter100 ATR 用 SMA 非 Wilder;TPO 價值區為粗 proxy
- 檔案:`research/strat_lab/iter100_features.py:76-77`(atr14/atr20=`true_range.rolling_mean`)、
  `80-82`(tpo_proxy val/poc/vah=typical price 的 rolling_quantile 0.15/0.50/0.85)
- 學理:ATR=Wilder SMMA(TR);TPO/Market Profile 的 POC/VA 定義在**價格 bin 上的成交量/
  時間分佈**(POC=量最大價位,VA=POC 兩側 70% 量的區間),需日內資料。
- 實作:TR 定義正確✓(line 42-48);ATR 用 SMA(近似);TPO 用「20 日典型價時間序列的
  分位數」近似(polars rolling_quantile 為 nearest-rank)。**模組 docstring 明確聲明是
  daily proxy、非嚴格 TPO**。
- 判定:**已聲明近似 → 可接受**;僅記錄 ATR 非 Wilder、TPO 非真量價分佈。ATR 僅作
  相對門檻(×0.85/×1.50)不影響量級。
- 修法(若要對齊教科書):ATR 改 Wilder;POC/VA 若要真實需接日內成交量分佈(現無資料,
  proxy 合理)。

### S7.（低)t02 海龜 N 用 SMA;進出場用收盤價 Donchian 非 high/low
- 檔案:`research/apex/experiments/t02_turtle_magic.py:52`(atr20=`tr.rolling_mean(20)`)、
  `53-54`(hh/ll=`close.shift(1).rolling_max/min`)
- 學理:原版海龜 `N = (19×PDN + TR)/20`(Wilder α=1/20);進場=突破 20/55 日**最高價**
  (highest high),出場=跌破**最低價**。
- 實作:N 用 SMA(20)of TR;突破用**收盤價**的 rolling_max/min。TR 定義正確✓。docstring
  聲明「pyramid 簡化」「2N 停損以 trail 0.16 近似」,但**未聲明 N=SMA、突破=收盤價**。
- 判定:N 僅作突破強度 score 正規化,實際出場用固定 0.16 trail → 影響小;列 SUSPECT/低。
- 修法:N 改 Wilder;Donchian 用 high/low;或在 docstring 補聲明這兩處簡化。

---

## OK(正確或已聲明的合理近似)

### O1. apex t01 因子(IC 廣篩,`t01_technical_factors.py`)
- `rsi14`(57-59):`g/(g+l)` = RSI/100(與 `100−100/(1+RS)` 代數恆等);SMA 版**已聲明**
  「非 Wilder EMA」。IC 為 Spearman 排序 → 單調變體不影響 → **OK**。
- `stoch_k14`(60-62):原始快 %K=RSV,標準式 → OK。
- `macd_hist`(63-64):12/26/9(polars `ewm_mean(span=N)`=α=2/(N+1),已實測✓);÷close 為
  截面正規化**已聲明** → OK。
- `boll_pctb`(65):`(close−ma20)/(2·sd20)` = 教科書 %B 的仿射變換(`2·%B−1`),IC 排序等價;
  sd20 為樣本 std(ddof=1,實測)vs Bollinger 母體 std,但同日同窗長 → 差一常數、截面排序
  不變 → OK。
- `boll_bw_neg`(66)、`mfi14`(67-69,≈PMF/(PMF+NMF),浮點典型價幾無並列 → ≈教科書 MFI/100)、
  `obv_slope20`(70-71)、`vwap20_dev`(72-74)、`avwap_dev`(87-98)→ 全 OK。VWAP 分子分母
  = Σ成交金額/Σ成交量,**實測 `trade_value/volume` 中位/close=1.0004**(量為股數、金額為
  NT$,單位一致)→ VWAP=價格,偏離因子正確。

### O2. apex f01/f03 因子
- `illiq_60`(f01:65):Amihud(2002)`mean(|ret|/成交額)` → OK(1e9 僅縮放)。
- `vpin_60`(f01:85,89):BVC buy fraction 用 `1/(1+exp(−1.702·ret/σ))`(1.702 為 logistic
  對標準常態 CDF 的標準近似✓),`mean(|2·buyfrac−1|)`;非量桶加權的嚴格 VPIN,屬**已命名
  日線 proxy** → OK。
- `vcp_atr`(f01:61)、`donchian_60`(62)、`ma_align`(79-82)、`ofi_20`(83)→ OK(自訂/proxy)。
- f03 `_fvg`(44):`low > high[t-2]` = ICT 多頭 FVG(3 bar,C3 低 > C1 高)標準定義✓;
  `_bos`/`_sweep`/`hvn_dist`(120d VWAP)/`range_pos_60`/`close_pos_20`(CLV)→ 已聲明 SMC/TPO
  proxy → OK。

### O3. 期貨手寫 MACD(`futures/strategies.py:312-313,450,470-471`)
- `ema12/26 = ewm_mean(span, adjust=False)`(標準遞迴 EMA,實測 α=2/(N+1)✓)、
  `macd_line=ema12−ema26`、`macd_signal=ema9(macd_line)`、`macd_hist=line−signal` ——
  **完全教科書 12/26/9**,且算在 `continuous_close`(正確還原序列)→ OK。
- `bb_z20`(386)、`donchian_pos55`(387-392):誠實命名為 z-score/pos,非宣稱 %B → OK。

### O4. stockstats 0.6.8 函式庫本身(公式面)
- 實測原始碼:RSI=Wilder SMMA(`smma`,α=1/N)、ATR=SMMA(TR)、MACD=12/26/9 EMA、
  KDJ=`2/3·prevK+1/3·RSV`(平滑 3)、WR=`(Hn−C)/(Hn−Ln)×−100`、MFI=典型價×量、
  CCI=`(TP−SMA)/(0.015·MD)` → **全教科書**。唯一慣例差:BOLL 用 pandas `.std()`(樣本
  ddof=1)而非 Bollinger 母體 std —— 屬 library 慣例,且期貨投票只用中軌 `ss_boll` 未用
  帶寬 → 無實質影響 → OK。

### O5. IC 評估管線(`apex/factors.py`)
- `forward_returns`:`fwd_k[t]=close[t+1+k]/close[t+1]−1`(shift(−(1+k))/shift(−1),T+1 起算,
  調整價)→ **零 look-ahead** ✓。
- IC:每日截面 rank 後 Pearson = Spearman;t=IR·√n;t_adj=t/√k(重疊樣本粗校正,已聲明)
  → 定義正確 → OK。故上述技術因子的 IC 檢定本身可信。

### O6. intraday / 出場
- `intraday/exit_calibration.py`:trailing stop 逐筆重放,peak=raw_close 累積 max、
  `lvl=peak×(1−pct)`、盤中分 K min ≤ lvl 觸發 —— 追蹤停損定義正確;`intraday/pull_kbars.py`
  僅存原始 kbar(close/volume/amount),無指標計算 → 無稽核對象。
- `experiments/chase_trailing_stop.py`:用 vectorbt 原生 `sl_trail`(標準 15% 追蹤停損)+
  empyrical 績效(屬另一稽核單位)→ 指標面 OK。

---

## 判定
- **verdict: SUSPECT** —— 無上線/報告失真的 BUG;但存在數處未完全聲明的定義偏差
  (S1 atr20_pct 名不符實且漏跳空、S2 期貨指標算在未還原原始價、S3-S5 SMA 非 Wilder /
  收盤價 stochastic),雖均在研究/非冠軍路徑且實測影響小,方法學上不乾淨,故不給 OK。
- 若只問「現役策略的技術指標可不可信」:現役策略**不使用**技術指標,不受影響。
