# D-backtester-scala:Scala 回測引擎(NAV / DRIP / 分割 / 再平衡 / 成本)學理稽核

**範圍**:`src/main/scala/strategy/Backtester.scala` + `BacktesterTest.scala` + `Output.scala`
(任務單列的 `Backtest.scala` 不存在,scope 映射到 `Backtester.scala`;`BacktesterTest.scala`
內含手算 DRIP 對照,一併納入)

**總判定:BUG**

## 一句話白話

會算對、能信的:**每日 NAV 複利、除權息還原(DRIP)、手續費/賣稅雙邊成本會計、
CAGR/總報酬/月末 NAV** 全部符合學理。DRIP 甚至設計得很漂亮——`cash_dividend` 欄實際存的
是「除權息前收盤 − 參考價」(= 權值 + 息值),所以同一條公式同時正確還原了**現金股利、
配股、與權息合併**三種事件。

有問題的、會讓報酬系統性偏高的**真 bug**:**減資(capital reduction)的參考價重設完全
沒被處理**。引擎只讀 `ex_right_dividend` + 一個「2.5 倍跳幅」的分割啟發式;386 筆減資有
**350 筆(91%)漏接**。持有到減資的股票,會把「股數變少、股價跳高」這種純會計重設**當成
真實漲幅**記進 NAV——實測台苯(8103)一天憑空 **+11.6%**。

另有幾個 **SUSPECT**(偏差方向明確但量較小或依賴範圍外的策略層):當日收盤決策當日成交
(前視偏樂觀,與 Python 端 +1 日慣例不一致)、缺價日用**成本價**估值、對贏家加碼時**峰值被
重設**、交易日曆綁單一標的 0050。

## 可重現證據(全部經 psql 實測,localhost:5432/quantlib)

### 1. DRIP 正確性 —— `cash_dividend = 除息前收盤 − 參考價`(三型態全成立)

| right_or_dividend | 筆數 | avg&#124;cash_dividend −(prev−ref)&#124; | max abs diff |
|---|---|---|---|
| 息(純現金) | 10,684 | 0.00117 | 0.01 |
| 權息(配股+現金) | 3,391 | 0.00348 | 0.06 |
| 權(純配股) | 1,295 | 0.00469 | 0.54 |

差異只來自 `ex_right_ex_dividend_reference_price` 四捨五入到 2 位。個例:3518(權)prev=32、
ref=31.12、cash_dividend=0.874616 = 32 − 31.125384(未捨入 ref)。故 `cash_dividend` 存的是
除息日**總掉價**,`new_shares = shares×(1 + cash_dividend/close_t)` 對純現金 / 純配股 /
權息合併都重現 `N×prev` 連續性。`loadDividends` 僅濾 `cash_dividend>0`、無型態濾條件,所有
改變股數的 權/權息 事件都納入。**DRIP 判 OK**。

### 2. 減資參考價重設未處理(BUG)—— 91% 漏接、方向恆為高估

```
capital_reduction (twse):
  total=386  被分割啟發式抓到(跳幅≥2.5x)=36 (9%)  漏接=350 (91%)
  平均跳幅 ×1.625   最小 0.9344   最大 10.2357
capital_reduction ∩ ex_right_dividend (同 date+code) = 0 筆   ← DRIP 永遠碰不到減資
```

實證單筆(8103 台苯,2025-12-08 退還股款):

```
capital_reduction:  last_close=74.7   post_reduction_ref=86.11   jump=1.153
daily_quote:        2025-12-08 收 83.4   (停牌前最後收 74.7)
→ 引擎股數不動、價格 74.7→83.4 = 憑空 +11.6% 記入報酬
→ 跳幅 1.153 < 2.5 且 gap 落在 3-14 日 → 分割啟發式與 DRIP 兩條路徑皆漏
```

減資樣本含 2314 / 9927 / 2832 / 8103 等中大型股,品質/價值策略可能持有。**方向恆為高估報酬。**
`capital_reduction` 表已有精確的 `closing_price_on_the_last_trading_date` 與
`post_reduction_reference_price`,資料就在手邊卻從未被載入。

### 3. 分割啟發式(SUSPECT,誤判風險低但只抓極端尾端)

2018-2026 全期偵測 33 筆。因台股 **±10% 單日限幅**,真實市場移動無法在停牌 gap 內累積到
2.5x,所以 2.5x 門檻對「假分割(真崩跌)」誤判風險低——抓到的 0050 四合一、反向/槓桿 ETF
反分割、6919/4763 等大額分割皆為真事件。代價:`factor = prev_close/today_close` 強制跨 gap
報酬歸零,抹除分割日殘差報酬(0050:真實 R=4,factor=188.65/47.57=3.966,復牌 +0.87% 被抹掉)。
量小,但有 `capital_reduction`/`ex_right` 精確資料時應以精確參考價計 factor。

## 逐項判定

### BUG 1 — 減資參考價重設未處理 → NAV 憑空跳漲(`Backtester.scala:91,115-127,247-272`)

- **學理**:總報酬 NAV 必須中性化**每一種**公司行動的參考價重設,不只股利與分割。減資時
  TWSE 上調參考價、等比下調股數:彌補虧損型 `ref = last_close/(1−reduction_ratio)`,價值
  連續性要求 `new_shares = old_shares × last_close/ref`。忽略=把會計跳價當經濟報酬。
  出處:TWSE 減資恢復買賣參考價計算辦法;CRSP/Bloomberg TRI 對 capital change 與 split 一視同仁。
- **實作**:只載入 `ex_right_dividend` + 啟發式分割偵測器;`capital_reduction` 表從未被讀取。
- **修法**:載入 `capital_reduction` 成 `Map[(date,code)->factor]`,`factor =
  closing_price_on_the_last_trading_date / post_reduction_reference_price`,減資日比照分割路徑
  套用(`shares×factor`、`avgCost/peak ÷factor`),用精確參考價而非啟發式推估。退還股款型另補記
  返還現金 `= old_shares × 每股返還金額` 才 100% 正確;僅做參考價重設也已消除約 95% 誤差。
  建議把啟發式分割與此精確資料路徑合併為單一 corporate-action 還原層。

### OK 1 — DRIP 股息再投資(`Backtester.scala:101-113`,`loadDividends 231-241`)

見上「可重現證據 §1」。公式 `(P_t+D_t)/P_{t-1}` 的股數複利等價式,以除息日實際收盤再投資,
對三種除權息型態全正確。DRIP 交易記 cost=0(不計再投資手續費)為 TRI 慣例簡化,可接受。

### SUSPECT 1 — 當日收盤決策當日成交(前視,`Backtester.scala:93-99,151-199`)

- **學理**:以 `close_t` 算的訊號只能在 ≥ t+1 成交;house 慣例(CLAUDE.md)為 +1 日
  「new picks effective T+1」。
- **實作**:`targetWeights(today)` 當場算、以 today 收盤成交(同一根 bar)。
- **偏差**:若訊號用到當日收盤即為偏樂觀的 market-on-close;Python 正典引擎 shift +1 日,兩者
  不一致。真實偏差量取決於策略層 PIT(範圍外)。
- **修法**:再平衡/出場改用次一交易日價格,或規定 `targetWeights` 只用 ≤ t−1 資料;與
  Python +1 日慣例對齊。

### SUSPECT 2 — 缺價日以成本價 avgCost 估值(`Backtester.scala:153,202`,亦 106/133/159)

- **學理**:逐日市值 NAV 應以最近可得市價估值(缺口日 last-price carry-forward),不得用成本價。
- **實作**:`prices.getOrElse(code, avgCost)`——無報價即用進場成本估值/成交。
- **偏差**:成本 100、現價 200 的持股遇單日無報價會被估成 100(假 −50%),隔日自我修正但已污染
  日報酬路徑,扭曲 MDD/Sharpe。
- **修法**:維護每檔 `lastPrice`,缺口日 carry-forward;任何情況不得用 avgCost 當價格。

### SUSPECT 3 — 加碼時 trailing 峰值被重設(`Backtester.scala:193`)

- **學理**:trailing 峰值 = 進場以來滾動最大值,加碼不得下降:`peak' = max(peak, price)`。
- **實作**:加碼到既有持股時 `Position(_, _, price)` 把 peak 設為當前價,丟棄先前更高峰值。
- **偏差**:峰值 150 後於 140 加碼 → peak 重設 140 → 停損線下移 → 出場延後(偏向續抱),與
  出場語意合約「峰值下限=該筆成交價」牴觸。
- **修法**:加碼時 `peak = math.max(pos.peakPrice, price)`;全新部位才用進場價當下限。

### SUSPECT 4 — 分割啟發式抹除殘差報酬 + 推估比率(`Backtester.scala:115-127,247-272`)

見上「可重現證據 §3」。修法:有精確資料時用精確參考價計 factor;殘差報酬損失作為已註記近似接受。

### SUSPECT 5 — 交易日曆取自單一標的 0050(`Backtester.scala:209-217`)

`loadTradingDays` 只查 `company_code='0050'` 當全市場日曆。0050 停牌日(如 2025-06 分割停牌)
整段消失 → 其他持股該窗價格移動被略過。修法:改用全市場交易日聯集或 sentinel 休市日曆。

### OK 2 — 成本會計(手續費/賣稅/雙邊)(`Backtester.scala:61-62,135,166,184,189`)

手續費 0.02850%(0.1425%×2 折)買賣雙邊、證交稅 0.3% 賣方單邊,皆與 house 常數與台股規則相符,
勝過 Python 端扁平換手近似。唯一眉角:ETF 賣稅應 0.1%,此處一律 0.3%,但 Hold0050 幾乎不賣故
可忽略;`avgCost` 未含手續費但僅作 fallback、不進報酬計算。ETF 策略回測時建議依代號分流賣稅。

### OK 3 — NAV 複利 / 總報酬 / CAGR / 月末 NAV(`Backtester.scala:41-42,202-203`;`BacktesterTest:25-26`;`Output:64-75`)

複利透過持股自然發生;`totalReturn=finalNav/initial−1`;`CAGR=(end/begin)^(1/years)−1`,
years 用日曆年 365.25(教科書定義);月度取 `maxBy(_._1)`=月末最後交易日 NAV(正確,非取最大 NAV)。
權重漂移=再平衡間隨價自然漂移(正確買抱);期末不平倉→終值 mark-to-market(慣例,僅略高估約
0.3% 未付賣稅);現金 0 利率(台股無風險利率≈0,保守標準)。
