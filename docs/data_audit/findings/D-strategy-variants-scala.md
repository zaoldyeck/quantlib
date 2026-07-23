# D-strategy-variants-scala — 策略變體因子與邏輯 vs 學理定義

判定:**🔴 BUG**(Greenblatt Magic Formula 多項確認錯誤 + 一個 52 週窗算錯)
稽核日:2026-07-23
範圍:`src/main/scala/strategy/` 的 RegimeAware / MagicFormulaPiot / ValueRevert /
MomentumValue / ValueMomentum / DividendYield / AlphaStack / MultiFactor
(含它們吃的 `Signals.scala`、`PublicationLag.scala`、`RebalanceCalendar.scala`、
`Backtester.scala`、`Universe.scala`、`QualityFilter.scala`)

---

## 一句話結論

**這批 Scala 策略是已凍結的歷史參考碼——實盤走的是 Python 的 Serenity `ev_v3_wf`,
根本不吃這些,所以沒有實盤資金風險。但單看「照學理算對了沒」,招牌的 Greenblatt
Magic Formula 兩根支柱都用錯了會計科目:**

1. 把「稅前淨利(EBT)」當成 EBIT——長榮 2603 因此被灌水 +85%,華航 2610 反而 −8%,
   連方向都不一致,選股排名真的會變(不是等比放大)。
2. Enterprise Value 的負債用「負債總計」(含應付帳款等無息營運負債),EV 灌水一成以上,
   Earnings Yield 系統性低估。
3. ROC 分母用「總資產 − 流動負債」(這是 ROCE/資本運用),不是 Greenblatt 的有形資本
   「淨營運資金 + 淨固定資產」。
4. EBIT 用的是累計 YTD(3/6/9/12 個月),不是 Greenblatt 明定的 TTM;而且 docstring
   還反過來寫「不用 TTM 才符合原著」——講反了。

**另外「52 週高點」其實只回看 252 個日曆天 ≈ 168 個交易日(約 34 週)。**

價值面那些反而算對了:pbBandPosition、殖利率、fcfYield、營收 YoY、z-score(用樣本
標準差 n−1)、百分位排名、以及發布落後 PublicationLag(PIT,除了 view 內部)都符合學理。

能不能信:
- **當成「歷史參考、已凍結」看** → 可以,但要知道它的 Magic Formula 排名有上述系統性偏差。
- **拿它的 ROIC / Earnings Yield 數字當真** → 不要,兩根支柱都用錯科目。
- **實盤** → 不受影響(實盤是 Python Serenity,零命中這些 Scala 碼)。

---

## 逐項對照

### 🔴 BUG-1｜Magic Formula 的 EBIT = 稅前淨利(用錯科目)

`Signals.greenblattROIC`(Signals.scala:606-609)與 `Signals.earningsYield`
(Signals.scala:651-654)的「EBIT」都抓 `繼續營業單位稅前淨利（淨損）` 這條 = **稅前
淨利 EBT**,變數還命名 `ebit_val`。

- **學理**:Greenblatt 兩根支柱都用 **EBIT**(營業利益,息前稅前),整個重點就是把
  不同利息/稅率的公司拉齊比較。EBIT ≈ 台灣的「營業利益」。
- **實測缺口(2023 全年,仟元)**:

  | 公司 | 營業利益(=EBIT) | 稅前淨利(程式用的) | 偏差 |
  |---|---|---|---|
  | 長榮 2603 | 34,750,086 | 64,171,957 | **+84.7%** |
  | 華航 2610 | 10,157,421 | 9,305,472 | −8.4% |
  | 長榮航 2618 | 29,566,265 | 28,839,755 | −2.5% |
  | 台積 2330(2022) | 1,121,278,851 | 1,144,190,718 | +2.0% |

  缺口的正負隨公司而變 → 橫截面排序真的會不同(不是等比放大就沒事)。
- **修法**:兩處 EBIT 子查詢改抓 `營業利益`/`營業利益（損失）`/`營業利益(損失)`
  ——這條在同表、且 `opIncomeGrowthYoY` 早就在用。

### 🔴 BUG-2｜Enterprise Value 的負債用「負債總計」而非有息負債

`Signals.earningsYield`(Signals.scala:667-671, 693-695):
`EV = 市值 + total_debt − 現金`,但 `total_debt` 抓 `負債總計`(Total Liabilities)。
程式註解自己寫的是「Total Debt」,取的欄位卻是負債總計,自相矛盾。

- **學理**:EV 的負債是**有息負債**(短期借款 + 應付短期票券 + 一年內到期長期負債 +
  長期借款 + 應付公司債 + 租賃負債),不含應付帳款/應付費用/合約負債等無息營運負債。
- **實測**:台積 2330 2022Q4 負債總計 = 2,004,290,011,光「流動負債合計」就
  944,226,817(大多是無息應付項)。有息負債遠小於 2.0 兆。把整包營運負債塞進 EV →
  EV 高估一成以上 → Earnings Yield 系統性低估,對「應付帳款多」的通路/代工懲罰最重。
- **修法**:`total_debt` 改為有息負債合計;必要時再加特別股 + 少數股權(Greenblatt 完整式)。

### 🟡 SUSPECT-3｜ROC 分母是 ROCE 的資本,不是 Greenblatt 的有形資本

`Signals.greenblattROIC`(Signals.scala:629-633):分母 = `資產總計 − 流動負債合計`。

- **學理**:Greenblatt ROC 分母 = **淨營運資金(NWC)+ 淨固定資產**,刻意排除商譽、
  無形資產、長期投資(只算對「有形營運資本」的報酬)。
- **實情**:總資產 − 流動負債 = NWC + **全部**非流動資產(含無形、商譽、長投、使用權、
  遞延稅)= 標準的 Capital Employed(ROCE 分母),相關但不同。docstring 也如實寫成
  TA−CL,屬「有註解的刻意選擇」,但與 focus 明列的 Greenblatt 定義不符。
- **實測**:台積 2330 2022Q4 程式分母 = 4,964,778,878 − 944,226,817 = 4,020,552,061;
  Greenblatt 有形資本(NWC + 淨 PP&E)≈ 3.56 兆 → 約大 13%,對商譽/長投重的控股公司
  差更多,ROC 被系統性低估。
- **修法**:分母 = (流動資產合計 − 流動負債合計) + 不動產廠房及設備淨額,排除無形/商譽/長投。

### 🟡 SUSPECT-4｜EBIT 是累計 YTD,不是 TTM;docstring 把 Greenblatt 講反

`greenblattROIC` docstring(Signals.scala:596)寫「Uses latest available quarterly
snapshot (not TTM) to match the original Magic Formula paper」——但 income_statement_
progressive 是**累計 YTD**(已驗:台積 2330 2022 營業利益 Q1..Q4 =
223,790,118→485,913,867→796,238,081→1,121,278,851,逐季累加)。

- **學理**:Greenblatt 用 **TTM** EBIT。所以「not TTM 才符合原著」是講反了——原著就是 TTM。
- **實情**:取「最新一季」的累計值 = 依 rebalance 落在哪季而定的 3/6/9/12 個月。同一天各股
  期間一致(排名 scale 一致),但整年在 3→6→9→12 個月間擺盪、從不等於 TTM;季節性強的
  公司在 3 個月 YTD vs TTM 下排名不同(Q1 rebalance 只用單季)。
- **修法**:改算 TTM EBIT(近 4 單季合計,或 YTD_latest + 去年全年 − 去年同期 YTD);修正 docstring。

### 🔴 BUG-5｜「52 週高點」其實只回看約 34 週

`Signals.distFrom52wHigh`(Signals.scala:289):`date >= asOf − INTERVAL '252 days'`
(日曆天),且**沒有** rn<=N 的交易日上限——用於 `ValueMomentumStrategy` 第二段排名。

- **學理**:52 週高點 = 近 ~252 個**交易日** ≈ 365 個日曆天的最高價。
- **實測**:252 日曆天(2330)= **168 交易日**(≈34 週);真 52 週(365 日曆天)= 245 交易日。
  窗只涵蓋約 69% 的一年 → 取到的「高點」系統性偏低 → 距高值系統性偏高(較不負),對「真高點
  落在 8–12 個月前」的股票排錯序。
- **對照**:同檔 `rsv120d`(180 天窗內 rn<=120)、`technicalConfirmation`(1 年窗內 rn<=200)
  都用交易日 rn-cap 做對了,唯獨這支用裸日曆窗。
- **修法**:窗改 `INTERVAL '365 days'`(或 ~380 天窗內 rn<=252),對齊 rn-cap 慣例。

### 🟡 SUSPECT-6｜同一根 K 棒「用收盤決策、又用收盤成交」(輕微前視)

`Backtester.run`(Backtester.scala:95-99, 151-199):rebalance 日以 `date<=today`
(含當日收盤、含當日盤後才發布的 T86/融資)算分,再以 `loadClosingPrices(today)`
= 當日收盤成交,無 +1 位移。價格型因子(距高 px_now、殖利率、EY 市值)都讀當日最新收盤。

- **學理 / 專案自訂**:CLAUDE.md「Asof-join +1 day shift」鐵律——as-of T 的 picks 應 T+1
  生效(T 收盤要收盤後才知道)。
- **判定**:月頻下偏誤小(約持有期 1/21)、且對所有候選對稱,但確為同棒前視。屬**有文件、
  已被接受的簡化**:CLAUDE.md 明載 Scala strategy/ 凍結、Python 為 canonical 且已做 +1
  位移,實盤 Serenity 不走此引擎。
- **修法**:凍結引擎明確標註為已知簡化即可;若拿來做 live 推論,成交改隔一交易日收盤。

### 🟡 SUSPECT-7｜F-Score 閘門吃的是非 Piotroski 的 view;註解把 9 項寫成 8 項

`QualityFilter`(MinFScore=5,QualityFilter.scala:20 註解寫「8 binary factors」)與
`MagicFormulaPiot`(minFScore=8,讀 `growth_analysis_ttm.f_score`)。

- **學理**:Piotroski (2000) F-Score = **9** 項二元、全部年對年。註解的「8」寫錯。
- **實情**:策略層自己不算 F-Score,讀的是 SQL view。該 view 的正確性已在姊妹單位
  **B-fscore-academic** 判為 BUG(Δ 項用 lag()=上一季而非去年,與正統僅 27.3% 逐格相同)。
  本單位只是「消費端」,真正算式在 view(屬 B 範疇)。
- **修法**:消費端改指向 B-fscore-academic 修正後的 `f_score_raw`;註解改「9 criteria」。

### 🟡 SUSPECT-8｜合成權重是魔術數字(無出處)

`AlphaStack`(AlphaStackStrategy.scala:97-101,0.30/0.25/0.15/0.15/0.15)、
`MomentumValue`(MomentumValueStrategy.scala:73,0.6/0.4)為裸字面權重,程式與 docstring
都無推導來源。兩者皆自述為基準/被否決策略(AlphaStack「比單用 pbBand 還差」)。對照
`MultiFactor` 用可辯護的等權 z-score。影響小,為完整性登記。

### 🟡 SUSPECT-9｜RegimeAware 門檻:docstring 0.10 vs 預設 0.05 不一致

`RegimeAwareStrategy.scala:36` 預設 `regimeThreshold=0.05`,但 docstring 30-32 行以
0.10 論述語意。屬文件債(讀者對「何時切 0050」會理解錯),非計算錯。修法:統一二者。

---

## ✅ 算對的部分(對照)

- **PublicationLag(PIT)**:季報 5/15、8/14、11/14、次年 3/31 各 +7 天緩衝、月營收次月
  10 日 +3 天;`asOfQuarter`/`asOfMonthlyRevenue` 取「截止不晚於決策日」的最新期——正確
  保守,無財報前視。所有基本面 signal 都經此閘,月中(day15+)rebalance 時點亦以此為據。
- **pbBandPosition**:現值 / 3.5 年中位 P/B,低=便宜,定義自洽,窗內 `date<=asOf` 無前視。
- **dividendYield / fcfYield / revenueYoYLatest / revenueYoY3M**:定義標準。
- **MultiFactor zscore**:除以 (n−1) 樣本標準差、clip 至 ±3σ,標準做法。
- **percentileRank**:(idx+1)/n,標準百分位。
- **relativeStrength(63 日 skip-5)/ momentum12m1m(12-1)**:Jegadeesh-Titman 精神正確
  (短跳過避開一週反轉)。
- **rsv120d**:交易日 rn<=120 取對,用收盤代盤中 H/L 是合理 close-only 近似(finlab 移植)。
- **capitalReductionBlacklist**:PIT 化(近 3 年減資),已修掉 notebook 原本寫死清單的前視。

（保留:`financial_index_ttm` / `growth_analysis_ttm` 這些 view 內部算式屬 B 範疇,不在本單位。）
