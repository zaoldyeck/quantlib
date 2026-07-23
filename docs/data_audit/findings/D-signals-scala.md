# D-signals-scala 稽核報告:32 個因子的學理定義正確性

**結論:大多數因子的算法忠於學理,但有 4 個計算式與它們自己宣稱的定義不符,會讓因子排序失真**——
其中最明確的是「52 週高」其實只看了約 8 個月、以及 Greenblatt 神奇公式那兩支(ROIC 和
獲利率)把「EBIT」拿成「稅前淨利」、把「有息負債」拿成「總負債」、把「投入資本」拿成
「總資產減流動負債」。這些不是抓不到證據的猜測,是用資料庫實際數字驗出來的偏差。

另有 5 個屬「方法學可疑但非硬錯」(SUSPECT):累計制營收成長的季別對不齊、現金流量品質比
的分母可為負、營收加速度沒去季節性、所有價格因子用未還原收盤價、以及 PIT 用法定截止日
(非實際申報日)對遲交公司會前視。

**重要背景(影響急迫性,不影響對錯判定)**:`Signals.scala` 屬 Scala `strategy/` 套件,
專案 CLAUDE.md 明載此套件為「凍結的歷史參考」,現役資金路徑是 Python(Serenity/apex)。
所以這些偏差是「參考碼在學理上寫錯了」,不是「現在的錢正踩在錯的因子上」——修法列出供
日後重用前清償,但不是線上事故。

- 受測檔:`src/main/scala/strategy/Signals.scala`(28 個計算函式,涵蓋動量/價值/籌碼/品質/技術)
- 消費端:Scala `ValueRevertStrategy`/`ValueMomentumStrategy`/`AlphaStackStrategy`/
  `DividendYieldStrategy`/`MagicFormulaPiotStrategy`/`MultiFactorStrategy`(全為凍結參考)
- 判定:**BUG**(4 個硬偏差 + 5 個 SUSPECT + 其餘 OK)

---

## 一、硬偏差(BUG)——與學理/自身文件不符,且會讓排序失真

### B1. 「52 週高」其實只看約 8 個月(距 52 週高 distFrom52wHigh)

- **學理**:George & Hwang (2004)「The 52-Week High and Momentum Investing」,52 週高 =
  過去 **52 週(≈365 天 / ≈250 個交易日)** 的最高價;近高程度 = 現價 / 52 週高。窗必須是滿一年。
- **程式**(Signals.scala:289):`date >= asOf − INTERVAL '252 days'`,取窗內 `MAX(closing_price)`
  當「52 週高」。這裡把「252」當成**日曆天**放進 interval——但 252 是**交易日**的年化天數,不是日曆天。
- **證據**:實測 2330 到 2024-06-28,`252 日曆天`窗內只有 **168 個交易日**;真正一年
  (`INTERVAL '1 year'`)有 **245-246 個交易日**。等於這個「52 週高」只看了 **約 8.3 個月(全年的 68%)**。
  一檔在 9-12 個月前見高的股票,它的高點被排除在窗外 → 算出來的「距高幅度」偏小(看起來比實際更貼近高點)。
  這是典型的交易日 / 日曆天混用。旁證:同檔其他因子的一年窗都寫對(momentum12m1m 用 `365 days`、
  technicalConfirmation 用 `1 year`),唯獨這裡踩雷。
- **修法**:`date >= asOf − INTERVAL '1 year'`(或 `'365 days'` / `'52 weeks'`)。若要更貼 George-Hwang,
  高點可改用 `MAX(highest_price)`(盤中最高)而非收盤最高。

### B2. 企業價值(EV)的「總負債」拿成了「負債總計」(Greenblatt 獲利率 earningsYield)

- **學理**:Greenblatt (2006)《The Little Book That Beats the Market》:獲利率 = EBIT / EV;
  EV = 股權市值 + **有息負債(interest-bearing debt)** − 現金。公司財務標準 EV 的「debt」指
  短期借款 + 一年內到期長期負債 + 長期借款 + 應付公司債等**有息**債務,**不含**應付帳款、
  應付費用等營業負債。
- **程式**(Signals.scala:670-672, 694, 701):`debt` CTE 取 `title IN ('負債總計','負債總額')` =
  **總負債(全部負債)**,再算 EV = 市值 + 總負債 − 現金。
- **證據**:2330 2023Q4,`負債總計 = 2,049,108,368`(仟元),但有息負債 = 應付公司債 913,899,843
  + 長期借款 4,382,965 + 短期借款 0 ≈ **918,282,808**。總負債是真有息負債的 **2.23 倍**,多出的
  1,130,825,560 全是營業負債(應付款/預收等)。EV 被灌水這一大塊 → 獲利率被低估,而且**灌水程度
  因股而異**——應付款重的產業(通路、零售、EMS 代工)被罰最重,直接扭曲截面排序。
- **修法**:只加總有息負債科目(短期借款 + 應付短期票券 + 一年內到期長期負債 + 長期借款 +
  應付公司債 + 視需要租賃負債),**不要用負債總計**。

### B3. 「投入資本」拿成了「總資產減流動負債」(Greenblatt ROIC greenblattROIC)

- **學理**:Greenblatt 神奇公式的資本報酬率 = EBIT / (**淨營運資金 + 淨固定資產**),其中淨固定資產
  = 不動產廠房及設備淨額;Greenblatt **刻意排除**商譽與無形資產(他主張這些不需資本去維持)。
- **程式**(Signals.scala:629, 633):投入資本 = **資產總計 − 流動負債合計**。docstring 還宣稱
  「to match the original Magic Formula paper」。
- **證據**:資產總計 − 流動負債 = (流動資產 − 流動負債) + 非流動資產 = 淨營運資金 + (淨固定資產 +
  **商譽 + 無形資產 + 長期投資 + …**)。由會計恆等式,它其實等於「權益 + 非流動負債」= **使用資本
  (Capital Employed)**。所以這支算的是 **ROCE(使用資本報酬率)**,不是 Greenblatt 的 ROC。對長期
  投資 / 無形資產龐大的公司(如 2330 資產總計 5,532,371,215 內含大額長期投資),資本基數被灌大 →
  ROIC 被低估;docstring「符合原論文」的宣稱不成立。
- **修法**:投入資本 = (流動資產 − 流動負債) + 不動產廠房及設備淨額,排除商譽 / 無形 / 長期投資,
  貼回 Greenblatt;或誠實改名為 ROCE 並拿掉「符合神奇公式」的宣稱。

### B4. 兩支神奇公式都把 EBIT 拿成「稅前淨利」(greenblattROIC + earningsYield 共病)

- **學理**:EBIT = 息前稅前利益 ≈ **營業利益**(非金融業);= 淨利 + 所得稅 + 利息費用。Greenblatt 的
  ROC 與獲利率兩根支柱都用 EBIT。
- **程式**:兩支的 `ebit` CTE(Signals.scala:606-609 與 651-654)都取
  `繼續營業單位稅前淨利(淨損)` = **稅前淨利(EBT)**,不是 EBIT。
- **證據**:2330 2023Q4(累計),`營業利益 = 921,465,606` vs `繼續營業單位稅前淨利 = 979,171,324`——
  差 6.3%(2330 是淨現金公司、有利息 / 投資收益,所以 EBT > 營業利益)。EBT 是**扣掉淨利息與業外
  後**的數字;對高財務槓桿、利息費用大的公司,差距更大且**方向相反**(EBT < 營業利益),使誤差符號
  因股而異 → 扭曲神奇公式排序。定義上 稅前淨利(EBT)≠ EBIT(EBIT = EBT + 利息費用)。
- **修法**:EBIT 改用 `營業利益`,或用 `稅前淨利 + 利息費用` 加回;兩支共用同一 `ebit` 來源,一起修
  (同源缺陷,舉一反三)。

---

## 二、方法學可疑(SUSPECT)——不一定當下爆,但學理上站不穩

### S1. 累計制營收成長,兩季可能對不齊(營業利益 YoY opIncomeGrowthYoY)

- **學理**:對**累計(YTD)**財報序列算 YoY,必須比**同一會計季**(今年到 Q2 累計 vs 去年到 Q2 累計);
  拿 9 個月比 6 個月毫無意義。
- **程式**(Signals.scala:744-758):`latest` 取 ≤(yr,qtr) 的最新季,`yearago` 獨立取 ≤(yr−1,qtr) 的
  最新季,**沒有強制兩者同一季**。
- **證據**:已實測 income_statement_progressive 是**累計制**(2330 2022 Q1→Q4 = 223M→485M→796M→1121M,
  單調遞增)。準時申報的公司兩邊都落在 quarter=qtr(對齊,沒事);但若某公司最新可得是 (yr, qtr−1)、
  而 yearago 落在 (yr−1, qtr),就會拿 YTD-3 個月 比 YTD-6 個月 → 爆出假性巨大成長。屬遲交 / 不規則
  申報的低頻個案,但這種離群值會在截面排序裡喧賓奪主。
- **修法**:由 `latest` 取出它的季別,強制 `yearago` = 去年同一季(join on quarter);或先把累計還原成
  單季再比。

### S2. 現金流量品質比的分母可以是負的(OCF/NI ocfToNetIncome)

- **學理**:盈餘品質 / 應計項目(Sloan 1996)的 OCF/NI,只有在 **NI > 0** 時可解讀(比值 > 1 = 保守
  會計);NI < 0 時這個比值沒有「品質」意義。
- **程式**(Signals.scala:530):`CASE WHEN ABS(profit) > 0 THEN ocf/profit`——只擋 0,**放行負分母**。
- **證據**:NI = −100、OCF = +50 → 比值 −0.5(有現金、虧損的公司被打成**負分**);NI = −100、OCF = −50 →
  +0.5(兩者皆負卻得正分)。虧損公司的「品質」分數符號被反轉 / 失義,把雜訊灌進截面。
- **修法**:分母改 `profit > 0`(只在有賺錢的公司間比品質),或改用規模無關的應計指標
  (如 (NI − OCF)/總資產,Sloan)。

### S3. 營收「加速度」沒去季節性(revenueAccel)

- **學理**:營收動量文獻的「加速度」= 成長**率**的變化(二階),如 ΔYoY = 本月 YoY − 上月 YoY,天生
  免疫季節性。用「最新月 / 前 3 月均」這種**水準比**,會把真加速度和季節性混在一起。
- **程式**(Signals.scala:808):最新月營收 / 前 3 個月營收均值(裸水準)。
- **證據**:台股月營收季節性極強(農曆年壓低 2 月、Q4 電子旺季)。例如 5 月讀數除以 avg(2,3,4 月)——
  而 2 月被農曆年壓低——就把「加速度」灌高,與基本面無關。此因子系統性地依公司的季節日曆位置給分,
  而非其營運趨勢。
- **修法**:改用 YoY 式加速度(ΔYoY),或和去年同月比,先去季節性;若確要水準比,先做季節調整。

### S4. 所有價格因子用「未還原」收盤價(系統性)

- **學理**:動量、52 週高、RSV、RSI、布林、實現波動這些價格因子,應以**還原(除權息 / 減資調整後)
  的總報酬價格**計算;裸收盤價在除息日與減資日有機械性跳空,那不是真報酬。
- **程式**:每個價格因子都讀裸 `daily_quote.closing_price`(relativeStrength、priceReturn、
  momentum12m1m、distFrom52wHigh、rsi14、bollingerPosition、lowVolatility60d、rsv120d、
  technicalConfirmation)。只有 lowVolatility60d 部分防護(`ABS(r) < 0.5` 濾掉大分割)。
- **證據**:專案 CLAUDE.md 自己的「canonical prices」鐵律明載——「讀裸 daily_quote.closing_price 會
  系統性少算現金股利再投入、忽略減資參考價重設」,`prices.py` 就是為此而生。台股除息日集中在 7-9 月;
  一檔 3% 殖利率的股票除息當天出現機械性 −3%,壓低它的 63 / 252 日動量、也拉低它的「52 週高」參考點——
  這是季節性、且因股而異的偏差,對動量 / 近高因子尤其致命。
- **修法**:價格因子改建在還原價面板上(等同 `prices.fetch_adjusted_panel` 的除權息前 / 後復權),
  至少在做窗口統計前對除息 / 減資參考價重設做回補調整。

### S5. PIT 用「法定截止日」而非「實際申報日」,對遲交公司會前視

- **學理**:point-in-time 回測只能在某公司某報告的**實際公布時點之後**才使用它。用全市場統一的法定
  截止日,會對任何「晚於截止日申報」的公司洩漏未來資料。
- **程式**:`PublicationLag.asOfQuarter/asOfMonthlyRevenue` 把 asOf 對應到「法定截止日 + 固定緩衝
  (季 7 天、月 3 天)已過」的最新期別;Signals 隨後納入所有 (year,quarter) ≤ 該天花板的列,**不看該列
  的真實申報日**。
- **證據**:台股法定截止 Q1 5/15、Q2 8/14、Q3 11/14、Q4 隔年 3/31,月營收次月 10 日。獲展延或晚於
  7 / 3 天緩衝申報的公司,其報告會被在「尚未公開」時就用上 → **正好對遲交公司(常是體質差的那些)前視**,
  使品質 / 價值因子被高估。多數公司準時,洩漏有界,緩衝也是有文件的取捨——但它是殘留的前視偏差,不是
  嚴格 PIT。
- **修法**:若能取得每份報告的實際申報時戳(MOPS),改用實際日過濾;否則把緩衝放寬到各報表型別實測的
  95 百分位申報延遲,並明載殘留洩漏。

---

## 三、學理正確 / 刻意近似(OK)——逐一背書

| 因子 | 學理定義(出處) | 判定 |
|---|---|---|
| relativeStrength(:80) | 63 日 skip-5 相對強度(rn=68→rn=5,跨 63 交易日、跳最近 5 日);符合動量避短期反轉慣例 | OK(近似;受 S4 影響) |
| momentum12m1m(:239) | Jegadeesh-Titman / Carhart UMD:12→1 月動量(跳最近 1 月)。程式 ~365 日前→~21-31 日前,符合 | OK(日曆天近似;skip 略短於 1 交易月) |
| shortTermReversal5d(:235)/priceReturn(:202) | 短期反轉(Jegadeesh 1990/Lehmann 1990),7 日曆天 ≈ 5 交易日,低者佳;泛用價格報酬 | OK |
| rsi14(:301) | Wilder(1978)RSI = 100−100/(1+RS)。程式用簡單均(Cutler 變體)。**已驗**:gain/loss 雖以整窗為分母,但同分母在 RS=gain/loss 相消,RS = Σgain/Σloss(14 筆)正是 Cutler 值;14 筆變動正確成形 | OK(公認變體;非 Wilder EMA 平滑) |
| bollingerPosition(:332) | 布林帶:中軌 20 日 SMA、帶寬 ±2σ 用**母體**標準差。程式 `STDDEV_POP` **正好符合**布林母體 σ 慣例;(P−MA20)/(2σ) 是 %b 的線性重映 | OK(完全正確) |
| lowVolatility60d(:360) | 實現波動 = 日對數報酬標準差 × √252。程式正確;母體 σ vs 樣本 σ 對 ~60 筆差 √(60/59)≈0.8%、單調不改排序 | OK(√252 正確;母體 σ 微註) |
| rsv120d(:709) | Lane 隨機指標 RSV =(C−最低)/(最高−最低)用盤中高低。程式用**收盤**高低(daily_quote 其實有 high/low) | OK(收盤變體;非盤中極值) |
| institutionalFlow20d(:50)/foreignNetBuy20d(:428)/dealerNetBuy20d(:449) | 法人買賣超 / 成交量(無量綱參與率)。**已驗單位一致**:分子(…difference,股)分母(trade_volume,股)同為股——2330 2024-06-20 外資 −8,939,051 / 量 52,144,900 = −0.171,合理 | OK(單位無誤) |
| marginCrowding20d(:385)/shortToMarginRatio(:405) | 融資使用率 = 融資餘額/融資限額;券資比 = 融券餘額/融資餘額(軋空代理)。均符標準台股籌碼定義,分母防零 | OK |
| pbBandPosition(:102)/peBandPosition(:471) | 估值帶位置 = 現值/自身 3.5 年中位數;`percentile_cont(0.5)` = 中位數正確;PIT(date≤asOf)正確。低者便宜——**符號需由排序層處理**(已於 docstring 標明) | OK(帶位置非倒數,符號交策略層) |
| revenueYoY3M(:24)/revenueYoYLatest(:575) | 營收 YoY =(本月−去年同月)/去年同月;月營收為**單月**(非累計)。3 月均為各月 YoY 平均,符合 docstring;去年基期>0 防護正確 | OK(受 S5 截止日 PIT 影響) |
| dividendYield(:504)/fcfYield(:542) | 股利殖利率 = DPS/價;自由現金流殖利率 = 每股 FCF/價(高者便宜)。PIT 正確 | OK(fcf 仰賴 view 為 TTM) |
| technicalConfirmation(:163) | 趨勢確認(SMA200 之上 + 50/200 黃金交叉 + 量能突增),三個 1/3 旗標。**docstring 寫 {0,0.5,1} 但實際產出 {0,⅓,⅔,1}**——僅文件筆誤,算術(三個 1/3)內部一致 | OK(改 docstring 為 {0,⅓,⅔,1}) |
| growthAnalysisField/financialIndexField/latestQuarterField(:138-158) | 最新可得季 PIT 載入器(DISTINCT ON 取 ≤ 天花板最新季);PIT 正確 | OK(受 S5 影響) |
| capitalReductionBlacklist(:772) | PIT 事件黑名單:asOf 前 lookbackYears 年內有減資的代號;date≤asOf 正確 | OK |

---

## 四、重現方式

```bash
# B1 52 週窗:252 日曆天 vs 一年 的交易日數
psql -h localhost -p 5432 -d quantlib -c "
SELECT (SELECT count(DISTINCT date) FROM daily_quote WHERE market='twse' AND company_code='2330'
   AND date<=DATE '2024-06-28' AND date>=DATE '2024-06-28'-INTERVAL '252 days') AS td_252cal,
  (SELECT count(DISTINCT date) FROM daily_quote WHERE market='twse' AND company_code='2330'
   AND date<=DATE '2024-06-28' AND date>=DATE '2024-06-28'-INTERVAL '1 year') AS td_1year;"
# 期望:168 vs 246

# B2/B4 EBIT 代理 與 EV 負債:2330 2023Q4
psql -h localhost -p 5432 -d quantlib -c "
SELECT title,value FROM income_statement_progressive WHERE company_code='2330' AND year=2023 AND quarter=4
  AND title IN ('營業利益（損失）','繼續營業單位稅前淨利（淨損）');
SELECT title,value FROM balance_sheet WHERE company_code='2330' AND year=2023 AND quarter=4
  AND title IN ('負債總計','應付公司債','長期借款');"
# 期望:營業利益 921,465,606 vs 稅前淨利 979,171,324;負債總計 2,049,108,368 vs 有息 ≈918,282,808

# S1 累計制驗證
psql -h localhost -p 5432 -d quantlib -c "
SELECT year,quarter,value FROM income_statement_progressive
WHERE company_code='2330' AND title='營業利益（損失）' AND year=2022 ORDER BY quarter;"
# 期望:223M→485M→796M→1121M(單調=累計)
```
