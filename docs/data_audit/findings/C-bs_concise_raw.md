# C-bs_concise_raw — cache 與 PostgreSQL 的一致性與缺漏

**結論:🔴 BUG。每一個數字都對,但「這一季有幾家公司」不能信。**

白話講三件事:

1. **cache 跟 PostgreSQL 完全一樣**——全表 214 萬列、7 個欄位,雙向逐列比對零差異。
   用 DuckDB 查跟用 PostgreSQL 查會拿到一模一樣的答案,這條可以放心。
2. **數字本身也對**——88,583 個「公司 × 季」的「資產 = 負債 + 權益」誤差是 **0**,
   一筆不差。原始檔裡「不適用」的欄位被正確丟掉,沒有被寫成 0。
3. **但 2026 年第一季只抓到 28% 的公司**:1,950 家應該有的,只有 539 家。
   原因是原始檔在**法定申報截止日(5/15)前 5 天**就被抓走,只撈到提前交卷的公司;
   而爬蟲的規則是「這一季只要有檔案就永遠不再抓」,所以今天(過期 68 天了)
   還停在那個殘缺的快照上。同樣的機制在過去也留下 13 個永久缺角的季度,
   **金融股被砍得最兇**(銀行業半年報截止日更晚,每次都趕不上快照)。

下游已經吃到這個洞:`research/raw_quarterly.parquet` 的 2026Q1 只有 539 家。
好消息是**現役的 Serenity 實盤引擎沒有用這份季報資料**
(`research/serenity/engine.py:918` 明寫 `register_raw_quarterly=False`),
所以今天的下單沒被影響;受影響的是所有做基本面因子的回測與研究。

---

## 一、cache vs PostgreSQL:逐列逐欄全等(🟢 OK)

不是抽樣,是**全體**。DuckDB 同時掛上 PG 與 cache,雙向 `EXCEPT ALL`:

| 項目 | PostgreSQL | DuckDB cache |
|---|---|---|
| 列數 | 2,142,171 | 2,142,171 |
| PG 有、cache 沒有的列 | **0** | |
| cache 有、PG 沒有的列 | | **0** |
| 重複主鍵 | — | **0** |

PG 全表其實有 348 萬列,cache 只取其中 214 萬列——因為
`research/cache_tables.py:88-91` 明寫 `WHERE market IN ('twse','tpex') AND type='consolidated'`:

| market | type | PG 列數 | 進 cache? |
|---|---|---|---|
| twse | consolidated | 1,242,726 | ✅ |
| tpex | consolidated | 899,445 | ✅ |
| twse | individual | 883,657 | ❌ 刻意排除 |
| tpex | individual | 455,882 | ❌ 刻意排除 |

1,242,726 + 899,445 = 2,142,171,與 cache 總數**一字不差**。

**schema 型別零降級**:

| 欄位 | PostgreSQL | DuckDB |
|---|---|---|
| market / type / company_code / title | `character varying` | `VARCHAR` |
| year / quarter | `integer` | `INTEGER` |
| value | `double precision` | `DOUBLE` |

PG 的 9 欄裡,cache 沒帶 `id`(序號,無分析價值)與 `company_name`(公司名,
可從別表取)。這是刻意投影,不是漏欄。

**逐季分組比對**:167 個 `(market, year, quarter)` 分組,列數 / 公司數 / 科目數
三個數字**全部相等,0 組不符**。

**抽樣佐證**(隨機 3 個季 × 5 檔,共 302 列 2,114 個欄位值):

| 季 | 抽到的公司 | cache 列數 | PG 列數 | `DataFrame.equals` |
|---|---|---|---|---|
| 2008Q1 | 3221 / 2464 / 3021 / 3303 / 2606 | 110 | 110 | **True** |
| 2016Q1 | 4426 / 1454 / 3003 / 2488 / 8415 | 82 | 82 | **True** |
| 2009Q1 | 6292 / 3211 / 1707 / 3022 / 9944 | 110 | 110 | **True** |

cache 建置時間 2026-07-21 08:16,PG 之後沒有新增列,兩邊沒有時間差。

---

## 二、2026Q1 只有 28% 的公司(🔴 BUG,最嚴重)

| | 2025Q4 | 2026Q1 | 缺 |
|---|---|---|---|
| twse | 1,069 家 | **311 家** | 758 |
| tpex | 881 家 | **228 家** | 653 |
| 合計 | 1,950 | **539** | **1,411(72%)** |

### 為什麼

原始檔的下載時間出賣了一切:

```
data/balance_sheet/twse/2026/2026_1_a_c_0.csv   78,852 B   mtime 2026-05-10
data/balance_sheet/twse/2026/2026_1_a_c_1.csv    1,815 B   mtime 2026-05-10
data/balance_sheet/tpex/2026/2026_1_a_c_0.csv      662 B   mtime 2026-05-10
data/balance_sheet/tpex/2026/2026_1_a_c_1.csv   55,215 B   mtime 2026-05-10
```

**2026-05-10 比證交法 §36 的第一季申報截止日 2026-05-15 還早 5 天。**
完整的一季 twse 應該有 6 個產業別表(2025Q4:10 + 3 + 1,033 + 13 + 6 + 4 = 1,069 家),
2026Q1 只抓到 2 個表、310 + 1 家。

而且**它永遠不會自己補回來**,兩段程式碼合起來造成:

- `src/main/scala/Task.scala:589-597` — `excludeYearToQuarter` 的解鎖時間是
  **月初**:Q1 在 5/1 解鎖、Q2 在 8/1、Q3 在 11/1、Q4 在 3/1。
  **四個視窗全部早於各自的法定截止日(5/15、8/14、11/14、3/31)。**
- `src/main/scala/Task.scala:599` — `.filterNot(existFiles)`,配上
  `src/main/scala/setting/Detail.scala:123-130` 的 `getTuplesOfExistFiles`
  以 `(year, quarter)` 為去重粒度:**該季只要存在任何一個檔,整季跳過。**

所以只要在解鎖日到截止日之間跑過一次 `Main update`,那一季就被永久釘死在殘缺狀態。

### 影響已經materialize

`research/raw_quarterly.parquet`(2026-07-07 產出,第一性原理季度因子面板,
Piotroski F9 / ROA / 毛利率都從它來):

| 季 | 列數 |
|---|---|
| 2025Q3 | 1,927 |
| 2025Q4 | 1,950 |
| **2026Q1** | **539** |

`raw_quarterly.py` 的 PIT 規則是「Q1 在 5/22 之後可用」,所以今天跑任何基本面
排序,2026 年的截面只看得到 28% 的市場。

---

## 三、同一根因的歷史殘留:13 個永久缺角的季(🔴 BUG)

把每一季的「原始檔最晚下載時間」對上「法定截止日」,再比對公司數,規律非常乾淨:
**離截止日越近抓,缺越多;隔一年以後才抓,一家不缺。**

| market | 季 | 下載日 | 距截止日 | 公司數 | 缺 |
|---|---|---|---|---|---|
| twse | 2026Q1 | 2026-05-10 | **−5 天** | 311 | **758** |
| tpex | 2026Q1 | 2026-05-10 | **−5 天** | 228 | **653** |
| twse | 2023Q2 | 2023-08-20 | +6 | 890 | 111 |
| twse | 2025Q2 | 2025-08-21 | +7 | 947 | 101 |
| twse | 2024Q1 | 2024-05-16 | +1 | 996 | 74 |
| twse | 2023Q3 | 2024-03-12 | +119 | 1,001 | 69 |
| tpex | 2023Q3 | 2024-03-12 | +119 | 815 | 66 |
| tpex | 2024Q1 | 2024-05-16 | +1 | 821 | 60 |
| tpex | 2025Q2 | 2025-08-21 | +7 | 828 | 38 |
| tpex | 2023Q2 | 2023-08-20 | +6 | 782 | 33 |
| twse | 2024Q3 | 2024-11-22 | +8 | 1,014 | 30 |
| twse | 2025Q3 | 2025-12-10 | +26 | 1,048 | 21 |
| tpex | 2024Q3 | 2024-11-22 | +8 | 832 | 21 |
| tpex | 2025Q3 | 2025-12-10 | +26 | 866 | 15 |
| twse | 2022Q4 | 2023-04-26 | +26 | 976 | 14 |

**反證(證明晚抓就沒事)**:tpex 2023Q1 在截止後 **1,074 天**才抓 → 缺 0;
twse 2023Q4 在截止後 753 天才抓 → 公司數反而比鄰季多 69 家。

### 金融股是最大受害者

金融業的半年報申報期限晚於一般業,而 2023Q2 / 2025Q2 的快照都在 8/20-8/21 拍,
剛好卡在中間:

| 表(依表頭辨識) | 2023Q1 家數 | 2023Q2 家數 |
|---|---|---|
| 銀行業(「存放央行及拆借銀行同業」) | 10 | **3** |
| 一般業 | 954 | **881** |
| 另一張金融業表(「存放央行及拆借金融同業」) | 13 | **整張表沒抓到** |
| 保險業(「應收款項 / 待出售資產」) | 6 | **2** |
| 一般格式小表 A | 3 | **整張表沒抓到** |
| 一般格式小表 B | 4 | 4 |

2025Q2 同型:銀行業 11 → **3**、另一張金融業表 14 → **1**、保險業 6 → **2**、
一般格式小表 A 3 → **整張表沒抓到**、一般業 1,005 → **937**。

---

## 四、異常值掃描:全清(🟢 OK)

| 檢查 | 結果 |
|---|---|
| `value` 為 NULL | 0 |
| `value` 為 NaN / Inf | 0 / 0 |
| 鍵欄位(market / company_code / title)為 NULL | 0 |
| 重複主鍵 `(market,type,year,quarter,code,title)` | 0 |
| `quarter` 不在 1..4 | 0 |
| 未來期別(> 2026Q2) | 0 |
| **資產總計 − (負債總計 + 權益總計)** | **最大誤差 0.0,violations = 0 / 88,711** |

恆等式再分兩套命名各自驗:post-IFRS「總計」56,570 個公司季、pre-IFRS「總額」
32,013 個公司季(合計 88,583,加上兩套命名混用的 128 個公司季 = 88,711),
**兩邊都是 0 違反、最大差 0.0**。這是「數字沒被解析錯位」最強的證據。

**「--」被正確丟棄,不是寫成 0**。以 1216 統一 2026Q1 為例,原始列有 4 個 `--`
(權益─具證券性質之虛擬通貨、庫藏股票、共同控制下前手權益、合併前非屬共同控制股權),
cache 該公司季只有 17 個 title,那 4 個科目**完全不存在**。對應程式:
`src/main/scala/reader/FinancialReader.scala:98` 的
`.filter(v => Try(v._2.toDouble).isSuccess)`。

**長格式(title 當鍵)讓欄位漂移自動被吸收**:2026 年新增的「權益─具證券性質之
虛擬通貨」欄位直接變成一個新 title,不會像寬表那樣把後面所有欄位往左推一格。
全表共 141 個 title,同時涵蓋 post-IFRS(資產總計)與 pre-IFRS(資產總額)兩代命名。

**極端值都有名字**:

| 現象 | 筆數 | 判定 |
|---|---|---|
| 資產總計 = 0 | 1 | TDR 發行人 / 已下市空殼(1258) |
| 股本 = 0 | 6 | 910801、910708(TDR)、4144、3990、1258 |
| 流動資產 / 流動負債 = 0 | 各 5 | 同上 |
| 權益總計 < 0 | 9 | 真的淨值為負(6497、6287、3043、3085) |

---

## 五、2018 年以前 Q1/Q3 少 40~80 家 = 真的,不是漏抓(⚪ REAL)

早年 Q1/Q3 系統性比 Q2/Q4 少幾十家,看起來很像漏抓,查證後是**法規**:

> 未上市、未上櫃之國內及外國公司……**得免公告申報第一季及第三季合併財務報告**
> ——[公開發行公司財務報告及營運情形公告申報特殊適用範圍辦法 §3](https://law.fsc.gov.tw/LawContent.aspx?id=GL000593)

決定性樣本(cache 裡該公司出現過的季):

- **2633 台灣高鐵**:2013Q2、2013Q4、2014Q2、2014Q4、2015Q2、2015Q4,
  然後 **2016Q1 起四季齊全**(2016-10-27 上市)。
- **5876 上海商銀**:2012Q4 起只有 Q2/Q4,**2018Q1 起四季齊全**(2018-10-19 上市)。
- 1760 寶齡富錦、1587 吉茂、2897 王道銀行同型態。

**排除「漏抓」的鐵證**:這些季的原始檔是 **2020-07-19** 抓的(離截止日 1,700~2,600 天),
而且 2015Q1 twse 有**完整 6 個表**——檔案裡就是沒有這些公司:

| 代號 | 2015Q1 原始檔出現次數 | 2015Q2 | 2015Q3 | 2015Q4 |
|---|---|---|---|---|
| 2633 台灣高鐵 | **0** | 1 | **0** | 1 |
| 5876 上海商銀 | **0** | 1 | **0** | 1 |
| 2897 王道銀行 | **0** | 1 | **0** | 1 |
| 1216 統一(對照組) | 1 | 1 | 1 | 1 |

**另一個要知道的邊界**:2004Q1~2007Q3 的 Q1/Q3 在 cache 幾乎是空的(2~22 家),
因為當年合併報表只出半年報與年報。PG 裡那些季的 `individual`(個體)型別有
1,000~1,144 家,但 `cache_tables.py` 的 `type='consolidated'` 過濾把它們全排除。
這是刻意設計,不是 bug——但**用 cache 的人要知道 2008 年以前沒有 Q1/Q3 合併資產負債表**。

> **給下游的規則**:「某季查無此公司」≠「公司消失/退市」。2018 年以前的 Q1/Q3 尤其。

---

## 六、`market` 欄的語意陷阱(🟡 SUSPECT)

`market` 的意思是「這列來自 MOPS 的哪個 TYPEK 頁面(sii / otc)」,
**不是「這檔股票在哪裡掛牌」**。尚未上市的公司會帶著 `market='twse'` 出現好幾年:

| 代號 | cache 最早季 | 實際上市日 |
|---|---|---|
| 2633 台灣高鐵 | 2013Q2(market=twse) | 2016-10-27 |
| 5876 上海商銀 | 2012Q4(market=twse) | 2018-10-19 |

拿 `bs_concise_raw.market` 當「可交易性」或「市場別」的過濾條件,會把當年還買不到的
公司算進 universe。判斷「當時能不能買」請用 `daily_quote` 當日有沒有報價。

其餘 market 相關檢查都健康:

- 同一季同一代號出現在兩個 market:**0 筆**
- 跨期換過 market 的代號:13 個(轉上市,正常)
- 6 碼代號(TDR 發行人,910322 / 911610 等):5,164 列,僅 2007~2013,
  twse 24 檔 + tpex 1 檔。**與 daily_quote 的 4 碼代號 join 不上會靜默消失。**

---

## 七、補抓怎麼做(順序不能反)

`FinancialReader.readBalanceSheet`(`src/main/scala/reader/FinancialReader.scala:63-75`)
會**跳過 DB 裡已有 `(market, type, year, quarter)` 的檔案**,而且用
`++=` 純 insert(無 upsert)。所以「只刪檔重抓」沒有用,「不刪 DB 直接重讀」會撞
unique index。正確順序:

```bash
# (1) 先刪 PG 的該季資料
psql -h localhost -p 5432 -d quantlib -c \
  "DELETE FROM concise_balance_sheet WHERE year=2026 AND quarter=1;"

# (2) 再刪原始檔(不刪的話 pullQuarterlyFiles 會整季跳過)
rm data/balance_sheet/twse/2026/2026_1_a_c_*.csv
rm data/balance_sheet/tpex/2026/2026_1_a_c_*.csv

# (3) 重抓 + 重讀 + 重建 cache
sbt "runMain Main pull balance_sheet"
sbt "runMain Main read balance_sheet"
uv run --project research python research/cache_tables.py
```

端點就是現有的 MOPS `t163sb05`(`TYPEK=sii` / `otc`,設定在
`application.conf` 的 `data.balanceSheet.page.afterIFRSs`),不需要新端點。

**建議一次補完的季**:
twse 2022Q4、2023Q2、2023Q3、2024Q1、2024Q3、2025Q2、2025Q3;
tpex 2023Q2、2023Q3、2024Q1、2024Q3、2025Q2、2025Q3;外加 2026Q1 兩市場。
這些季距截止日都已超過一年,重抓即可拿到完整名單。

**根因守護(不做就會再犯)**:

1. 把 `Task.scala:589-597` 的解鎖時間改成「法定截止日 + 緩衝」
   (Q1 → 6/1、Q2 → 9/1、Q3 → 12/1、Q4 → 5/1),不要在月初就開窗。
2. 去重粒度不能只看「該季有沒有檔」。加一條「該季公司數 < 鄰季 95% 就重抓」
   或「檔案 mtime 早於截止日 + 30 天就重抓」的規則。
3. 加測試鎖死「任一季的公司數不得低於前後季最大值的 95%」,先紅後綠驗證。

> **注意**:`concise_income_statement_progressive` 與 `cash_flows_progressive`
> 走同一條 `pullQuarterlyFiles` 路徑,**幾乎確定有一模一樣的破洞**,建議一起檢查補齊。

---

## 附錄:重跑腳本

四支探針(本次稽核用,邏輯已完整寫在上文,可依此重建):

| 探針 | 做什麼 |
|---|---|
| `bs_parity.py` | ATTACH pg + ATTACH cache,雙向 `EXCEPT ALL` 全列比對 + 重複鍵檢查 |
| `bs_gaps.py` | 找「前後季都有、這季沒有」的公司(排除 IPO / 下市的雜訊) |
| `bs_download_timing.py` | 每季原始檔 mtime vs 法定截止日 vs 公司數 vs 鄰季最大值 |
| `bs_anomaly.py` | NULL/NaN/Inf、重複鍵、期別、極端值、會計恆等式、title 詞彙 |

核心 SQL(全列比對):

```sql
-- DuckDB: ATTACH postgres AS pg, ATTACH cache AS ca
CREATE TEMP TABLE p AS
SELECT market, type, year, quarter, company_code, title, value
FROM pg.public.concise_balance_sheet
WHERE market IN ('twse','tpex') AND type = 'consolidated';

SELECT COUNT(*) FROM (SELECT * FROM p EXCEPT ALL SELECT * FROM ca.bs_concise_raw);  -- 0
SELECT COUNT(*) FROM (SELECT * FROM ca.bs_concise_raw EXCEPT ALL SELECT * FROM p);  -- 0
```

會計恆等式:

```sql
WITH w AS (
  SELECT market, year, quarter, company_code,
         MAX(CASE WHEN title IN ('資產總計','資產總額') THEN value END) a,
         MAX(CASE WHEN title IN ('負債總計','負債總額') THEN value END) l,
         MAX(CASE WHEN title IN ('權益總計','權益總額') THEN value END) e
  FROM bs_concise_raw GROUP BY 1,2,3,4)
SELECT COUNT(*) rows_with_all3,
       COUNT(*) FILTER (WHERE abs(a-(l+e)) > 1e-6*greatest(abs(a),1)) violations,
       MAX(abs(a-(l+e))) maxdiff
FROM w WHERE a IS NOT NULL AND l IS NOT NULL AND e IS NOT NULL;
-- 88711 / 0 / 0.0
```
