# A-operating_revenue — 解析正確性稽核

**結論一句話**:月營收原始檔的每一個「數字」欄位都正確、完整、單位無誤地落到
PostgreSQL,解析本身可以信任。橫跨 2001-2026、兩市場、個別/合併、三種實體檔案格式
(早期 HTML、2005-2012 Big5 的 11 欄 CSV、2013 後 UTF-8 的 14 欄 CSV)獨立重解析逐欄
比對,值全數相符。**唯三個別儲格不能信、但都不是解析錯**:那是原始檔自己就壞掉的值
(聯發科 2007-7 月營收暴衝 1000 倍、720 筆百分比是來源的溢位哨兵 999999.99、1805 在
2004-9 的公司名亂碼),reader 只是忠實照抄。另有一欄「備註」從頭到尾沒進資料庫
(與 C-operating_revenue 的 dim C 結論一致)。

**verdict: OK**(解析正確;不能信的儲格是來源問題,已標記供下游處理)

---

## 受測對象

- Setting:`src/main/scala/setting/OperatingRevenueSetting.scala`
- Reader:`FinancialReader.readOperatingRevenue`(`src/main/scala/reader/FinancialReader.scala:293-400`)
- 落地表:PostgreSQL `operating_revenue`(481,529 列、794 期別、2001-2026)
- 原始檔:`data/operating_revenue/{twse,tpex}/`,516 CSV + 278 HTML = 794 檔

## reader 宣告的三種格式與欄位對映(讀碼確認)

| 格式 | 年代/型別 | 欄數 | reader 分支 | 對映 |
|---|---|---|---|---|
| HTML | 2001-2012 個別(`_i.html`) | 每列 10 cell | `getData` tailrec(line 344) | 代號=cell0、名稱=cell1、8 個 Double=cell2..9;產業由 `產業別：(.*)` 列累積 |
| CSV(IFRS 前合併) | 2005-2012(`_c.csv`, Big5) | 資料列 11 欄 | `case 11`(line 371) | 代號=col0、名稱=col1、產業=None、8 個 Double=col2..9、`.init` 丟掉尾端空備註 col10 |
| CSV(IFRS 後) | 2013+(`_c.csv`, UTF-8) | 14 欄 | `case _`(line 375) | 代號=col2、名稱=col3、產業=col4、8 個 Double=col5..12、`.init` 丟掉備註 col13 |

- 編碼:HTML 恆 `Big5-HKSCS`(line 330);CSV `if (year>2012) UTF-8 else Big5-HKSCS`(line 366)。
- 期別(year/month)一律取自**檔名** regex,內容的 出表日期/資料年月 欄不看。
- `QuantlibCSVReader`:讀取前對每列 `.replace("=","")`(拆 TWSE 的 `="2330"` 防呆包裝),
  並跳過含 `""` 但不含 `,""` 的列;`.toDoubleOption` 把 `N/A`、`&nbsp;`、空字串轉成 NULL。
- 兩個 CSV 分支剛好用「欄數 11 vs 14」互斥,不會誤判。

---

## 逐項查證(方法:自寫 Python 讀原始檔,**不呼叫** reader,再對 PG 逐欄比對)

### 1. 值保真 — 5 個跨時代樣本逐欄全對(OK)

`scripts/A-operating_revenue/rowcount_parity.py` + 手動比對:

| 樣本 | 原始值 → PG 值 |
|---|---|
| twse 個別 2001-6 台泥 1101 | `1,690,526`→`1690526`、產業「水泥」、YoY 16.93、cum `9,761,724`→`9761724` 全對 |
| tpex 個別 2001-6 五鼎生技 4101 | 8 個數字 + 產業「生物科技」全對 |
| twse 合併 2008-1 三芳 1307 | `="1307"`→`1307`、產業 NULL、8 數字全對 |
| twse 合併 2008-1 遠東新 1402 | `N/A`→NULL(上月營收、上月比較增減)、其餘全對 |
| twse 合併 2024-1 台泥 1101 | 產業「水泥工業」、月營收 `7237930`、百分比全精度 `-23.923687931066922` 保真 |

千分位逗號正確剝除、`="XXXX"` 正確還原、`N/A`→NULL、浮點全精度保留。

### 2. 單位(OK)

月營收/累計營收單位為**仟元**,reader 原數照存不做任何 ×1000 或 ÷1000。經濟錨:
台泥 2024-1 = 7,237,930 仟元 = NT$72.4 億(對真實月報一致);百分比原數照存
(-23.92 代表 -23.92%,不是 -0.2392)。

### 3. Schema drift / 欄位錯位免疫(OK)— 這是最關鍵的系統性檢查

`scripts/A-operating_revenue/full_scan.py` 掃全部 CSV 檔的資料列欄數分佈:
**只有 {11, 14} 兩種,11 欄 28,405 列、14 欄 259,418 列,無任何其他欄數。**
代表 reader 的 `case 11`/`case _` 二分法對 25 年全部歷史檔案都命中,不存在
「新增欄位 → fall-through 把值放錯欄」的無聲錯位。若哪天出現第三種欄數(如 12/13),
`case _` 的 `transferValues(7)` 會 IndexOutOfBounds **整檔炸掉(fail-loud)**,不會靜默錯值。

HTML 路徑對「壞掉的 HTML」也穩:2001 tpex 的 4108 懷特新藥那一列**缺 `</tr>` 閉合標籤**,
我的 stdlib/regex 土炮解析器會把它和下一列 `合計` 併成 19 cell 而漏掉,但 reader 用的
Jsoup 有 HTML5 錯誤修復(遇到新 `<tr>` 自動關掉上一個),正確切出 10 cell → 月營收 0、
百分比 `&nbsp;`→NULL,PG 完全正確。**這裡 reader 比我的獨立解析器更強**。

### 4. 正負號(OK / REAL)

負月營收全部落在金融保險/金控(新光金 2888、華南金 2880、富邦金 2881、第一金控 2892),
最負 -44,427,526(新光金 2025-6 合併)逐位對原始檔相符,原始檔備註自證
「主係增提外匯價格變動準備金 611 億元所致」。這是 CLAUDE.md 已載明的真實現象
(金控以合併子公司損益申報「營業收入」),不是符號 bug。

### 5. 編碼(OK,唯一例外見發現 F4)

- CSV:UTF-8/Big5 分界(以 2012 為界)正確,全部真 CSV 檔用 reader 選的編碼解碼**零亂碼**
  (`�` 出現數 = 0)。
- HTML:278 檔中 277 檔 Big5 嚴格解碼乾淨;唯一 `twse/2004/2004_9_i.html` 第 59769 byte
  有非法 Big5 byte 0x84 → 見 F4(只毀一個公司名,數字不受影響)。

### 6. 日期(OK)

reader 用檔名定期別。`scripts/A-operating_revenue/date_and_sign.py` 對 324 個 IFRS 後
CSV 檔,把內容的「資料年月」(民國)轉西元後對檔名比對,**MISMATCH = 0**。民國→西元
換算正確,檔名日期 = 內容日期。IFRS 前 HTML/CSV 無內容日期欄,靠值保真間接確認落對期別。

### 7. DB 健全性(OK)

`company_code`/`company_name` 空值 = 0;八個數字欄全 NULL 的列 = 0(無整列解析失敗);
產業 NULL 全部 28,405 列 = IFRS 前 11 欄合併檔(來源本就沒有產業欄),個別型 0 NULL、
空字串產業 0 列(HTML 的產業累積器沒有「資料列早於第一個產業標題」的漏洞)。

---

## 發現(都不是 reader 解析 bug)

### F1 — 聯發科 2454 合併 2007-7 月營收 = 6,932,075,206 仟元,約大 1000 倍(REAL:來源錯,忠實照抄)

全表唯一 > NT$1 兆的月營收。原始檔 `data/operating_revenue/twse/2007/2007_7_c.csv` 那一列
本身就是壞的:`="2454",聯發科,6932075206,6010229,0,115237.95,999999.99,39979216644,0,999999.99,`
——月營收 6,932,075,206、上月比較增減 +115237.95%、去年同月增減 999999.99(來源溢位哨兵)。
鄰月月營收都在 6-9 百萬仟元量級,**同月的個別(HTML)檔卻是正常的 6,389,013**。
所以這是來源合併 CSV 自己的錯,reader 逐位照抄(原始 = PG),不是解析錯。
但這是下游地雷:任何用合併月營收算聯發科 2007-7 的策略會看到 1000 倍暴衝。
本專案的 `src/quantlib/audits/05_revenue_audit.py`(極端 YoY 掃描)本該抓到它,但該腳本目前
一跑就爆(見 C-operating_revenue),所以沒被攔下。

### F2 — 720 筆百分比欄是來源哨兵 999999.99,原數照存(SUSPECT:忠實但下游有風險)

TWSE 用 999999.99 當「無法計算/溢位」的百分比哨兵(如去年當月營收為 0 時的 YoY)。
reader 忠實存成字面 999999.99 而非 NULL。任何對三個百分比欄做平均/加總的消費端會被毒化。
非解析錯(來源如此),但值不可當真實百分比用。

### F3 — 1805(凱聚)在 twse 2004-9 個別的公司名亂碼(REAL:來源 byte 毀損,忠實照抄)

`twse/2004/2004_9_i.html` 名稱欄的 byte 是非法 Big5 `\x84P\xbe\xdb`,PG 存成「�擄」,
而同一代號在其他月份都是「凱聚」。Jsoup 寬鬆解碼照過。**只毀公司名這一格,代號 1805
與所有數字(月營收 85、累計 8083)完全正確、沒有欄位位移**。策略以代號 + 數字為鍵,
顯示名亂碼無分析影響。屬來源/下載 byte 毀損,非解析邏輯錯。

### F4 — 「備註」欄從未進資料庫(OK/資訊損失,與 C-operating_revenue 同源結論)

reader 的 CSV 分支 `.init` 丟掉最後一欄(IFRS 後 = 備註 col13;IFRS 前合併 = 尾端空欄),
Slick 表也無此欄。備註多半是「-」/「無」,但抽樣 2020+2024 見 2,662 個不同的實質備註,
包含正是判斷金融負營收真偽所需的說明(「增提外匯價格變動準備金…」「自結數尚未經
會計師簽證」「海外子公司之營收係以當月平均匯率換算」等)。**諷刺的是 CLAUDE.md 自己說
「金融業負營收…always has 備註 field, check before flagging」,但備註沒進庫,這個 check
從資料庫做不到。** 非數字正確性 bug,是完整性缺口。dim C 已列同一項;此處從 reader
角度覆述並給修法。

---

## 修法建議(供主流程裁決;稽核不改碼)

- **F1/F2**:修好並接回每日健檢 `src/quantlib/audits/05_revenue_audit.py`(它現在會 crash,
  見 C-operating_revenue),讓極端值/哨兵能被例行攔下;或在 reader 端把 999999.99 這類
  已知哨兵映射成 NULL(需先確認是否所有消費端都想要 NULL)。
- **F3**:單格來源毀損,建議在 `docs/data/data_quality_incidents.md` 記一筆即可,不需改碼。
- **F4**:若要讓 research 端能自行判斷負營收真偽,在 `db/table/OperatingRevenue.scala` 加
  `remark` 欄、reader 兩個 CSV 分支保留最後一欄(IFRS 前合併該欄恆空、IFRS 後為備註)、
  並同步 `research/cache_tables.py` 的 SELECT。

## 可重跑證據

```bash
# 全欄數 + 編碼掃描(schema drift 免疫證明)
python3 docs/data_audit/scripts/A-operating_revenue/full_scan.py
# 獨立重解析 + 逐檔 distinct-code 列數對帳
python3 docs/data_audit/scripts/A-operating_revenue/rowcount_parity.py
# 檔名 vs 內容日期 + 負值
python3 docs/data_audit/scripts/A-operating_revenue/date_and_sign.py

# F1 原始檔自證
python3 -c "import csv;print([r for r in csv.reader(open('data/operating_revenue/twse/2007/2007_7_c.csv',encoding='big5-hkscs')) if r and r[0].strip('=\"')=='2454'])"
# F1/F2 PG 端
psql -h localhost -p 5432 -d quantlib -c "SELECT year,month,type,monthly_revenue FROM operating_revenue WHERE company_code='2454' AND year=2007 ORDER BY type,month;"
psql -h localhost -p 5432 -d quantlib -c "SELECT count(*) FROM operating_revenue WHERE \"monthly_revenue_compared_last_year(%))\"=999999.99;"
```
