# B-matview-4_cash_flows_with_titles:現金流「取標題」視圖的財務定義審查

**審查對象**:`src/main/resources/sql/materialized_view/4_cash_flows_with_titles.sql`
**跑法**:`psql -h localhost -p 5432 -d quantlib -f docs/data_audit/scripts/B-matview-4_cash_flows_with_titles/checks.sql`

## 結論(白話)

**這張表把每檔股票的現金流量表拆成 5 個欄位(折舊、存貨變動、營業現金流、資本支出、
現金股利),取值本身抄得對,但有一條「只留負數、丟掉正數」的濾網把資料砍掉一大半——
最嚴重的是「存貨變動」欄,105,954 筆裡有 48,167 筆(45%)被砍成空值,其中 47,511 筆
是真實發生的「存貨減少、現金流入」,不是壞資料。** 台積電近 6 季就有 3 季的存貨變動被
砍成空白。這些空值到了下游會被當成 0,把「現金流量允當比率」算歪,還會讓季表的
「每股自由現金流」直接變成空值(約 1 萬個公司季)。**判定:BUG(視圖自身的選取邏輯錯)。**

另外這張表原封不動地繼承了上游 `cash_flows_individual`(matview 3)的「累計轉單季」
差分 BUG(半年報公司跨年度亂減)——本檔沒有能力也沒有嘗試修正它,只是把汙染值傳下去;
根因與修法見 `B-matview-3_cash_flows_individual`。佐證:本應恆正的「折舊」欄有 3,671 筆
(3.2%)是負的,就是上游差分汙染穿過本檔的痕跡。

好消息:**現役 Python 選股引擎(Serenity/apex)不讀這條鏈**(它自己從原始累計表重算),
所以實盤沒被毒到;中招的是 Scala 因子研究鏈(`financial_index_ttm/quarterly`、
`growth_analysis_ttm`、`Main.scala` 把 `cbs` 當因子掃 IC)與任何直接查這幾張視圖的人。

---

## 這張表在做什麼(逐欄公式)

**沒有任何四則運算**,純粹「照標題挑一列、把 `value` 抄過來」。驅動表是 `ocf`,其餘 4 欄
用 `left join (year, quarter, company_code)` 掛上去:

| 輸出欄 | 取的標題 | 額外濾網 |
|---|---|---|
| `depreciation` | `折舊費用` | `value is not null` |
| `increase_in_inventories` | `存貨(增加)減少` 或 `存貨（增加）減少` | **`value < 0`** |
| `ocf` | `營業活動之淨現金流入（流出）` 或半形括號版 | `value is not null` |
| `capital_expense` | `取得不動產、廠房及設備` / `取得不動產及設備` / `購置固定資產` | **`value < 0`** |
| `cash_dividends_paid` | `分配現金股利` 或 `發放現金股利` | **`value < 0`** |

單位=千元、幣別=新台幣、期間=**單季**(來自 matview 3 的差分結果)、market 欄恆為 `'tw'`
(來源就是常數,見下 OK 項)。

---

## 🔴 BUG:`value < 0` 濾網靜默丟掉合法資料(視圖自身的根因,全類三欄一起看)

濾網對三個欄位生效,同一個缺陷類,blast radius 不同(全類掃描,證據見 checks.sql [5][6][8]):

| 欄位 | 總 title-列 | `value ≥ 0` 被砍 | 佔比 | 其中真實正值 | 說明 |
|---|---|---|---|---|---|
| **increase_in_inventories** | 105,954 | 48,167 | **45.46%** | **47,511**(存貨減少的現金流入)+ 656 個 0 | **最嚴重** |
| capital_expense | 114,467 | 9,039 | 7.90% | 混差分假影 | 下游另有 NULL 傳染(見下) |
| cash_dividends_paid | 50,050 | 31,956 | 63.85% | 9,825 正值 + 22,131 個 0 | 22,131 個 0 被 coalesce 救回,實害最小 |

### 為什麼是 BUG(不是設計取捨)

1. **欄位名與內容不符**:現金流量表的「存貨(增加)減少」是**帶號數字**——正=存貨下降釋出現金
   (流入)、負=存貨增加占用現金(流出),兩者都是真實經營結果。欄位叫 `increase_in_inventories`
   卻只在負值時給數、正值一律給 NULL,任何不知道這條暗濾網的消費者(算淨營運資金變動、
   算存貨相關應計項)都會拿到錯的答案。視圖名為「with_titles」意即「照標題取值」,不該偷偷
   只取一半符號。

2. **實證(2330 台積電,checks.sql [8],旗艦股就中):**
   | 年季 | raw `存貨（增加）減少` | matview `increase_in_inventories` |
   |---|---|---|
   | 2025Q4 | **+579,578**(存貨下降) | (NULL,被砍) |
   | 2025Q3 | **+15,504,653** | (NULL,被砍) |
   | 2025Q2 | −10,806,101 | −10,806,101 ✓ |
   | 2025Q1 | −5,518,805 | −5,518,805 ✓ |
   | 2024Q4 | **+5,015,120** | (NULL,被砍) |
   | 2024Q3 | −20,393,343 | −20,393,343 ✓ |
   近 6 季有 3 季被砍;砍掉的全是真實的存貨去化現金流入。

3. **下游把 NULL 當 0,把比率算歪**:
   - `cash_flow_adequacy_ratio`(現金流量允當比率,`4_financial_index_ttm.sql:30-41`
     與 `3_financial_index_quarterly.sql:25-36`):分母=`−(Σcapex + Σ存貨變動 + Σ股利)`,
     三項都被 `coalesce(...,0)`。存貨只留「增加(負)」季、丟掉「減少(正)」季 →
     五年存貨投資額被系統性算成「只累加占用、不扣抵釋出」,與教科書「最近五年存貨淨增加額」
     定義不符 → 允當比率偏差(方向依個股而定)。學理上該用**期間淨變動**(期末−期初存貨,
     或把該現金流列**含正負**加總),不是丟掉正值。
   - **季表 `fcf_par_share = (ocf + capital_expense)/capital_stock`
     (`3_financial_index_quarterly.sql:66`)沒有 coalesce**:capex 一旦被砍成 NULL,
     `ocf + NULL = NULL` → 整個每股自由現金流變空。checks.sql [6] 實測**10,965 個
     「有 OCF 但 capex=NULL」的公司季**(約占 113,488 的 9.7%),這些季的季度 FCF/股
     被靜默清空。

### 建議修法

- 三個 CTE 的 `and value < 0` 一律拿掉,忠實傳遞帶號值(讓下游自己決定要不要取絕對值或
  截斷);若下游某比率真要「只計存貨增加額」,該截斷邏輯放在**用它的那支比率**裡並註明出處,
  不要在資料抽取層偷砍。
- 資本支出若要當「現金需求」,用**絕對值累計**(`sum(abs(value))` 或 `-least(value,0)`)
  而非丟正值;`fcf_par_share` 的 `capital_expense` 補 `coalesce(...,0)` 免 NULL 傳染
  (與 TTM 表 line 95-99 已 coalesce 的寫法對齊)。
- 此三欄濾網屬**同一缺陷類**,必須同批修;capex 那條先前已由 `B-view-4_financial_index_ttm`
  第 13 點指出,本檔補上 inventory(45%)與 dividend 的全類量化與第二個下游傷害(FCF NULL 傳染)。

---

## 🟡 SUSPECT:標題列舉不全,現代資料有小面積漏收

視圖把標題寫死成固定字串集合;`cash_flows_individual` 有 3,141 種 distinct title,
掃描(checks.sql [7] + 追加 era 查詢)發現同概念的其他寫法沒被收進來:

| 概念 | 漏收的標題 | 筆數 | 年代 | 判定 |
|---|---|---|---|---|
| 現金股利(付出) | `支付之股利` | 1,095 | **2013–2026** | 明確漏收(IFRS 常見寫法),那些公司季 `cash_dividends_paid`=NULL |
| 存貨變動 | `存貨減少(增加)之調整數`(含全形) | 1,104 | **2020–2026** | 語意存疑:可能是同一列的反序命名,也可能是「調整數」補充列 → 收進來恐重複計 |
| 資本支出 | `購買固定資產支付現金數`、`固定資產增加數` | 16,688 | **僅 2009–2012** | era 分隔;對有 OCF 的公司季只影響 **8 筆**(checks.sql [A][B])→ 實務可忽略 |

- 折舊(`折舊費用` 116,236)與營業現金流(兩括號變體共 116,363)**涵蓋完整**,無漏收。
- OCF 是最重要的欄(餵 F-score CFO 檢定),涵蓋完整,這點放心。
- 修法:把 `支付之股利` 併入 `cash_dividends_paid` 的 title 集合(需確認符號為付出=負);
  `存貨減少(增加)之調整數` 要先確認它是「主變動列的別名」還是「額外調整列」再決定收不收,
  避免與 `存貨（增加）減少` 對同一公司季雙重計入。

## 🟡 SUSPECT(latent):股利兩變體同季共存 6 筆,是潛在扇出源

`分配現金股利` 與 `發放現金股利` 在同一 `(market,year,quarter,company_code)` 共存的有
**6 組**(checks.sql [4])。join 沒有防重鍵,若這 6 組的兩列都通過 `value<0` 又都對到 OCF 列,
matview 會扇出成重複列。**目前 0 實害**(checks.sql [2] 全表 0 個重複 (y,q,code)),
但屬結構脆弱:資料一變就可能無聲多出列。修法:同概念多變體用 `coalesce`/優先序取一,
或在 CTE 內 `distinct on (y,q,code)`。

---

## 🔵 繼承(非本檔自身 BUG,但汙染穿過本檔):累計轉單季差分錯

`ocf`/`capital_expense`/`depreciation` 等值直接來自 matview 3 的單季差分,而 matview 3 的
差分對半年報公司會跨年度亂減(`B-matview-3_cash_flows_individual`,已判 BUG)。本檔沒有日期/
年度切窗、無法也未嘗試修正,只是傳遞。**佐證**:概念上恆為正的 `depreciation` 欄,113,368 筆
非空值裡有 **3,671 筆(3.2%)是負的**(checks.sql [9])——單季差分把折舊算成負數,就是上游
汙染穿過本檔的直接證據。根因與修法在 matview 3,本檔不重複計列,但下游任何用到這些欄的比率
都同時扛「差分 BUG」+「value<0 BUG」兩重。

---

## 🟢 OK(查過沒問題,別再查一次)

1. **無扇出、驅動列 1:1 對齊**:matview 113,488 列,全表 0 個重複 `(year,quarter,company_code)`;
   ocf CTE 的 distinct 驅動列數 = 113,488 = matview 列數(checks.sql [2][3])。capex 三變體、
   ocf 兩變體、inventory 兩變體同季共存均為 0 組(checks.sql [4]),故 left join 不扇出。
2. **抓到的值抄得對**:2330 近 12 季逐筆比對 raw individual vs matview,ocf/折舊/capex/股利
   **完全吻合**(checks.sql [8]);被砍的只有 inventory 正值列。純抽取無算術錯。
3. **`market = 'tw'` 常數 join 是對的**:本表 market 欄全為 `'tw'`(來源 `cash_flows_progressive`
   由 `FinancialReader.readFinancialStatements()` 寫死 `"tw"`),下游 `financial_index_*`
   用 `cash_flows_with_titles.market = 'tw'` 是正確過濾(見 `B-view-4` 第 17 點)。select 的
   未限定 `market` 解析為 `ocf.market`,無歧義。
4. **本檔無除法**:不會產生 ±inf;分母保護是下游 `financial_index_*` 的事(那裡的 2 處裸除
   已由 `B-view-4` 第 14 點記錄)。
5. **無 TTM / 無 PIT 邏輯**:本檔只做單季抽取,TTM 加總在 `financial_index_*`(rows between
   3/19 preceding)。本檔不引入前視偏誤(每列只抽該季自身的列);但本檔**沒有公告日欄**,
   任何用 `(year,quarter)` as-of 季末 join 的消費者需自行補 publication-lag(見 `PublicationLag.scala`),
   這是系統層而非本檔缺陷。

---

## 命名地雷(文件風險,提醒下一個人)

`cash_flows_individual` 的「individual」指**單季(vs progressive 累計)**,**不是個體報表(個體 vs 合併)**。
合併/個體之分取決於爬蟲寫進 `cash_flows_progressive` 的是哪種報表,無法從本檔判定,屬 C 維
原始資料稽核範圍。別被表名誤導成「個體財報」。

---

## 查了什麼(供覆核涵蓋度)

1. 精讀 matview 4 全文(44 行,5 欄純抽取無算術),逐欄記錄取的標題與濾網。
2. 追消費鏈:matview 3 → **matview 4(本檔)** → `financial_index_quarterly`/`financial_index_ttm`
   (view 3/4,用 ocf/capex/inventory/dividend 算 cash_flow_adequacy_ratio、fcf、F-score CFO)→
   `growth_analysis_ttm`(view 5)→ `Main.scala:357` 把 `cbs` 註冊成因子。Python 側 `src/quantlib/db.py`、
   `cache_tables.py`、`raw_quarterly.py` 走原始累計表、不讀本鏈。
3. 對照姊妹單位既有結論:`B-matview-3`(差分 BUG 根因)、`B-view-4`(capex value<0 第 13 點、
   market='tw' 第 17 點)——本檔補全 inventory/dividend 全類量化 + FCF NULL 傳染 + 標題涵蓋掃描。
4. 實跑 checks.sql 全套:列數/market、扇出、驅動對齊、多變體共存、三欄 value≥0 丟失量、
   下游 FCF NULL 公司季數、標題涵蓋(折舊/存貨/股利/不動產設備/營業活動)、2330 逐筆對照、
   折舊負值計數;另追加 capex/股利/存貨替代標題的 era 分佈。
5. 逐筆驗證 2330 raw vs matview(近 12 季),確認抽取忠實、僅 inventory 正值被砍。
