# B-matview-6_concise_financial_statement_with_titles — 財務定義與算式審查

- **稽核對象**：`src/main/resources/sql/materialized_view/6_concise_financial_statement_with_titles.sql`（149 行）
- **日期**：2026-07-23
- **結論**：🔴 **BUG**

---

## 一句話結論

**這張表是下游那一堆「算錯的財務比率」的源頭**——它把台股財報的各科目從「一欄一個
title」攤平成「一列一家公司」，但攤平的方式有三個定義層級的錯：(1) 13 個科目 CTE 裡有
11 個沒去重、而且同義詞清單彼此不互斥，2006-2012 年直接把金融證券股的同一季複製成最多
**64 份**（858 組重複鍵）；(2) `profit`（本期稅後淨利）的同義詞清單混進了「稅前純益」，
2005 年以前 96.6% 的列稅前被當稅後；(3) 有個叫 `ebit` 的欄位其實裝的是**稅前淨利
（EBT，利息已扣）**，不是 EBIT。**好消息**：2013 年以後的原始金額本身是對的（台積電 2024
四季營收、營益、稅後淨利、EPS 全部對得上公告），這張 view 本身只做 join 不做除法所以不會
產生 ±inf；而且現役 Python 交易系統根本不讀它。**壞消息**：`financial_index_ttm` 讀的就是
這張表、Scala 凍結策略層還在用，任何 Scala 時期 2006-2012 的財務因子回測地基是壞的。

---

## 這張 view 在做什麼

台股的簡明財報在資料庫裡是「長格式」（一列一個科目，靠 `title` 欄位區分「資產合計」
「營業收入」…）。這張 matview 把資產負債表（`concise_balance_sheet_individual`）與損益表
（`concise_income_statement_individual`）**攤平成寬格式**：一列 =（市場, 年, 季, 公司），
欄位 = total_assets / total_equity / total_operating_revenue / profit / eps …等 15 個科目。

做法是每個科目開一個 CTE，用 `WHERE title IN (一串同義詞)` 撈值，再一路 `LEFT JOIN`
串起來，最後資產負債表區塊與損益表區塊 `FULL JOIN` 在（年,季,公司）上。

**關鍵**：這張 view **沒有任何除法、沒有比率、沒有 TTM**——它只做「撈 title + join」。
所以 ROE/ROIC/毛利率的**算式**不在這一層（在下游 `financial_index_quarterly` /
`financial_index_ttm`，已另案稽核 B-view-3 / B-view-4）。這一層的錯是**取值定義錯**：
撈錯 title、把兩個概念混成一欄、去重沒做好。取值錯了，下游算式再對也是錯的。

**單位**：新台幣千元（EPS 為元/股）。損益科目已在上游 matview-5 做過「本季累計 − 上季
累計」的單季化（差分正確性屬 B-matview-5 範疇，已審）。資產負債科目是季末時點值。

---

## 🔴 BUG 1（根因，嚴重）— 11 個 CTE 沒去重 + 同義詞不互斥 → 2^n 笛卡兒積

第 2-88 行的 13 個科目 CTE，**只有 `net_operating_income`（行 49）與 `profit`（行 72）
兩個加了 `DISTINCT ON`**。其餘 11 個（`total_assets` / `total_current_assets` /
`total_liabilities` / `total_current_liabilities` / `total_non_current_liabilities` /
`total_equity` / `total_retained_earnings` / `total_operating_revenue` /
`total_operating_costs` / `operating_expenses` / `ebit` / `eps`）都沒有。

而它們的 title 同義詞清單**彼此不互斥**：一家公司同一季若同時報「資產合計」與「資產總計」
（行 4-6），`total_assets` CTE 就回 **2 列**。多個這種 CTE 一 join，就變成 2×2×2… 的
笛卡兒積。

**直接量測（view 6 本身，非下游）**：

```sql
WITH d AS (SELECT market,year,quarter,company_code,count(*) n
           FROM concise_financial_statement_with_titles GROUP BY 1,2,3,4)
SELECT count(*) FILTER (WHERE n>1) dup_keys, max(n) max_mult FROM d;
-- dup_keys = 858 ，max_mult = 64 ，全部落在 2006-2012
-- 複本數分佈：2 份 413 組、4 份 180 組、8 份 118 組、64 份 147 組
```

**64 份怎麼來的（統一證 2855, 2011Q4）**：

```
assets 2 × equity 2 × revenue 2 × opex 2 × ebit 2 × eps 2 = 2^6 = 64
```
（`costs` 該季 0 筆 → left join 保留一列填 null；`net_operating_income` 與 `profit`
有 distinct-on 所以各收斂成 1 列。）

**傷害**：這 858 組重複鍵原封傳給下游 `financial_index_*`，造成(a) 查詢**非決定性**
（`rows between N preceding` 視窗對同分列無 tie-break）、(b) 排名分母被灌水、(c) TTM 視窗
可能整個裝同一季的複本。細節見 `B-view-3_financial_index_quarterly.md` BUG 1（該單位從
下游角度量到 23.3% 幽靈列）。**本單位確認根因就在 view 6 這 11 個缺 distinct-on 的 CTE。**

**修法**：每個科目 CTE 加 `DISTINCT ON (year, quarter, company_code)`，並用**明確的
title 優先序**（不是靠 `ORDER BY title` 的字典序）決定取哪一個同義詞。修完加守護測試：
「(market,year,quarter,company_code) 零重複」。

---

## 🔴 BUG 2（定義錯）— `profit`（本期稅後淨利）混進「稅前純益」

第 72-80 行 `profit` CTE 的同義詞清單把 `稅前純益` 和後面幾個真正的稅後科目
（`本期稅後淨利（淨損）` / `本期淨利（淨損）`）放在一起。這兩者差一整筆所得稅。

**為什麼大多數時候沒事、老資料就爆**：DB collation 是 **C（UTF-8 位元組序）**，
`DISTINCT ON … ORDER BY …, title` 取位元組序最小的 title。`合併總損益`（合 = E5）<
`本期*`（本 = E6）< `稅前純益`（稅 = E7）。所以「稅前純益」只有在它是**唯一存在**的
title 時才會被選中 → 就是 2005 年以前的舊制資料。

```sql
SELECT count(*) n, count(*) FILTER (WHERE profit=ebit) eq
FROM concise_financial_statement_with_titles WHERE year BETWEEN 1995 AND 2005;
-- 30040 列中 29026 列 (96.6%) profit 與 ebit 逐位相同 → profit 裝的是稅前
```

台積電 2330 **2003** 四季 `profit` 相加 ≈ 510 億（= 當年**稅前**淨利），而 2003 年
台積電稅後淨利約 473 億。於是 `profit_margin` / `roic` / `roa` 在 2005/2006 有個**純屬
口徑切換的假斷層**——把「稅前變稅後」誤讀成「獲利突然惡化」。

（附帶：collation C 下 `合併總損益` 在 2005-2014 會優先勝出，而舊制合併總損益可能含
少數股權、不等於母公司稅後淨利。需另確認，屬次要。）

**修法**：從 `profit` 清單移除 `稅前純益`；舊期若真的只有稅前數就給 NULL，不要用另一個
概念頂替。

---

## 🔴 BUG 3（定義錯 / 命名地雷）— `ebit` 欄位其實是稅前淨利（EBT），不是 EBIT

第 60-71 行的 `ebit` CTE 撈的全是 `稅前純益` / `稅前淨利（淨損）` /
`繼續營業單位稅前*` 這類 title。這些是 **稅前淨利 = EBT（Earnings Before Taxes）**，
利息費用**已經扣掉**。EBIT（Earnings Before Interest and Taxes）要把利息**加回來**，
兩者差一筆利息。把欄位命名成 `ebit` 是定義錯誤。

**硬證據（2330 2024Q1）**：

```
ebit 欄位 = 266,543,204  ==  來源 concise_income_statement_progressive 的「稅前淨利（淨損）」
稅前 266,543,204 − 所得稅費用 41,321,941 = 225,221,263 = profit（本期淨利）  ✓ 逐位吻合
```

所以 `ebit` 欄位裝的是稅前淨利。任何人拿它當真 EBIT 去算 EV/EBIT、利息保障倍數，
對高槓桿公司會系統性偏低（少加了利息）。

**目前是死欄位**：下游 `financial_index_ttm` / `financial_index_quarterly` 的 ROIC 用的是
`profit`（不是 `ebit`），沒有任何存活路徑讀 `ebit`。**但這個死欄位不是無害**——它的 CTE
沒有 distinct-on 且多 title，正是 BUG 1 那 2^n 的其中一個 ×2 因子（2855 的 ebit=2）。

**修法**：改名 `pretax_income` / `ebt`，或直接刪掉這個沒人用的欄位；若保留務必補 distinct-on。

---

## 🟡 SUSPECT 4 — `net_operating_income` 混了「營業利益」與「稅前淨利」

第 49-59 行把 `營業利益（損失）`（營業利益）與 `繼續營業單位稅前*`（含業外的稅前利益）
放進同一個 CTE。collation C 下 `營`（E7 87）< `繼`（E7 B9），所以有「營業利益」時取
營業利益（正確），只有在缺「營業利益」title 時才退回稅前。

```sql
-- distinct-on 實際選中稅前類 title 的列數
-- PRETAX_selected 2,355 列（2006-2026）；operating_selected 154,573 列
```

那 2,355 列的下游 `operating_margin`（`3_financial_index_quarterly.sql:58`、
`4_financial_index_ttm.sql:80`）其實是**稅前利益率**，不是營益率。而且「取哪個 title」
靠的是**位元組序**——換個 DB collation（或搬到 DuckDB / 別的環境）結果會**無聲改變**。
（此點 `B-view-3` SUSPECT 12 已從下游記過；本單位確認源頭在 view 6 且量到 2,355 列。）

**修法**：`net_operating_income` 只收營業利益類 title；缺就給 NULL，不要用稅前頂替。

---

## 🟡 SUSPECT 5 — 合併/個體口徑未標記、跨 2013 混用（繼承自上游）

來源 `concise_*_individual` 用 `DISTINCT ON … ORDER BY …, type`，字母序
`consolidated < individual` → 有合併取合併、只有個體才退個體（見 `B-matview-5` /
`B-view-3` SUSPECT 13：2013 前約 20% 格子是個體口徑）。view 6 **沒有帶任何
`statement_type` 欄位**，所以跨 2013 的時序比較會無聲混用合併與個體報表範圍。
這是繼承自上游的問題、非 view 6 originated，但 view 6 把它原封傳下去且沒加標記。

**修法**：輸出加 `statement_type` 欄位讓消費端能自己決定；根治在上游（見 B-matview-5）。

---

## 🟡 SUSPECT 6 — 死碼：定義了卻沒用的 CTE / 欄位

- `total_liabilities` CTE（行 11-14）**定義了但根本沒被 select**（行 97 與 join 都被
  註解掉）——純死碼。
- `ebit`（見 BUG 3）、`operating_expenses`、`total_retained_earnings` 三個欄位有產出，
  但下游沒有任何存活路徑讀（`financial_index_*` 只用 revenue/costs/net_operating_income/
  profit/eps/資產負債類）。`ebit`、`operating_expenses` 兩個 CTE 缺 distinct-on，還在
  幫 BUG 1 加乘複本。

**修法**：刪掉不用的 CTE / 欄位（連同它們對 fan-out 的貢獻一起消掉），或補齊 distinct-on。

---

## 🟢 OK / ⚪ REAL（查過沒問題，別再查一次）

1. **⚪ 2013 年以後的原始金額本身正確。** 2330 2024 四季（view 6 輸出）：

   | 季 | 營收 | 營業利益(net_operating_income) | 稅前(ebit 欄) | 稅後(profit) | EPS |
   |---|---|---|---|---|---|
   | Q1 | 592,644,201 | 249,018,306 | 266,543,204 | 225,221,263 | 8.70 |
   | Q2 | 673,510,177 | 286,555,542 | 306,310,575 | 247,661,438 | 9.55 |
   | Q3 | 759,692,143 | 360,766,289 | 384,186,852 | 325,080,170 | 12.55 |
   | Q4 | 868,461,178 | 425,712,913 | 448,798,004 | 374,468,888 | 14.45 |

   四季 EPS 相加 = 45.25 = 台積電 2024 全年 EPS（公告一致）。營收、營益、稅後淨利
   皆與公告吻合。**壞的是取值定義（BUG 1-3）與跨年口徑，不是現代年份的金額本身。**

2. **🟢 這張 view 不做除法 → 不可能除以零、不可能產生 ±inf / NaN。** 只有撈 title + join +
   passthrough。分母保護（負權益、零營收、極小分母）是下游 `financial_index_*` 的責任
   （已由 B-view-3 / B-view-4 稽核，該層確有 nullif 缺漏等問題）。

3. **🟢 PIT（時點對齊）本檔無責。** view 只帶 (year, quarter) 沒有公告日，公告落後由
   下游 `strategy/PublicationLag.scala` 換算可用季別處理——設計如此，非 view 6 的 bug。

4. **🟢 FULL JOIN 的 key coalesce 正確。** 行 90-94 用 `coalesce(total_assets.*,
   total_operating_revenue.*)` 補鍵，資產負債表區塊與損益表區塊各自的 anchor（total_assets /
   total_operating_revenue）在 full join 兩側缺料時能正確補回市場/年/季/代號/名稱。
   最終 170,503 列 > 任一側，是「只有 BS 或只有 IS 的公司-季」聯集 + BUG 1 的複本共同造成。

---

## 消費者鏈與爆炸半徑

```
concise_balance_sheet_individual / concise_income_statement_individual  (上游, 已審)
  └─ 6_concise_financial_statement_with_titles          ← 本單位（取值定義層）
       ├─ financial_index_ttm        (Scala 凍結策略層在用：Signals.scala:142,531,549)
       └─ financial_index_quarterly  (零存活消費者)
```

- PG `pg_depend`：只有 `financial_index_ttm` / `financial_index_quarterly` 依賴本表。
- **現役 Python 路徑（Serenity `ev_v3_wf` / apex / Evergreen）完全不經此表**——
  它們直讀 `concise_income_statement_progressive` 且鎖 `type='consolidated'`
  （`research/cache_tables.py`）。`financial_index_quarterly` 已從 cache 移除
  （`cache_tables.py:86`）、`src/quantlib/db.py:42` 明文取代 PG views。
- refresh 掛在 `src/main/scala/reader/FinancialReader.scala:161`。

**白話**：實盤沒中彈。但 `financial_index_ttm` 讀的就是這張表，Scala 凍結策略層
（ValueRevert / MultiFactor / DividendYield / ValueMomentum / Signals）還在用它，
所以任何**引用 Scala 時期 2006-2012 財務因子回測**的結論，地基是壞的，要重跑或作廢。

---

## 建議修法（不在本單位執行）

依嚴重度：

1. **根因（BUG 1）**：11 個科目 CTE 全部加 `DISTINCT ON (year,quarter,company_code)` +
   **明確 title 優先序**（別靠 collation 字典序）。加「零重複鍵」守護測試（先紅後綠）。
2. **BUG 2**：`profit` 清單移除 `稅前純益`；只有稅前數的舊期給 NULL。確認 `合併總損益`
   是否含少數股權。
3. **BUG 3**：`ebit` 欄位改名 `pretax_income`（或刪），別讓下一個人當 EBIT 用。
4. **SUSPECT 4**：`net_operating_income` 只收營業利益類 title。
5. **SUSPECT 5**：加 `statement_type` 欄位標記合併/個體。
6. **SUSPECT 6**：刪 `total_liabilities` 死 CTE 與 `operating_expenses` /
   `total_retained_earnings` 死欄位。
7. **或**——這張表下游只剩凍結的 Scala 層在用，最省的做法是連同 `financial_index_*`
   一起評估退役；但退役前要確認 Scala 策略層不再被引用。

---

## 這一輪查了什麼（避免下次重查）

- 精讀 `6_concise_financial_statement_with_titles.sql` 全 149 行；寫出 13 個科目 CTE 的
  title 對照與 join 結構；標出哪 2 個有 distinct-on、哪 11 個沒有
- 精讀上游 `1_concise_balance_sheet_individual.sql`（無差分，時點值，正確）、
  `5_concise_income_statement_individual.sql`（有累計差分）、`7_income_statement_individual.sql`
- 精讀下游 `3_financial_index_quarterly.sql`、`4_financial_index_ttm.sql`，確認哪些 view-6
  欄位被消費（revenue/costs/net_operating_income/profit/eps/資產負債類）、哪些是死欄位
  （ebit/operating_expenses/total_retained_earnings）
- 量測 view 6 本身的重複鍵：858 組、max 64×、全 2006-2012；複本數分佈
- 拆解 2855 2011Q4 = 2^6，逐 CTE 數 title 命中數
- 量測 `profit==ebit` 逐段比例（pre2006 96.6%）；確認 collation = C 決定 distinct-on 勝者
- 量測 `net_operating_income` 選中稅前 title 的列數（2,355）
- 對 2330 2024 逐季驗算 revenue/op-income/EBT/profit/EPS，並用 progressive 原始
  「稅前淨利 − 所得稅 = 本期淨利」逐位對帳，證明 `ebit` 欄 = EBT、`profit` 欄 = 稅後
- `pg_depend`×`pg_rewrite` 盤點消費者（financial_index_ttm / financial_index_quarterly）
- 確認現役 Python 路徑不讀本表（cache_tables.py / db.py）、refresh 掛點
- 交叉比對既有 `B-view-3_financial_index_quarterly.md`、`B-matview-5_…individual.md`、
  `B-view-4_financial_index_ttm.md`，避免重複並把根因釘回 view 6 這一層
- 驗證 view 6 不做除法 → 無 ±inf；PIT 由下游負責 → 本檔無責
