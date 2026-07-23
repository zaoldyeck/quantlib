# B-view-6_cbs_ttm_5y_over75 — 財務定義與算式審查

- 對象:`src/main/resources/sql/view/6_cbs_ttm_5y_over75.sql`（71 列）
- 上游:`5_growth_analysis_ttm.sql` → `4_financial_index_ttm.sql` →
  `materialized_view/6_concise_financial_statement_with_titles.sql`
- 姊妹單位:`B-view-5_growth_analysis_ttm`（cbs 上游）、`B-view-1_cbs_by_year`（年度版 cbs）
- 證據腳本（可重跑）:`docs/data_audit/scripts/B-view-6_cbs_ttm_5y_over75/verify.sql`
- 結論:**SUSPECT**

---

## 一句話

**這張清單「今天這 35 檔」可以信,但這張 view 不能拿去做歷史選股。** 它做的事很單純
且演算法正確:挑出「最近連續 20 季（5 年）品質綜合分 cbs 都 > 75」的公司，跑出來的 35
檔全是貨真價實的高品質藍籌（台積電 2330、信驊 5274、億豐 8464、大立光 3008…），
每一檔的高分都由真實的高 ROIC（11%～50%）撐起，沒有一檔是靠資料破洞灌水、也沒有一檔
是姊妹稽核抓到的金融業扇出受害股。

但這張 view **完全沒有時點（PIT）紀律**:它把每家公司錨定在「各自 DB 裡最新那一季」，
所以同一天跑出來的入選股其實被拿**不同財季**在評比（實測 20 檔看到 2025Q4、15 檔已看到
2026Q1）；沒有公告日防線，一旦拿去回測就會前視；一家早就下市、財報停在幾年前的公司只要
它最後 20 季夠好也會照樣入選（目前沒發生，但沒有任何守門）。加上它腳下那個 cbs 分數在
2006-2012 是壞的（姊妹單位 BUG 1）、對缺財報公司會灌到滿分——只是今天這批入選股的觀察
窗（2020-2026）剛好落在乾淨區、又剛好都不是受害股，才沒被波及。

**它現在沒有任何程式在讀（孤兒 view）**，所以沒有實盤資金踩在上面。定位:當「現在有哪些
公司連續 5 年體質好」的手動看板可以用；當**選股規則、或任何跨時間的篩選，不行**。

---

## 1. 它到底在算什麼

view `cbs_ttm_5y_over75`（`6_cbs_ttm_5y_over75.sql`）:

1. `pass` CTE（行 2-64）:對 `growth_analysis_ttm` 每支股票，用
   `distinct on (market, company_code) … order by … year desc, quarter desc` 取**最新一季**，
   該季的 `pass` = `cbs > 75 AND lag(cbs,1) > 75 AND … AND lag(cbs,19) > 75`
   （current + 19 個 lag = 20 季 = 5 年）。
2. 外層（行 66-71）:把 `pass = true` 的公司的**所有季**原樣輸出。

`cbs`（0-100 綜合品質分，定義在 `4_financial_index_ttm.sql:120-176`）:

```
cbs = operating_performance * 0.25   -- ROIC 全市場百分位（TTM 稅後淨利 / 投入資本）
    + return_on_investment  * 0.25   -- ROA  全市場百分位
    + capital_structure     * 0.10   -- 權益乘數 產業內百分位（越低槓桿分越高）
    + liquidity             * 0.10   -- 流動比/速動比 分桶（0-100）
    + cash_flow             * 0.30   -- 現金比率 + 現金流三條件 + 收現天數 分桶（0-100）
```

單位:cbs、各子分皆 0-100 無量綱；`> 75` 是絕對門檻。

---

## 2. 演算法正檢 —— 這部分是對的（別再查一次）

### OK 1 — distinct-on + window-lag 機制正確

最容易踩的雷是「DISTINCT ON 會不會把 window function 算爛」。**不會**:PostgreSQL 先算
window（over 全部列）再套 DISTINCT ON，所以留下的那一列（最新季）的 `lag(1..19)` 確實
指向它前面 19 季。實測 2330:最新季 2025Q4，往前到 2021Q1 共 20 季 cbs 分別
90.6/90.9/89.1/87.7/86.6/… 一路到 81.5，全部 > 75 → 正確入選。`5y = 20 季` 的季數也對。

### OK 2 — 各子分方向正確、liquidity 沒有被單位錯配鎖死

姊妹單位 `B-view-1 cbs_by_year` 的頭號 BUG 是 liquidity 因「百分比（250）vs 倍數（2.5）」
錯配對 99.69% 公司鎖死在 100。**TTM 版沒有這個病**:`4_financial_index_ttm.sql:23-25` 的
`current_ratio` / `quick_ratio` 是自己用 `流動資產/流動負債` 現算的**真實倍數**，CASE 門檻
（`>2.5`、`>1.5`…）尺度對得上。實測 2025Q4 liquidity 分布 0/20/30/40/50/60/80/100
均勻散開（684 檔滿分 ≈ 35%，對現金充沛的台股合理），**未鎖死**。ROIC/ROA 越高分越高、
槓桿越低分越高，方向都對。

### OK 3 — 當前 35 名入選股乾淨可信

抽 10 檔看成分（`verify.sql [6c]`）:全部 `roic` 非 NULL、roic 介於 0.113（3008）到
0.498（5274），operating_performance 72-98 由**真實高獲利**驅動，不是靠資料破洞。入選名單
（1264/1537/1707/2330/2752/3093/3570/4728/5236/5274/5287/5609/6231/6263/6788/7556/
8016/8081/8284/8416/1232/1730/2059/3008/3014/3034/3169/3217/3529/4205/6146/6161/
6206/6683/8464）**無一是金融股**（姊妹 BUG 1 扇出受害股全是 2880-2892 金控、券商 6005-6026），
觀察窗都在 2020-2026（避開 2006-2012 污染區）。

### OK 4 — 孤兒 view,不在任何資金/研究路徑上

repo 全文搜 `cbs_ttm_5y_over75`,除了本次稽核自己的報告，**沒有任何 Scala / Python /
conf / 其他 SQL 引用**。姊妹單位 `B-view-5` OK 3 已證 `growth_analysis_ttm` 整條鏈不進
DuckDB cache、現役 Serenity / apex 都不碰。所以以下所有缺陷目前都是**潛伏**，不傷實盤。

---

## 3. 問題（都有證據,但都要看「拿去幹嘛」才定生死 → SUSPECT）

### SUSPECT 1 ★最關鍵★ — 完全沒有時點紀律，不能拿去做跨時間選股

view 只有 `(year, quarter)`，**沒有公告日欄位、沒有公告落後（publication lag）防線、
沒有共同 as-of 對齊**。`distinct on … order by year desc` 把每家公司錨定在**它自己 DB 裡
最新那一季**——不是一個共同的「截至今天」。三重後果:

- **同一次查詢，入選股被拿不同財季評比。** 實測（2026-07-23）:20 檔錨定 2025Q4、
  15 檔錨定 2026Q1（`verify.sql [4]`）。誰被評到 2026Q1、誰停在 2025Q4，取決於**那家公司
  的 Q1 財報進 DB 了沒**，不是取決於日期。換個匯入狀態再跑，同一支股票的錨會跳動。
- **前視偏誤（若拿去回測）。** 沒有 lag 防線,任何「截至某過去日」的篩選都會用到那天
  還沒公告的財報。姊妹單位甚至抓到現役消費端 `QualityFilter.scala` 對同一條鏈的 PIT 用錯。
- **殭屍公司照樣入選。** 下市/停止申報的公司，財報凍結在幾年前，只要它最後 20 季夠好，
  今天仍會出現在名單裡（目前 35 檔都 ≥2025Q4，沒發生，但**零守門**）。

判 SUSPECT 而非 BUG:當「現在有哪些公司體質好」的**即時快照**用（今天資料本就都已公告），
浮動錨頂多是小瑕疵、名單仍可信；但當**選股規則或歷史篩選**用，就是硬缺陷。使用者問的
「能不能拿來選股」——答案是:手動看當下可以,自動化/跨時間不行。

### SUSPECT 2 — 腳下的 cbs 分數有已證缺陷會滲進來（只是今天剛好沒踩到）

`cbs` 從 `financial_index_ttm` 繼承三個已證問題:

- **(a) 2006-2012 扇出污染**（姊妹 `B-view-5` BUG 1）:金融業科目別名扇出 → 那幾年
  全市場排名分母灌水 30-37%、TTM 加總算錯、且**不可重現**。→ 任何用本 view 做
  **2013 年以前**的篩選都是壞的。
- **(b) 缺財報公司在 ROIC 排名被灌到滿分**（與 `B-view-1` BUG 2 同型）:實測 2025Q4 有
  **30 家 `roic` 為 NULL 卻全拿 operating_performance = 98.5（最高分）**——`rank() over
  (order by roic)` 的 NULLS LAST 把資料破洞排到最高百分位（`verify.sql [6b]`）。這批公司
  佔掉排名頂端 30 格，把其他人的百分位整體下壓 ~1.5%。
- **(c) ROIC 分子稅前/稅後混用**（姊妹 `B-view-1` 量到 ~24% firm-year 用稅前）:源頭
  `concise_financial_statement_with_titles` 的 `profit` CTE 標題清單含 `稅前純益` 當
  fallback，`distinct on … order by title` 讓部分公司取到稅前。

三者今天都**沒污染** 35 名輸出（OK 3 已證:入選股 roic 全非 NULL、全非金融、窗在乾淨區），
但它們是這張 view 的地基。判 SUSPECT:當前輸出乾淨是**運氣**（窗落對地方），不是設計保證。

（附帶未爆彈,姊妹 `B-view-5` SUSPECT 4:上游 `4_financial_index_ttm.sql:91-92` 的
`inventories/total_assets`、`receivable/total_assets` 沒包 `nullif`。目前 7 列
`total_assets=0` 僥倖因分子為 NULL 沒炸;哪天同一列同時有存貨數字與零總資產，
`select * from cbs_ttm_5y_over75` 會整句 `ERROR: division by zero`。）

### SUSPECT 3 — `lag(n)` 是「往前 n 列」不是「往前 n 季」，可放行假 5 年連續

window 只寫 `order by year, quarter`，沒有日曆對齊。缺季的公司，`lag(1..19)` 會**跳過破洞**
往更早的好季抓——於是「中間有一段體質變差、但那幾季剛好沒進 DB」的公司，會被誤判成
「連續 5 年 > 75」。實測:全市場 2077 家中 1719 家（82.8%）歷史上有缺季；**149 家**的
**最近 20 列**就含缺季（`verify.sql [3]`）。判 SUSPECT 而非 BUG:當前 35 檔入選股**全部**
最近 20 列連續（span19 = 19），這個機制目前沒放行任何假陽性,但只要資料補齊狀態一變就會咬。

### SUSPECT 4 — `lag` 的 `partition by company_code` 漏了 `market`

`distinct on` 與最終 join 都用 `(market, company_code)`，但 20 條 `lag` 視窗全部只
`partition by company_code`。轉板（上櫃→上市）而同一 code 橫跨兩市場的股票，其 lag 序列會
把 TWSE 與 TPEx 兩段季別**混成一條**，且 distinct-on 會對同一 code 各市場各吐一列（重複掛牌）。
實測有 **13 檔跨市場 code**（1597,1752,3092,3652,4736,5306,6423,6426,6438,6446,6472,
6589,8476,`verify.sql [1]`）;**無一落在 35 名內**，故目前潛伏。修法:改成
`partition by market, company_code`。

---

## 4. 魔術數字（設計選擇,非算錯,但無 repo 內校準證據）

門檻 `cbs > 75`、以及 cbs 內所有分桶切點（流動比 2.5/1；速動比 1.5/1/0.5；現金比率
0.25/0.2/0.15/0.1/0.05；收現天數 15/30/60/90/150/180）與權重（0.25/0.25/0.1/0.1/0.3）
在 repo 裡都**找不到出處或校準回測**。cbs 是自訂綜合分、不是教科書比率，所以「cbs > 75」
**不等於**「全市場前 25%」，只是「這個特定加權公式跨過 75」。當啟發式看板可接受;當**已驗證
的選股規則**則不合格（需樣本外驗證門檻與權重）。

---

## 5. 建議修法（依序;此 view 本身修補範圍）

1. **先釘死定位**:既然是孤兒 view，最乾淨的解是標 `deprecated` 或直接下架;若保留，
   在檔頭與欄位語意寫明「**現況快照,非 PIT,禁用於回測/歷史篩選**」。
2. **補時點紀律**（SUSPECT 1):加一個共同 as-of 參數，把所有公司收斂到「≤ as-of 且已過
   公告落後」的同一最新季（沿用 `strategy/PublicationLag` 的 Q1→5/22、Q2→8/21、Q3→11/21、
   Q4→次年 4/7 規格），並排除「最新季早於 as-of 一年以上」的殭屍公司。
3. **日曆對齊**（SUSPECT 3）:先建連續季索引 `year*4+quarter`，用 `range between` 或
   self-join 要求 20 季**真的連續**，缺季直接讓該窗為 NULL 而非跳格比較。
4. **market 進 partition**（SUSPECT 4）:20 條 `lag` 全改 `partition by market, company_code`。
5. **上游治本**(SUSPECT 2，非本 view 修補範圍，指向姊妹單位)：依 `B-view-5` 建議修
   `concise_financial_statement_with_titles` 扇出、`rank` 的 NULLS 處理、profit 統一取稅後、
   `4_financial_index_ttm.sql:91-92` 補 `nullif`。修好前，本 view 的 2013 年以前輸出一律不可信。
