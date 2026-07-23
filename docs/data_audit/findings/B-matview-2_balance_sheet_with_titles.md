# B-matview-2_balance_sheet_with_titles:財務定義與算式審查

**審查對象**:`src/main/resources/sql/materialized_view/2_balance_sheet_with_titles.sql`
**結論(白話)**:這張表把資產負債表攤平成一格一列。**大部分欄位是對的、可以信,但「應收款項(receivable)」這一欄有一個真的錯誤:它會把同一筆應收帳款算兩次**。有 8,765 個「公司×季」(約 7.5%)的應收被灌水,最嚴重的多算了將近一倍。牽連到的下游指標是「應收帳款收現天數(DSO)」和「應收佔資產比」——其中 367 家公司因為「有時候報兩行、有時候報一行」,DSO 會無緣無故忽高忽低,騙過「收現天數改善」這類趨勢訊號。好消息:這張 view 現在的實盤引擎(Python)根本沒讀它,踩到的只有已凍結的舊 Scala 策略鏈,所以現在沒賠到錢——但只要有人拿這欄去研究或選股,數字就是髒的。

---

## 這張 view 在做什麼

來源是 `balance_sheet` 這張「一格一列」(EAV:`title` 科目名 + `value` 金額)的長表,
把六類科目 pivot 成寬表欄位:`cash / receivable / inventories / prepaid_expenses /
property_plant_and_equipment / total_capital_stock`,主鍵 `(market, year, quarter, company_code)`。

**先講一個關鍵的『沒有錯』**:資產負債表是**季末當天的存量快照**,不像損益表是累計數。
所以這張 view **正確地完全不做累計差分**(全文零 `lag`、零減法)。稽核單上問的
「TTM 累計差分有沒有做對、跨年第一季有沒有處理」——**對這張 artifact 不適用**,
而它也正確地沒有去碰,這是對的。

---

## 主要缺陷:receivable 重複計算(BUG)

`receivable` CTE(:6-19)把一長串應收類科目 `sum(value)` 加總。加**不同**的應收
子項(貿易應收 + 應收票據 + 其他應收 + 關係人應收)本身是對的——那是「總應收」。
**錯在清單同時列了同一筆的『毛額』與『淨額』、以及『明細』與『合計』**,一起加進去 → 同一筆算兩次。

### 鐵證(可重跑)
```sql
-- 華榮 1608,2026 Q1,balance_sheet 原始列:
--   其他應收款      76,675
--   應收帳款     2,338,491   ← 毛額
--   應收帳款淨額 2,338,491   ← 淨額(此公司無備抵,兩者相同)
--   應收票據淨額     5,572
SELECT receivable FROM balance_sheet_with_titles
WHERE company_code='1608' AND year=2026 AND quarter=1;
--  → 4,759,229
```
`76,675 + 2,338,491 + 2,338,491 + 5,572 = 4,759,229`(view 實際輸出)。
**貿易應收 2,338,491 被算了兩次。**
- view 的值:**4,759,229**
- 應有的總應收:`其他 76,675 + 應收帳款淨額 2,338,491 + 應收票據淨額 5,572 = 2,420,738`
- **灌水 +96.6%**(幾乎多算一倍)。

### 規模
```sql
WITH k AS (
 SELECT year,quarter,company_code,
   bool_or(title='應收帳款') AND bool_or(title='應收帳款淨額') AS d1,
   bool_or(title='應收票據') AND bool_or(title='應收票據淨額') AS d2,
   bool_or(title='應收款項') AND bool_or(title='應收款項淨額') AS d3
 FROM balance_sheet WHERE value IS NOT NULL GROUP BY year,quarter,company_code)
SELECT count(*) FILTER (WHERE d1 OR d2 OR d3), count(*) FROM k;
--  → 8,765 中彈 / 116,720 全體(≈7.5%);單「應收帳款+應收帳款淨額」就 7,471 個
```
備抵損失科目多半存負值(`備抵損失－應收帳款` avg −46、`備抵損失－其他應收款` avg −6,450),
所以就算有備抵,`毛額 + 淨額 = 毛額 +(毛額−備抵)≈ 2×毛額`,依然是接近兩倍的高估。

### 為什麼會咬到選股(下游傳染)
`receivable` 餵進 `financial_index_ttm`(view/4:48-53、:92):
- **days_sales_outstanding(收現天數 DSO)** = `(receivable + 去年同季 receivable)/2 × 365 / TTM 營收`
- **receivables_ratio** = `receivable / 總資產`

再傳到 **`growth_analysis_ttm`**(view/5:160+,CLAUDE.md 列為策略消費端)的
`days_sales_outstanding_decline_5y_overall` 等**趨勢旗標**。

如果一家公司**每季都固定多報一行**,2× 的比例在「這季 DSO vs 去年 DSO」的比較裡會**互相抵消**,
趨勢旗標不受影響。真正的問題是**忽兩行忽一行的公司**:
```sql
WITH flag AS (
  SELECT company_code,year,quarter,
    (bool_or(title='應收帳款') AND bool_or(title='應收帳款淨額'))::int AS dbl,
    bool_or(title IN ('應收帳款','應收帳款淨額'))::int AS has_ar
  FROM balance_sheet WHERE value IS NOT NULL GROUP BY company_code,year,quarter),
agg AS (SELECT company_code,sum(dbl) dbl_q,sum(has_ar) ar_q FROM flag GROUP BY company_code)
SELECT count(*) FILTER(WHERE dbl_q>0) any_double,
       count(*) FILTER(WHERE dbl_q>0 AND dbl_q<ar_q) intermittent,
       count(*) FILTER(WHERE dbl_q>0 AND dbl_q=ar_q) always_double FROM agg;
--  → any_double 414 / intermittent 367 / always_double 47
```
**414 家曾中彈的公司裡,367 家(89%)是『時報兩行時報一行』**——它們的 `receivable`
會在切換報表格式那一季**憑空跳約 2× 再跳回來**,製造出與真實應收無關的假 DSO 波動,
會誤觸「收現天數惡化/改善」訊號。

### 現在賠到錢了嗎?(blast radius)
**沒有,但地雷埋著。**
- `financial_index_ttm` / `financial_index_quarterly` / `balance_sheet_with_titles`
  **都不在 Python cache**(`research/cache_tables.py:86` 明文移除 `financial_index_quarterly`,
  `financial_index_ttm` 從未快取)——現役 Python/實盤引擎不讀。
- Scala `strategy/` 套件已凍結(CLAUDE.md)。
- `Signals.scala` 對 `financial_index_ttm` 的具名讀取只有 `fcf_per_share`(:548-565),
  它的分母是**乾淨的 `total_capital_stock`**,不碰 receivable → 不受影響。
- 但 `Signals.latestQuarterField("financial_index_ttm", col, …)`(:142)的 `col` 是參數,
  任何策略只要點名 `days_sales_outstanding` / `receivables_ratio` 就會吃到髒值。

---

## 次要缺陷

### SUSPECT — FULL JOIN 孤兒把其他欄位打成 NULL(邏輯漏洞,量小)
主表 `total_capital_stock` 對 `receivable` 用 **FULL JOIN**(:63),但後續 `cash /
inventories / prepaid / ppe` 的 LEFT JOIN 全部**以 `total_capital_stock.year/quarter/
company_code` 為鍵**(:66-74)。所以「只有 receivable、沒有 total_capital_stock」的孤兒列,
`total_capital_stock.*` 是 NULL → 那幾個 LEFT JOIN 對不上 → `cash/inventories/prepaid/ppe`
**即使原表有值也被強制 NULL**。實務只有約 3 列(matview 116,455 列 vs `total_capital_stock`
非空 116,452)——影響微小,但是真的邏輯瑕疵:哪天 receivable 的覆蓋率超過股本,就會擴大。
**修法**:後續 join 改用 `coalesce(total_capital_stock.year, receivable.year)` 等協調鍵,
或整段改成先 `full`/`union` 出完整鍵集再逐項 left join。

### SUSPECT — property_plant_and_equipment 任意取值 + 死欄位
PP&E(:30-43)用 `distinct on (year,quarter,company_code) … order by title` 在
`不動產、廠房及設備`/`…淨額`/`…合計` 等變體間**按科目名字母序任意挑一個**——毛額/淨額
可能被挑到不一致的口徑。但這欄**下游完全沒人用**(全庫 grep `property_plant_and_equipment`
除本檔零引用),所以是**死欄位**,目前只是壞味道,不是活 bug。**修法**:若未來要用,
明確定義優先序(通常取「不動產、廠房及設備」淨額口徑);否則刪欄。

### 壞味道 — 三種去重策略並存、不一致
同一張 view 對六類科目用了**三種**不同手法:`cash/inventories/total_capital_stock`
用裸 select(有扇出風險)、`receivable/prepaid` 用 `sum`(有重複計算風險,已在 receivable 實現)、
`ppe` 用 `distinct on`(任意挑)。沒有單一原則。實測 `cash/inventories/total_capital_stock`
目前零扇出(沒有公司季同時有多個同類 title),所以裸 select 暫時沒爆,但設計本身脆弱。

---

## 查了、確認沒問題的(負結果,免得下一個人重查)

- **`market = 'tw'` join 對得上**:`balance_sheet` 全表 market 一律 `'tw'`(7,205,860 列全部,
  無 twse/tpex 之分),matview 輸出也全 `'tw'`;消費端 `financial_index_ttm` 的
  `balance_sheet_with_titles.market = 'tw'`(view/4:101)**匹配成功**。一度懷疑是
  `'tw'` vs `'twse'/'tpex'` 對不上會把整段打成 NULL,**證偽**。
- **join 鍵漏掉 market**:因為 market 全域一致為 `'tw'`,不會造成跨市場污染。無害。
- **cash / inventories / total_capital_stock 扇出**:各自的 title 集在同一公司季**零**多重命中
  → 裸 select 不會列複製。乾淨。
- **prepaid_expenses 的 `sum`**:多 title 的 2,078 個公司季裡,2,045 個是
  `預付款項合計 + 預付費用合計`(兩個**不同**類別,合加是對的),真正「明細+自己合計」
  的重疊近乎 0(`預付款項+預付款項合計`=0、`預付費用+預付費用合計`=0)→ **prepaid 不重複計算,OK**。
- **quick_ratio 不受 receivable 污染**:速動比(view/4:24)= `(流動資產 − 存貨 − 預付)/流動負債`,
  用的是 inventories+prepaid(兩者皆乾淨),**根本不碰 receivable**。
- **TTM/累計差分**:N/A——資產負債表是存量快照,不該差分;view 正確地不差分。
- **PIT/前視**:本 view 是純 pivot,無前瞻計算;公告落後由消費端(`PublicationLag`)負責,
  非本 artifact 的責任。
- **既有事故簿**:`docs/data/data_quality_incidents.md` 未記錄本 view 或 receivable 重複計算
  → 這是**新發現**,不是已知真實邊界。
