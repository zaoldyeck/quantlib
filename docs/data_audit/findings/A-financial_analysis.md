# A-financial_analysis — 解析正確性稽核

**結論（白話）**：這張表 **不能整張信**。近半數的資料（41,144 列裡有 18,591 列，
佔 45%，全部是 **2011 年（含）以前** 的年度）**欄位整體錯位一格**，最後 6 個財務比率
欄全放到隔壁去了。最嚴重的是 **每股盈餘（EPS）欄裡裝的其實是「純益率(%)」**——不是
盈餘、連單位都不對（是百分比不是元）。真正的 EPS 被擠去了「現金流量比率」欄，而真正的
「現金再投資比率」整欄被丟掉沒存。**2012 年起（含）的資料是對的。**

好消息：目前沒有任何現役 Python 策略在吃這張表（不在 cache、`research/` 全庫零引用），
唯一下游是一個退役的 Scala 選股 view（`cbs_by_year`），所以**不在現在的資金路徑上**。
但只要有人未來拿 2011 前的 EPS 或現金流量比率來研究，就會拿到錯的數字。

**判定：BUG**

---

## 根因

`FinancialReader.readFinancialAnalysis`（`src/main/scala/reader/FinancialReader.scala:44-55`）
用**寫死的欄位位置**把原始 CSV 第 2 欄之後逐格塞進 19 個 DB 欄（`transferValues(0)`～
`transferValues(18)`，對應原始檔第 2～20 欄），**完全沒有依欄位數分支**。

但這個資料源有**兩套 schema**（實測 76 個原始檔全掃）：

| Schema | 檔名 | 年份 | 原始欄數 | 指標欄數 | 特徵 |
|---|---|---|---|---|
| **A（IFRS 後）** | `{year}_a.csv` | 2012–2025 | 21 | 19 | 與 DB schema 完全對齊 ✅ |
| **B（IFRS 前）** | `{year}_b.csv` | 1989–2014 | 22 | 20 | 多一欄 `[15] 營業利益佔實收資本比率` ❌ |

DB 表 + 讀取器是照 **A（19 欄）** 設計的。碰到 **B（20 欄）** 時，因為第 15 欄多了
「營業利益佔實收資本比率」，**從第 15 欄起全部往後移一格**，尾端的「現金再投資比率」被截掉。

這正是 `CLAUDE.md` 已記載的 **「TWSE CSV schema drift」** 壞味道——只是這裡連 `case _`
fall-through 都沒有，是純硬編碼索引，比 fall-through 更脆。姊妹讀取器
`readBalanceSheet` / `readIncomeStatement` 用 `allWithHeaders()` 依**中文欄名**對應（抗欄位
增減），這支卻用**位置**，是設計不一致的孤例。

## 錯位對照（每個 _b 列都這樣，逐位實測 0 例外）

| DB 欄位 | 存進去的**實際**原始欄 | **應該**是的原始欄 | 對錯 |
|---|---|---|---|
| `return_on_equity(%)` 及之前 13 欄 | 第 2–14 欄 | 第 2–14 欄 | ✅ 對齊（22 萬格 0 錯） |
| `profit_before_tax_to_capital(%)` | [15] 營業利益佔實收資本比率 | 稅前純益佔實收資本比率 | ❌ |
| `profit_to_sales(%)` | [16] 稅前純益佔實收資本比率 | 純益率 | ❌ |
| **`earnings_per_share(NTD)`** | **[17] 純益率(%)** | **每股盈餘(元)** | ❌ **錯指標＋錯單位** |
| `cash_flow_ratio(%)` | [18] 每股盈餘(元) | 現金流量比率 | ❌（真 EPS 藏這） |
| `cash_flow_adequacy_ratio(%)` | [19] 現金流量比率 | 現金流量允當比率 | ❌ |
| `cash_flow_reinvestment_ratio(%)` | [20] 現金流量允當比率 | 現金再投資比率 | ❌ |
| （無）| [21] 現金再投資比率 | — | ❌ **整欄丟棄** |

## 影響範圍（實測）

- **錯位列 = 18,591 / 41,144（45.2%）**，全部 year ≤ 2011：
  - 純 _b 市場-年（42 個）：TWSE 1989–2011、TPEx 1993–2011 = 17,144 列
  - 2012–2014 混合年裡的 _b 部分 = 1,447 列（這幾年多數公司還沒轉 IFRS，仍走 _b）
- **正確列 = 22,553（54.8%）**：2015–2025 兩市場，及 2012–2014 已轉 IFRS 的公司。
- 除了 schema 錯位，**解析→DB 本身零瑕疵**：逐列逐欄比對，PG 100% 忠實反映讀取器的
  位置對應（0 例「兩套都不符」），無千分位逗號造成的靜默 NULL（0 格），Big5-HKSCS
  編碼正常（依股票代號比對，代號/名稱皆正確）。

## 下游消費者

`src/main/resources/sql/view/1_cbs_by_year.sql`（退役 CBS 選股法）在 cash_flow 子分
（權重 0.3）用了 `cash_flow_ratio` / `cash_flow_adequacy_ratio` /
`cash_flow_reinvestment_ratio` 三個**錯位欄**（第 48-50、79-81、113-123 行）→ 2011 前
所有 firm-year 的現金流量子分是拿錯指標在算。這個 view 只被姊妹 view 引用，無現役策略
消費（見兄弟稽核單位 `B-view-1_cbs_by_year`）。

## 如何重現

```bash
# 1) 證明兩套 schema：_b 檔 22 欄、_a 檔 21 欄
for f in data/financial_analysis/twse/*.csv; do \
  printf "%s  cols=%s\n" "$f" "$(head -1 "$f" | tr ',' '\n' | wc -l)"; done
#   → 1989_b..2014_b 都是 22；2012_a..2025_a 都是 21

# 2) 錨點：台泥(1101) 2011(_b, 錯) vs 2020(_a, 對)
psql -h localhost -p 5432 -d quantlib -c "SELECT year, \"earnings_per_share(NTD)\" AS eps, \
  \"cash_flow_ratio(%)\" AS cfr FROM financial_analysis \
  WHERE market='twse' AND company_code='1101' AND year IN (2011,2020) ORDER BY year;"
#   2011: eps=35.84 (其實是純益率%，台泥真 EPS≈1.8), cfr=2.33 (其實才是真 EPS)
#   2020: eps=4.32  (正確)
```

原始檔第 17 欄（純益率）與第 18 欄（每股盈餘）的表頭可用
`head -1 data/financial_analysis/twse/2011_b.csv` 直接核對。

## 建議修法

`readFinancialAnalysis` 改為**依表頭中文欄名對應**（比照 `readBalanceSheet` 的
`allWithHeaders()`），或至少**依欄位數分支**（22→跳過 [15] 營業利益佔實收資本比率、
補收 [21] 現金再投資；21→現行對應）。修完必須**重跑 2011 前所有年度的 read**（先清該
區間列），並對 `cbs_by_year` view 重新物化。DB 若要保留「營業利益佔實收資本比率」這個
_b 專有欄，需加欄；否則明確記錄「_b 年度此欄不存」。
