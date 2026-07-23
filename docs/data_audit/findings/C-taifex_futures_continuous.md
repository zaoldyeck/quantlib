# C-taifex_futures_continuous — cache 一致性與缺漏稽核

**結論(一句話):這張表「大部分能信,但 2026 年這一段不能信」。** 整個 2026 年
1 月到 2 月底(33 個交易日)的期貨資料整段沒抓到,連帶讓 3 月 2 日那天冒出一個
物理上不可能的單日 +21%(金融期是 +25%)假跳空,把 3 月以後的連續指數整條墊高
了約 21%。除此之外:歷史其餘部分乾淨、cache 跟 PostgreSQL 完全一致、連續合約的
接軌演算法本身正確無誤。

判定:**BUG**(完整性有真缺口+下游真汙染;至於「cache 與 PG 一不一致」這題,答案是
一致——缺口是上游沒爬到,cache 只是忠實照抄)。

---

## 這張表是什麼(先講清楚,才知道稽核在查什麼)

`taifex_futures_continuous` **不是**從 PostgreSQL 直接同步過來的表,PG 裡根本沒有
這張表。它是在 cache 內部由 `src/quantlib/futures/taifex.py::build_taifex_futures_tables`
用 cache 的 `taifex_futures_daily`(這張才是 PG→cache 同步的)加工出來的「報酬接軌
連續期貨序列」:每天取最近月合約(front month),換月時用新合約自己的報酬接軌,
避免把新舊合約的價差灌進研究資料。涵蓋 5 個商品:TX(台指)、MTX(小台)、
TE(電子)、TF(金融)、TMF(微台),共 26,645 列,1998-07-21 ~ 2026-05-21。

所以「cache vs PG 一致性」這題要拆成兩半來答:
1. **上游來源** `taifex_futures_daily` 的 cache 有沒有忠實反映 PG?→ 有,一模一樣。
2. **加工邏輯**有沒有 drift(cache 裡物化的表 vs 現在的程式碼)?→ 沒有,逐列相同。

---

## 主要發現

### 1.【BUG】2026 年 1–2 月整段期貨資料缺漏,並在 3/2 產生假跳空

**根因在最上游——原始檔根本沒下載到。**
`data/taifex/futures_daily/2026/` 底下只有 `2026_3.csv`、`2026_4.csv`、`2026_5.csv`,
**缺 `2026_1.csv` 和 `2026_2.csv`**(其他每一年都是完整的單一 `YYYY_fut.csv`)。
於是整條鏈一路空下去:

| 層 | 狀態 |
|---|---|
| 原始檔 `data/taifex/futures_daily/2026/` | 缺 `2026_1.csv`、`2026_2.csv` |
| PG `taifex_futures_daily` 2026-01-01~02-26 | **0 筆**(全商品) |
| cache `taifex_futures_daily` 同窗 | **0 筆**(忠實照抄 PG) |
| cache `taifex_futures_continuous` | 從 2025-12-31 直接跳到 2026-03-02(61 日曆天缺口) |
| 同窗股市 `daily_quote` twse | **開盤 33 個交易日**(2026-01-02~02-26) |

**下游汙染:**連續序列在缺口後第一天(2026-03-02)的「單日報酬」被算成把 2 個月
漲幅壓成一天——

- TX / MTX / TMF `daily_return` = **+0.2101(+21.0%)**
- TE `daily_return` = **+0.2540(+25.4%)**
- 台指期單日漲跌幅上限是 **±10%**,+21%~+25% 物理上不可能出現。

連續指數 `continuous_close` 因此從 11,755(12/31)一次墊高到 14,225(+21%),
**3/2 之後每一個交易日(每商品 56 列)都站在被灌了 21% 的基準上**。任何用到
`daily_return` 的計算(波動率、Sharpe、回撤路徑、日報酬因子、單日門檻訊號)在這個
接縫上都是垃圾值;用 `continuous_close` 做水準/淨值分析的,3 月起整條被系統性抬高。

> 補充:嚴格說,`continuous_close` 的**水準**在缺口後大致「碰巧正確」——因為接軌用的是
> 202603 這個真實合約自己跨越缺口的真實漲幅(29,119→35,238);但它被表達成「單日
> +21%」,本質是 2 個月報酬被貼上「一天」的標籤。所以水準勉強能用、日報酬絕對不能用。

**演算法出處**:`src/quantlib/futures/taifex.py:75-78`(`lag(...) OVER (PARTITION BY
product, contract_month ORDER BY date)`)只看「這個合約上一次出現」,不檢查那是不是
「前一個交易日」;`:98-107` 再把它累乘成連續指數。缺口未被守護,所以跨缺口的
「上一次」直接被當成「昨天」。

**修法(不要自己下載,交主流程統一補)**:
1. 補下載 TAIFEX 每日期貨 2026-01、2026-02(`sbt "runMain Main pull taifex"` 產出
   `2026_1.csv`/`2026_2.csv`)→ `Main read taifex` 匯入 PG →
   `uv run python research/cache_tables.py` 重建 cache(連續表會一併重算)。
2. 次要防護(治本補強,非替代):builder 在接軌時加「上一筆必須是前一交易日」的
   守護,遇到多日缺口時把該日 `daily_return` 設 NULL(維持水準、不製造假日報酬),
   而不是把跨月漲幅當單日。根因仍是缺 raw,守護只是防再犯。

---

### 2.【SUSPECT】尾端約 2 個月不新鮮(2026-05-21 之後全缺)

5 個商品最後一筆都停在 **2026-05-21**,今天是 2026-07-23,尾端缺約 **39 個交易日**。
原始檔只到 `2026_5.csv`(檔案 mtime 5/21),沒有 6、7 月的檔。cache 與 PG 同樣都止於
05-21,**所以這不是 cache 不一致,是上游爬取新鮮度問題**——期貨爬取不在每日
`Main update` loop 裡(它走獨立的 `Main pull taifex`),所以會長期落後。

**修法**:週期性補跑 `Main pull/read taifex`(或把它納入每日 loop)後重建 cache。

---

### 3.【SUSPECT・低影響】2009-12-12(六)單日缺

該補行交易日股市開盤(`daily_quote` twse 772 檔),但 `taifex_futures_daily` 該日
**0 筆**,連續表也沒有這天。這是 2004 年(股市日曆可回溯的起點)以來**唯一**一個
「股市開、期貨缺」的單日。需查 TAIFEX 該補行日到底有沒有期貨盤;若有就補該日,
影響很小(15 年前、單日)。

---

## 查了沒問題的部分(負結果,一樣落盤)

### 4.【OK】cache ↔ PG 上游完全一致,且加工無 drift
- `taifex_futures_daily`:cache **5,780,185** 筆 == PG **5,780,185** 筆,日期範圍與
  distinct dates(6,877)完全相同。
- `taifex_futures_final_settlement`:cache **3,152** == PG **3,152**。
- **無 drift**:用現行 committed 的 `src/quantlib/futures/taifex.py` 就地把連續表重算一遍,
  與 cache 物化的 26,645 列做雙向 `EXCEPT`,兩邊差異都是 **0**。cache 物化的內容 =
  現行程式碼的產物,沒有舊版殘留。

### 5.【OK】接軌演算法正確、前月選取乾淨、無其他異常值
- **正常換月接軌正確**:2025-12 換月(202512→202601)那幾天的 `daily_return` 全部
  <2%,用的是新合約自身的隔日報酬,沒有把跨合約價差灌進去。
- **前月序列從不選到價差合約**:`contract_month` 含 `/`(如 202512/202601 這種
  calendar spread)的列 = **0**。價差合約雖然會進到 `contract_rank` 的 month_rank=2,
  但因排序 tie-break 用 `contract_month` 升冪,outright 永遠排在 spread 前面,
  front(month_rank=1)不受污染。
- **無負/零價**:`close / continuous_close / continuous_open/high/low / settlement_price`
  全部 >0;raw `high<low` = 0。
- **唯一的內部小不一致(可解釋、非 bug)**:109/26,645 列(0.4%)`continuous_close`
  落在 `[continuous_low, continuous_high]` 之外。抽查全部是因為 `continuous_close`
  以「結算價(settlement_price)」為錨,而結算價本來就可能落在當日成交高低之外
  (期貨結算價不是收盤價,尤其週選/週期貨薄量合約),H/L 卻是用成交高低換算的。
  這是期貨結算價的真實性質,**不是資料錯誤**;但下游程式不可假設連續 OHLC 滿足
  `low ≤ close ≤ high`。

---

## 附帶(超出本單位,建議另立單位追)

同一支 builder 產的姊妹表 **`taifex_futures_daily_factors`** 用 `month_rank=2` 取
「次月」期限結構(`tx_next_contract_month` / `tx_next_term_spread(_pct)`),而
month_rank=2 有可能是**價差合約**(如 202512/202601,close=96 這種只有幾十點的價差),
不是真正的次一到期月 → 這些期限結構欄位可能被污染。本單位的 `continuous` 表只用
month_rank=1,已驗證乾淨、**不受影響**,故此點不計入本單位判定,但建議對
`taifex_futures_daily_factors` 另開稽核。

---

## 重現指令

```bash
# 原始檔缺口(根因)
ls data/taifex/futures_daily/2026/          # 只有 2026_3/4/5.csv,缺 1、2 月

# PG 與 cache 同時為空
psql -h localhost -p 5432 -d quantlib -c \
  "SELECT COUNT(*) FROM taifex_futures_daily WHERE date BETWEEN '2026-01-01' AND '2026-02-26';"  # 0

# 接縫假報酬
uv run --project . python -c "import duckdb; from research import paths; \
c=duckdb.connect(str(paths.CACHE_DB),read_only=True); \
print(c.execute(\"SELECT product,date,daily_return FROM taifex_futures_continuous WHERE date='2026-03-02'\").fetchall())"

# drift 驗證(rebuild vs materialized 逐列 EXCEPT = 0):
#   scratchpad/verify_continuous.py(PYTHONPATH=<repo> uv run --project . python ...)
```
