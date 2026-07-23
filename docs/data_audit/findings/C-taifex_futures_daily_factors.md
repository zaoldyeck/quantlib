# C-taifex_futures_daily_factors — 這份資料能不能信?

**一句話結論:大部分能信,但有一個真 bug。** 這張表是 cache 自己算出來的期貨每日因子表(PostgreSQL 沒有這張表),
算出來的東西跟原料表逐格對得起來、沒有走樣;報價、法人淨部位、現貨基差這些欄可以信。**但『次月價差』
這個因子壞了**:程式挑「下個月合約」時,常常挑到台指期的「跨月價差組合單」,害 `tx_next_term_spread_pct`
在 6875 天裡有 515 天(7.5%)算成 ≈ −100% 的垃圾值。**直接拿這個因子當訊號的策略會中招。**

- **表定位**:cache-only 衍生表,無 PG 對應。由 `research/futures/taifex.py::build_taifex_futures_tables`
  從三張原料表算出:`taifex_futures_contract_rank`(←`taifex_futures_daily`+`_final_settlement`)、
  `market_index`(大盤現貨)、`taifex_futures_institutional`(三大法人)。
- **範圍**:1998-07-21(台指期上市日)..2026-05-21,6875 列,一天一列(grain 乾淨)。
- **重跑**:`PYTHONPATH=<repo> uv run --project research python docs/data_audit/scripts/C-taifex_futures_daily_factors/audit{,2,3}.py`

---

## Verdict: BUG

| # | 嚴重度 | 問題 | 是不是本表的錯 |
|---|---|---|---|
| 1 | **BUG** | 次月價差 `tx_next_term_spread(_pct)` 被價差組合單污染成 ≈ −100%,515/6875 列(7.5%) | 是(衍生邏輯 bug) |
| 2 | **BUG** | TX-MTX 價差 `tx_mtx_close_spread(_pct)` 拿 MTX 週合約比 TX 月合約,985/6152 列(16%) | 是(同一根因) |
| 3 | **BUG** | 時間序列兩段洞:2026-01/02(33 交易日)+ 尾端 2026-05-22 起(39 交易日) | 否(上游沒抓,已立案) |
| 4 | **REAL** | 基差在 2016-05-26 = +7.32%(大盤指數那天存錯值);2009 以前基差全 NULL | 否(market_index 已立案 / 先天界線) |
| 5 | **OK** | 表無 drift、grain 乾淨、硬掃全清、法人範圍合理、上市首日 NULL 皆真實 | — |

---

## Finding 1(BUG,最嚴重):次月價差被「價差組合單」污染

**白話**:台指期每天掛的合約,除了正常的「某月月合約」(例如 202409),還有一種**跨月價差組合單**,
合約名長得像 `202409/202410`,它的「報價」不是指數點位(兩萬多點),而是**兩個月之間的價差(通常只有
1 點)**。程式在找「下個月合約」時,用了一個只看「開頭 6 碼數字」的規則,結果把 `202409/202410` 這種
價差單也收進來,而且因為它跟前月同「月份鍵」、字串又緊排在月合約後面,**搶走了「第二近月」的位置**,
真正的次月(202410)反而被擠到第三。於是「次月 − 前月」的價差,變成「1 點的價差單報價 − 22318 點的前月」
= −22317 點 = −99.996%。

**證據**(2024-08-30,`audit.py` 第 2-3 段):

base `taifex_futures_daily` 的 TX 一般盤合約:

| contract_month | close | settlement | 說明 |
|---|---|---|---|
| 202409 | 22323 | 22318 | 前月(正確 rank1) |
| **202409/202410** | **1.0** | NULL | **價差組合單(被搶走 rank2)** |
| 202410 | 22320 | 22316 | 真次月(被擠到 rank3) |

stored 該列:`tx_next_contract_month=202409/202410`、`tx_next_term_spread=-22317`、
`tx_next_term_spread_pct=-0.999955`。**真值應為** 202410 vs 202409 = −2 點 / −0.009%。

- 影響範圍:`tx_next_contract_month LIKE '%/%'` 共 **515 列**,與 `|tx_next_term_spread_pct|>0.10` 完全同一批;
  橫跨 2007-10-09..2026-05-21(2015 年 81 天、2024 年 142、2025 年 126、2026 年 56 最密集)。
- 逐列真值對照(`audit2.py` 第 b 段):全批 stored ≈ −0.99,true ≈ ±0.003。例:2026-05-21 stored −0.9943 vs
  true(202607)−0.00036。
- 前月本身**沒被污染**:`tx_contract_month` 6875 列全是純月合約(價差單字串排在月合約之後,搶不到 rank1),
  所以 `tx_close`/`tx_settlement`/基差都正確。壞的只有次月那幾欄。

**根因程式**:`research/futures/taifex.py:56`
`AND regexp_matches(d.contract_month, '^\d{6}')` — 只要求「開頭 6 碼」,價差單(`YYYYMM/YYYYMM`)與週合約
(`YYYYMMWn`)都通過。

**修法**:把餵給前月/次月排名的合約限縮成**純月合約**:第 56 行改
`regexp_full_match(d.contract_month, '\d{6}')`(等價 `NOT LIKE '%/%' AND NOT LIKE '%W%'`)。
注意 `taifex_futures_contract_rank` 也被 `taifex_futures_continuous` 共用,改後要重跑 continuous 的
parity 測試重新定基;改完 `research/cache_tables.py` 重建 cache。

---

## Finding 2(BUG,同一根因):TX-MTX 價差拿週合約比月合約

**白話**:`tx_mtx_close_spread` 本意是比「大台(TX)」和「小台(MTX)」同一標的的微幅價差,理論上應該 ≈0。
但 MTX 有**週合約**,而同樣那個「只看開頭 6 碼」的排名規則,讓 MTX 的「最近月」常常是一張**週合約**,
去跟 TX 的**月合約**比 → 比的是不同到期日,價差反映的是跨月期限結構而不是微幅錯價,最大到 2.57%。

**證據**(`audit2.py` 第 a 段、`audit3.py` 三日抽樣):`mtx_contract_month` 非空 6152 列中,**985 列(16%)含
'W'(週合約)**。實例 2015-08-24:TX 前月=`201509` close 7340,MTX 前月=`201508W4`(週約)close 7400。
2022-06-16~22 群集 `tx_mtx_close_spread_pct` ~2.4-2.57%(mtx=202206W4 vs tx=202207)。

**修法**:同 Finding 1,把 MTX 前月排名排除週合約,使其比較同月月合約。

---

## Finding 3(BUG,非本表過錯):時間序列兩段洞

- **2026-01-02..2026-02-26(33 交易日)整段缺** → factor 序列從 2025-12-31 直跳 2026-03-02(61 日曆天)。
- **尾端停在 2026-05-21**,今天 2026-07-23,缺約 **39 個交易日**。

兩段都是**上游期貨沒抓到**、cache 忠實照缺:factor date 集合 vs base `taifex_futures_daily`(TX 一般盤)
雙向 0 不對稱,洞完全來自 base(`audit.py` 第 7 段;其餘 >7 天缺口全是農曆年假,真休市)。已於
**C-taifex_futures_daily**(根因:`data/taifex` 缺 `2026_1.csv`/`2026_2.csv`)與 **C-taifex_futures_continuous**
立案。**用到這兩段期間期貨因子的回測/訊號會拿到空序列或被靜靜截斷。**

**修法**:交主流程統一補下載 TAIFEX 期貨 2026-01/02 並把爬取納入每日 loop 補齊尾端,重建 cache 後 factor 洞自動消失。**不要自己下載。**

---

## Finding 4(REAL,非本表 bug):基差的兩個先天特性

1. **2016-05-26 `tx_spot_basis_pct` = +7.32%(物理不可能的正溢價)**,根因是那天大盤加權指數收盤在
   `market_index` 存成 **7811.18**(鄰日 8396/8463、且它自己的 `change_pct=+0.63%` 與 7811 自相矛盾,
   真值應 ~8449)。這個髒值被 spot 選取邏輯**正確**選中後流入基差。已於 **C-market_index** 立案(其 summary
   明載「2016-05-26 甚至算出台指期溢價 7.32%」,並指出另有 3 天基差正負號被算反)。
   （對照:2020-03-19 −3.75% 是 COVID 崩盤真實逆價差、2012-06-25 −3.05% 亦真實,非錯值。）
2. **2009-01-05 以前基差全 NULL**(約 2646 列),因 `market_index` cache 自 2009-01-05 起才有資料——這是
   涵蓋界線,不是漏抓。

**修法**:基差可信度上限=`market_index`;待 C-market_index 修好、重建 cache 即自動修正。本表 spot 選取邏輯
(`name LIKE '%發行量加權股價指數%'` 且優先 exact `發行量加權股價指數`)本身正確,無須改。

---

## Finding 5(OK):衍生正確性與表自洽,全清

- **無 drift**:以現行 committed 邏輯從原料表獨立重算,`tx_close`/`tx_next_term_spread`/`taiex_close`/
  `tx_spot_basis`/`foreign_tx_net_oi` 五欄 vs stored **逐日 0 mismatch**(`audit.py` 第 4 段)。
  → 表忠實反映當下 cache 原料;上面的 bug 是**衍生邏輯**的錯,不是 cache 同步走樣。
- **grain**:6875 rows = 6875 distinct dates。
- **硬性不可能值全 0**:負/零 tx_close、負 volume、負 open_interest、未來日期、taiex_close≤0(`audit.py` 第 6 段)。
- **法人淨部位範圍合理**:foreign net_oi [−59873, 33609]、trust [−6274, 51973]、dealer [−14031, 9299]、
  foreign net_vol [±9200],量級無爆值。
- **各商品上市首日 NULL 皆真實**:TX 1998-07-21、TE/TF 1999-07-21、MTX 2001-04-09、TMF 2024-07-29
  (對齊 base 前月首筆,即真實產品上市日);法人衍生欄僅 2023-05-22+ 有值(來源滾動三年),皆為來源涵蓋界線而非漏抓。

---

## 給下一個人的重點

- **可以信**:`tx_*`(報價/結算/未平倉)、`foreign/trust/dealer_*_net_oi/volume`(2023-05-22+)、
  `taiex_close`/`tx_spot_basis`(2009+ 且避開 market_index 髒日,主要是 2016-05-26)。
- **先別用**:`tx_next_term_spread`、`tx_next_term_spread_pct`、`tx_mtx_close_spread(_pct)` —— 修好 Finding 1/2
  的排名邏輯、重建 cache 之前,這三個因子在部分日子是壞的。
- 修 Finding 1 的排名邏輯會一併修好 Finding 2(同一根因),但要順帶重驗 `taifex_futures_continuous`。
