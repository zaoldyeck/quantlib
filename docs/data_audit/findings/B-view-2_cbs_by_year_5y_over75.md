# B-view-2_cbs_by_year_5y_over75 — 「連續 5 年財務品質 > 75」選股 view 稽核

審查對象：`src/main/resources/sql/view/2_cbs_by_year_5y_over75.sql`
（`cbs_by_year_5y_over75`，2022 legacy；直接建在 `cbs_by_year` 之上）

## 一句話結論

**這個 view 不能拿來選股，兩個獨立的理由各自就足以否決它。**
第一，它唯一的篩選條件是 `cbs > 75`，而 `cbs` 這個分數本身已在姊妹單位
`B-view-1_cbs_by_year` 被判 BUG（流動性子分近乎常數、缺財報反而拿高分、
稅前稅後混用）——門檻蓋在一個算壞的分數上。第二，**這個 view 自己的邏輯有前視
偏誤**：它只在每家公司「最新一年」判斷「近 5 年是否都 > 75」，然後把這家公司
**整段歷史**（含分數很低的年份）全部吐出來。實測 2,049 列輸出裡有 **794 列
（38.75%）自己的 `cbs` 根本 ≤ 75**；104 家入選公司裡有 **84 家（81%）**在被吐出的
歷史中至少有一年分數低於門檻，最慘的一列 `cbs = 14.8`。好消息：目前**沒有任何
Scala/Python/其他 view 在吃它**（全庫 grep + PostgreSQL `pg_depend` 都是空的），
所以它是 legacy 死 view，不在現役資金路徑上——但只要有人把它接上策略，就是一顆
前視地雷。

## 這個 view 在做什麼

```sql
with pass as (                              -- 每家公司「最新一年」算一個布林
  select distinct on (market, company_code) market, company_code,
    cbs > 75
    and lag(cbs,1) over (partition by company_code order by year) > 75
    and lag(cbs,2) over (partition by company_code order by year) > 75
    and lag(cbs,3) over (partition by company_code order by year) > 75
    and lag(cbs,4) over (partition by company_code order by year) > 75 as pass
  from cbs_by_year
  order by market, company_code, year desc)  -- desc → distinct on 取「最新年」那列
select cbs_by_year.market, year, ..., cbs, 五個子分
from cbs_by_year join pass
  on market/company_code 相等 and pass.pass is true   -- 沒有 year 條件！
order by company_code, year desc;
```

- `pass` CTE：`order by year desc` + `distinct on` → 每個 (market, code) 只保留
  **最新一年**那一列，在該列上用 `lag(1..4)` 回看前 4 年，判「最新 5 年是否都 > 75」。
- 主查詢 join **只比對 market + company_code，沒有 year 條件** → 一旦某公司 pass=true，
  它**所有年份**的列全部被輸出。

物化基底 `cbs_by_year` 有 41,226 列（2,042 個 market×code，1989–2025 年，5,888 列
`cbs > 75`）。本 view 輸出 2,049 列、**104 家**入選公司。

---

## BUG 1（高，本 view 自己的缺陷）：前視偏誤——「最新一年」的資格被回貼到整段歷史

`pass` 只在公司最新一年判定，主 join 卻不帶 year 條件，於是把整段歷史都吐出來。
後果是輸出面板**在時間上不誠實**：

- 全庫實測：輸出 2,049 列中 **794 列（38.75%）自己的 `cbs ≤ 75`**——一個名字寫著
  `over75` 的 view，近四成的列根本沒過 75。
- 104 家入選公司裡 **84 家（81%）**在被吐出的歷史中至少有一年 `cbs ≤ 75`，
  最低吐出到 `cbs = 14.8`。
- 具體案例 **3046 建碁**：1998–2020 年 `cbs` 一路在 14.8～69（2004=16.90、
  2005=14.83、2016=31.68），**只有 2021–2025 才 > 75**（75.22 / 84.59 / 83.13 /
  83.75 / 82.87）。它的 5 年連續其實是 2021–2025，但 view 把它 **28 列全吐出**，
  包含 2005 年 `cbs=14.83` 那列。

任何消費端若把 `(year, company, cbs, 子分)` 當時間序列讀，就會拿到「2005 年建碁
是連續 5 年財務品質 > 75 的好公司」這種完全錯誤的結論——因為這個「資格」是用
**該公司最新一年（未來）**的資料回算後貼上去的。要在 2005 年知道建碁會不會入選，
得先知道它 2021–2025 的分數，這是教科書等級的 look-ahead。

- 證據：`2_cbs_by_year_5y_over75.sql:23-26`（join 無 year 條件）；下方可重跑 SQL。
- 修法：若要當「逐年 PIT 面板」，`pass` 必須**逐年**計算（每一列都用它自己那年的
  近 5 年窗判定），而不是只算最新年、再無條件 join 整段歷史；若只是想要「目前
  合格公司名單」，就別輸出 `year` 逐列歷史（改成每家一列的快照），避免被誤讀成時序。

```sql
-- 可重跑：先物化基底(cbs_by_year 是重 view,直接查會逾時),再重現 view 邏輯
create temp table cbs_mat as select * from cbs_by_year;
create temp table pass_mat as
  select distinct on (market, company_code) market, company_code,
    cbs>75 and lag(cbs,1) over (partition by company_code order by year)>75
    and lag(cbs,2) over (partition by company_code order by year)>75
    and lag(cbs,3) over (partition by company_code order by year)>75
    and lag(cbs,4) over (partition by company_code order by year)>75 as pass
  from cbs_mat order by market, company_code, year desc;
-- 38.75% 的輸出列自己 <=75:
select count(*) filter (where m.cbs<=75) le75, count(*) total
from cbs_mat m join pass_mat p
  on m.market=p.market and m.company_code=p.company_code and p.pass is true;
```

## BUG 2（高，繼承自 view-1）：篩選門檻蓋在一個已被判壞的分數上

本 view 唯一的選股邏輯是 `cbs > 75`。但 `B-view-1_cbs_by_year` 已確認 `cbs` 的
五個子分裡有三個算壞：① 流動性子分因「百分比 vs 倍數」單位錯配，99.69% 的公司
被鎖在滿分；② 缺財報的公司因 `rank()` 升冪 `NULLS LAST` 在營運績效（0.25 權重）
反而拿近滿分；③ ROIC 分子有約 24% 的 firm-year 用「稅前純益」、其餘用稅後，
放在同一橫截面排名。這三個病直接決定誰的 `cbs` 會 > 75。因此這 104 家入選公司
是**依一個失真的分數**挑出來的——即使把 BUG 1 的前視修好，名單本身仍不可信。

- 證據：`2_cbs_by_year_5y_over75.sql:4-8`（唯一條件即 `cbs > 75`）；根因與實測
  分佈見 `docs/data_audit/findings/B-view-1_cbs_by_year.md`。
- 修法：先修好 view-1 的三個 BUG（詳見該單位），本 view 的門檻才有意義。

## SUSPECT（中）：`lag` 是「列偏移」不是「年偏移」，5 家入選公司的「連續 5 年」其實有缺口

`lag(cbs, n) over (order by year)` 取的是**前 n 列**，不是**前 n 年**。若公司某年缺
財報（該 year 在 `cbs_by_year` 不存在），`lag` 會跳過缺口往更早年份抓，於是「連續
5 列 > 75」不等於「連續 5 個日曆年 > 75」。實測 104 家入選公司中 **5 家**
（4728、2755、5274、8480、8924）的「最新 5 列」跨越的日曆年不是 5 年：4728 跨
2019–2025（缺 2 年）、其餘 4 家跨 6 個日曆年（缺 1 年）。對這 5 家，view 宣稱的
「連續 5 年」在字面上不成立——中間缺的那年很可能正是分數掉下來、財報缺漏的年份。

- 證據：`2_cbs_by_year_5y_over75.sql:5-8`；下方 SQL 可重跑列出這 5 家。
- 修法：改用年值判定（`lag` 前先確認 `year = 前一列 year + 1`，或用
  `generate_series` 補齊年格再判），或明確接受「5 個資料點」語義並改名/註明。

```sql
with latest as (
  select company_code, year,
    row_number() over (partition by company_code order by year desc) rn
  from cbs_mat where company_code in (select company_code from pass_mat where pass is true))
select company_code,
  max(year) filter (where rn=1) - max(year) filter (where rn=5) as calendar_span
from latest where rn<=5 group by company_code
having max(year) filter (where rn=1) - max(year) filter (where rn=5) <> 4;
-- → 4728(span 6), 2755, 5274, 8480, 8924 (span 5); 其餘 99 家 span=4(正常)
```

## SUSPECT（低）：`lag` 分區只用 `company_code`，`distinct on`／join 卻用 `market+code`——轉板公司分區不一致

`lag(...) over (partition by company_code ...)` **沒帶 market**，但 `distinct on
(market, company_code)` 與最終 join 都帶 market。對從上櫃轉上市（轉板）的公司，
`lag` 會把 TPEx + TWSE 兩段年份混在同一個分區裡排序，理論上可能把跨市場的列拉進
近 5 年判定。實測基底有 **15 個 code 同時出現在兩個市場**，其中只有 **1 個
（4736）**進入入選集——它是以 **TPEx 端點（最新年 2022）**入選，`lag(1..4)` 回看
2021→2018 全在 TPEx（TWSE 的 2023–2025 在年序上更晚、`lag` 往回抓不到），故此唯一
入選案例**沒有跨市場污染**。結論：這是潛在不一致，但在現有資料上**沒有造成任何
入選翻轉**，影響為零。

- 證據：`2_cbs_by_year_5y_over75.sql:5-8`（`partition by company_code` 缺 market）；
  15 個跨市場 code、1 個入選（4736）皆可重跑驗證。
- 修法：`lag` 的 `partition by` 補上 `market`，與 `distinct on`／join 對齊。

## SUSPECT（設計，繼承自 view-1）：無時點對齊（PIT）

基底 `cbs_by_year` 純用日曆年 join，年度（Q4）財報要到隔年 Q1 才公告；本 view 疊在
其上、再加「最新年回貼整段歷史」（BUG 1），前視程度更重。目前無現役消費端，屬潛在
設計缺口；一旦上線務必補公告落差（as-of 財報公告日）。

---

## 查了什麼（負結果一併落盤，避免重查）

- **消費端盤點：0 個（OK，legacy 死 view）**。全庫 `grep cbs_by_year_5y_over75 /
  5y_over75` 只命中三支 view 的 `.sql` 定義本身；PostgreSQL `pg_depend` 查
  `cbs_by_year_5y_over75` 的下游依賴為**空**。無任何 Scala/Python 策略或其他 view
  消費它 → 不在現役 Serenity/Evergreen/Iter 資金路徑上。
- **物化狀態（OK）**：`cbs_by_year`、`cbs_by_year_5y_over75`、`cbs_ttm_5y_over75`
  三者在 PG 都是**普通 view（relkind='v'）**，非 matview。直接查
  `cbs_by_year_5y_over75` 會逾時（>300s，因基底 `cbs_by_year` 是重管線）——本稽核以
  session 內 temp table 物化基底後驗證。
- **`pass` 對 <5 年公司的處理（OK）**：不足 5 年的公司 `lag(4)` 回 NULL →
  `... and NULL > 75` = NULL，`pass.pass is true` 過濾掉 NULL，正確排除。無誤收。
- **輸出的市場切分與 join 一致（OK）**：join 帶 market，轉板公司的 TPEx 段與 TWSE 段
  各自對應各自的 `pass` 列，不會把 TWSE 歷史掛到 TPEx 的 pass（4736 即以 TPEx 段入選、
  TWSE 段因各自 pass=false 未被吐出）。市場層面的「整段歷史吐出」是 per-market 一致的。
- **資料真實性（OK）**：入選/範例公司皆真實存在。3046=建碁、4736 等皆為真實
  TWSE/TPEx 個股，非髒資料造成的假入選。
- **會計語義沿用 view-1 的既有結論**：`cbs` 的 TTM/累計制、合併 vs 個體、子分定義
  等已在 `B-view-1_cbs_by_year` 逐項查證，本 view 未新增任何算式，僅套 `cbs > 75`
  門檻 + 5 年窗，故不重複查證。
