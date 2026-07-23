-- 稽核單位 B-matview-3_cash_flows_individual 的可重跑證據
-- 對象:src/main/resources/sql/materialized_view/3_cash_flows_individual.sql
-- 跑法:psql -h localhost -p 5432 -d quantlib -f docs/data_audit/scripts/B-matview-3_cash_flows_individual/checks.sql
-- 前提:PG 已建好 cash_flows_progressive / cash_flows_individual(Main init + Main read financial_statements)

\echo '=== [0] 部署定義 vs 檔案(確認 matview 未漂移) ==='
select pg_get_viewdef('cash_flows_individual'::regclass, true);

\echo '=== [1] market 值域(確認 partition by market 是 no-op) ==='
select market, count(*) from cash_flows_progressive group by 1;

\echo '=== [2] Q1 直取累計值:逐位相同(跨年邊界正確) ==='
select count(*) q1_rows, count(*) filter (where p.value = i.value) q1_identical
from cash_flows_progressive p join cash_flows_individual i using (id) where p.quarter = 1;

\echo '=== [3] 輸出 NULL 統計(本 view 無 where value is not null,NULL 直接留在表裡) ==='
select count(*) total,
       count(*) filter (where value is null) null_value,
       count(*) filter (where quarter = 1) q1_rows,
       count(*) filter (where quarter <> 1 and value is null) nonq1_null
from cash_flows_individual;

\echo '=== [4] BUG A:lag 落到不相鄰期別的分佈(全 title) ==='
with w as (
  select market, year, quarter, company_code, title, value,
         lag(value) over p pv, lag(year) over p py, lag(quarter) over p pq
  from cash_flows_progressive
  window p as (partition by market, company_code, title order by year, quarter)
)
select quarter, py = year as prev_same_year, pq as prev_quarter, count(*) n
from w
where quarter <> 1 and pv is not null and not (py = year and pq = quarter - 1)
group by 1,2,3 order by n desc;

\echo '=== [5] BUG A 成因拆解:整季不見 vs 該季有資料但這個 title 沒出現(科目字典漂移) ==='
with q as (select distinct company_code, year, quarter from cash_flows_progressive),
 w as (
  select year, quarter, company_code, title,
         lag(year) over p py, lag(quarter) over p pq, lag(value) over p pv
  from cash_flows_progressive
  window p as (partition by market, company_code, title order by year, quarter)
 )
select case when q.company_code is null then 'whole quarter missing' else 'quarter exists, TITLE missing' end cause,
       count(*) n, count(distinct w.company_code) ncomp
from w
left join q on q.company_code = w.company_code and q.year = w.year and q.quarter = w.quarter - 1
where w.quarter <> 1 and w.pv is not null and not (w.py = w.year and w.pq = w.quarter - 1)
group by 1 order by n desc;

\echo '=== [6] BUG A 在下游真正被消費的科目 OCF 上的比例 ==='
with w as (
  select year, quarter, company_code, value,
         lag(value) over p pv, lag(year) over p py, lag(quarter) over p pq
  from cash_flows_progressive
  where title in ('營業活動之淨現金流入（流出）','營業活動之淨現金流入(流出)')
  window p as (partition by market, company_code, title order by year, quarter)
)
select case when pv is null then 'no-prev (NULL out)'
            when py = year and pq = quarter - 1 then 'adjacent OK'
            else 'NON-ADJACENT (wrong)' end k,
       count(*) n, count(distinct company_code) ncomp
from w where quarter <> 1 group by 1 order by n desc;

\echo '=== [7] BUG A:OCF 受害列 × 該年實際有交易的公司(2018+) ==='
with w as (
  select year, quarter, company_code, value,
         lag(value) over p pv, lag(year) over p py, lag(quarter) over p pq
  from cash_flows_progressive
  where title in ('營業活動之淨現金流入（流出）','營業活動之淨現金流入(流出)')
  window p as (partition by market, company_code, title order by year, quarter)
),
bad as (select * from w where quarter <> 1 and pv is not null and not (py = year and pq = quarter - 1)),
tv as (select company_code, extract(year from date)::int yr, count(*) days, sum(trade_value) val
       from daily_quote group by 1,2)
select bad.year,
       count(*) n_rows,
       count(distinct bad.company_code) ncomp,
       count(*) filter (where tv.days >= 150) n_rows_traded,
       count(distinct bad.company_code) filter (where tv.days >= 150) ncomp_traded,
       round((sum(tv.val) filter (where tv.days >= 150)) / 1e8) traded_value_e8
from bad left join tv on tv.company_code = bad.company_code and tv.yr = bad.year
where bad.year >= 2018
group by 1 order by 1 desc;

\echo '=== [8] BUG B:存量科目(餘額)被當流量差分 — 期初現金逐季分佈 ==='
select quarter, count(*) n,
       count(*) filter (where value = 0) diff_zero,
       count(*) filter (where value <> 0) diff_nonzero,
       round(min(value)::numeric, 0) mn, round(max(value)::numeric, 0) mx
from cash_flows_individual
where title = '期初現金及約當現金餘額' and value is not null
group by 1 order by 1;

\echo '=== [9] BUG B:2330 台積電 2024 逐季手算對帳(原始累計 vs matview) ==='
select p.year, p.quarter, p.title, p.value progressive, i.value matview
from cash_flows_progressive p join cash_flows_individual i using (market, year, quarter, company_code, title)
where p.company_code = '2330' and p.year = 2024
  and p.title in ('期初現金及約當現金餘額','期末現金及約當現金餘額','營業活動之淨現金流入（流出）','本期現金及約當現金增加（減少）數')
order by p.title, p.quarter;

\echo '=== [10] BUG B:存量科目總列數(這些 title 差分後語意被破壞) ==='
select title, count(*) n
from cash_flows_progressive
where title in ('期初現金及約當現金餘額','期末現金及約當現金餘額','資產負債表帳列之現金及約當現金')
group by 1 order by n desc;

\echo '=== [11] BUG C 指紋:1590 亞德客-KY 2025(缺 Q2)單季 OCF ==='
select p.year, p.quarter, p.value progressive_ytd, i.value matview_single_q
from cash_flows_progressive p join cash_flows_individual i using (market, year, quarter, company_code, title)
where p.company_code = '1590' and p.year between 2024 and 2025
  and p.title = '營業活動之淨現金流入（流出）'
order by p.year, p.quarter;

\echo '=== [12] 報表基礎(合併 cr / 個體 ir)換基:祥碩 5269 ==='
select p.year, p.quarter, p.value progressive_ytd, i.value matview_single_q
from cash_flows_progressive p join cash_flows_individual i using (market, year, quarter, company_code, title)
where p.company_code = '5269' and p.year between 2024 and 2025
  and p.title = '營業活動之淨現金流入（流出）'
order by p.year, p.quarter;

\echo '=== [13] 單季 OCF 為負但四季累計為正的異常指紋(BUG A 的輸出端指紋) ==='
with i as (
  select year, quarter, company_code, value
  from cash_flows_individual
  where title = '營業活動之淨現金流入（流出）' and value is not null
)
select year, count(*) filter (where value < 0) neg_rows, count(*) n_rows,
       round(100.0 * count(*) filter (where value < 0) / count(*), 1) neg_pct
from i where year >= 2013 group by 1 order by 1;

\echo '=== [14] 下游 cash_flows_with_titles 的 join 是否會扇出(同季同 code 雙市場) ==='
select count(*) from (
  select year, quarter, company_code from (select distinct market, year, quarter, company_code from cash_flows_progressive) d
  group by 1,2,3 having count(distinct market) > 1) t;
