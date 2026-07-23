-- 稽核單位 B-matview-4_cash_flows_with_titles 的可重跑證據
-- 對象:src/main/resources/sql/materialized_view/4_cash_flows_with_titles.sql
-- 跑法:psql -h localhost -p 5432 -d quantlib -f docs/data_audit/scripts/B-matview-4_cash_flows_with_titles/checks.sql
-- 前提:PG 已建好 cash_flows_progressive / cash_flows_individual / cash_flows_with_titles
--       (Main init + Main read financial_statements,並 refresh 三張 matview)

\echo '=== [0] view 定義原文 ==='
select pg_get_viewdef('cash_flows_with_titles'::regclass, true);

\echo '=== [1] 列數 + market 分佈(市場欄是不是常數 tw) ==='
select market, count(*) from cash_flows_with_titles group by market;

\echo '=== [2] 扇出檢查:matview 是否有重複 (year,quarter,company_code) ==='
select count(*) as dup_groups
from (select year, quarter, company_code, count(*) c
      from cash_flows_with_titles group by 1,2,3 having count(*) > 1) t;

\echo '=== [3] 驅動列對齊:ocf CTE 的 distinct (y,q,code) 應等於 matview 列數 ==='
select (select count(*) from cash_flows_with_titles)                         as matview_rows,
       (select count(*) from (select distinct year, quarter, company_code
                              from cash_flows_individual
                              where title in ('營業活動之淨現金流入（流出）','營業活動之淨現金流入(流出)')
                                and value is not null) t)                     as ocf_driving_rows;

\echo '=== [4] OR 多變體是否在同一 (m,y,q,code) 共存 → 潛在扇出源 ==='
\echo '--- capex 三變體 ---'
select count(*) as capex_multi_title_groups from (
  select market,year,quarter,company_code, count(*) c
  from cash_flows_individual
  where title in ('取得不動產、廠房及設備','取得不動產及設備','購置固定資產')
  group by 1,2,3,4 having count(*) > 1) t;
\echo '--- ocf 兩變體 ---'
select count(*) as ocf_multi_title_groups from (
  select market,year,quarter,company_code, count(*) c
  from cash_flows_individual
  where title in ('營業活動之淨現金流入（流出）','營業活動之淨現金流入(流出)')
  group by 1,2,3,4 having count(*) > 1) t;
\echo '--- inventory 兩變體 ---'
select count(*) as inv_multi_title_groups from (
  select market,year,quarter,company_code, count(*) c
  from cash_flows_individual
  where title in ('存貨(增加)減少','存貨（增加）減少')
  group by 1,2,3,4 having count(*) > 1) t;
\echo '--- dividend 兩變體 ---'
select count(*) as div_multi_title_groups from (
  select market,year,quarter,company_code, count(*) c
  from cash_flows_individual
  where title in ('分配現金股利','發放現金股利')
  group by 1,2,3,4 having count(*) > 1) t;

\echo '=== [5] value<0 濾網丟掉多少「合法」單季值(全類掃描:capex/inventory/dividend) ==='
\echo '--- capital_expense:總 title-列 / value>=0 被丟 / 佔比 ---'
select count(*) filter (where true)          as total_rows,
       count(*) filter (where value >= 0)    as dropped_ge0,
       round(100.0*count(*) filter (where value >= 0)/nullif(count(*),0),2) as pct_dropped
from cash_flows_individual
where title in ('取得不動產、廠房及設備','取得不動產及設備','購置固定資產');
\echo '--- increase_in_inventories:總 / value>=0 被丟(其中 value>0 是真實存貨下降的現金流入) ---'
select count(*)                              as total_rows,
       count(*) filter (where value >= 0)    as dropped_ge0,
       count(*) filter (where value > 0)     as dropped_gt0_real_inflow,
       count(*) filter (where value = 0)     as dropped_eq0,
       round(100.0*count(*) filter (where value >= 0)/nullif(count(*),0),2) as pct_dropped
from cash_flows_individual
where title in ('存貨(增加)減少','存貨（增加）減少');
\echo '--- cash_dividends_paid:總 / value>=0 被丟(多為單季差分=0 的非發放季,coalesce 後無害) ---'
select count(*)                              as total_rows,
       count(*) filter (where value >= 0)    as dropped_ge0,
       count(*) filter (where value > 0)     as dropped_gt0,
       count(*) filter (where value = 0)     as dropped_eq0,
       round(100.0*count(*) filter (where value >= 0)/nullif(count(*),0),2) as pct_dropped
from cash_flows_individual
where title in ('分配現金股利','發放現金股利');

\echo '=== [6] 下游具體傷害:季表 fcf_par_share = (ocf + capital_expense)/capital_stock 未包 coalesce ==='
\echo '--- 有 ocf 但 capex 被濾成 NULL 的公司季數(這些季 fcf_par_share 會被 NULL 掉) ---'
select count(*) as ocf_present_capex_null
from cash_flows_with_titles
where ocf is not null and capital_expense is null;

\echo '=== [7] 標題涵蓋完整性:各概念在 3141 個 distinct title 裡還有沒有沒被收進來的變體 ==='
\echo '--- 折舊(view 只收 折舊費用) ---'
select title, count(*) from cash_flows_individual where title like '%折舊%' group by 1 order by 2 desc limit 15;
\echo '--- 存貨(view 只收 存貨(增加)減少 兩種括號) ---'
select title, count(*) from cash_flows_individual where title like '%存貨%' group by 1 order by 2 desc limit 15;
\echo '--- 現金股利(view 收 分配/發放現金股利) ---'
select title, count(*) from cash_flows_individual where title like '%股利%' group by 1 order by 2 desc limit 15;
\echo '--- 取得不動產/固定資產/設備(view 收 3 變體) ---'
select title, count(*) from cash_flows_individual where title like '%取得不動產%' or title like '%固定資產%' or title like '%取得%設備%' group by 1 order by 2 desc limit 20;
\echo '--- 營業活動淨現金流(view 收 2 括號變體) ---'
select title, count(*) from cash_flows_individual where title like '%營業活動%淨現金%' group by 1 order by 2 desc limit 10;

\echo '=== [8] 抽樣逐筆驗證:2330 台積電 raw individual vs matview 是否吻合(近 6 季) ==='
select year, quarter, depreciation, increase_in_inventories, ocf, capital_expense, cash_dividends_paid
from cash_flows_with_titles where company_code='2330' order by year desc, quarter desc limit 6;
\echo '--- 對照:2330 raw individual(同 5 個 title) ---'
select year, quarter, title, value
from cash_flows_individual
where company_code='2330'
  and title in ('折舊費用','存貨(增加)減少','存貨（增加）減少',
                '營業活動之淨現金流入（流出）','營業活動之淨現金流入(流出)',
                '取得不動產、廠房及設備','取得不動產及設備','購置固定資產',
                '分配現金股利','發放現金股利')
  and year >= 2023
order by year desc, quarter desc, title;

\echo '=== [9] depreciation 有沒有負值(單季差分後的異常;add-back 概念上應為正) ==='
select count(*) as neg_depreciation_rows,
       count(*) filter (where depreciation < 0) as strictly_neg
from cash_flows_with_titles where depreciation is not null;
