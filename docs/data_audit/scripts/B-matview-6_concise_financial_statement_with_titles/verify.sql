-- B-matview-6_concise_financial_statement_with_titles verification queries
-- Run: psql -h localhost -p 5432 -d quantlib -f <this file>
-- Target: src/main/resources/sql/materialized_view/6_concise_financial_statement_with_titles.sql

-- [1] existence / rowcount / period span
SELECT count(*) matview_exists FROM pg_matviews WHERE matviewname='concise_financial_statement_with_titles';  -- 1
SELECT count(*) rows FROM concise_financial_statement_with_titles;                                             -- 170503
SELECT min(year::text||'Q'||quarter::text) min_p, max(year::text||'Q'||quarter::text) max_p
FROM concise_financial_statement_with_titles;                                                                  -- 1989Q1 .. 2026Q1

-- [2] FAN-OUT: duplicate (market,year,quarter,company_code) keys (should be 0; root cause of downstream non-determinism)
WITH d AS (SELECT market,year,quarter,company_code,count(*) n
           FROM concise_financial_statement_with_titles GROUP BY 1,2,3,4)
SELECT count(*) FILTER (WHERE n>1) dup_keys, max(n) max_mult, min(year) FILTER (WHERE n>1) ymin, max(year) FILTER (WHERE n>1) ymax
FROM d;                                                            -- 858 | 64 | 2006 | 2012
WITH d AS (SELECT market,year,quarter,company_code,count(*) n
           FROM concise_financial_statement_with_titles GROUP BY 1,2,3,4)
SELECT n mult, count(*) keys FROM d WHERE n>1 GROUP BY n ORDER BY n;  -- 2:413  4:180  8:118  64:147

-- [3] mechanism: 2855 2011Q4 = 2^6 = 64 (each non-distinct-on CTE multi-matches)
SELECT 'assets'  k, count(*) FROM concise_balance_sheet_individual   WHERE company_code='2855' AND year=2011 AND quarter=4 AND title IN ('資產合計','資產總計','資產總額')
UNION ALL SELECT 'equity',  count(*) FROM concise_balance_sheet_individual   WHERE company_code='2855' AND year=2011 AND quarter=4 AND title IN ('權益總額','權益總計','股東權益總計','股東權益','股東權益合計')
UNION ALL SELECT 'revenue', count(*) FROM concise_income_statement_individual WHERE company_code='2855' AND year=2011 AND quarter=4 AND title IN ('營業收入','利息淨收益','收益','收入','營業收入淨額')
UNION ALL SELECT 'opex',    count(*) FROM concise_income_statement_individual WHERE company_code='2855' AND year=2011 AND quarter=4 AND title IN ('營業費用','費用')
UNION ALL SELECT 'ebit',    count(*) FROM concise_income_statement_individual WHERE company_code='2855' AND year=2011 AND quarter=4 AND title IN ('稅前純益','稅前淨利（淨損）','繼續營業單位稅前淨利','繼續營業單位稅前損益','繼續營業單位稅前淨利（淨損）','繼續營業單位稅前淨利(淨損)','繼續營業單位稅前純益（純損）','繼續營業單位稅前純益(純損)','繼續營業單位稅前合併淨利(淨損)','繼續營業部門稅前淨利（淨損）')
UNION ALL SELECT 'eps',     count(*) FROM concise_income_statement_individual WHERE company_code='2855' AND year=2011 AND quarter=4 AND title IN ('每股稅後盈餘(元)','基本每股盈餘（元）','基本每股盈餘','每股盈餘','每股稅後盈餘','基本每股盈餘(元)');
-- assets 2, equity 2, revenue 2, opex 2, ebit 2, eps 2 -> 2^6 = 64

-- [4] profit CTE contaminated by 稅前純益 (after-tax vs pre-tax) — profit==ebit fingerprint
SELECT 'pre2006'   seg, count(*) n, count(*) FILTER (WHERE profit=ebit) eq FROM concise_financial_statement_with_titles WHERE year BETWEEN 1995 AND 2005
UNION ALL SELECT '2006_2012', count(*),   count(*) FILTER (WHERE profit=ebit) FROM concise_financial_statement_with_titles WHERE year BETWEEN 2006 AND 2012
UNION ALL SELECT '2013plus',  count(*),   count(*) FILTER (WHERE profit=ebit) FROM concise_financial_statement_with_titles WHERE year >= 2013;
-- pre2006 30040 | 29026 (96.6%)   2006_2012 47446|2059   2013plus 89119|5602

-- [5] server collation controls distinct-on winner
SELECT datcollate FROM pg_database WHERE datname='quantlib';  -- C  (UTF-8 byte order)

-- [6] net_operating_income mixes operating income with pre-tax income
WITH picked AS (
  SELECT DISTINCT ON (year,quarter,company_code) year,quarter,company_code,title
  FROM concise_income_statement_individual
  WHERE title IN ('繼續營業單位稅前淨利（淨損）','營業利益（損失）','營業利益','繼續營業單位稅前損益',
                  '繼續營業單位稅前淨利(淨損)','繼續營業單位稅前合併淨利(淨損)','營業淨利(淨損)','營業利益(損失)')
  ORDER BY year,quarter,company_code,title)
SELECT CASE WHEN title LIKE '%稅前%' THEN 'PRETAX_selected' ELSE 'operating_selected' END g,
       count(*), min(year), max(year) FROM picked GROUP BY 1;
-- PRETAX_selected 2355 (2006-2026) | operating_selected 154573

-- [7] modern-year correctness (OK side) + ebit=EBT proof
SELECT year,quarter,total_operating_revenue,net_operating_income,ebit,profit,eps
FROM concise_financial_statement_with_titles WHERE company_code='2330' AND market='twse' AND year=2024 ORDER BY quarter;
-- Q1 rev 592,644,201 | op_inc 249,018,306 | ebit(EBT) 266,543,204 | profit 225,221,263 | eps 8.70
SELECT title,value FROM concise_income_statement_progressive
WHERE company_code='2330' AND market='twse' AND year=2024 AND quarter=1 AND type='consolidated'
  AND (title LIKE '%稅前%' OR title LIKE '%所得稅%' OR title LIKE '%本期淨利%' OR title='營業利益');
-- 稅前淨利（淨損）266,543,204 ; 所得稅費用（利益）41,321,941 ; 本期淨利（淨損）225,221,263
-- => ebit column = 稅前淨利 (EBT, pre-tax, interest already deducted), NOT EBIT.  266,543,204-41,321,941=225,221,263=profit ✓

-- [8] consumers of the matview (pg_depend)
SELECT DISTINCT dependent.relname FROM pg_depend d
JOIN pg_rewrite r ON r.oid=d.objid
JOIN pg_class dependent ON dependent.oid=r.ev_class
JOIN pg_class src ON src.oid=d.refobjid
WHERE src.relname='concise_financial_statement_with_titles' AND dependent.relname<>'concise_financial_statement_with_titles';
-- financial_index_quarterly | financial_index_ttm
