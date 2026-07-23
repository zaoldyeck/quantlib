-- B-view-6_cbs_ttm_5y_over75 稽核證據(可重跑)
-- 對象:src/main/resources/sql/view/6_cbs_ttm_5y_over75.sql
-- 執行:psql -h localhost -p 5432 -d quantlib -f <此檔>
-- 定案日:2026-07-23
--
-- view 語意:對每支股票取「最新一季 + 往前 19 季」共 20 季(=5 年),
-- 若這 20 季的品質綜合分 cbs 全部 > 75 則入選。cbs 來自 growth_analysis_ttm
-- (= financial_index_ttm 的 cbs,見 4_financial_index_ttm.sql:120-176)。

\echo '=== [0] 消費者盤點:此 view 有沒有被任何程式引用(repo 根跑 grep) ==='
\echo '   grep -rIn "cbs_ttm_5y_over75" . | grep -v .git/ | grep -v 6_cbs_ttm_5y_over75.sql'
\echo '   => 只出現在 docs/data_audit 自己的報告;無 Scala/Python/conf 消費者(孤兒 view)'

\echo ''
\echo '=== [1] 跨市場 company_code:lag 視窗 partition by company_code 漏掉 market ==='
-- distinct-on 與最終 join 都用 (market, company_code),但 lag 只 partition by company_code。
-- 轉板(上櫃→上市)的股票同一 code 橫跨兩市場 → lag 鏈把兩市場季別混在一條序列。
select company_code, string_agg(distinct market,'+' order by market) markets
from concise_financial_statement_with_titles
where market in ('twse','tpex')
group by company_code having count(distinct market) > 1
order by company_code;                 -- 實測:13 檔(1597,1752,3092,3652,4736,5306,6423,6426,6438,6446,6472,6589,8476)

\echo ''
\echo '=== [2] 位移非日曆:lag(1..19) 是「前 19 列」不是「前 19 季」 ==='
-- 全市場層級:多少公司在整段歷史有缺季(distinct 季數 != 首末季跨距)。
select count(*) companies_total,
       count(*) filter (where n <> span) companies_with_any_gap
from (
  select market, company_code,
         count(*) n,
         max(year*4+quarter) - min(year*4+quarter) + 1 span
  from (select distinct market, company_code, year, quarter
        from concise_financial_statement_with_titles where market in ('twse','tpex')) d
  group by market, company_code
) t;                                    -- 實測:2077 家中 1719 家(82.8%)有缺季

\echo ''
\echo '=== [3] 重建 pass 判定 + 診斷(單次掃 growth_analysis_ttm) ==='
-- 逐位重現 view 的 pass 邏輯,並額外量測:入選家數 / 最近 20 列是否真的連續 20 季。
with base as (
  select market, company_code, year, quarter, cbs, (year*4+quarter) qidx
  from growth_analysis_ttm
),
anchor as (
  select distinct on (market, company_code) market, company_code, year, quarter,
     (cbs>75
       and lag(cbs,1)  over ww>75 and lag(cbs,2)  over ww>75 and lag(cbs,3)  over ww>75
       and lag(cbs,4)  over ww>75 and lag(cbs,5)  over ww>75 and lag(cbs,6)  over ww>75
       and lag(cbs,7)  over ww>75 and lag(cbs,8)  over ww>75 and lag(cbs,9)  over ww>75
       and lag(cbs,10) over ww>75 and lag(cbs,11) over ww>75 and lag(cbs,12) over ww>75
       and lag(cbs,13) over ww>75 and lag(cbs,14) over ww>75 and lag(cbs,15) over ww>75
       and lag(cbs,16) over ww>75 and lag(cbs,17) over ww>75 and lag(cbs,18) over ww>75
       and lag(cbs,19) over ww>75) as pass,
     (qidx - lag(qidx,19) over ww) as span19
  from base window ww as (partition by company_code order by year, quarter)
  order by market, company_code, year desc, quarter desc
)
select coalesce(pass::text,'null') pass,
       count(*) companies,
       count(*) filter (where span19 = 19) recent20_contiguous,
       count(*) filter (where span19 > 19) recent20_has_gap,       -- 假 5 年連續的機制
       count(*) filter (where span19 is null) fewer_than_20_rows
from anchor group by pass order by pass;
-- 實測:pass=true 35 家(全部 recent20 連續);false 2032(其中 149 家最近 20 列有缺季);null 10

\echo ''
\echo '=== [4] PIT 破口:入選股被錨定在「各自最新一季」,同一天卻是不同財季 ==='
with base as (select market, company_code, year, quarter, cbs from growth_analysis_ttm),
anchor as (
  select distinct on (market, company_code) market, company_code, year, quarter,
     (cbs>75 and lag(cbs,1) over ww>75 and lag(cbs,2) over ww>75 and lag(cbs,3) over ww>75
       and lag(cbs,4) over ww>75 and lag(cbs,5) over ww>75 and lag(cbs,6) over ww>75
       and lag(cbs,7) over ww>75 and lag(cbs,8) over ww>75 and lag(cbs,9) over ww>75
       and lag(cbs,10) over ww>75 and lag(cbs,11) over ww>75 and lag(cbs,12) over ww>75
       and lag(cbs,13) over ww>75 and lag(cbs,14) over ww>75 and lag(cbs,15) over ww>75
       and lag(cbs,16) over ww>75 and lag(cbs,17) over ww>75 and lag(cbs,18) over ww>75
       and lag(cbs,19) over ww>75) as pass
  from base window ww as (partition by company_code order by year, quarter)
  order by market, company_code, year desc, quarter desc)
select year latest_year, quarter latest_q, count(*) passers,
       string_agg(company_code,',' order by company_code) codes
from anchor where pass group by year, quarter order by year, quarter;
-- 實測(2026-07-23):20 家錨定 2025Q4、15 家錨定 2026Q1 → 同一次查詢的入選股被拿不同財季評比

\echo ''
\echo '=== [5] 演算法正確性正檢:2330 最近 22 季 cbs(應見最近 20 季全 > 75) ==='
select year, quarter, round(cbs::numeric,2) cbs, (cbs>75) gt75
from growth_analysis_ttm where company_code='2330' and market='twse'
order by year desc, quarter desc limit 22;
-- 實測:2025Q4~2021Q1 共 20 季 cbs 81~91 全 > 75 → 2330 正確入選(distinct-on/window 機制無誤)

\echo ''
\echo '=== [6] 上游 cbs 缺陷是否污染本 view 的當前輸出 ==='
-- (a) TTM 版 liquidity 子分是否被「百分比 vs 倍數」單位錯配鎖死(cbs_by_year 的 BUG)?
select liquidity, count(*) from financial_index_ttm
where year=2025 and quarter=4 group by liquidity order by liquidity;
-- 實測:0/20/30/40/50/60/80/100 均勻分布(未鎖死)=> TTM 版重算真實比率,無此 BUG(正面)

-- (b) NULL-roic 是否因 rank NULLS LAST 拿到最高 operating_performance(cbs_by_year BUG 2 同型)?
select (roic is null) roic_is_null, count(*) n,
       round(avg(operating_performance)::numeric,1) avg_op,
       round(max(operating_performance)::numeric,1) max_op
from financial_index_ttm where year=2025 and quarter=4 group by (roic is null);
-- 實測:30 家 NULL-roic 全拿 op=98.5(最高)=> 此 BUG 有傳進來,但…

-- (c) …35 家入選股是否真乾淨(非 NULL-roic 灌水、非金融扇出受害股)?抽 10 檔看成分。
select company_code, round(cbs::numeric,1) cbs,
       round(operating_performance::numeric,1) op, round(return_on_investment::numeric,1) roi,
       round(capital_structure::numeric,1) cap, liquidity liq, cash_flow cf,
       (roic is null) roic_null, round(roic::numeric,3) roic
from financial_index_ttm
where (year,quarter) in ((2025,4),(2026,1))
  and company_code in ('1264','2330','5274','8016','6231','1232','3008','6683','8464','3034')
order by company_code, year, quarter;
-- 實測:全部 roic_null=f、roic 0.11~0.50(真高獲利藍籌)=> 當前 35 名輸出乾淨、可信
