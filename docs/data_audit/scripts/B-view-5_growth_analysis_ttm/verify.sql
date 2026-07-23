-- B-view-5_growth_analysis_ttm 稽核證據（全部可重跑）
-- 用法：psql -h localhost -p 5432 -d quantlib -f docs/data_audit/scripts/B-view-5_growth_analysis_ttm/verify.sql
-- 對象：src/main/resources/sql/view/5_growth_analysis_ttm.sql
--       （上游 src/main/resources/sql/view/4_financial_index_ttm.sql
--         與 src/main/resources/sql/materialized_view/6_concise_financial_statement_with_titles.sql）

\echo '=== [0] 基礎規模 ==='
select count(*) as rows_in_view from growth_analysis_ttm;
select min(f_score), max(f_score), round(avg(f_score)::numeric,3) avg_f,
       min(growth_score), max(growth_score), round(avg(growth_score)::numeric,3) avg_g,
       min(drop_score), max(drop_score), round(avg(drop_score)::numeric,3) avg_d
from growth_analysis_ttm;

\echo '=== [BUG1] 上游重複列規模（2/4/8/64 倍 fan-out）==='
with d as (select company_code, year, quarter, count(*) c
           from concise_financial_statement_with_titles
           where market in ('twse','tpex') group by 1,2,3)
select c as rows_per_company_quarter, count(*) as n_company_quarters, count(distinct company_code) as n_companies
from d group by 1 order by 1;

\echo '=== [BUG1] 重複列把百分位排名的分母灌水（2006-2012）==='
select year, quarter, count(*) as rank_denominator, count(distinct company_code) as real_companies,
       count(*) - count(distinct company_code) as phantom_rows
from financial_index_ttm where year between 2008 and 2013 group by 1,2 order by 1,2;

\echo '=== [BUG1] 2881 富邦金 2012Q4：真 TTM 淨利 vs view 的兩個互斥值 ==='
select year, quarter, count(*) n, min(profit) view_ttm_a, max(profit) view_ttm_b
from financial_index_ttm where company_code='2881' and year=2012 and quarter=4 group by 1,2;
select sum(profit) as true_ttm_profit
from (select profit from concise_financial_statement_with_titles
      where company_code='2881' and ((year=2012 and quarter between 1 and 4)) group by year, quarter, profit) t;

\echo '=== [BUG2] equity_multiplier 的 5y 基準錯用 total_assets_turnover（view 行 112-113 / 329-330）==='
with base as (
  select company_code, year, quarter, equity_multiplier,
         lag(equity_multiplier,20)      over w as em_lag20,
         lag(total_assets_turnover,20)  over w as tat_lag20
  from financial_index_ttm window w as (partition by company_code order by year, quarter))
select count(*) filter (where em_lag20 is not null and tat_lag20 is not null) as n_comparable,
       count(*) filter (where equity_multiplier < tat_lag20) as as_written_decline_true,
       count(*) filter (where equity_multiplier < em_lag20)  as correct_decline_true,
       count(*) filter (where equity_multiplier > tat_lag20) as as_written_increase_true,
       count(*) filter (where equity_multiplier > em_lag20)  as correct_increase_true,
       count(*) filter (where (equity_multiplier < tat_lag20) is distinct from (equity_multiplier < em_lag20)) as disagree
from base;
-- 量綱對照（為何幾乎恆真）
select round(percentile_cont(0.5) within group (order by equity_multiplier)::numeric,3)     as median_equity_multiplier,
       round(percentile_cont(0.5) within group (order by total_assets_turnover)::numeric,3) as median_asset_turnover
from financial_index_ttm where equity_multiplier between -100 and 100 and total_assets_turnover between -100 and 100;

\echo '=== [BUG2] 對 QualityFilter 的 drop_score < 10 閘門的實際影響（2023 年）==='
with g as (select company_code, year, quarter, drop_score from growth_analysis_ttm where year=2023),
     b as (select company_code, year, quarter, equity_multiplier,
                  lag(equity_multiplier,20) over w em20, lag(total_assets_turnover,20) over w tat20
           from financial_index_ttm window w as (partition by company_code order by year, quarter))
select count(*) n,
       count(*) filter (where g.drop_score >= 10) as rejected_as_written,
       count(*) filter (where (g.drop_score + coalesce((b.equity_multiplier>b.em20)::int,0)
                                            - coalesce((b.equity_multiplier>b.tat20)::int,0)) >= 10) as rejected_corrected
from g join b using (company_code, year, quarter);

\echo '=== [BUG3] revenue_growth_rate_increase_5y_overall 的括號錯位（view 行 38-46）==='
with b as (select company_code, year, quarter, total_operating_revenue r,
                  lag(total_operating_revenue,4)  over w r4,
                  lag(total_operating_revenue,20) over w r20,
                  lag(total_operating_revenue,24) over w r24
           from financial_index_ttm window w as (partition by company_code order by year, quarter)),
     g as (select *, r/nullif(r4,0)-1 g_now, r20/nullif(r24,0)-1 g_old from b)
select count(*) filter (where g_now is not null and g_old is not null) as n,
       count(*) filter (where g_now > (1.2*r20/nullif(r24,0) - 1)) as as_written_true,   -- = 1.2*g_old + 0.2
       count(*) filter (where g_now > 1.2*g_old)                    as intended_true,
       count(*) filter (where (g_now > (1.2*r20/nullif(r24,0)-1)) is distinct from (g_now > 1.2*g_old)) as disagree
from g;

\echo '=== [BUG4] 1.2x「進步」測試在基期為負時方向相反 ==='
with b as (select company_code, year, quarter, profit_margin, eps, roa, fcf_per_share, operating_margin,
                  lag(profit_margin,20)    over w pm20, lag(eps,20) over w eps20, lag(roa,20) over w roa20,
                  lag(fcf_per_share,20)    over w fcf20, lag(operating_margin,20) over w om20
           from financial_index_ttm window w as (partition by company_code order by year, quarter))
select count(*) filter (where pm20  < 0 and profit_margin    > 1.2*pm20  and profit_margin    < pm20)  as profit_margin_false_pass,
       count(*) filter (where om20  < 0 and operating_margin > 1.2*om20  and operating_margin < om20)  as operating_margin_false_pass,
       count(*) filter (where roa20 < 0 and roa              > 1.2*roa20 and roa              < roa20) as roa_false_pass,
       count(*) filter (where eps20 < 0 and eps              > 1.2*eps20 and eps              < eps20) as eps_false_pass,
       count(*) filter (where fcf20 < 0 and fcf_per_share    > 1.2*fcf20 and fcf_per_share    < fcf20) as fcf_false_pass
from b;

\echo '=== [BUG5] 7 個 *_growth_rate 欄位符號相反（1 - x/lag(x)）==='
select count(*) filter (where total_capital_stock_growth_rate < 0) as tcs_gr_negative,
       count(*) filter (where total_capital_stock_growth_rate > 0) as tcs_gr_positive
from growth_analysis_ttm;
-- 具體反例：股本增加 30% 卻拿到 -0.303
select * from (
  select company_code, year, quarter, total_capital_stock, lag(total_capital_stock) over w prev,
         1 - total_capital_stock/nullif(lag(total_capital_stock) over w,0) as col_named_growth_rate
  from financial_index_ttm window w as (partition by company_code order by year, quarter)) t
where company_code='1294' and year=2024 and quarter=2;

\echo '=== [BUG6] f_score 在 2010 年前結構性封頂 6 分（現金流缺料）==='
select year, count(*) n, max(f_score) max_f, round(avg(f_score)::numeric,2) avg_f,
       round(100.0*count(*) filter (where f_score>=5)/count(*),1) as pct_pass_f5
from growth_analysis_ttm where year between 2006 and 2013 group by 1 order by 1;
select year, round(100.0*count(*) filter (where ocf is null)/count(*),1) as pct_ocf_null,
       round(100.0*count(*) filter (where total_capital_stock is null)/count(*),1) as pct_capital_stock_null
from financial_index_ttm where year between 2006 and 2013 group by 1 order by 1;

\echo '=== [BUG7] lag(n) 是位移不是日曆對齊 ==='
with base as (
  select lag(year,20) over w y20, lag(quarter,20) over w q20,
         lag(year,4)  over w y4,  lag(quarter,4)  over w q4, year, quarter
  from financial_index_ttm window w as (partition by company_code order by year, quarter))
select count(*) filter (where y20 is not null) as n_lag20,
       round(100.0*count(*) filter (where y20 is not null and not (year-y20=5 and quarter=q20))
             /nullif(count(*) filter (where y20 is not null),0),2) as pct_lag20_not_5y,
       count(*) filter (where y4 is not null) as n_lag4,
       round(100.0*count(*) filter (where y4 is not null and not (year-y4=1 and quarter=q4))
             /nullif(count(*) filter (where y4 is not null),0),2) as pct_lag4_not_1y
from base;

\echo '=== [SUSPECT1] growth_score 內含 f_score（view 行 492）==='
select round(corr(f_score, growth_score)::numeric,4) as corr_fscore_growthscore,
       round(avg(growth_score - f_score)::numeric,3) as avg_growth_excluding_fscore
from growth_analysis_ttm;

\echo '=== [SUSPECT2] growth/drop 門檻不對稱 ==='
select round(100.0*avg(coalesce(roa_increase_5y_overall::int,0)),1) as pct_roa_increase_needs_plus20pct,
       round(100.0*avg(coalesce(roa_decline_5y_overall::int,0)),1)  as pct_roa_decline_needs_any_drop
from growth_analysis_ttm;

\echo '=== [SUSPECT3] f_score 用上一季而非去年同期 ==='
with f as (
  select (case when roa>0 then 1 else 0 end + case when ocf>0 then 1 else 0 end + case when ocf>profit then 1 else 0 end
        + case when total_non_current_liabilities < lag(total_non_current_liabilities) over w then 1 else 0 end
        + case when current_ratio > lag(current_ratio) over w then 1 else 0 end
        + case when total_capital_stock <= lag(total_capital_stock) over w then 1 else 0 end
        + case when roa > lag(roa) over w then 1 else 0 end
        + case when gross_margin > lag(gross_margin) over w then 1 else 0 end
        + case when total_assets_turnover > lag(total_assets_turnover) over w then 1 else 0 end) as f_qoq,
         (case when roa>0 then 1 else 0 end + case when ocf>0 then 1 else 0 end + case when ocf>profit then 1 else 0 end
        + case when total_non_current_liabilities < lag(total_non_current_liabilities,4) over w then 1 else 0 end
        + case when current_ratio > lag(current_ratio,4) over w then 1 else 0 end
        + case when total_capital_stock <= lag(total_capital_stock,4) over w then 1 else 0 end
        + case when roa > lag(roa,4) over w then 1 else 0 end
        + case when gross_margin > lag(gross_margin,4) over w then 1 else 0 end
        + case when total_assets_turnover > lag(total_assets_turnover,4) over w then 1 else 0 end) as f_yoy
  from financial_index_ttm window w as (partition by company_code order by year, quarter))
select count(*) n, round(100.0*count(*) filter (where f_qoq<>f_yoy)/count(*),1) as pct_score_differs,
       round(100.0*count(*) filter (where (f_qoq>=5) is distinct from (f_yoy>=5))/count(*),1) as pct_ge5_gate_flips
from f;

\echo '=== [SUSPECT4] 除以零未爆彈：4_financial_index_ttm.sql:91-92 沒有 nullif ==='
select c.market, c.company_code, c.year, c.quarter, c.total_assets, b.inventories, b.receivable
from concise_financial_statement_with_titles c
left join balance_sheet_with_titles b
       on b.market='tw' and b.year=c.year and b.quarter=c.quarter and b.company_code=c.company_code
where c.total_assets = 0;

\echo '=== [OK] 累計制差分正確：2330 單季 → TTM ==='
select year, quarter, total_operating_revenue as single_quarter_revenue, eps as single_quarter_eps
from concise_financial_statement_with_titles where company_code='2330' and year=2024 order by quarter;
select year, quarter, total_operating_revenue as ttm_revenue, eps as ttm_eps
from financial_index_ttm where company_code='2330' and year=2024 order by quarter;   -- 2024Q4 應為 2,894,307,699 千元 / EPS 45.25

\echo '=== [OK] 手算複核 f_score：2330 2024Q4 應為 7 ==='
select year, quarter, f_score, roa, ocf, profit, total_non_current_liabilities, current_ratio,
       total_capital_stock, gross_margin, total_assets_turnover
from growth_analysis_ttm where company_code='2330' and year=2024 and quarter=4;

\echo '=== [OK] 無 ±inf / NaN ==='
select count(*) filter (where roa_growth_rate in ('Infinity','-Infinity','NaN')) as roa_gr_nonfinite,
       count(*) filter (where abs(revenue_growth_rate) > 100) as revenue_gr_abs_gt_100
from growth_analysis_ttm;
