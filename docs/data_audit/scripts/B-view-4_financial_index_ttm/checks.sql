-- 稽核單位 B-view-4_financial_index_ttm 的可重現證據
-- 對象:src/main/resources/sql/view/4_financial_index_ttm.sql
-- 跑法:psql -h localhost -p 5432 -d quantlib -f docs/data_audit/scripts/B-view-4_financial_index_ttm/checks.sql
-- 每一節前面的註解 = 該節要證明的結論。所有數字取自 2026-07-22 的 quantlib DB。

\echo '=== [1] BUG total_assets_turnover 用 lag(total_assets,5) 當去年同期(roa 用 lag 4) ==='
-- 期望:view_value 與 turnover_lag5_asis 完全相符、與 turnover_lag4_correct 不符
with src as (select year, quarter, total_assets, total_operating_revenue
             from concise_financial_statement_with_titles where company_code = '2330' and market = 'twse'),
     w as (select year, quarter, total_assets,
                  sum(total_operating_revenue) over (order by year, quarter rows between 3 preceding and current row) ttm_rev,
                  lag(total_assets, 4) over (order by year, quarter) ta4,
                  lag(total_assets, 5) over (order by year, quarter) ta5,
                  lag(year, 4) over (order by year, quarter) y4, lag(quarter, 4) over (order by year, quarter) q4,
                  lag(year, 5) over (order by year, quarter) y5, lag(quarter, 5) over (order by year, quarter) q5
           from src)
select w.year, w.quarter, y4 || 'Q' || q4 lag4_period, y5 || 'Q' || q5 lag5_period,
       round((w.ttm_rev / ((w.total_assets + w.ta4) / 2))::numeric, 4) turnover_lag4_correct,
       round((w.ttm_rev / ((w.total_assets + w.ta5) / 2))::numeric, 4) turnover_lag5_asis,
       round(f.total_assets_turnover::numeric, 4)                      view_value
from w join financial_index_ttm f on f.company_code = '2330' and f.year = w.year and f.quarter = w.quarter
where w.year between 2022 and 2024 order by w.year, w.quarter;

\echo '=== [2] BUG roic/roa 為 NULL 的列被 rank(ASC NULLS LAST) 排到最後 -> 拿滿分 ==='
-- 期望:roic_is_null = t 的平均 operating_performance 遠高於 50
select roic is null roic_is_null, count(*) n,
       round(min(operating_performance)::numeric, 1) min_op,
       round(max(operating_performance)::numeric, 1) max_op,
       round(avg(operating_performance)::numeric, 1) avg_op
from financial_index_ttm group by 1;
select roa is null roa_is_null, count(*) n,
       round(avg(return_on_investment)::numeric, 1) avg_roi
from financial_index_ttm group by 1;

\echo '=== [3] BUG 應收款重複計算(毛額 + 淨額同時 sum) ==='
-- 1108 幸福 2019Q1:原始表同時有「應收帳款」與「應收帳款淨額」且值相同
select company_code, year, quarter, title, value from balance_sheet
where company_code = '1108' and year = 2019 and quarter = 1 and title like '%應收%' order by title;
select company_code, year, quarter, receivable view_receivable
from balance_sheet_with_titles where company_code = '1108' and year = 2019 and quarter = 1;
-- 全史精確盤點:毛額科目存在且其淨額對應科目同值 = 確定的重複
with pairs(g, n) as (values ('應收帳款', '應收帳款淨額'), ('應收帳款－關係人', '應收帳款－關係人淨額'),
                            ('應收票據', '應收票據淨額'), ('應收款項', '應收款項－淨額')),
     dup as (select bg.year, bg.quarter, bg.company_code, sum(bg.value) dup_amt
             from balance_sheet bg
                      join pairs p on bg.title = p.g
                      join balance_sheet bn on bn.year = bg.year and bn.quarter = bg.quarter
                          and bn.company_code = bg.company_code and bn.title = p.n and bn.value = bg.value
             group by 1, 2, 3)
select count(*) rows_affected, count(*) filter (where b.year >= 2018) rows_since_2018,
       count(distinct b.company_code) codes,
       round(avg(100.0 * d.dup_amt / nullif(b.receivable, 0))::numeric, 1) avg_pct_of_reported,
       round(max(100.0 * d.dup_amt / nullif(b.receivable, 0))::numeric, 1) max_pct_of_reported
from balance_sheet_with_titles b join dup d using (year, quarter, company_code) where d.dup_amt > 0;

\echo '=== [4] BUG TTM 視窗是「前 3 列」不是「前 3 季」;缺季靜靜算錯 ==='
-- 2881 富邦金:2023 缺 Q2、2024 缺 Q1/Q3
select year, quarter, round((value / 1e6)::numeric, 2) cum_bn from concise_income_statement_progressive
where company_code = '2881' and market = 'twse' and title = '本期稅後淨利（淨損）' and year between 2022 and 2024
order by year, quarter;
select year, quarter, round((profit / 1e6)::numeric, 2) view_ttm_profit_bn, round(eps::numeric, 2) view_ttm_eps
from financial_index_ttm where company_code = '2881' and year between 2022 and 2024 order by year, quarter;
-- 全表規模
with x as (select company_code, year, quarter,
                  lag(year, 3) over (partition by company_code order by year, quarter) py3,
                  lag(quarter, 3) over (partition by company_code order by year, quarter) pq3
           from (select distinct company_code, year, quarter from concise_financial_statement_with_titles) t)
select count(*) filter (where year >= 2018) rows_2018plus,
       count(*) filter (where year >= 2018 and (py3 is null or (year * 4 + quarter) - (py3 * 4 + pq3) <> 3)) window_broken_2018plus
from x;
-- 缺 Q1 的公司年(差分會減去前一年 Q4 累計)
with q as (select company_code, year, array_agg(distinct quarter order by quarter) qs
           from concise_income_statement_progressive where market in ('twse', 'tpex') group by 1, 2)
select count(*) filter (where not (1 = any (qs))) missing_q1_years,
       count(*) filter (where not (1 = any (qs)) and year >= 2018) missing_q1_since2018 from q;

\echo '=== [5] BUG 2006-2012 重複列 -> 同一支查詢跑三次三個答案 ==='
select case when year < 2006 then 'pre-2006' when year < 2013 then '2006-2012' else '2013+' end era,
       sum(n) rows_in_view, sum(d) distinct_codes, round(100.0 * (sum(n) - sum(d)) / sum(n), 2) pct_phantom
from (select year, quarter, count(*) n, count(distinct company_code) d from financial_index_ttm group by 1, 2) t
group by 1 order by 1;
select round(avg(cbs)::numeric, 6) avg_cbs_2010, round(avg(roic)::numeric, 6) avg_roic_2010 from financial_index_ttm where year = 2010;
select round(avg(cbs)::numeric, 6) avg_cbs_2010, round(avg(roic)::numeric, 6) avg_roic_2010 from financial_index_ttm where year = 2010;
select round(avg(cbs)::numeric, 6) avg_cbs_2010, round(avg(roic)::numeric, 6) avg_roic_2010 from financial_index_ttm where year = 2010;
select round(avg(cbs)::numeric, 6) avg_cbs_2019, round(avg(roic)::numeric, 6) avg_roic_2019 from financial_index_ttm where year = 2019;
select round(avg(cbs)::numeric, 6) avg_cbs_2019, round(avg(roic)::numeric, 6) avg_roic_2019 from financial_index_ttm where year = 2019;
-- 重複列來源:證券商同時申報多個同義標題,上游 CTE 只 OR 列舉沒有 distinct on
select market, year, quarter, company_code, count(*) c from concise_financial_statement_with_titles
group by 1, 2, 3, 4 having count(*) > 1 order by c desc limit 5;

\echo '=== [6] BUG 存貨/預付款缺料被 coalesce 成 0 -> 速動比率退化成流動比率 ==='
select case when year < 2009 then 'pre-2009' else '2009+' end era, count(*) n,
       count(*) filter (where abs(quick_ratio - current_ratio) < 1e-9) identical_to_current_ratio,
       round(avg(liquidity)::numeric, 1) avg_liquidity
from financial_index_ttm where quick_ratio is not null group by 1;

\echo '=== [7] BUG 2009 前 cash_flow 分項恆為 5 分(現金流/資產負債全表 2009 才有) ==='
select case when year < 2009 then 'pre-2009' when year < 2013 then '2009-2012' else '2013+' end era, count(*) n,
       round(100.0 * count(*) filter (where ocf is null) / count(*), 1)                    ocf_null_pct,
       round(100.0 * count(*) filter (where cash_ratio is null) / count(*), 1)             cash_ratio_null_pct,
       round(100.0 * count(*) filter (where days_sales_outstanding is null) / count(*), 1) dso_null_pct,
       round(100.0 * count(*) filter (where fcf_per_share is null) / count(*), 1)          fcf_null_pct,
       round(avg(cash_flow)::numeric, 1)                                                   avg_cash_flow_score
from financial_index_ttm group by 1 order by 1;

\echo '=== [9] SUSPECT 分母為負時 ROIC 反向(獲利公司變最差) ==='
select count(*) roic_neg_while_profit_pos, count(*) filter (where year >= 2018) since_2018
from financial_index_ttm where roic < 0 and profit > 0;

\echo '=== [10] SUSPECT fcf_per_share 分母是股本不是股數(差 10 倍);同表 eps 卻是正確元/股 ==='
select year, quarter, round((ocf / 1e6)::numeric, 1) ttm_ocf_bn,
       round((total_capital_stock / 10.0 / 1000)::numeric, 0) implied_shares_mn,
       round(fcf_per_share::numeric, 3) view_fcf_ps, round(eps::numeric, 2) ttm_eps
from financial_index_ttm where company_code = '2330' and year = 2024 and quarter = 4;
-- 以 EPS 反推面額:非 10 元面額的公司,縮放不是常數 -> 橫斷面排序也被扭曲
with x as (select total_capital_stock * 1000.0 / nullif(profit * 1000.0 / nullif(eps, 0), 0) implied_par
           from financial_index_ttm
           where year >= 2018 and eps is not null and abs(eps) > 1 and profit is not null and total_capital_stock is not null)
select count(*) n, count(*) filter (where implied_par between 8 and 12) near_par10,
       count(*) filter (where implied_par between 4 and 6) near_par5,
       count(*) filter (where implied_par between 0.5 and 2) near_par1,
       round(percentile_cont(0.5) within group (order by implied_par)::numeric, 2) median_par
from x where implied_par > 0 and implied_par < 100;

\echo '=== [11] SUSPECT 產業別取「最新一筆」套用全史 = 前視偏誤 ==='
select (select count(*) from (select company_code from operating_revenue where market in ('twse', 'tpex')
                              group by 1 having count(distinct industry) > 1) t)                     codes_with_multiple_industry,
       (select count(distinct company_code) from operating_revenue where market in ('twse', 'tpex')) total_codes,
       (select count(*) from financial_index_ttm where industry is null)                             null_industry_rows;

\echo '=== [13] SUSPECT 5 年視窗湊不滿 20 季 + capex 正值被丟掉 ==='
with s as (select row_number() over (partition by company_code order by year, quarter) rn
           from (select distinct company_code, year, quarter from concise_financial_statement_with_titles) t)
select count(*) filter (where rn < 20) rows_with_short_5y_window, count(*) total from s;
select count(*) filter (where value >= 0) capex_diff_nonneg_dropped, count(*) total
from cash_flows_individual where title in ('取得不動產、廠房及設備', '取得不動產及設備', '購置固定資產');

\echo '=== [14] REAL 金融股沒有毛利率、營業利益率 >1 是定義落差不是解析錯 ==='
select company_code, company_name, industry, round(gross_margin::numeric, 3) gm,
       round(operating_margin::numeric, 3) om, round(profit_margin::numeric, 3) pm
from financial_index_ttm where year = 2024 and quarter = 4
  and company_code in ('2881', '2882', '2891', '2412', '1101', '2330') order by company_code;

\echo '=== [15] OK 現代密集資料的四則運算正確(台積電外部錨) ==='
-- 官方對照:2024 營收 2.894 兆、EPS 45.25、稅後淨利 1.1733 兆、毛利率 56.1%、營益率 45.7%、淨利率 40.5%
with s as (select year, quarter, total_assets, total_current_liabilities, total_equity, profit
           from concise_financial_statement_with_titles where company_code = '2330' and market = 'twse'),
     w as (select *, sum(profit) over (order by year, quarter rows between 3 preceding and current row) ttm_profit,
                  lag(total_assets, 4) over (order by year, quarter) ta4 from s)
select w.year, w.quarter, round((w.ttm_profit / 1e6)::numeric, 1) ttm_profit_bn,
       round((w.ttm_profit / (w.total_assets - w.total_current_liabilities))::numeric, 4) hand_roic,
       round(f.roic::numeric, 4)                                                          view_roic,
       round((w.ttm_profit / ((w.total_assets + w.ta4) / 2))::numeric, 4)                  hand_roa,
       round(f.roa::numeric, 4)                                                           view_roa,
       round((w.total_assets / w.total_equity)::numeric, 3)                                hand_em,
       round(f.equity_multiplier::numeric, 3)                                             view_em,
       round(f.gross_margin::numeric, 3) gm, round(f.operating_margin::numeric, 3) om,
       round(f.profit_margin::numeric, 3) pm, round(f.eps::numeric, 2) ttm_eps,
       round((f.total_operating_revenue / 1e6)::numeric, 0) ttm_rev_bn
from w join financial_index_ttm f on f.company_code = '2330' and f.year = w.year and f.quarter = w.quarter
where w.year = 2024 order by w.quarter;

\echo '=== [16] OK  market=''tw'' 的 join 條件正確,不是筆誤 ==='
select 'balance_sheet_with_titles' t, market, count(*) from balance_sheet_with_titles group by 1, 2
union all select 'cash_flows_with_titles', market, count(*) from cash_flows_with_titles group by 1, 2
union all select 'concise_financial_statement_with_titles', market, count(*) from concise_financial_statement_with_titles group by 1, 2;

\echo '=== [17] OK net_operating_income 現代沒有被「繼續營業單位稅前淨利」污染 ==='
with cand as (select year, quarter, company_code,
                     max(value) filter (where title in ('營業利益（損失）', '營業利益', '營業淨利(淨損)', '營業利益(損失)')) op_val,
                     max(value) filter (where title in ('繼續營業單位稅前淨利（淨損）', '繼續營業單位稅前損益',
                                                        '繼續營業單位稅前淨利(淨損)', '繼續營業單位稅前合併淨利(淨損)')) pretax_val
              from concise_income_statement_individual group by 1, 2, 3)
select count(*) n, count(*) filter (where f.net_operating_income = c.op_val) picks_operating,
       count(*) filter (where f.net_operating_income = c.pretax_val and f.net_operating_income <> c.op_val) picks_pretax
from concise_financial_statement_with_titles f join cand c using (year, quarter, company_code)
where c.op_val is not null and c.pretax_val is not null and c.op_val <> c.pretax_val;

\echo '=== [18] OK 跨市場不會重複計算(window partition 沒有 market 也安全) ==='
select (select count(*) from (select company_code, year, quarter from concise_financial_statement_with_titles
                              group by 1, 2, 3 having count(distinct market) > 1) t) same_period_both_markets,
       (select count(*) from (select company_code from concise_financial_statement_with_titles
                              group by 1 having count(distinct market) > 1) t)       codes_that_migrated_market;

\echo '=== [19] SUSPECT 除以零未爆彈:行 91-92 兩處裸除法 ==='
select count(*) total_assets_zero_rows from concise_financial_statement_with_titles where total_assets = 0;
select null::float8 / 0 as null_div_zero_is_null;  -- PG 對 NULL/0 回 NULL,只有非 NULL 才 ERROR

\echo '=== [21] OK 爆炸半徑:pre-tax profit 污染只到 2005,不進回測窗 ==='
with p as (select year, quarter, company_code,
                  bool_or(title in ('本期稅後淨利（淨損）', '本期淨利（淨損）', '合併總損益', '本期損益', '本期淨利(淨損)')) has_after,
                  bool_or(title = '稅前純益') has_pretax
           from concise_income_statement_individual
           where title in ('本期稅後淨利（淨損）', '本期淨利（淨損）', '合併總損益', '本期損益', '本期淨利(淨損)', '稅前純益')
           group by 1, 2, 3)
select max(year) filter (where has_pretax and not has_after) last_year_with_pretax_only,
       count(*) filter (where has_pretax and not has_after)  pretax_only_periods,
       count(*)                                              total_periods
from p;
