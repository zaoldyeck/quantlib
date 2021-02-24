create view financial_index_quarterly as
with industry as (select distinct on (company_code) company_code, industry
                  from operating_revenue
                  where market = 'twse'
                     or market = 'tpex'
                  order by company_code, year desc, month desc),
     financial_index as (select cfswt.market,
                                cfswt.year,
                                cfswt.quarter,
                                cfswt.company_code,
                                company_name,
                                industry,
                                profit / nullif((total_assets - total_current_liabilities), 0) as roic,
                                profit / nullif((total_assets + lag(total_assets)
                                                                over (partition by cfswt.company_code order by cfswt.year, cfswt.quarter)) /
                                                2, 0)                                          as roa,
                                total_assets / nullif(total_equity, 0)                         as equity_multiplier,
                                total_current_assets / nullif(total_current_liabilities, 0)    as current_ratio,
                                (total_current_assets - coalesce(inventories, 0) - coalesce(prepaid_expenses, 0)) /
                                nullif(total_current_liabilities, 0)                           as quick_ratio,
                                cash / nullif(total_assets, 0)                                 as cash_ratio,
                                sum(ocf)
                                over (partition by cfswt.company_code order by cfswt.year, cfswt.quarter rows between 3 preceding and current row) /
                                nullif(total_current_liabilities, 0)                           as cash_flow_ratio,
                                sum(ocf)
                                over (partition by cfswt.company_code order by cfswt.year, cfswt.quarter rows between 19 preceding and current row) /
                                nullif(- (coalesce(sum(capital_expense)
                                                   over (partition by cfswt.company_code order by cfswt.year, cfswt.quarter rows between 19 preceding and current row),
                                                   0) +
                                          coalesce(sum(increase_in_inventories)
                                                   over (partition by cfswt.company_code order by cfswt.year, cfswt.quarter rows between 19 preceding and current row),
                                                   0) +
                                          coalesce(sum(cash_dividends_paid)
                                                   over (partition by cfswt.company_code order by cfswt.year, cfswt.quarter rows between 19 preceding and current row),
                                                   0)),
                                       0)                                                      as cash_flow_adequacy_ratio,
                                (sum(ocf)
                                 over (partition by cfswt.company_code order by cfswt.year, cfswt.quarter rows between 3 preceding and current row) +
                                 coalesce(sum(cash_dividends_paid)
                                          over (partition by cfswt.company_code order by cfswt.year, cfswt.quarter rows between 3 preceding and current row),
                                          0)) /
                                nullif((total_assets - total_current_liabilities), 0)          as cash_flow_reinvestment_ratio,
                                (receivable + lag(receivable)
                                              over (partition by cfswt.company_code order by cfswt.year, cfswt.quarter)) /
                                2 * 91.25 /
                                nullif(total_operating_revenue, 0)                             as days_sales_outstanding,
                                ocf,
                                profit,
                                total_non_current_liabilities,
                                total_capital_stock,
                                (total_operating_revenue - total_operating_costs) /
                                nullif(total_operating_revenue, 0)                             as gross_margin,
                                total_operating_revenue / nullif(((total_assets + lag(total_assets)
                                                                                  over (partition by cfswt.company_code order by cfswt.year, cfswt.quarter)) /
                                                                  2), 0)                       as total_assets_turnover,
                                total_operating_revenue,
                                profit / nullif(total_operating_revenue, 0)                    as profit_margin,
                                net_operating_income / nullif(total_operating_revenue, 0)      as operating_margin,
                                (inventories + lag(inventories)
                                               over (partition by cfswt.company_code order by cfswt.year, cfswt.quarter)) /
                                2 * 91.25 /
                                nullif(total_operating_costs, 0)                               as days_sales_of_inventory,
                                inventories / total_assets                                     as inventories_ratio,
                                receivable / total_assets                                      as receivables_ratio,
                                eps,
                                (ocf + capital_expense) / nullif(total_capital_stock, 0)       as fcf_par_share
                         from concise_financial_statement_with_titles cfswt
                                  left join balance_sheet_with_titles on balance_sheet_with_titles.market = 'tw'
                             and cfswt.year = balance_sheet_with_titles.year
                             and cfswt.quarter = balance_sheet_with_titles.quarter
                             and cfswt.company_code = balance_sheet_with_titles.company_code
                                  left join cash_flows_with_titles on cash_flows_with_titles.market = 'tw'
                             and cfswt.year = cash_flows_with_titles.year
                             and cfswt.quarter = cash_flows_with_titles.quarter
                             and cfswt.company_code = cash_flows_with_titles.company_code
                                  left join industry on cfswt.company_code = industry.company_code
                         where cfswt.market = 'twse'
                            or cfswt.market = 'tpex'),
     rank as (select *,
                     rank() over (partition by year, quarter order by roic)                      as roic_rank,
                     rank() over (partition by year, quarter order by roa)                       as roa_rank,
                     rank()
                     over (partition by year, quarter, industry order by equity_multiplier desc) as equity_multiplier_rank,
                     count(*) over (partition by year, quarter)                                  as count_by_year,
                     count(*) over (partition by year, quarter, industry)                        as count_by_year_industry
              from financial_index),
     cbs as (select *,
                    roic_rank / count_by_year::DOUBLE PRECISION * 100                       as operating_performance,
                    roa_rank / count_by_year::DOUBLE PRECISION * 100                        as return_on_investment,
                    equity_multiplier_rank / count_by_year_industry::DOUBLE PRECISION * 100 as capital_structure,
                    case
                        when current_ratio > 2.5 then 40
                        when current_ratio > 1 then 20
                        when current_ratio > 0 then 10
                        else 0 end +
                    case
                        when quick_ratio > 1.5 then 60
                        when quick_ratio > 1 then 40
                        when quick_ratio > 0.5 then 20
                        when quick_ratio > 0 then 10
                        else 0 end                                                          as liquidity,
                    case
                        when cash_ratio > 0.25 then 50
                        when cash_ratio > 0.2 then 40
                        when cash_ratio > 0.15 then 30
                        when cash_ratio > 0.1 then 20
                        when cash_ratio > 0.05 then 10
                        when cash_ratio > 0 then 5
                        else 0
                        end +
                    case
                        when cash_flow_ratio > 1
                            and cash_flow_adequacy_ratio > 1
                            and cash_flow_reinvestment_ratio > 0.1
                            then 20
                        when cash_flow_ratio > 0
                            and cash_flow_adequacy_ratio > 0
                            and cash_flow_reinvestment_ratio > 0
                            then 10
                        else 0
                        end +
                    case
                        when days_sales_outstanding < 15 then 30
                        when days_sales_outstanding < 30 then 25
                        when days_sales_outstanding < 60 then 20
                        when days_sales_outstanding < 90 then 16
                        when days_sales_outstanding < 150 then 12
                        when days_sales_outstanding < 180 then 8
                        else 5
                        end                                                                 as cash_flow
             from rank)

select market,
       year,
       quarter,
       company_code,
       company_name,
       industry,
       operating_performance * 0.25 +
       return_on_investment * 0.25 +
       capital_structure * 0.1 +
       liquidity * 0.1 +
       cash_flow * 0.3 as cbs,
       operating_performance,
       return_on_investment,
       capital_structure,
       liquidity,
       cash_flow,
       roic,
       roa,
       equity_multiplier,
       current_ratio,
       quick_ratio,
       cash_ratio,
       cash_flow_ratio,
       cash_flow_adequacy_ratio,
       cash_flow_reinvestment_ratio,
       days_sales_outstanding,
       ocf,
       profit,
       total_non_current_liabilities,
       total_capital_stock,
       gross_margin,
       total_assets_turnover,
       total_operating_revenue,
       profit_margin,
       operating_margin,
       days_sales_of_inventory,
       inventories_ratio,
       receivables_ratio,
       eps,
       fcf_par_share
from cbs;