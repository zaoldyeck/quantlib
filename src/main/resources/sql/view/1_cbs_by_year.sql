create view cbs_by_year as
with total_assets as (select year, quarter, company_code, value
                      from concise_balance_sheet_individual
                      where (market = 'twse' or market = 'tpex')
                        and quarter = 4
                        and (title = '資產合計'
                          or title = '資產總計'
                          or title = '資產總額'
                          or title = '資產合計')),
     total_current_liabilities as (select year, company_code, value
                                   from concise_balance_sheet_individual
                                   where (market = 'twse' or market = 'tpex')
                                     and quarter = 4
                                     and title = '流動負債'),
     cash as (select year, company_code, value
              from balance_sheet
              where market = 'tw'
                and quarter = 4
                and (title = '現金及約當現金' or title = '現金及約當現金合計' or title = '現金及約當現金總額')),
     profit as (select distinct on (year, company_code) year, company_code, value
                from concise_income_statement_progressive
                where (market = 'twse' or market = 'tpex')
                  and quarter = 4
                  and (title = '本期稅後淨利（淨損）'
                    or title = '本期淨利（淨損）'
                    or title = '合併總損益'
                    or title = '本期損益'
                    or title = '本期淨利(淨損)'
                    or title = '稅前純益')
                order by year, company_code, title),
     industry as (select distinct on (company_code) company_code, industry
                  from operating_revenue
                  where market = 'twse'
                     or market = 'tpex'
                  order by company_code, year desc, month desc),
     financial_analysis as (select financial_analysis.market,
                                   financial_analysis.year,
                                   financial_analysis.company_code,
                                   company_name,
                                   industry,
                                   profit.value /
                                   nullif(total_assets.value - total_current_liabilities.value, 0) as roic,
                                   "return_on_total_assets(%)",
                                   "liabilities/assets_ratio(%)",
                                   "current_ratio(%)",
                                   "quick_ratio(%)",
                                   cash.value / total_assets.value                                 as cash_ratio,
                                   "cash_flow_ratio(%)",
                                   "cash_flow_adequacy_ratio(%)",
                                   "cash_flow_reinvestment_ratio(%)",
                                   average_collection_days
                            from financial_analysis
                                     left join total_assets on financial_analysis.year = total_assets.year
                                and financial_analysis.company_code = total_assets.company_code
                                     left join total_current_liabilities
                                               on financial_analysis.year = total_current_liabilities.year
                                                   and
                                                  financial_analysis.company_code =
                                                  total_current_liabilities.company_code
                                     left join cash on financial_analysis.year = cash.year
                                and financial_analysis.company_code = cash.company_code
                                     left join profit on financial_analysis.year = profit.year
                                and financial_analysis.company_code = profit.company_code
                                     left join industry on financial_analysis.company_code = industry.company_code),
     rank as (select market,
                     year,
                     company_code,
                     company_name,
                     industry,
                     rank() over (partition by year order by roic)                                  as roic_rank,
                     rank() over (partition by year order by "return_on_total_assets(%)")           as roa_rank,
                     rank()
                     over (partition by year, industry order by "liabilities/assets_ratio(%)" desc) as "liabilities/assets_ratio(%)_rank",
                     count(*) over (partition by year)                                              as count_by_year,
                     count(*) over (partition by year, industry)                                    as count_by_year_industry,
                     "current_ratio(%)",
                     "quick_ratio(%)",
                     cash_ratio,
                     "cash_flow_ratio(%)",
                     "cash_flow_adequacy_ratio(%)",
                     "cash_flow_reinvestment_ratio(%)",
                     average_collection_days
              from financial_analysis),
     cbs as (select market,
                    year,
                    company_code,
                    company_name,
                    industry,
                    roic_rank / count_by_year::DOUBLE PRECISION * 100 as operating_performance,
                    roa_rank / count_by_year::DOUBLE PRECISION * 100  as return_on_investment,
                    "liabilities/assets_ratio(%)_rank" / count_by_year_industry::DOUBLE PRECISION *
                    100                                               as capital_structure,
                    case
                        when "current_ratio(%)" > 2.5 then 40
                        when "current_ratio(%)" > 1 then 20
                        when "current_ratio(%)" > 0 then 10
                        else 0 end +
                    case
                        when "quick_ratio(%)" > 1.5 then 60
                        when "quick_ratio(%)" > 1 then 40
                        when "quick_ratio(%)" > 0.5 then 20
                        when "quick_ratio(%)" > 0 then 10
                        else 0 end                                    as liquidity,
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
                        when "cash_flow_ratio(%)" > 1
                            and "cash_flow_adequacy_ratio(%)" > 1
                            and "cash_flow_reinvestment_ratio(%)" > 0.1
                            then 20
                        when "cash_flow_ratio(%)" > 0
                            and "cash_flow_adequacy_ratio(%)" > 0
                            and "cash_flow_reinvestment_ratio(%)" > 0
                            then 10
                        else 0
                        end +
                    case
                        when average_collection_days < 15 then 30
                        when average_collection_days < 30 then 25
                        when average_collection_days < 60 then 20
                        when average_collection_days < 90 then 16
                        when average_collection_days < 150 then 12
                        when average_collection_days < 180 then 8
                        else 5
                        end                                           as cash_flow
             from rank)

select market,
       year,
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
       cash_flow
from cbs;