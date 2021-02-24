create materialized view concise_financial_statement_with_titles as
with total_assets as (select market, year, quarter, company_code, company_name, value
                      from concise_balance_sheet_individual
                      where title = '資產合計'
                         or title = '資產總計'
                         or title = '資產總額'
                         or title = '資產合計'),--117535
     total_current_assets as (select year, quarter, company_code, value
                              from concise_balance_sheet_individual
                              where title = '流動資產'),--115509
     total_liabilities as (select year, quarter, company_code, value
                           from concise_balance_sheet_individual
                           where title = '負債總計'
                              or title = '負債總額'),--116422
     total_current_liabilities as (select year, quarter, company_code, value
                                   from concise_balance_sheet_individual
                                   where title = '流動負債'),--115509
     total_non_current_liabilities as (select year, quarter, company_code, value
                                       from concise_balance_sheet_individual
                                       where title = '長期負債'
                                          or title = '非流動負債'),--114934
     total_equity as (select year, quarter, company_code, value
                      from concise_balance_sheet_individual
                      where title = '權益總額'
                         or title = '權益總計'
                         or title = '股東權益總計'
                         or title = '股東權益'
                         or title = '股東權益合計'),--117308
     total_retained_earnings as (select year, quarter, company_code, value
                                 from concise_balance_sheet_individual
                                 where title = '保留盈餘'
                                    or title = '保留盈餘（或累積虧損）'),--117248
     total_operating_revenue as (select market, year, quarter, company_code, company_name, value
                                 from concise_income_statement_individual
                                 where title = '營業收入'
                                    or title = '利息淨收益'
                                    or title = '收益'
                                    or title = '收入'
                                    or title = '營業收入淨額'),--115573
     total_operating_costs as (select year, quarter, company_code, value
                               from concise_income_statement_individual
                               where title = '營業成本'
                                  or title = '支出'
                                  or title = '營業支出'),--113467
     operating_expenses as (select year, quarter, company_code, value
                            from concise_income_statement_individual
                            where title = '營業費用'
                               or title = '費用'),--115212
     net_operating_income as (select distinct on (year,quarter,company_code) year, quarter, company_code, value
                              from concise_income_statement_individual
                              where title = '繼續營業單位稅前淨利（淨損）'
                                 or title = '營業利益（損失）'
                                 or title = '營業利益'
                                 or title = '繼續營業單位稅前損益'
                                 or title = '繼續營業單位稅前淨利(淨損)'
                                 or title = '繼續營業單位稅前合併淨利(淨損)'
                                 or title = '營業淨利(淨損)'
                                 or title = '營業利益(損失)'
                              order by year, quarter, company_code, title),--115061
     ebit as (select year, quarter, company_code, value
              from concise_income_statement_individual
              where title = '稅前純益'
                 or title = '稅前淨利（淨損）'
                 or title = '繼續營業單位稅前淨利'
                 or title = '繼續營業單位稅前損益'
                 or title = '繼續營業單位稅前淨利（淨損）'
                 or title = '繼續營業單位稅前淨利(淨損)'
                 or title = '繼續營業單位稅前純益（純損）'
                 or title = '繼續營業單位稅前純益(純損)'
                 or title = '繼續營業單位稅前合併淨利(淨損)'
                 or title = '繼續營業部門稅前淨利（淨損）'),--115591
     profit as (select distinct on (year, quarter, company_code) year, quarter, company_code, value
                from concise_income_statement_individual
                where title = '本期稅後淨利（淨損）'
                   or title = '本期淨利（淨損）'
                   or title = '合併總損益'
                   or title = '本期損益'
                   or title = '本期淨利(淨損)'
                   or title = '稅前純益'
                order by year, quarter, company_code, title),--115287
     eps as (select year, quarter, company_code, value
             from concise_income_statement_individual
             where title = '每股稅後盈餘(元)'
                or title = '基本每股盈餘（元）'
                or title = '基本每股盈餘'
                or title = '每股盈餘'
                or title = '每股稅後盈餘'
                or title = '基本每股盈餘(元)')--110259

select coalesce(total_assets.market, total_operating_revenue.market)             as market,
       coalesce(total_assets.year, total_operating_revenue.year)                 as year,
       coalesce(total_assets.quarter, total_operating_revenue.quarter)           as quarter,
       coalesce(total_assets.company_code, total_operating_revenue.company_code) as company_code,
       coalesce(total_assets.company_name, total_operating_revenue.company_name) as company_name,
       total_assets.value                                                        as total_assets,
       total_current_assets.value                                                as total_current_assets,
--     total_liabilities.value                                                   as total_liabilities,
       total_current_liabilities.value                                           as total_current_liabilities,
       total_non_current_liabilities.value                                       as total_non_current_liabilities,
       total_equity.value                                                        as total_equity,
       total_retained_earnings.value                                             as total_retained_earnings,
       total_operating_revenue.value                                             as total_operating_revenue,
       total_operating_costs.value                                               as total_operating_costs,
       operating_expenses.value                                                  as operating_expenses,
       net_operating_income.value                                                as net_operating_income,
       ebit.value                                                                as ebit,
       profit.value                                                              as profit,
       eps.value                                                                 as eps
from (total_assets left join total_current_assets on total_assets.year = total_current_assets.year
    and total_assets.quarter = total_current_assets.quarter
    and total_assets.company_code = total_current_assets.company_code
    --          full join total_liabilities on total_assets.year = total_liabilities.year
--     and total_assets.quarter = total_liabilities.quarter
--     and total_assets.company_code = total_liabilities.company_code
    left join total_current_liabilities on total_assets.year = total_current_liabilities.year
        and total_assets.quarter = total_current_liabilities.quarter
        and total_assets.company_code = total_current_liabilities.company_code
    left join total_non_current_liabilities on total_assets.year = total_non_current_liabilities.year
        and total_assets.quarter = total_non_current_liabilities.quarter
        and total_assets.company_code = total_non_current_liabilities.company_code
    left join total_equity on total_assets.year = total_equity.year
        and total_assets.quarter = total_equity.quarter
        and total_assets.company_code = total_equity.company_code
    left join total_retained_earnings on total_assets.year = total_retained_earnings.year
        and total_assets.quarter = total_retained_earnings.quarter
        and total_assets.company_code = total_retained_earnings.company_code)
         full join
     (total_operating_revenue left join total_operating_costs on
             total_operating_revenue.year = total_operating_costs.year
             and total_operating_revenue.quarter = total_operating_costs.quarter
             and total_operating_revenue.company_code = total_operating_costs.company_code
         left join operating_expenses on total_operating_revenue.year = operating_expenses.year
             and total_operating_revenue.quarter = operating_expenses.quarter
             and total_operating_revenue.company_code = operating_expenses.company_code
         left join net_operating_income on total_operating_revenue.year = net_operating_income.year
             and total_operating_revenue.quarter = net_operating_income.quarter
             and total_operating_revenue.company_code = net_operating_income.company_code
         left join ebit on total_operating_revenue.year = ebit.year
             and total_operating_revenue.quarter = ebit.quarter
             and total_operating_revenue.company_code = ebit.company_code
         left join profit on total_operating_revenue.year = profit.year
             and total_operating_revenue.quarter = profit.quarter
             and total_operating_revenue.company_code = profit.company_code
         left join eps on total_operating_revenue.year = eps.year
             and total_operating_revenue.quarter = eps.quarter
             and total_operating_revenue.company_code = eps.company_code)
     on total_assets.year = total_operating_revenue.year
         and total_assets.quarter = total_operating_revenue.quarter
         and total_assets.company_code = total_operating_revenue.company_code;--128369