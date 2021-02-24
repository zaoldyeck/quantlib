create materialized view cash_flows_with_titles as
with depreciation as (select year, quarter, company_code, value
                      from cash_flows_individual
                      where title = '折舊費用'
                        and value is not null),--64099
     increase_in_inventories as (select year, quarter, company_code, value
                                 from cash_flows_individual
                                 where (title = '存貨(增加)減少' or title = '存貨（增加）減少')
                                   and value < 0),--29997
     ocf as (select market, year, quarter, company_code, value
             from cash_flows_individual
             where (title = '營業活動之淨現金流入（流出）' or title = '營業活動之淨現金流入(流出)')
               and value is not null),--64133
     capital_expense as (select year, quarter, company_code, value
                         from cash_flows_individual
                         where (title = '取得不動產、廠房及設備' or title = '取得不動產及設備' or title = '購置固定資產')
                           and value < 0),--55260
     cash_dividends_paid as (select year, quarter, company_code, value
                             from cash_flows_individual
                             where (title = '分配現金股利' or title = '發放現金股利')
                               and value < 0)--7471

select market,
       ocf.year,
       ocf.quarter,
       ocf.company_code,
       depreciation.value            as depreciation,
       increase_in_inventories.value as increase_in_inventories,
       ocf.value                     as ocf,
       capital_expense.value         as capital_expense,
       cash_dividends_paid.value     as cash_dividends_paid
from ocf
         left join depreciation on ocf.year = depreciation.year
    and ocf.quarter = depreciation.quarter
    and ocf.company_code = depreciation.company_code
         left join increase_in_inventories on ocf.year = increase_in_inventories.year
    and ocf.quarter = increase_in_inventories.quarter
    and ocf.company_code = increase_in_inventories.company_code
         left join capital_expense on ocf.year = capital_expense.year
    and ocf.quarter = capital_expense.quarter
    and ocf.company_code = capital_expense.company_code
         left join cash_dividends_paid on ocf.year = cash_dividends_paid.year
    and ocf.quarter = cash_dividends_paid.quarter
    and ocf.company_code = cash_dividends_paid.company_code;--64133