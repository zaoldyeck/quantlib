create materialized view balance_sheet_with_titles as
with cash as (select year, quarter, company_code, value
              from balance_sheet
              where (title = '現金及約當現金' or title = '現金及約當現金合計' or title = '現金及約當現金總額')
                and value is not null),--66577
     receivable as (select market, year, quarter, company_code, sum(value) as value
                    from balance_sheet
                    where (title = '應收款項淨額' or title = '應收帳款淨額合計' or title = '應收帳款淨額' or title = '應收帳款－關係人淨額' or
                           title = '其他應收款－關係人' or title = '應收款項－淨額' or title = '應收票據淨額' or title = '其他應收款' or
                           title = '應收款項' or title = '應收票據' or title = '應收期貨交易保證金' or
                           title = '備抵損失－應收借貸款項－不限用途' or title = '應收帳款' or title = '備抵損失－應收帳款' or
                           title = '應收帳款－關係人' or title = '備抵損失－其他應收款' or title = '其他應收款－關係人' or
                           title = '應收建造合約款' or title = '應收票據－關係人淨額' or title = '應收建造合約款－關係人' or
                           title = '應收票據合計' or title = '應收帳款合計' or title = '其他應收款合計' or title = '其他應收款－關係人合計' or
                           title = '應收證券融資款' or title = '應收轉融通擔保價款' or title = '應收證券借貸款項合計' or
                           title = '應收票據淨額合計' or title = '其他應收款-關係人合計' or title = '應收帳款-關係人合計' or
                           title = '應收款項合計')
                      and value is not null
                    group by market, year, quarter, company_code),--66221
     inventories as (select year, quarter, company_code, value
                     from balance_sheet
                     where (title = '存貨合計' or title = '存貨')--58224
                       and value is not null),
     prepaid_expenses as (select year, quarter, company_code, sum(value) as value
                          from balance_sheet
                          where (title = '預付款項合計' or title = '預付款項' or title = '預付費用合計' or title = '預付費用' or
                                 title = '預付費用及其他預付款合計')
                            and value is not null
                          group by year, quarter, company_code),--40373
     property_plant_and_equipment as (select distinct on (year,quarter,company_code) year,
                                                                                     quarter,
                                                                                     company_code,
                                                                                     value
                                      from balance_sheet
                                      where (title = '不動產、廠房及設備'
                                          or title = '不動產、廠房及設備合計'
                                          or title = '不動產、廠房及設備淨額'
                                          or title = '不動產及設備'
                                          or title = '不動產及設備合計'
                                          or title = '不動產及設備淨額')
                                        and value is not null
                                        and value <> 0
                                      order by year, quarter, company_code, title),--47612
     total_capital_stock as (select market, year, quarter, company_code, value
                             from balance_sheet
                             where (title = '股本淨額' or title = '股本合計')
                               and value is not null)--66590

select coalesce(total_capital_stock.market, receivable.market)             as market,
       coalesce(total_capital_stock.year, receivable.year)                 as year,
       coalesce(total_capital_stock.quarter, receivable.quarter)           as quarter,
       coalesce(total_capital_stock.company_code, receivable.company_code) as company_code,
       cash.value                                                          as cash,
       receivable.value                                                    as receivable,
       inventories.value                                                   as inventories,
       prepaid_expenses.value                                              as prepaid_expenses,
       property_plant_and_equipment.value                                  as property_plant_and_equipment,
       total_capital_stock.value                                           as total_capital_stock
from total_capital_stock
         left join cash on total_capital_stock.year = cash.year
    and total_capital_stock.quarter = cash.quarter
    and total_capital_stock.company_code = cash.company_code
         full join receivable on total_capital_stock.year = receivable.year
    and total_capital_stock.quarter = receivable.quarter
    and total_capital_stock.company_code = receivable.company_code
         left join inventories on total_capital_stock.year = inventories.year
    and total_capital_stock.quarter = inventories.quarter
    and total_capital_stock.company_code = inventories.company_code
         left join prepaid_expenses on total_capital_stock.year = prepaid_expenses.year
    and total_capital_stock.quarter = prepaid_expenses.quarter
    and total_capital_stock.company_code = prepaid_expenses.company_code
         left join property_plant_and_equipment on total_capital_stock.year = property_plant_and_equipment.year
    and total_capital_stock.quarter = property_plant_and_equipment.quarter
    and total_capital_stock.company_code = property_plant_and_equipment.company_code;--66591