create view cbs_by_year_5y_over75 as
with pass as (select distinct on (market, company_code) market,
                                                        company_code,
                                                        cbs > 75 and
                                                        lag(cbs, 1) over (partition by company_code order by year) > 75 and
                                                        lag(cbs, 2) over (partition by company_code order by year) > 75 and
                                                        lag(cbs, 3) over (partition by company_code order by year) > 75 and
                                                        lag(cbs, 4) over (partition by company_code order by year) > 75 as pass
              from cbs_by_year
              order by market, company_code, year desc)

select cbs_by_year.market,
       year,
       cbs_by_year.company_code,
       company_name,
       industry,
       cbs,
       operating_performance,
       return_on_investment,
       capital_structure,
       liquidity,
       cash_flow
from cbs_by_year
         join pass on cbs_by_year.market = pass.market
    and cbs_by_year.company_code = pass.company_code
    and pass.pass is true
order by cbs_by_year.company_code, cbs_by_year.year desc;