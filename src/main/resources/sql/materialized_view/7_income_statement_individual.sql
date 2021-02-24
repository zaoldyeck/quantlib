create materialized view income_statement_individual as
select id,
       market,
       year,
       quarter,
       company_code,
       title,
       case
           when quarter = 1 then value
           else value - lag(value)
                        over (partition by market, company_code, title order by year, quarter) end as value
from income_statement_progressive;