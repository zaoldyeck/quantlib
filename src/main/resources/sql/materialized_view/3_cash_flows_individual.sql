create materialized view cash_flows_individual as
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
from cash_flows_progressive;