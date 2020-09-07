create materialized view concise_income_statement_individual as
with "distinct" as (select distinct on (market, year, quarter, company_code, title) id,
                                                                                    market,
                                                                                    year,
                                                                                    quarter,
                                                                                    company_code,
                                                                                    company_name,
                                                                                    title,
                                                                                    value
                    from concise_income_statement_progressive
                    order by market, year, quarter, company_code, title, type),
     individual as (select id,
                           market,
                           year,
                           quarter,
                           company_code,
                           company_name,
                           title,
                           case
                               when quarter = 1 then value
                               else value - lag(value)
                                            over (partition by company_code, title order by year, quarter) end as value
                    from "distinct"
                    where market = 'twse'
                       or market = 'tpex')
select *
from individual
where value is not null;