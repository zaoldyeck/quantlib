--3.5, 5, 7, 10, all
create view sheet_music_3y6m as
with price as (select market,
                      company_code,
                      company_name,
                      closing_price,
                      rank() over (partition by company_code order by date) as pos
               from daily_quote
               where (market = 'twse' or market = 'tpex')
                 and date > (current_date - interval '3' year - interval '6' month)),
     ratio as (select distinct on (company_code) market,
                                                 company_code,
                                                 company_name,
                                                 closing_price,
                                                 regr_slope(closing_price, pos) over (partition by company_code)     as slope,
                                                 regr_intercept(closing_price, pos) over (partition by company_code) as intercept,
                                                 stddev(closing_price) over (partition by company_code)              as sd,
                                                 pos
               from price
               where closing_price is not null
               order by company_code, pos desc)

select market,
       company_code,
       company_name,
       closing_price,
       slope * pos + intercept + sd * 2 as highest,
       slope * pos + intercept + sd     as high,
       slope * pos + intercept          as tl,
       slope * pos + intercept - sd     as low,
       slope * pos + intercept - sd * 2 as lowest,
       slope,
       intercept,
       sd
from ratio;