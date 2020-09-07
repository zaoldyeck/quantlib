create view valuation_1q as
with price as (select sppdy.market,
                      sppdy.date,
                      date_part('year', sppdy.date)                                                                                                  as year,
                      date_part('month', sppdy.date)                                                                                                 as month,
                      sppdy.company_code,
                      sppdy.company_name,
                      closing_price,
                      max(closing_price)
                      over (partition by sppdy.company_code order by sppdy.date range between interval '1' year preceding and current row)           as past_year_highest_price,
                      min(closing_price)
                      over (partition by sppdy.company_code order by sppdy.date range between interval '1' year preceding and current row)           as past_year_lowest_price,
                      price_to_earning_ratio,
                      max(price_to_earning_ratio)
                      over (partition by sppdy.company_code order by sppdy.date range between interval '3 years 6 months' preceding and current row) as "past_3.5y_highest_per",
                      min(price_to_earning_ratio)
                      over (partition by sppdy.company_code order by sppdy.date range between interval '3 years 6 months' preceding and current row) as "past_3.5y_lowest_per",
                      price_book_ratio,
                      max(price_book_ratio)
                      over (partition by sppdy.company_code order by sppdy.date range between interval '3 years 6 months' preceding and current row) as "past_3.5y_highest_pbr",
                      min(price_book_ratio)
                      over (partition by sppdy.company_code order by sppdy.date range between interval '3 years 6 months' preceding and current row) as "past_3.5y_lowest_pbr",
                      dividend_yield,
                      max(dividend_yield)
                      over (partition by sppdy.company_code order by sppdy.date range between interval '3 years 6 months' preceding and current row) as "past_3.5y_highest_dividend_yield",
                      min(dividend_yield)
                      over (partition by sppdy.company_code order by sppdy.date range between interval '3 years 6 months' preceding and current row) as "past_3.5y_lowest_dividend_yield",
                      rank() over (partition by sppdy.company_code order by sppdy.date)                                                              as x
               from stock_per_pbr_dividend_yield sppdy
                        left join daily_quote dq
                                  on (dq.market = 'twse' or dq.market = 'tpex')
                                      and sppdy.date = dq.date
                                      and sppdy.company_code = dq.company_code
               where (sppdy.market = 'twse'
                   or sppdy.market = 'tpex')
                 and sppdy.date >= (current_date - interval '3' year - interval '7' month)),
     linear_regression as (select *,
                                  regr_slope(closing_price, x)
                                  over (partition by company_code order by date range between interval '3 years 6 months' preceding and current row) as slope,
                                  regr_intercept(closing_price, x)
                                  over (partition by company_code order by date range between interval '3 years 6 months' preceding and current row) as intercept,
                                  stddev(closing_price)
                                  over (partition by company_code order by date range between interval '3 years 6 months' preceding and current row) as sd
                           from price),
     channel as (select *,
                        slope * x + intercept + sd * 2 as highest,
                        slope * x + intercept + sd     as high,
                        slope * x + intercept          as tl,
                        slope * x + intercept - sd     as low,
                        slope * x + intercept - sd * 2 as lowest
                 from linear_regression
                 where date >= (current_date - interval '3' month)),
     eps_growth_rate as (select year,
                                quarter,
                                company_code,
                                industry,
                                cbs,
                                f_score,
                                growth_score,
                                drop_score,
                                roic,
                                roa,
                                revenue_growth_rate,
                                eps,
                                fcf_per_share,
                                days_sales_of_inventory,
                                days_sales_outstanding,
                                operating_performance,
                                return_on_investment,
                                capital_structure,
                                liquidity,
                                cash_flow,
                                eps / nullif(lag(eps, 4) over (partition by company_code order by year, quarter), 0) -
                                1 as eps_growth_rate
                         from growth_analysis_ttm),
     eps_growth_average as (select *,
                                   sum(eps_growth_rate)
                                   over (partition by company_code order by year, quarter rows between 11 preceding and current row) /
                                   12   as eps_growth_rate_3y,
                                   sum(eps_growth_rate)
                                   over (partition by company_code order by year, quarter rows between 19 preceding and current row) /
                                   20   as eps_growth_rate_5y,
                                   sum(eps_growth_rate)
                                   over (partition by company_code order by year, quarter desc rows between 39 preceding and current row) /
                                   40   as eps_growth_rate_10y,
                                   0.12 as discount_rate,
                                   0.04 as terminal_growth_rate,
                                   10   as years_of_growth_rate,
                                   10   as years_of_terminal_growth
                            from eps_growth_rate),
     dcf_parameters as (select *,
                               (1 + eps_growth_rate) / nullif((1 + discount_rate), 0)      as x_1y,
                               (1 + eps_growth_rate_3y) / nullif((1 + discount_rate), 0)   as x_3y,
                               (1 + eps_growth_rate_5y) / nullif((1 + discount_rate), 0)   as x_5y,
                               (1 + eps_growth_rate_10y) / nullif((1 + discount_rate), 0)  as x_10y,
                               (1 + terminal_growth_rate) / nullif((1 + discount_rate), 0) as y
                        from eps_growth_average
                        where year >= date_part('year', (current_date - interval '1' year))),
     dcf as (select *,
                    eps * x_1y * (1 - x_1y ^ years_of_growth_rate) / nullif((1 - x_1y), 0) +
                    eps * x_1y ^ years_of_growth_rate * y * (1 - y ^ years_of_terminal_growth) /
                    nullif((1 - y), 0) as dcf_1y,
                    eps * x_3y * (1 - x_3y ^ years_of_growth_rate) / nullif((1 - x_3y), 0) +
                    eps * x_3y ^ years_of_growth_rate * y * (1 - y ^ years_of_terminal_growth) /
                    nullif((1 - y), 0) as dcf_3y,
                    eps * x_5y * (1 - x_5y ^ years_of_growth_rate) / nullif((1 - x_5y), 0) +
                    eps * x_5y ^ years_of_growth_rate * y * (1 - y ^ years_of_terminal_growth) /
                    nullif((1 - y), 0) as dcf_5y,
                    eps * x_10y * (1 - x_10y ^ years_of_growth_rate) / nullif((1 - x_10y), 0) +
                    eps * x_10y ^ years_of_growth_rate * y * (1 - y ^ years_of_terminal_growth) /
                    nullif((1 - y), 0) as dcf_10y
             from dcf_parameters)

select market,
       date,
       dcf.year,
       quarter,
       channel.company_code,
       company_name,
       industry,
       cbs,
       f_score,
       growth_score,
       drop_score,
       roic,
       roa,
       revenue_growth_rate,
       eps_growth_rate,
       eps_growth_rate_3y,
       eps_growth_rate_5y,
       eps_growth_rate_10y,
       eps,
       fcf_per_share,
       days_sales_of_inventory,
       days_sales_outstanding,
       operating_performance,
       return_on_investment,
       capital_structure,
       liquidity,
       cash_flow,
       closing_price,
       slope,
       case
           when closing_price >= highest then -2
           when closing_price >= high then -1
           when closing_price <= low then 1
           when closing_price <= lowest then 2
           else 0 end                                                                      as evaluation,
       (tl - closing_price) / nullif(closing_price, 0)                                     as price_err,
       ("past_3.5y_highest_per" + "past_3.5y_lowest_per" - 2 * price_to_earning_ratio) /
       nullif(("past_3.5y_highest_per" - "past_3.5y_lowest_per"), 0)                       as per_err,
       ("past_3.5y_highest_pbr" + "past_3.5y_lowest_pbr" - 2 * price_book_ratio) /
       nullif(("past_3.5y_highest_pbr" - "past_3.5y_lowest_pbr"), 0)                       as pbr_err,
       (2 * dividend_yield - "past_3.5y_highest_dividend_yield" - "past_3.5y_lowest_dividend_yield") /
       nullif(("past_3.5y_highest_dividend_yield" - "past_3.5y_lowest_dividend_yield"), 0) as dividend_yield_err,
       (dcf_10y - closing_price) / nullif(closing_price, 0)                                as dcf_10y_err,
       (dcf_5y - closing_price) / nullif(closing_price, 0)                                 as dcf_5y_err,
       (dcf_3y - closing_price) / nullif(closing_price, 0)                                 as dcf_3y_err,
       (dcf_1y - closing_price) / nullif(closing_price, 0)                                 as dcf_1y_err,
       past_year_highest_price,
       past_year_lowest_price,
       dcf_10y,
       dcf_5y,
       dcf_3y,
       dcf_1y,
       highest,
       high,
       tl,
       low,
       lowest,
       price_to_earning_ratio,
       "past_3.5y_highest_per",
       "past_3.5y_lowest_per",
       price_book_ratio,
       "past_3.5y_highest_pbr",
       "past_3.5y_lowest_pbr",
       dividend_yield,
       "past_3.5y_highest_dividend_yield",
       "past_3.5y_lowest_dividend_yield"
from channel
         left join dcf on channel.company_code = dcf.company_code
    and case
            when month < 4 then channel.year = dcf.year + 1 and quarter = 3
            when month < 6 then channel.year = dcf.year + 1 and quarter = 4
            when month < 9 then channel.year = dcf.year and quarter = 1
            when month < 12 then channel.year = dcf.year and quarter = 2
            else channel.year = dcf.year and quarter = 3 end;