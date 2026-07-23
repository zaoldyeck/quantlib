create view valuation as
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
               where sppdy.market = 'twse'
                  or sppdy.market = 'tpex'),
     -- 五線譜通道 σ:中心線是 OLS 趨勢線,故 ±σ/±2σ 的 σ 必須是「價格對趨勢線的殘差
     -- 標準差」(STEYX/估計標準誤),不是收盤價原始標準差。恆等式 σ_resid²=σ_price²·(1−R²);
     -- 原本用 stddev(closing_price) 把趨勢本身的變異算進頻寬,系統性撐寬通道(倍數
     -- 1/√(1−R²),趨勢越乾淨撐越兇)。var_samp(close)·(1−regr_r2) 即殘差變異,與研究端
     -- fiveline_z_neg(對數殘差 z)同口徑;此處保留線性價,slope 輸出欄語意不變。
     linear_regression as (select *,
                                  regr_slope(closing_price, x) over w                        as slope,
                                  regr_intercept(closing_price, x) over w                    as intercept,
                                  sqrt(greatest(var_samp(closing_price) over w *
                                                (1 - regr_r2(closing_price, x) over w), 0))  as sd
                           from price
                               window w as (partition by company_code order by date range between interval '3 years 6 months' preceding and current row)),
     channel as (select *,
                        slope * x + intercept + sd * 2 as highest,
                        slope * x + intercept + sd     as high,
                        slope * x + intercept          as tl,
                        slope * x + intercept - sd     as low,
                        slope * x + intercept - sd * 2 as lowest
                 from linear_regression),
     growth_analysis as (select year,
                                quarter,
                                company_code,
                                industry,
                                cbs,
                                operating_performance,
                                return_on_investment,
                                capital_structure,
                                liquidity,
                                cash_flow,
                                f_score,
                                growth_score,
                                drop_score,
                                roic,
                                roa,
                                eps,
                                roic_growth_rate,
                                roa_growth_rate,
                                eps_growth_rate,
                                fcf_per_share_growth_rate,
                                revenue_growth_rate,
                                profit_growth_rate,
                                ocf_growth_rate,
                                total_assets_turnover_growth_rate,
                                days_sales_of_inventory_growth_rate,
                                days_sales_outstanding_growth_rate,
                                equity_multiplier_growth_rate,
                                current_ratio_growth_rate,
                                quick_ratio_growth_rate,
                                total_non_current_liabilities_growth_rate,
                                cash_ratio_growth_rate,
                                cash_flow_ratio_growth_rate,
                                cash_flow_adequacy_ratio_growth_rate,
                                cash_flow_reinvestment_ratio_growth_rate,
                                profit_margin_growth_rate,
                                operating_margin_growth_rate,
                                gross_margin_growth_rate,
                                inventories_ratio_growth_rate,
                                receivables_ratio_growth_rate,
                                total_capital_stock_growth_rate,
                                -- 1 年 EPS 成長率(YoY);盈餘 DCF 只對正盈餘端點有意義,eps≤0 或
                                -- 去年 eps≤0 一律 NULL(域外),避免除零與穿越零時的爆量成長率。
                                case
                                    when eps > 0
                                        and lag(eps, 4) over (partition by company_code order by year, quarter) > 0
                                        then eps / lag(eps, 4) over (partition by company_code order by year, quarter) - 1
                                    end as eps_growth_rate_1y
                         from growth_analysis_ttm),
     -- N 年成長率改用幾何 CAGR(端點對端點),取代原本「逐季重疊 YoY 的算術平均」。
     -- AM-GM:算術平均 ≥ 幾何,把算術平均當 g 餵 (1+g)^t 會系統性高估內在價值;原 10y
     -- 條另誤用 order by ... desc(非時序/前視)。g_Ny=(EPS_t/EPS_{t−4N})^(1/N)−1,lag 一律
     -- 時序遞增 order by year, quarter;端點盈餘須皆 >0 否則 NULL(域外,避免穿越零爆量)。
     eps_growth_average as (select *,
                                   case
                                       when eps > 0 and lag(eps, 12) over (partition by company_code order by year, quarter) > 0
                                           then power(eps / lag(eps, 12) over (partition by company_code order by year, quarter), 1.0 / 3) - 1
                                       end  as eps_growth_rate_3y,
                                   case
                                       when eps > 0 and lag(eps, 20) over (partition by company_code order by year, quarter) > 0
                                           then power(eps / lag(eps, 20) over (partition by company_code order by year, quarter), 1.0 / 5) - 1
                                       end  as eps_growth_rate_5y,
                                   case
                                       when eps > 0 and lag(eps, 40) over (partition by company_code order by year, quarter) > 0
                                           then power(eps / lag(eps, 40) over (partition by company_code order by year, quarter), 1.0 / 10) - 1
                                       end  as eps_growth_rate_10y,
                                   0.12 as discount_rate,
                                   0.04 as terminal_growth_rate,
                                   10   as years_of_growth_rate
                            from growth_analysis),
     dcf_parameters as (select *,
                               (1 + eps_growth_rate_1y) / nullif((1 + discount_rate), 0)  as x_1y,
                               (1 + eps_growth_rate_3y) / nullif((1 + discount_rate), 0)  as x_3y,
                               (1 + eps_growth_rate_5y) / nullif((1 + discount_rate), 0)  as x_5y,
                               (1 + eps_growth_rate_10y) / nullif((1 + discount_rate), 0) as x_10y
                        from eps_growth_average),
     -- 兩階段 DCF:第一段=EPS 以 g 成長 N 年的折現成長年金 eps·x·(1−x^N)/(1−x);
     -- 第二段終值改用 Gordon 永續 eps·x^N·(1+g_t)/(r−g_t)。原本第二段是「有限 10 年
     -- 成長年金」eps·x^N·y·(1−y^M)/(1−y)(第 20 年後歸零、只擷取永續約 52%),系統性
     -- 低估合理價。註:折現標的用會計 EPS 屬零售式代理(EPS 全額當可分配又假設靠再投資
     -- 成長,對成長股偏高估);嚴格 DCF 應折現 FCFE/股利,惟本檢視無派息/每股 FCF 可接。
     dcf as (select *,
                    eps * x_1y * (1 - x_1y ^ years_of_growth_rate) / nullif((1 - x_1y), 0) +
                    eps * x_1y ^ years_of_growth_rate * (1 + terminal_growth_rate) /
                    nullif((discount_rate - terminal_growth_rate), 0) as dcf_1y,
                    eps * x_3y * (1 - x_3y ^ years_of_growth_rate) / nullif((1 - x_3y), 0) +
                    eps * x_3y ^ years_of_growth_rate * (1 + terminal_growth_rate) /
                    nullif((discount_rate - terminal_growth_rate), 0) as dcf_3y,
                    eps * x_5y * (1 - x_5y ^ years_of_growth_rate) / nullif((1 - x_5y), 0) +
                    eps * x_5y ^ years_of_growth_rate * (1 + terminal_growth_rate) /
                    nullif((discount_rate - terminal_growth_rate), 0) as dcf_5y,
                    eps * x_10y * (1 - x_10y ^ years_of_growth_rate) / nullif((1 - x_10y), 0) +
                    eps * x_10y ^ years_of_growth_rate * (1 + terminal_growth_rate) /
                    nullif((discount_rate - terminal_growth_rate), 0) as dcf_10y
             from dcf_parameters)

select market,
       date,
       dcf.year,
       quarter,
       channel.company_code,
       company_name,
       industry,
       cbs,
       operating_performance,
       return_on_investment,
       capital_structure,
       liquidity,
       cash_flow,
       f_score,
       growth_score,
       drop_score,
       roic,
       roa,
       eps,
       roic_growth_rate,
       roa_growth_rate,
       eps_growth_rate,
       fcf_per_share_growth_rate,
       revenue_growth_rate,
       profit_growth_rate,
       ocf_growth_rate,
       total_assets_turnover_growth_rate,
       days_sales_of_inventory_growth_rate,
       days_sales_outstanding_growth_rate,
       equity_multiplier_growth_rate,
       current_ratio_growth_rate,
       quick_ratio_growth_rate,
       total_non_current_liabilities_growth_rate,
       cash_ratio_growth_rate,
       cash_flow_ratio_growth_rate,
       cash_flow_adequacy_ratio_growth_rate,
       cash_flow_reinvestment_ratio_growth_rate,
       profit_margin_growth_rate,
       operating_margin_growth_rate,
       gross_margin_growth_rate,
       inventories_ratio_growth_rate,
       receivables_ratio_growth_rate,
       total_capital_stock_growth_rate,
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