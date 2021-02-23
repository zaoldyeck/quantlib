create view growth_analysis_ttm as
with index as (select *,
                      case when roa > 0 then 1 else 0 end +
                      case when ocf > 0 then 1 else 0 end +
                      case when ocf > profit then 1 else 0 end +
                      case
                          when total_non_current_liabilities <
                               lag(total_non_current_liabilities)
                               over (partition by company_code order by year, quarter)
                              then 1
                          else 0 end +
                      case
                          when current_ratio >
                               lag(current_ratio) over (partition by company_code order by year, quarter)
                              then 1
                          else 0 end +
                      case
                          when total_capital_stock <=
                               lag(total_capital_stock)
                               over (partition by company_code order by year, quarter)
                              then 1
                          else 0 end +
                      case
                          when roa > lag(roa) over (partition by company_code order by year, quarter)
                              then 1
                          else 0 end +
                      case
                          when gross_margin >
                               lag(gross_margin) over (partition by company_code order by year, quarter)
                              then 1
                          else 0 end +
                      case
                          when total_assets_turnover >
                               lag(total_assets_turnover)
                               over (partition by company_code order by year, quarter)
                              then 1
                          else 0 end                                                                    as f_score,
                      (total_operating_revenue /
                       nullif(lag(total_operating_revenue, 4)
                              over (partition by company_code order by year, quarter), 0) -
                       1) >
                      (1.2 * lag(total_operating_revenue, 20)
                             over (partition by company_code order by year, quarter) /
                       nullif(lag(total_operating_revenue, 24)
                              over (partition by company_code order by year, quarter), 0) -
                       1)                                                                               as revenue_growth_rate_increase_5y_overall,
                      total_operating_revenue >
                      lag(total_operating_revenue, 4)
                      over (partition by company_code order by year, quarter) and
                      lag(total_operating_revenue, 4)
                      over (partition by company_code order by year, quarter) >
                      lag(total_operating_revenue, 8)
                      over (partition by company_code order by year, quarter) and
                      lag(total_operating_revenue, 8)
                      over (partition by company_code order by year, quarter) >
                      lag(total_operating_revenue, 12)
                      over (partition by company_code order by year, quarter) and
                      lag(total_operating_revenue, 12)
                      over (partition by company_code order by year, quarter) >
                      lag(total_operating_revenue, 16)
                      over (partition by company_code order by year, quarter) and
                      lag(total_operating_revenue, 16)
                      over (partition by company_code order by year, quarter) >
                      lag(total_operating_revenue, 20)
                      over (partition by company_code order by year, quarter)                           as revenue_growth_rate_increase_5y_continuous,
                      profit_margin > 1.2 * lag(profit_margin, 20)
                                            over (partition by company_code order by year, quarter)     as profit_margin_increase_5y_overall,
                      profit_margin >
                      lag(profit_margin, 4) over (partition by company_code order by year, quarter) and
                      lag(profit_margin, 4) over (partition by company_code order by year, quarter) >
                      lag(profit_margin, 8) over (partition by company_code order by year, quarter) and
                      lag(profit_margin, 8) over (partition by company_code order by year, quarter) >
                      lag(profit_margin, 12) over (partition by company_code order by year, quarter) and
                      lag(profit_margin, 12) over (partition by company_code order by year, quarter) >
                      lag(profit_margin, 16) over (partition by company_code order by year, quarter) and
                      lag(profit_margin, 16) over (partition by company_code order by year, quarter) >
                      lag(profit_margin, 20)
                      over (partition by company_code order by year, quarter)                           as profit_margin_increase_5y_continuous,
                      operating_margin > 1.2 * lag(operating_margin, 20)
                                               over (partition by company_code order by year, quarter)  as operating_margin_increase_5y_overall,
                      operating_margin >
                      lag(operating_margin, 4) over (partition by company_code order by year, quarter) and
                      lag(operating_margin, 4) over (partition by company_code order by year, quarter) >
                      lag(operating_margin, 8) over (partition by company_code order by year, quarter) and
                      lag(operating_margin, 8) over (partition by company_code order by year, quarter) >
                      lag(operating_margin, 12) over (partition by company_code order by year, quarter) and
                      lag(operating_margin, 12) over (partition by company_code order by year, quarter) >
                      lag(operating_margin, 16) over (partition by company_code order by year, quarter) and
                      lag(operating_margin, 16) over (partition by company_code order by year, quarter) >
                      lag(operating_margin, 20)
                      over (partition by company_code order by year, quarter)                           as operating_margin_increase_5y_continuous,
                      total_assets_turnover >
                      1.2 * lag(total_assets_turnover, 20)
                            over (partition by company_code order by year, quarter)                     as total_assets_turnover_increase_5y_overall,
                      total_assets_turnover >
                      lag(total_assets_turnover, 4)
                      over (partition by company_code order by year, quarter) and
                      lag(total_assets_turnover, 4) over (partition by company_code order by year, quarter) >
                      lag(total_assets_turnover, 8)
                      over (partition by company_code order by year, quarter) and
                      lag(total_assets_turnover, 8) over (partition by company_code order by year, quarter) >
                      lag(total_assets_turnover, 12)
                      over (partition by company_code order by year, quarter) and
                      lag(total_assets_turnover, 12)
                      over (partition by company_code order by year, quarter) >
                      lag(total_assets_turnover, 16)
                      over (partition by company_code order by year, quarter) and
                      lag(total_assets_turnover, 16)
                      over (partition by company_code order by year, quarter) >
                      lag(total_assets_turnover, 20)
                      over (partition by company_code order by year, quarter)                           as total_assets_turnover_increase_5y_continuous,
                      equity_multiplier < lag(total_assets_turnover, 20)
                                          over (partition by company_code order by year, quarter)       as equity_multiplier_decline_5y_overall,
                      equity_multiplier <
                      lag(equity_multiplier, 4) over (partition by company_code order by year, quarter) and
                      lag(equity_multiplier, 4) over (partition by company_code order by year, quarter) <
                      lag(equity_multiplier, 8) over (partition by company_code order by year, quarter) and
                      lag(equity_multiplier, 8) over (partition by company_code order by year, quarter) <
                      lag(equity_multiplier, 12) over (partition by company_code order by year, quarter) and
                      lag(equity_multiplier, 12) over (partition by company_code order by year, quarter) <
                      lag(equity_multiplier, 16) over (partition by company_code order by year, quarter) and
                      lag(equity_multiplier, 16) over (partition by company_code order by year, quarter) <
                      lag(equity_multiplier, 20)
                      over (partition by company_code order by year, quarter)                           as equity_multiplier_decline_5y_continuous,
                      roa >
                      1.2 * lag(roa, 20)
                            over (partition by company_code order by year, quarter)                     as roa_increase_5y_overall,
                      roa >
                      lag(roa, 4) over (partition by company_code order by year, quarter) and
                      lag(roa, 4) over (partition by company_code order by year, quarter) >
                      lag(roa, 8) over (partition by company_code order by year, quarter) and
                      lag(roa, 8) over (partition by company_code order by year, quarter) >
                      lag(roa, 12) over (partition by company_code order by year, quarter) and
                      lag(roa, 12) over (partition by company_code order by year, quarter) >
                      lag(roa, 16) over (partition by company_code order by year, quarter) and
                      lag(roa, 16) over (partition by company_code order by year, quarter) >
                      lag(roa, 20)
                      over (partition by company_code order by year, quarter)                           as roa_increase_5y_continuous,
                      days_sales_of_inventory < lag(days_sales_of_inventory, 20)
                                                over (partition by company_code order by year, quarter) as days_sales_of_inventory_decline_5y_overall,
                      days_sales_of_inventory <
                      lag(days_sales_of_inventory, 4)
                      over (partition by company_code order by year, quarter) and
                      lag(days_sales_of_inventory, 4)
                      over (partition by company_code order by year, quarter) <
                      lag(days_sales_of_inventory, 8)
                      over (partition by company_code order by year, quarter) and
                      lag(days_sales_of_inventory, 8)
                      over (partition by company_code order by year, quarter) <
                      lag(days_sales_of_inventory, 12)
                      over (partition by company_code order by year, quarter) and
                      lag(days_sales_of_inventory, 12)
                      over (partition by company_code order by year, quarter) <
                      lag(days_sales_of_inventory, 16)
                      over (partition by company_code order by year, quarter) and
                      lag(days_sales_of_inventory, 16)
                      over (partition by company_code order by year, quarter) <
                      lag(days_sales_of_inventory, 20)
                      over (partition by company_code order by year, quarter)                           as days_sales_of_inventory_decline_5y_continuous,
                      days_sales_outstanding < lag(days_sales_outstanding, 20)
                                               over (partition by company_code order by year, quarter)  as days_sales_outstanding_decline_5y_overall,
                      days_sales_outstanding <
                      lag(days_sales_outstanding, 4)
                      over (partition by company_code order by year, quarter) and
                      lag(days_sales_outstanding, 4)
                      over (partition by company_code order by year, quarter) <
                      lag(days_sales_outstanding, 8)
                      over (partition by company_code order by year, quarter) and
                      lag(days_sales_outstanding, 8)
                      over (partition by company_code order by year, quarter) <
                      lag(days_sales_outstanding, 12)
                      over (partition by company_code order by year, quarter) and
                      lag(days_sales_outstanding, 12)
                      over (partition by company_code order by year, quarter) <
                      lag(days_sales_outstanding, 16)
                      over (partition by company_code order by year, quarter) and
                      lag(days_sales_outstanding, 16)
                      over (partition by company_code order by year, quarter) <
                      lag(days_sales_outstanding, 20)
                      over (partition by company_code order by year, quarter)                           as days_sales_outstanding_decline_5y_continuous,
                      inventories_ratio < lag(inventories_ratio, 20)
                                          over (partition by company_code order by year, quarter)       as inventories_ratio_decline_5y_overall,
                      inventories_ratio <
                      lag(inventories_ratio, 4) over (partition by company_code order by year, quarter) and
                      lag(inventories_ratio, 4) over (partition by company_code order by year, quarter) <
                      lag(inventories_ratio, 8) over (partition by company_code order by year, quarter) and
                      lag(inventories_ratio, 8) over (partition by company_code order by year, quarter) <
                      lag(inventories_ratio, 12) over (partition by company_code order by year, quarter) and
                      lag(inventories_ratio, 12) over (partition by company_code order by year, quarter) <
                      lag(inventories_ratio, 16) over (partition by company_code order by year, quarter) and
                      lag(inventories_ratio, 16) over (partition by company_code order by year, quarter) <
                      lag(inventories_ratio, 20)
                      over (partition by company_code order by year, quarter)                           as inventories_ratio_decline_5y_continuous,
                      receivables_ratio < lag(receivables_ratio, 20)
                                          over (partition by company_code order by year, quarter)       as receivables_ratio_decline_5y_overall,
                      receivables_ratio <
                      lag(receivables_ratio, 4) over (partition by company_code order by year, quarter) and
                      lag(receivables_ratio, 4) over (partition by company_code order by year, quarter) <
                      lag(receivables_ratio, 8) over (partition by company_code order by year, quarter) and
                      lag(receivables_ratio, 8) over (partition by company_code order by year, quarter) <
                      lag(receivables_ratio, 12) over (partition by company_code order by year, quarter) and
                      lag(receivables_ratio, 12) over (partition by company_code order by year, quarter) <
                      lag(receivables_ratio, 16) over (partition by company_code order by year, quarter) and
                      lag(receivables_ratio, 16) over (partition by company_code order by year, quarter) <
                      lag(receivables_ratio, 20)
                      over (partition by company_code order by year, quarter)                           as receivables_ratio_decline_5y_continuous,
                      quick_ratio > 1.2 *
                                    lag(quick_ratio, 20)
                                    over (partition by company_code order by year, quarter)             as quick_ratio_increase_5y_overall,
                      quick_ratio >
                      lag(quick_ratio, 4) over (partition by company_code order by year, quarter) and
                      lag(quick_ratio, 4) over (partition by company_code order by year, quarter) >
                      lag(quick_ratio, 8) over (partition by company_code order by year, quarter) and
                      lag(quick_ratio, 8) over (partition by company_code order by year, quarter) >
                      lag(quick_ratio, 12) over (partition by company_code order by year, quarter) and
                      lag(quick_ratio, 12) over (partition by company_code order by year, quarter) >
                      lag(quick_ratio, 16) over (partition by company_code order by year, quarter) and
                      lag(quick_ratio, 16) over (partition by company_code order by year, quarter) >
                      lag(quick_ratio, 20)
                      over (partition by company_code order by year, quarter)                           as quick_ratio_increase_5y_continuous,
                      cash_ratio > 1.2 *
                                   lag(cash_ratio, 20)
                                   over (partition by company_code order by year, quarter)              as cash_ratio_increase_5y_overall,
                      cash_ratio >
                      lag(cash_ratio, 4) over (partition by company_code order by year, quarter) and
                      lag(cash_ratio, 4) over (partition by company_code order by year, quarter) >
                      lag(cash_ratio, 8) over (partition by company_code order by year, quarter) and
                      lag(cash_ratio, 8) over (partition by company_code order by year, quarter) >
                      lag(cash_ratio, 12) over (partition by company_code order by year, quarter) and
                      lag(cash_ratio, 12) over (partition by company_code order by year, quarter) >
                      lag(cash_ratio, 16) over (partition by company_code order by year, quarter) and
                      lag(cash_ratio, 16) over (partition by company_code order by year, quarter) >
                      lag(cash_ratio, 20)
                      over (partition by company_code order by year, quarter)                           as cash_ratio_increase_5y_continuous,
                      eps >
                      1.2 * lag(eps, 20)
                            over (partition by company_code order by year, quarter)                     as eps_increase_5y_overall,
                      eps >
                      lag(eps, 4) over (partition by company_code order by year, quarter) and
                      lag(eps, 4) over (partition by company_code order by year, quarter) >
                      lag(eps, 8) over (partition by company_code order by year, quarter) and
                      lag(eps, 8) over (partition by company_code order by year, quarter) >
                      lag(eps, 12) over (partition by company_code order by year, quarter) and
                      lag(eps, 12) over (partition by company_code order by year, quarter) >
                      lag(eps, 16) over (partition by company_code order by year, quarter) and
                      lag(eps, 16) over (partition by company_code order by year, quarter) >
                      lag(eps, 20)
                      over (partition by company_code order by year, quarter)                           as eps_increase_5y_continuous,
                      fcf_per_share >
                      1.2 * lag(fcf_per_share, 20)
                            over (partition by company_code order by year, quarter)                     as fcf_per_share_increase_5y_overall,
                      fcf_per_share >
                      lag(fcf_per_share, 4) over (partition by company_code order by year, quarter) and
                      lag(fcf_per_share, 4) over (partition by company_code order by year, quarter) >
                      lag(fcf_per_share, 8) over (partition by company_code order by year, quarter) and
                      lag(fcf_per_share, 8) over (partition by company_code order by year, quarter) >
                      lag(fcf_per_share, 12) over (partition by company_code order by year, quarter) and
                      lag(fcf_per_share, 12) over (partition by company_code order by year, quarter) >
                      lag(fcf_per_share, 16) over (partition by company_code order by year, quarter) and
                      lag(fcf_per_share, 16) over (partition by company_code order by year, quarter) >
                      lag(fcf_per_share, 20)
                      over (partition by company_code order by year, quarter)                           as fcf_per_share_increase_5y_continuous,
                      total_operating_revenue < lag(total_operating_revenue, 20)
                                                over (partition by company_code order by year, quarter) as revenue_growth_rate_decline_5y_overall,
                      total_operating_revenue <
                      lag(total_operating_revenue, 4)
                      over (partition by company_code order by year, quarter) and
                      lag(total_operating_revenue, 4)
                      over (partition by company_code order by year, quarter) <
                      lag(total_operating_revenue, 8)
                      over (partition by company_code order by year, quarter) and
                      lag(total_operating_revenue, 8)
                      over (partition by company_code order by year, quarter) <
                      lag(total_operating_revenue, 12)
                      over (partition by company_code order by year, quarter) and
                      lag(total_operating_revenue, 12)
                      over (partition by company_code order by year, quarter) <
                      lag(total_operating_revenue, 16)
                      over (partition by company_code order by year, quarter) and
                      lag(total_operating_revenue, 16)
                      over (partition by company_code order by year, quarter) <
                      lag(total_operating_revenue, 20)
                      over (partition by company_code order by year, quarter)                           as revenue_growth_rate_decline_5y_continuous,
                      profit_margin < lag(profit_margin, 20)
                                      over (partition by company_code order by year, quarter)           as profit_margin_decline_5y_overall,
                      profit_margin <
                      lag(profit_margin, 4) over (partition by company_code order by year, quarter) and
                      lag(profit_margin, 4) over (partition by company_code order by year, quarter) <
                      lag(profit_margin, 8) over (partition by company_code order by year, quarter) and
                      lag(profit_margin, 8) over (partition by company_code order by year, quarter) <
                      lag(profit_margin, 12) over (partition by company_code order by year, quarter) and
                      lag(profit_margin, 12) over (partition by company_code order by year, quarter) <
                      lag(profit_margin, 16) over (partition by company_code order by year, quarter) and
                      lag(profit_margin, 16) over (partition by company_code order by year, quarter) <
                      lag(profit_margin, 20)
                      over (partition by company_code order by year, quarter)                           as profit_margin_decline_5y_continuous,
                      operating_margin < lag(operating_margin, 20)
                                         over (partition by company_code order by year, quarter)        as operating_margin_decline_5y_overall,
                      operating_margin <
                      lag(operating_margin, 4) over (partition by company_code order by year, quarter) and
                      lag(operating_margin, 4) over (partition by company_code order by year, quarter) <
                      lag(operating_margin, 8) over (partition by company_code order by year, quarter) and
                      lag(operating_margin, 8) over (partition by company_code order by year, quarter) <
                      lag(operating_margin, 12) over (partition by company_code order by year, quarter) and
                      lag(operating_margin, 12) over (partition by company_code order by year, quarter) <
                      lag(operating_margin, 16) over (partition by company_code order by year, quarter) and
                      lag(operating_margin, 16) over (partition by company_code order by year, quarter) <
                      lag(operating_margin, 20)
                      over (partition by company_code order by year, quarter)                           as operating_margin_decline_5y_continuous,
                      total_assets_turnover < lag(total_assets_turnover, 20)
                                              over (partition by company_code order by year, quarter)   as total_assets_turnover_decline_5y_overall,
                      total_assets_turnover <
                      lag(total_assets_turnover, 4)
                      over (partition by company_code order by year, quarter) and
                      lag(total_assets_turnover, 4) over (partition by company_code order by year, quarter) <
                      lag(total_assets_turnover, 8)
                      over (partition by company_code order by year, quarter) and
                      lag(total_assets_turnover, 8) over (partition by company_code order by year, quarter) <
                      lag(total_assets_turnover, 12)
                      over (partition by company_code order by year, quarter) and
                      lag(total_assets_turnover, 12)
                      over (partition by company_code order by year, quarter) <
                      lag(total_assets_turnover, 16)
                      over (partition by company_code order by year, quarter) and
                      lag(total_assets_turnover, 16)
                      over (partition by company_code order by year, quarter) <
                      lag(total_assets_turnover, 20)
                      over (partition by company_code order by year, quarter)                           as total_assets_turnover_decline_5y_continuous,
                      equity_multiplier > lag(total_assets_turnover, 20)
                                          over (partition by company_code order by year, quarter)       as equity_multiplier_increase_5y_overall,
                      equity_multiplier >
                      lag(equity_multiplier, 4) over (partition by company_code order by year, quarter) and
                      lag(equity_multiplier, 4) over (partition by company_code order by year, quarter) >
                      lag(equity_multiplier, 8) over (partition by company_code order by year, quarter) and
                      lag(equity_multiplier, 8) over (partition by company_code order by year, quarter) >
                      lag(equity_multiplier, 12) over (partition by company_code order by year, quarter) and
                      lag(equity_multiplier, 12) over (partition by company_code order by year, quarter) >
                      lag(equity_multiplier, 16) over (partition by company_code order by year, quarter) and
                      lag(equity_multiplier, 16) over (partition by company_code order by year, quarter) >
                      lag(equity_multiplier, 20)
                      over (partition by company_code order by year, quarter)                           as equity_multiplier_increase_5y_continuous,
                      roa < lag(roa, 20)
                            over (partition by company_code order by year, quarter)                     as roa_decline_5y_overall,
                      roa <
                      lag(roa, 4) over (partition by company_code order by year, quarter) and
                      lag(roa, 4) over (partition by company_code order by year, quarter) <
                      lag(roa, 8) over (partition by company_code order by year, quarter) and
                      lag(roa, 8) over (partition by company_code order by year, quarter) <
                      lag(roa, 12) over (partition by company_code order by year, quarter) and
                      lag(roa, 12) over (partition by company_code order by year, quarter) <
                      lag(roa, 16) over (partition by company_code order by year, quarter) and
                      lag(roa, 16) over (partition by company_code order by year, quarter) <
                      lag(roa, 20)
                      over (partition by company_code order by year, quarter)                           as roa_decline_5y_continuous,
                      days_sales_of_inventory > lag(days_sales_of_inventory, 20)
                                                over (partition by company_code order by year, quarter) as days_sales_of_inventory_increase_5y_overall,
                      days_sales_of_inventory >
                      lag(days_sales_of_inventory, 4)
                      over (partition by company_code order by year, quarter) and
                      lag(days_sales_of_inventory, 4)
                      over (partition by company_code order by year, quarter) >
                      lag(days_sales_of_inventory, 8)
                      over (partition by company_code order by year, quarter) and
                      lag(days_sales_of_inventory, 8)
                      over (partition by company_code order by year, quarter) >
                      lag(days_sales_of_inventory, 12)
                      over (partition by company_code order by year, quarter) and
                      lag(days_sales_of_inventory, 12)
                      over (partition by company_code order by year, quarter) >
                      lag(days_sales_of_inventory, 16)
                      over (partition by company_code order by year, quarter) and
                      lag(days_sales_of_inventory, 16)
                      over (partition by company_code order by year, quarter) >
                      lag(days_sales_of_inventory, 20)
                      over (partition by company_code order by year, quarter)                           as days_sales_of_inventory_increase_5y_continuous,
                      days_sales_outstanding > lag(days_sales_outstanding, 20)
                                               over (partition by company_code order by year, quarter)  as days_sales_outstanding_increase_5y_overall,
                      days_sales_outstanding >
                      lag(days_sales_outstanding, 4)
                      over (partition by company_code order by year, quarter) and
                      lag(days_sales_outstanding, 4)
                      over (partition by company_code order by year, quarter) >
                      lag(days_sales_outstanding, 8)
                      over (partition by company_code order by year, quarter) and
                      lag(days_sales_outstanding, 8)
                      over (partition by company_code order by year, quarter) >
                      lag(days_sales_outstanding, 12)
                      over (partition by company_code order by year, quarter) and
                      lag(days_sales_outstanding, 12)
                      over (partition by company_code order by year, quarter) >
                      lag(days_sales_outstanding, 16)
                      over (partition by company_code order by year, quarter) and
                      lag(days_sales_outstanding, 16)
                      over (partition by company_code order by year, quarter) >
                      lag(days_sales_outstanding, 20)
                      over (partition by company_code order by year, quarter)                           as days_sales_outstanding_increase_5y_continuous,
                      inventories_ratio > lag(inventories_ratio, 20)
                                          over (partition by company_code order by year, quarter)       as inventories_ratio_increase_5y_overall,
                      inventories_ratio >
                      lag(inventories_ratio, 4) over (partition by company_code order by year, quarter) and
                      lag(inventories_ratio, 4) over (partition by company_code order by year, quarter) >
                      lag(inventories_ratio, 8) over (partition by company_code order by year, quarter) and
                      lag(inventories_ratio, 8) over (partition by company_code order by year, quarter) >
                      lag(inventories_ratio, 12) over (partition by company_code order by year, quarter) and
                      lag(inventories_ratio, 12) over (partition by company_code order by year, quarter) >
                      lag(inventories_ratio, 16) over (partition by company_code order by year, quarter) and
                      lag(inventories_ratio, 16) over (partition by company_code order by year, quarter) >
                      lag(inventories_ratio, 20)
                      over (partition by company_code order by year, quarter)                           as inventories_ratio_increase_5y_continuous,
                      receivables_ratio > lag(receivables_ratio, 20)
                                          over (partition by company_code order by year, quarter)       as receivables_ratio_increase_5y_overall,
                      receivables_ratio >
                      lag(receivables_ratio, 4) over (partition by company_code order by year, quarter) and
                      lag(receivables_ratio, 4) over (partition by company_code order by year, quarter) >
                      lag(receivables_ratio, 8) over (partition by company_code order by year, quarter) and
                      lag(receivables_ratio, 8) over (partition by company_code order by year, quarter) >
                      lag(receivables_ratio, 12) over (partition by company_code order by year, quarter) and
                      lag(receivables_ratio, 12) over (partition by company_code order by year, quarter) >
                      lag(receivables_ratio, 16) over (partition by company_code order by year, quarter) and
                      lag(receivables_ratio, 16) over (partition by company_code order by year, quarter) >
                      lag(receivables_ratio, 20)
                      over (partition by company_code order by year, quarter)                           as receivables_ratio_increase_5y_continuous,
                      quick_ratio <
                      lag(quick_ratio, 20)
                      over (partition by company_code order by year, quarter)                           as quick_ratio_decline_5y_overall,
                      quick_ratio <
                      lag(quick_ratio, 4) over (partition by company_code order by year, quarter) and
                      lag(quick_ratio, 4) over (partition by company_code order by year, quarter) <
                      lag(quick_ratio, 8) over (partition by company_code order by year, quarter) and
                      lag(quick_ratio, 8) over (partition by company_code order by year, quarter) <
                      lag(quick_ratio, 12) over (partition by company_code order by year, quarter) and
                      lag(quick_ratio, 12) over (partition by company_code order by year, quarter) <
                      lag(quick_ratio, 16) over (partition by company_code order by year, quarter) and
                      lag(quick_ratio, 16) over (partition by company_code order by year, quarter) <
                      lag(quick_ratio, 20)
                      over (partition by company_code order by year, quarter)                           as quick_ratio_decline_5y_continuous,
                      cash_ratio <
                      lag(cash_ratio, 20)
                      over (partition by company_code order by year, quarter)                           as cash_ratio_decline_5y_overall,
                      cash_ratio <
                      lag(cash_ratio, 4) over (partition by company_code order by year, quarter) and
                      lag(cash_ratio, 4) over (partition by company_code order by year, quarter) <
                      lag(cash_ratio, 8) over (partition by company_code order by year, quarter) and
                      lag(cash_ratio, 8) over (partition by company_code order by year, quarter) <
                      lag(cash_ratio, 12) over (partition by company_code order by year, quarter) and
                      lag(cash_ratio, 12) over (partition by company_code order by year, quarter) <
                      lag(cash_ratio, 16) over (partition by company_code order by year, quarter) and
                      lag(cash_ratio, 16) over (partition by company_code order by year, quarter) <
                      lag(cash_ratio, 20)
                      over (partition by company_code order by year, quarter)                           as cash_ratio_decline_5y_continuous,
                      eps < lag(eps, 20)
                            over (partition by company_code order by year, quarter)                     as eps_decline_5y_overall,
                      eps <
                      lag(eps, 4) over (partition by company_code order by year, quarter) and
                      lag(eps, 4) over (partition by company_code order by year, quarter) <
                      lag(eps, 8) over (partition by company_code order by year, quarter) and
                      lag(eps, 8) over (partition by company_code order by year, quarter) <
                      lag(eps, 12) over (partition by company_code order by year, quarter) and
                      lag(eps, 12) over (partition by company_code order by year, quarter) <
                      lag(eps, 16) over (partition by company_code order by year, quarter) and
                      lag(eps, 16) over (partition by company_code order by year, quarter) <
                      lag(eps, 20)
                      over (partition by company_code order by year, quarter)                           as eps_decline_5y_continuous,
                      fcf_per_share < lag(fcf_per_share, 20)
                                      over (partition by company_code order by year, quarter)           as fcf_per_share_decline_5y_overall,
                      fcf_per_share <
                      lag(fcf_per_share, 4) over (partition by company_code order by year, quarter) and
                      lag(fcf_per_share, 4) over (partition by company_code order by year, quarter) <
                      lag(fcf_per_share, 8) over (partition by company_code order by year, quarter) and
                      lag(fcf_per_share, 8) over (partition by company_code order by year, quarter) <
                      lag(fcf_per_share, 12) over (partition by company_code order by year, quarter) and
                      lag(fcf_per_share, 12) over (partition by company_code order by year, quarter) <
                      lag(fcf_per_share, 16) over (partition by company_code order by year, quarter) and
                      lag(fcf_per_share, 16) over (partition by company_code order by year, quarter) <
                      lag(fcf_per_share, 20)
                      over (partition by company_code order by year, quarter)                           as fcf_per_share_decline_5y_continuous
               from financial_index_ttm)

select market,
       year,
       quarter,
       company_code,
       company_name,
       industry,
       cbs,
       operating_performance,
       return_on_investment,
       capital_structure,
       liquidity,
       cash_flow,
       f_score,
       f_score + coalesce(revenue_growth_rate_increase_5y_overall::INT, 0) +
       coalesce(revenue_growth_rate_increase_5y_continuous::INT, 0) +
       coalesce(profit_margin_increase_5y_overall::INT, 0) +
       coalesce(profit_margin_increase_5y_continuous::INT, 0) +
       coalesce(operating_margin_increase_5y_overall::INT, 0) +
       coalesce(operating_margin_increase_5y_continuous::INT, 0) +
       coalesce(total_assets_turnover_increase_5y_overall::INT, 0) +
       coalesce(total_assets_turnover_increase_5y_continuous::INT, 0) +
       coalesce(equity_multiplier_decline_5y_overall::INT, 0) +
       coalesce(equity_multiplier_decline_5y_continuous::INT, 0) +
       coalesce(roa_increase_5y_overall::INT, 0) +
       coalesce(roa_increase_5y_continuous::INT, 0) +
       coalesce(days_sales_of_inventory_decline_5y_overall::INT, 0) +
       coalesce(days_sales_of_inventory_decline_5y_continuous::INT, 0) +
       coalesce(days_sales_outstanding_decline_5y_overall::INT, 0) +
       coalesce(days_sales_outstanding_decline_5y_continuous::INT, 0) +
       coalesce(inventories_ratio_decline_5y_overall::INT, 0) +
       coalesce(inventories_ratio_decline_5y_continuous::INT, 0) +
       coalesce(receivables_ratio_decline_5y_overall::INT, 0) +
       coalesce(receivables_ratio_decline_5y_continuous::INT, 0) +
       coalesce(quick_ratio_increase_5y_overall::INT, 0) +
       coalesce(quick_ratio_increase_5y_continuous::INT, 0) +
       coalesce(cash_ratio_increase_5y_overall::INT, 0) +
       coalesce(cash_ratio_increase_5y_continuous::INT, 0) +
       coalesce(eps_increase_5y_overall::INT, 0) +
       coalesce(eps_increase_5y_continuous::INT, 0) +
       coalesce(fcf_per_share_increase_5y_overall::INT, 0) +
       coalesce(fcf_per_share_increase_5y_continuous::INT, 0)                                  as growth_score,
       coalesce(revenue_growth_rate_decline_5y_overall::INT, 0) +
       coalesce(revenue_growth_rate_decline_5y_continuous::INT, 0) +
       coalesce(profit_margin_decline_5y_overall::INT, 0) +
       coalesce(profit_margin_decline_5y_continuous::INT, 0) +
       coalesce(operating_margin_decline_5y_overall::INT, 0) +
       coalesce(operating_margin_decline_5y_continuous::INT, 0) +
       coalesce(total_assets_turnover_decline_5y_overall::INT, 0) +
       coalesce(total_assets_turnover_decline_5y_continuous::INT, 0) +
       coalesce(equity_multiplier_increase_5y_overall::INT, 0) +
       coalesce(equity_multiplier_increase_5y_continuous::INT, 0) +
       coalesce(roa_decline_5y_overall::INT, 0) +
       coalesce(roa_decline_5y_continuous::INT, 0) +
       coalesce(days_sales_of_inventory_increase_5y_overall::INT, 0) +
       coalesce(days_sales_of_inventory_increase_5y_continuous::INT, 0) +
       coalesce(days_sales_outstanding_increase_5y_overall::INT, 0) +
       coalesce(days_sales_outstanding_increase_5y_continuous::INT, 0) +
       coalesce(inventories_ratio_increase_5y_overall::INT, 0) +
       coalesce(inventories_ratio_increase_5y_continuous::INT, 0) +
       coalesce(receivables_ratio_increase_5y_overall::INT, 0) +
       coalesce(receivables_ratio_increase_5y_continuous::INT, 0) +
       coalesce(quick_ratio_decline_5y_overall::INT, 0) +
       coalesce(quick_ratio_decline_5y_continuous::INT, 0) +
       coalesce(cash_ratio_decline_5y_overall::INT, 0) +
       coalesce(cash_ratio_decline_5y_continuous::INT, 0) +
       coalesce(eps_decline_5y_overall::INT, 0) +
       coalesce(eps_decline_5y_continuous::INT, 0) +
       coalesce(fcf_per_share_decline_5y_overall::INT, 0) +
       coalesce(fcf_per_share_decline_5y_continuous::INT, 0)                                   as drop_score,
       roic,
       roa,
       eps,
       roic / nullif(lag(roic) over (partition by company_code order by year, quarter), 0) - 1 as roic_growth_rate,
       roa / nullif(lag(roa) over (partition by company_code order by year, quarter), 0) - 1   as roa_growth_rate,
       eps / nullif(lag(eps) over (partition by company_code order by year, quarter), 0) - 1   as eps_growth_rate,
       fcf_per_share / nullif(lag(fcf_per_share) over (partition by company_code order by year, quarter), 0) -
       1                                                                                       as fcf_per_share_growth_rate,

       total_operating_revenue /
       nullif(lag(total_operating_revenue) over (partition by company_code order by year, quarter), 0) -
       1                                                                                       as revenue_growth_rate,
       profit / nullif(lag(profit) over (partition by company_code order by year, quarter), 0) -
       1                                                                                       as profit_growth_rate,
       ocf / nullif(lag(ocf) over (partition by company_code order by year, quarter), 0) -
       1                                                                                       as ocf_growth_rate,
       total_assets_turnover /
       nullif(lag(total_assets_turnover) over (partition by company_code order by year, quarter), 0) -
       1                                                                                       as total_assets_turnover_growth_rate,
       1 - days_sales_of_inventory /
           nullif(lag(days_sales_of_inventory) over (partition by company_code order by year, quarter),
                  0)                                                                           as days_sales_of_inventory_growth_rate,
       1 - days_sales_outstanding /
           nullif(lag(days_sales_outstanding) over (partition by company_code order by year, quarter),
                  0)                                                                           as days_sales_outstanding_growth_rate,

       1 - equity_multiplier / nullif(lag(equity_multiplier) over (partition by company_code order by year, quarter),
                                      0)                                                       as equity_multiplier_growth_rate,
       current_ratio / nullif(lag(current_ratio) over (partition by company_code order by year, quarter), 0) -
       1                                                                                       as current_ratio_growth_rate,
       quick_ratio / nullif(lag(quick_ratio) over (partition by company_code order by year, quarter), 0) -
       1                                                                                       as quick_ratio_growth_rate,
       1 - total_non_current_liabilities /
           nullif(lag(total_non_current_liabilities) over (partition by company_code order by year, quarter),
                  0)                                                                           as total_non_current_liabilities_growth_rate,
       cash_ratio / nullif(lag(cash_ratio) over (partition by company_code order by year, quarter), 0) -
       1                                                                                       as cash_ratio_growth_rate,
       cash_flow_ratio / nullif(lag(cash_flow_ratio) over (partition by company_code order by year, quarter), 0) -
       1                                                                                       as cash_flow_ratio_growth_rate,
       cash_flow_adequacy_ratio /
       nullif(lag(cash_flow_adequacy_ratio) over (partition by company_code order by year, quarter), 0) -
       1                                                                                       as cash_flow_adequacy_ratio_growth_rate,
       cash_flow_reinvestment_ratio /
       nullif(lag(cash_flow_reinvestment_ratio) over (partition by company_code order by year, quarter), 0) -
       1                                                                                       as cash_flow_reinvestment_ratio_growth_rate,
       profit_margin / nullif(lag(profit_margin) over (partition by company_code order by year, quarter), 0) -
       1                                                                                       as profit_margin_growth_rate,

       operating_margin / nullif(lag(operating_margin) over (partition by company_code order by year, quarter), 0) -
       1                                                                                       as operating_margin_growth_rate,

       gross_margin / nullif(lag(gross_margin) over (partition by company_code order by year, quarter), 0) -
       1                                                                                       as gross_margin_growth_rate,
       1 - inventories_ratio / nullif(lag(inventories_ratio) over (partition by company_code order by year, quarter),
                                      0)                                                       as inventories_ratio_growth_rate,
       1 - receivables_ratio / nullif(lag(receivables_ratio) over (partition by company_code order by year, quarter),
                                      0)                                                       as receivables_ratio_growth_rate,
       1 - total_capital_stock /
           nullif(lag(total_capital_stock) over (partition by company_code order by year, quarter),
                  0)                                                                           as total_capital_stock_growth_rate,
       fcf_per_share,
       total_operating_revenue,
       profit,
       ocf,
       total_assets_turnover,
       days_sales_of_inventory,
       days_sales_outstanding,
       equity_multiplier,
       current_ratio,
       quick_ratio,
       total_non_current_liabilities,
       cash_ratio,
       cash_flow_ratio,
       cash_flow_adequacy_ratio,
       cash_flow_reinvestment_ratio,
       profit_margin,
       operating_margin,
       gross_margin,
       inventories_ratio,
       receivables_ratio,
       total_capital_stock,
       revenue_growth_rate_increase_5y_overall,
       revenue_growth_rate_increase_5y_continuous,
       profit_margin_increase_5y_overall,
       profit_margin_increase_5y_continuous,
       operating_margin_increase_5y_overall,
       operating_margin_increase_5y_continuous,
       total_assets_turnover_increase_5y_overall,
       total_assets_turnover_increase_5y_continuous,
       equity_multiplier_decline_5y_overall,
       equity_multiplier_decline_5y_continuous,
       roa_increase_5y_overall,
       roa_increase_5y_continuous,
       days_sales_of_inventory_decline_5y_overall,
       days_sales_of_inventory_decline_5y_continuous,
       days_sales_outstanding_decline_5y_overall,
       days_sales_outstanding_decline_5y_continuous,
       inventories_ratio_decline_5y_overall,
       inventories_ratio_decline_5y_continuous,
       receivables_ratio_decline_5y_overall,
       receivables_ratio_decline_5y_continuous,
       quick_ratio_increase_5y_overall,
       quick_ratio_increase_5y_continuous,
       cash_ratio_increase_5y_overall,
       cash_ratio_increase_5y_continuous,
       eps_increase_5y_overall,
       eps_increase_5y_continuous,
       fcf_per_share_increase_5y_overall,
       fcf_per_share_increase_5y_continuous,
       revenue_growth_rate_decline_5y_overall,
       revenue_growth_rate_decline_5y_continuous,
       profit_margin_decline_5y_overall,
       profit_margin_decline_5y_continuous,
       operating_margin_decline_5y_overall,
       operating_margin_decline_5y_continuous,
       total_assets_turnover_decline_5y_overall,
       total_assets_turnover_decline_5y_continuous,
       equity_multiplier_increase_5y_overall,
       equity_multiplier_increase_5y_continuous,
       roa_decline_5y_overall,
       roa_decline_5y_continuous,
       days_sales_of_inventory_increase_5y_overall,
       days_sales_of_inventory_increase_5y_continuous,
       days_sales_outstanding_increase_5y_overall,
       days_sales_outstanding_increase_5y_continuous,
       inventories_ratio_increase_5y_overall,
       inventories_ratio_increase_5y_continuous,
       receivables_ratio_increase_5y_overall,
       receivables_ratio_increase_5y_continuous,
       quick_ratio_decline_5y_overall,
       quick_ratio_decline_5y_continuous,
       cash_ratio_decline_5y_overall,
       cash_ratio_decline_5y_continuous,
       eps_decline_5y_overall,
       eps_decline_5y_continuous,
       fcf_per_share_decline_5y_overall,
       fcf_per_share_decline_5y_continuous
from index;