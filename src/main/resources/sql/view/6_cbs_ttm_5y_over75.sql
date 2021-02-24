create view cbs_ttm_5y_over75 as
with pass as (select distinct on (market, company_code) market,
                                                        company_code,
                                                        cbs > 75
                                                            and lag(cbs, 1)
                                                                over (partition by company_code order by year, quarter) >
                                                                75
                                                            and lag(cbs, 2)
                                                                over (partition by company_code order by year, quarter) >
                                                                75
                                                            and lag(cbs, 3)
                                                                over (partition by company_code order by year, quarter) >
                                                                75
                                                            and lag(cbs, 4)
                                                                over (partition by company_code order by year, quarter) >
                                                                75
                                                            and lag(cbs, 5)
                                                                over (partition by company_code order by year, quarter) >
                                                                75
                                                            and lag(cbs, 6)
                                                                over (partition by company_code order by year, quarter) >
                                                                75
                                                            and lag(cbs, 7)
                                                                over (partition by company_code order by year, quarter) >
                                                                75
                                                            and lag(cbs, 8)
                                                                over (partition by company_code order by year, quarter) >
                                                                75
                                                            and lag(cbs, 9)
                                                                over (partition by company_code order by year, quarter) >
                                                                75
                                                            and lag(cbs, 10)
                                                                over (partition by company_code order by year, quarter) >
                                                                75
                                                            and lag(cbs, 11)
                                                                over (partition by company_code order by year, quarter) >
                                                                75
                                                            and lag(cbs, 12)
                                                                over (partition by company_code order by year, quarter) >
                                                                75
                                                            and lag(cbs, 13)
                                                                over (partition by company_code order by year, quarter) >
                                                                75
                                                            and lag(cbs, 14)
                                                                over (partition by company_code order by year, quarter) >
                                                                75
                                                            and lag(cbs, 15)
                                                                over (partition by company_code order by year, quarter) >
                                                                75
                                                            and lag(cbs, 16)
                                                                over (partition by company_code order by year, quarter) >
                                                                75
                                                            and lag(cbs, 17)
                                                                over (partition by company_code order by year, quarter) >
                                                                75
                                                            and lag(cbs, 18)
                                                                over (partition by company_code order by year, quarter) >
                                                                75
                                                            and lag(cbs, 19)
                                                                over (partition by company_code order by year, quarter) >
                                                                75 as pass

              from growth_analysis_ttm
              order by market, company_code, year desc, quarter desc)

select growth_analysis_ttm.*
from growth_analysis_ttm
         join pass on growth_analysis_ttm.market = pass.market
    and growth_analysis_ttm.company_code = pass.company_code
    and pass.pass is true
order by growth_analysis_ttm.company_code, growth_analysis_ttm.year desc, growth_analysis_ttm.quarter desc;