create materialized view concise_balance_sheet_individual as
select distinct on (market, year, quarter, company_code, title) id,
                                                                market,
                                                                year,
                                                                quarter,
                                                                company_code,
                                                                company_name,
                                                                title,
                                                                value
from concise_balance_sheet
order by market, year, quarter, company_code, title, type;