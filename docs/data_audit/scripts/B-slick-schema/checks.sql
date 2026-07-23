-- B-slick-schema — 財務定義與算式審查(對象:src/main/scala/db/table/*.scala 型別/單位/唯一索引)
-- 跑法:psql -h localhost -p 5432 -d quantlib -f docs/data_audit/scripts/B-slick-schema/checks.sql
-- 稽核日 2026-07-22。每一區塊上方的「應有值」是稽核當日實測。

\pset pager off
\timing off

\echo '=============================================================='
\echo 'OK-1  Slick <-> PostgreSQL 逐欄 parity(欄名 / 型別 / nullability)'
\echo '      應有值:23 張表全部與 db/table/*.scala 一致'
\echo '=============================================================='
SELECT table_name, count(*) AS n_cols,
       count(*) FILTER (WHERE is_nullable = 'NO') AS not_null_cols
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public')
GROUP BY 1 ORDER BY 1;

\echo ''
\echo 'OK-1b 唯一索引(Slick def idx = index(..., unique = true) 的落地結果)'
SELECT tablename, indexname, indexdef
FROM pg_indexes WHERE schemaname = 'public' AND indexdef LIKE 'CREATE UNIQUE%'
  AND indexname NOT LIKE '%_pkey' ORDER BY 1;

\echo ''
\echo '=============================================================='
\echo 'BUG-1  ex_right_dividend.cash_dividend 不是現金股利,是「除權息調整總額」'
\echo '       應有值(legacy 段 pre>0):'
\echo '         權息 5119 列 / 5118 列等於 pre-ref / 平均 6.830'
\echo '         權   2245 列 / 2237 列等於 pre-ref / 平均 2.722  <- 純除權,真實現金股利應為 0'
\echo '         息  20273 列 /20273 列等於 pre-ref / 平均 2.043  <- 純除息,這一類才是對的'
\echo '=============================================================='
SELECT right_or_dividend,
       count(*) FILTER (WHERE closing_price_before_ex_right_ex_dividend > 0) AS legacy_rows,
       count(*) FILTER (WHERE closing_price_before_ex_right_ex_dividend > 0
             AND abs(cash_dividend - (closing_price_before_ex_right_ex_dividend
                                      - ex_right_ex_dividend_reference_price)) <= 0.011) AS eq_pre_minus_ref,
       round(avg(cash_dividend) FILTER (WHERE closing_price_before_ex_right_ex_dividend > 0)::numeric, 3) AS avg_value
FROM ex_right_dividend GROUP BY 1 ORDER BY 2 DESC;

\echo ''
\echo 'BUG-1b 個案:山富 2743 2025-09-18「權息」— 實際配息 2 元、配股 30%,'
\echo '       DB 的 cash_dividend = 27.846154(= 114 - 112/1.3),誇大 13.9 倍'
SELECT date, company_code, company_name,
       closing_price_before_ex_right_ex_dividend AS pre,
       ex_right_ex_dividend_reference_price      AS ref,
       cash_dividend, right_or_dividend
FROM ex_right_dividend
WHERE company_code IN ('2743','4123','2330') AND date >= '2020-01-01'
ORDER BY company_code, date;

\echo ''
\echo 'BUG-1c 語意在 2024-07 翻轉:MOPS 世代不再填 pre/ref,cash_dividend 變成真現金股利'
\echo '       應有值:2024-06 之前 has_pre = n;2024-07 起 has_pre < n 且逐年下降'
SELECT to_char(date,'YYYY-MM') AS ym, count(*) AS n,
       count(*) FILTER (WHERE closing_price_before_ex_right_ex_dividend > 0) AS has_pre
FROM ex_right_dividend WHERE date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY 1 ORDER BY 1;

\echo ''
\echo '=============================================================='
\echo 'BUG-3  int32 型別已實際溢位(schema 根因;A-daily_trading_details 已回報同一事件)'
\echo '       應有值:00403A 2026-05-13 原始檔三大法人合計 -2,725,272,226,DB 存 0'
\echo '=============================================================='
SELECT date, company_code, company_name,
       dealers_hedge_difference, dealers_difference, total_difference
FROM daily_trading_details
WHERE market='twse' AND company_code='00403A' AND date IN ('2026-05-12','2026-05-13');

\echo ''
\echo 'BUG-3b 同類型別掃描:其餘 Int 欄的實測極值 vs int32 上限 2147483647'
SELECT 'daily_trading_details 最大量'  AS col, max(GREATEST(abs(foreign_investors_total_buy),
        abs(foreign_investors_total_sell), abs(dealers_total_buy), abs(total_difference)))::text AS max_abs
FROM daily_trading_details
UNION ALL SELECT 'margin_transactions 最大量(單位:張)', max(GREATEST(abs(margin_balance_of_the_day),
        abs(margin_quota), abs(short_quota)))::text FROM margin_transactions
UNION ALL SELECT 'daily_quote.transaction', max(transaction)::text FROM daily_quote
UNION ALL SELECT 'daily_quote.trade_volume (bigint,已超 int32)', max(trade_volume)::text FROM daily_quote
UNION ALL SELECT 'int32 上限', '2147483647';

\echo ''
\echo '=============================================================='
\echo 'SUSPECT-1  market 詞彙分裂:三張明細財報表只有 tw,其餘表用 twse/tpex'
\echo '           應有值:balance_sheet / income_statement_progressive / cash_flows_progressive = tw'
\echo '=============================================================='
SELECT 'balance_sheet' t, market, count(*) FROM balance_sheet GROUP BY 2
UNION ALL SELECT 'income_statement_progressive', market, count(*) FROM income_statement_progressive GROUP BY 2
UNION ALL SELECT 'cash_flows_progressive', market, count(*) FROM cash_flows_progressive GROUP BY 2
UNION ALL SELECT 'concise_balance_sheet', market, count(*) FROM concise_balance_sheet GROUP BY 2
UNION ALL SELECT 'concise_income_statement_progressive', market, count(*) FROM concise_income_statement_progressive GROUP BY 2
UNION ALL SELECT 'operating_revenue', market, count(*) FROM operating_revenue GROUP BY 2
UNION ALL SELECT 'daily_quote', market, count(*) FROM daily_quote GROUP BY 2
ORDER BY 1, 2;

\echo ''
\echo '=============================================================='
\echo 'SUSPECT-2  長式 (title, value) 沒有單位欄:同一欄混新台幣千元與元/股'
\echo '           應有值:台積電 2024Q4 營業收入 2,894,307,699(千元) vs 基本每股盈餘 45.25(元)'
\echo '=============================================================='
SELECT title, value FROM concise_income_statement_progressive
WHERE year=2024 AND quarter=4 AND company_code='2330' AND type='consolidated'
  AND title IN ('營業收入','營業毛利（毛損）','本期淨利（淨損）','基本每股盈餘（元）')
ORDER BY value DESC;

\echo ''
\echo '=============================================================='
\echo 'SUSPECT-3  type(合併/個體)與資料同表,唯一索引含 type -> 同一邏輯鍵可有兩列'
\echo '           應有值:cbs 532288 鍵 / cis 355162 鍵 / opr 27365 鍵,全部落在 2005-2012'
\echo '=============================================================='
SELECT 'concise_balance_sheet 同鍵跨 type' AS chk, count(*) FROM
  (SELECT market,year,quarter,company_code,title FROM concise_balance_sheet GROUP BY 1,2,3,4,5 HAVING count(DISTINCT type)>1) s
UNION ALL SELECT 'concise_income_statement_progressive 同鍵跨 type', count(*) FROM
  (SELECT market,year,quarter,company_code,title FROM concise_income_statement_progressive GROUP BY 1,2,3,4,5 HAVING count(DISTINCT type)>1) s
UNION ALL SELECT 'operating_revenue 同鍵跨 type', count(*) FROM
  (SELECT market,year,month,company_code FROM operating_revenue GROUP BY 1,2,3,4 HAVING count(DISTINCT type)>1) s;

\echo ''
\echo 'SUSPECT-3b type=consolidated 硬過濾的代價:2004/2005 只剩 4 家公司'
SELECT year, count(DISTINCT company_code) AS all_types,
       count(DISTINCT company_code) FILTER (WHERE type='consolidated') AS consolidated_only
FROM concise_balance_sheet WHERE year BETWEEN 2004 AND 2013 GROUP BY 1 ORDER BY 1;

\echo ''
\echo '=============================================================='
\echo 'SUSPECT-5  非 Option 數值欄把「沒有」寫成 0'
\echo '           應有值:daily_quote 134,222 列 closing_price IS NULL 但 change = 0(100%)'
\echo '=============================================================='
SELECT count(*) FILTER (WHERE change = 0 AND closing_price IS NULL) AS change_zero_no_close,
       count(*) FILTER (WHERE closing_price IS NULL)                AS no_close_total
FROM daily_quote;

\echo ''
\echo '=============================================================='
\echo 'SUSPECT-6  tdcc_shareholding 的「差異數」(16)與「合計」(17)與真級距同表無旗標'
\echo '           應有值:17 個 tier 的 pct_of_outstanding 相加平均 199.95%'
\echo '=============================================================='
WITH s AS (SELECT data_date, company_code,
             sum(pct_of_outstanding) FILTER (WHERE holding_tier BETWEEN 1 AND 17) AS pct_all
           FROM tdcc_shareholding GROUP BY 1,2)
SELECT count(*) AS keys, round(avg(pct_all)::numeric, 2) AS avg_pct_all_tiers FROM s;

\echo ''
\echo '=============================================================='
\echo 'SUSPECT-8  兩個本益比欄位:daily_quote 只有 TWSE,TPEx 100% NULL'
\echo '=============================================================='
SELECT market, count(*) FILTER (WHERE price_earning_ratio IS NOT NULL) AS has_per, count(*) AS n
FROM daily_quote GROUP BY 1;

SELECT extract(year from q.date)::int AS yr,
       count(*) FILTER (WHERE q.price_earning_ratio IS NOT NULL AND p.price_to_earning_ratio IS NOT NULL
                          AND abs(q.price_earning_ratio - p.price_to_earning_ratio) > 0.02) AS disagree
FROM daily_quote q JOIN stock_per_pbr_dividend_yield p USING (market, date, company_code)
WHERE q.date >= '2024-01-01' GROUP BY 1 ORDER BY 1;

\echo ''
\echo '=============================================================='
\echo 'OK-2 / OK-3  單位與符號約定實測(schema 註解沒寫,但資料自洽)'
\echo '  應有值:margin 兩條恆等式 0/8,341,265;sbl 用 + adjustment(38 例外)vs - adjustment(6,730 例外)'
\echo '=============================================================='
SELECT '融資餘額 = 前日 + 買進 - 賣出 - 現償' AS chk, count(*) AS n,
       count(*) FILTER (WHERE margin_balance_of_the_day <> margin_balance_of_previous_day
                          + margin_purchase - margin_sales - cash_redemption) AS bad FROM margin_transactions
UNION ALL SELECT '融券餘額 = 前日 + 賣出 - 買進 - 券償', count(*),
       count(*) FILTER (WHERE short_balance_of_the_day <> short_balance_of_previous_day
                          + short_sale - short_covering - stock_redemption) FROM margin_transactions
UNION ALL SELECT '借券餘額 = 前日 + 賣出 - 還券 + 調整', count(*),
       count(*) FILTER (WHERE daily_balance <> prev_day_balance + daily_sold - daily_returned + daily_adjustment) FROM sbl_borrowing
UNION ALL SELECT '借券餘額 = 前日 + 賣出 - 還券 - 調整(對照組)', count(*),
       count(*) FILTER (WHERE daily_balance <> prev_day_balance + daily_sold - daily_returned - daily_adjustment) FROM sbl_borrowing
UNION ALL SELECT '外資持股比率 = 持股/發行 x 100', count(*),
       count(*) FILTER (WHERE outstanding_shares > 0
                          AND abs(foreign_held_ratio - 100.0*foreign_held_shares/outstanding_shares) > 0.02) FROM foreign_holding_ratio;

\echo ''
\echo '=============================================================='
\echo 'OK-4  ex_right_dividend 唯一索引粒度正確(TWSE 同日除權+除息併成「權息」一列)'
\echo '=============================================================='
SELECT right_or_dividend, count(*) FROM ex_right_dividend GROUP BY 1 ORDER BY 2 DESC;

\echo ''
\echo '=============================================================='
\echo 'OK-5  double precision 對財報金額無精度損失(2^53 = 9,007,199,254,740,992)'
\echo '=============================================================='
SELECT 'concise_balance_sheet'                AS t, max(abs(value))::text FROM concise_balance_sheet
UNION ALL SELECT 'concise_income_statement_progressive', max(abs(value))::text FROM concise_income_statement_progressive
UNION ALL SELECT 'cash_flows_progressive',               max(abs(value))::text FROM cash_flows_progressive
UNION ALL SELECT '2^53 (double 可精確表示的整數上限)',    '9007199254740992';

\echo ''
\echo '=============================================================='
\echo 'OK-6  company_code 正規化(0 列長度 < 4、0 列含前後空白)'
\echo '=============================================================='
SELECT 'daily_quote'           AS t, count(*) FILTER (WHERE length(company_code)<4) AS short_code,
       count(*) FILTER (WHERE company_code <> trim(company_code)) AS spaced FROM daily_quote
UNION ALL SELECT 'operating_revenue', count(*) FILTER (WHERE length(company_code)<4),
       count(*) FILTER (WHERE company_code <> trim(company_code)) FROM operating_revenue
UNION ALL SELECT 'concise_balance_sheet', count(*) FILTER (WHERE length(company_code)<4),
       count(*) FILTER (WHERE company_code <> trim(company_code)) FROM concise_balance_sheet
UNION ALL SELECT 'cash_flows_progressive', count(*) FILTER (WHERE length(company_code)<4),
       count(*) FILTER (WHERE company_code <> trim(company_code)) FROM cash_flows_progressive
UNION ALL SELECT 'margin_transactions', count(*) FILTER (WHERE length(company_code)<4),
       count(*) FILTER (WHERE company_code <> trim(company_code)) FROM margin_transactions;

\echo ''
\echo '=============================================================='
\echo 'OK-7  NOT NULL 日期欄無哨兵值'
\echo '=============================================================='
SELECT 'treasury_stock_buyback 期間異常' AS chk,
       count(*) FILTER (WHERE period_start < '1990-01-01' OR period_end < '1990-01-01' OR period_end < period_start) AS bad,
       count(*) AS n FROM treasury_stock_buyback
UNION ALL SELECT 'insider_holding 申報日異常',
       count(*) FILTER (WHERE declare_date < '1990-01-01' OR declare_date > report_date), count(*) FROM insider_holding;

\echo ''
\echo '=============================================================='
\echo 'REAL-1  三大法人恆等式:2015 年以前大量不成立(reader 欄位錯位,非 schema 定義錯)'
\echo '        應有值:2015 起 twse/tpex 皆 0(唯二例外 = BUG-3 的 int32 溢位兩列)'
\echo '=============================================================='
SELECT market, extract(year from date)::int AS yr, count(*) AS n,
       count(*) FILTER (WHERE dealers_difference::bigint <> dealers_total_buy::bigint - dealers_total_sell::bigint) AS bad_dealers,
       count(*) FILTER (WHERE total_difference::bigint <> foreign_investors_difference::bigint
                          + securities_investment_trust_companies_difference::bigint
                          + dealers_difference::bigint) AS bad_total
FROM daily_trading_details GROUP BY 1,2 ORDER BY 1,2;
