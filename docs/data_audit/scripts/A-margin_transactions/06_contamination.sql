-- A-margin_transactions #6 — 汙染日與缺日掃描(全部可直接重跑)
--   psql -h localhost -p 5432 -d quantlib -f docs/data_audit/scripts/A-margin_transactions/06_contamination.sql
-- 2026-07-22 實測結果寫在每段註解裡。

\echo '--- (1) 內容指紋重複:同一份資料被掛在兩個日期'
-- 結果:9 對。tpex 7 對全是「颱風/休市日沿用前一交易日」;
--       twse 2 對是「檔名日期 != 內容日期」(2003-09-12 裝的是 09-18、2011-03-26 裝的是 2017-12-18)。
WITH f AS (
  SELECT market, date,
         md5(string_agg(company_code || ':' || margin_purchase || ':' ||
                        margin_balance_of_the_day || ':' || short_balance_of_the_day,
                        ',' ORDER BY company_code)) AS fp,
         count(*) AS n
  FROM margin_transactions GROUP BY 1, 2
)
SELECT a.market, a.date AS date_a, b.date AS date_b, a.n
FROM f a JOIN f b ON a.market = b.market AND a.fp = b.fp AND a.date < b.date
ORDER BY 1, 2;

\echo '--- (2) 幽靈交易日:margin 有資料,但當天全市場沒有任何報價(=休市)'
-- 結果:8 天 5,028 列(twse 2011-03-26;tpex 2012-08-02 / 2014-07-23 / 2015-07-10 /
--       2015-09-29 / 2016-07-08 / 2016-09-27 / 2016-09-28)。
WITH td AS (SELECT DISTINCT date FROM daily_quote)
SELECT m.market, m.date, to_char(m.date, 'Dy') AS dow, count(*) AS n
FROM margin_transactions m LEFT JOIN td ON td.date = m.date
WHERE td.date IS NULL AND m.date >= '2007-07-02'   -- daily_quote 兩市場都有資料之後才可比
GROUP BY 1, 2, 3 ORDER BY 2;

\echo '--- (3) 缺日:真的有開市,但 margin 一列都沒有'
-- 結果:11 天(twse 10 + tpex 1)。原始檔存在但只有標頭/4 bytes,
--       且 readMarginTransactions 的「(market,檔名) 已在 DB 就跳過」規則不會重試。
WITH q AS (SELECT DISTINCT market, date FROM daily_quote),
     m AS (SELECT DISTINCT market, date FROM margin_transactions)
SELECT q.market, q.date, to_char(q.date, 'Dy') AS dow
FROM q LEFT JOIN m USING (market, date) WHERE m.date IS NULL ORDER BY 1, 2;

\echo '--- (4) 券限額不變式:餘額不可能超過自己的限額'
-- 結果:era B(2007-06-01~2008-09-26)違反 65,821 / 135,679 = 48.5%;
--       era A 0/38,718;2009 年後 6,713/2,735,985 = 0.25%;twse 8,065/5,403,352 = 0.15%。
--       → era B 的 short_quota 不是券限額(是被錯放的「資券相抵」)。
SELECT 'tpex eraA 2007-01..05' AS era, count(*) AS n,
       sum((short_balance_of_the_day > short_quota)::int) AS bal_gt_quota
FROM margin_transactions WHERE market = 'tpex' AND date BETWEEN '2007-01-01' AND '2007-05-31'
UNION ALL SELECT 'tpex eraB 2007-06..2008-09-29', count(*),
       sum((short_balance_of_the_day > short_quota)::int)
FROM margin_transactions WHERE market = 'tpex' AND date BETWEEN '2007-06-01' AND '2008-09-29'
UNION ALL SELECT 'tpex eraC 2009+', count(*),
       sum((short_balance_of_the_day > short_quota)::int)
FROM margin_transactions WHERE market = 'tpex' AND date >= '2009-01-01'
UNION ALL SELECT 'twse all', count(*),
       sum((short_balance_of_the_day > short_quota)::int)
FROM margin_transactions WHERE market = 'twse';

\echo '--- (5) 逐月觀察 tpex 券限額 / 資券相抵 的斷層(肉眼即可判讀)'
-- 2007-04 起 avg_off 掉到 0.00(來源端把資券相抵清成 0);
-- 2007-06~2008-09 avg_sq 掉到 54~231(正常是 3.5 萬),2008-10 起回到 1.4 萬、2009-01 起回到 3.5 萬。
SELECT to_char(date, 'YYYY-MM') AS ym, count(*) AS n,
       sum((short_balance_of_the_day > short_quota)::int) AS bal_gt_quota,
       sum((short_quota = margin_quota)::int) AS sq_eq_mq,
       round(avg(short_quota)) AS avg_sq, round(avg(margin_quota)) AS avg_mq,
       round(avg(offsetting_of_margin_purchases_and_short_sales), 2) AS avg_off
FROM margin_transactions
WHERE market = 'tpex' AND date BETWEEN '2007-01-01' AND '2009-06-30'
GROUP BY 1 ORDER BY 1;
