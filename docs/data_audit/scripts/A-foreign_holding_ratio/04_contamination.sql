-- A-foreign_holding_ratio 佐證 SQL:單位不變式 / 汙染日 / 幽靈日 / 缺日
-- psql -h localhost -p 5432 -d quantlib -f docs/data_audit/scripts/A-foreign_holding_ratio/04_contamination.sql
-- 實測全檔約 1 分鐘(2026-07-22)。所有相關子查詢都改寫成集合 JOIN,避免逐列 NOT EXISTS。

\timing on

\echo ''
\echo '(1) 單位與欄位對位不變式:持股比率 == 100 * 持有股數 / 發行股數'
\echo '    期望:max_err <= 0.01(來源四捨五入到小數 2 位)、violations = 0、neg_shares = 0'
SELECT market,
       count(*) AS n,
       max(abs(foreign_held_ratio - 100.0 * foreign_held_shares / NULLIF(outstanding_shares, 0))) AS max_err,
       count(*) FILTER (WHERE abs(foreign_held_ratio - 100.0 * foreign_held_shares / NULLIF(outstanding_shares, 0)) > 0.02) AS violations,
       count(*) FILTER (WHERE foreign_held_shares < 0 OR foreign_remaining_shares < 0) AS neg_shares
FROM foreign_holding_ratio GROUP BY market;

\echo ''
\echo '(2) 整日內容與前一日完全相同(stale snapshot 指紋)'
\echo '    期望:只有 tpex 360 天(2010-01-05 ~ 2010-12-31),twse 0 天'
WITH s AS (SELECT market, date, count(*) n, sum(foreign_held_shares) h, sum(outstanding_shares) o
           FROM foreign_holding_ratio GROUP BY 1, 2),
     l AS (SELECT market, date, n, h, o,
                  lag(n) OVER w pn, lag(h) OVER w ph, lag(o) OVER w po
           FROM s WINDOW w AS (PARTITION BY market ORDER BY date))
SELECT market, count(*) AS identical_to_prev_day, min(date), max(date)
FROM l WHERE n = pn AND h = ph AND o = po GROUP BY market;

\echo ''
\echo '(3) TPEx 2010 汙染:與 2026-04-24 快照逐檔同值?期望 884 / 884'
SELECT count(*) AS common_codes,
       count(*) FILTER (WHERE a.outstanding_shares = b.outstanding_shares
                          AND a.foreign_held_shares = b.foreign_held_shares
                          AND a.foreign_held_ratio = b.foreign_held_ratio) AS identical
FROM foreign_holding_ratio a
JOIN foreign_holding_ratio b USING (market, company_code)
WHERE a.market = 'tpex' AND a.date = '2010-06-15' AND b.date = '2026-04-24';

\echo ''
\echo '(4) TPEx 2010 汙染:快照裡有多少檔在 2010 年根本還沒掛牌?期望 884 檔中 412 檔'
WITH c AS (SELECT DISTINCT company_code FROM foreign_holding_ratio WHERE market = 'tpex' AND date = '2010-06-15'),
     q AS (SELECT company_code, min(date) AS first_quote FROM daily_quote GROUP BY 1)
SELECT count(*) AS codes_in_snapshot,
       count(*) FILTER (WHERE q.first_quote IS NULL OR q.first_quote >= '2011-01-01') AS never_listed_in_2010
FROM c LEFT JOIN q USING (company_code);

\echo ''
\echo '(5) 幽靈日:fhr 有資料但 daily_quote 該市場當天沒報價'
\echo '    期望:tpex 112 天 = 111 天在 2010 汙染區 + 2023-06-08(那天是 daily_quote/tpex 自己缺料,'
\echo '    fhr 內容日期 112/06/08、815 檔是對的,同日 twse 有 1,194 檔報價);twse 18 天全是農曆年前的申報公告日,非汙染'
WITH f AS (SELECT DISTINCT market, date FROM foreign_holding_ratio),
     q AS (SELECT DISTINCT market, date FROM daily_quote)
SELECT f.market, count(*) AS ghost_days, min(f.date), max(f.date)
FROM f LEFT JOIN q USING (market, date)
WHERE q.date IS NULL GROUP BY f.market;

\echo ''
\echo '(6) 缺日:真交易日(任一市場有報價)但兩個市場都沒有 fhr。期望 4 天'
WITH d AS (SELECT DISTINCT date FROM daily_quote WHERE date >= '2011-01-01'),
     f AS (SELECT DISTINCT date FROM foreign_holding_ratio WHERE date >= '2011-01-01')
SELECT d.date, to_char(d.date, 'Dy') AS dow
FROM d LEFT JOIN f USING (date)
WHERE f.date IS NULL ORDER BY 1;
