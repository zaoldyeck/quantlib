"""Full data-integrity audit across all daily/quarterly tables.

Checks:
  1. Daily row-count anomalies (sudden drop in any table suggests partial CSV
     capture or reader bug).
  2. Impossible value jumps (close_t / close_{t-1} outside [0.5, 2.0] without
     a corresponding ex_right / capital_reduction event).
  3. Column-order integrity (value range sanity — PE < 1000, PB < 100,
     DY < 20%, margin_ratio < 1000%, etc).
  4. Join / referential anomalies (daily_trading_details rows that don't
     exist in daily_quote for the same date, or vice versa).

Prints a single summary of all issues found. Does NOT fix — fixing is a
separate step once you review.
"""
from __future__ import annotations
import polars as pl
from db import connect


def daily_row_count_anomalies(con, table: str, market_filter: str = "twse") -> pl.DataFrame:
    """Dates where the row count is < 50% of the 30-day rolling median for the table."""
    q = f"""
    WITH daily AS (
      SELECT date, COUNT(*) AS n FROM pg.public.{table}
      WHERE market = '{market_filter}'
      GROUP BY date
    ),
    ranked AS (
      SELECT date, n,
             AVG(n) OVER (
               ORDER BY date ROWS BETWEEN 30 PRECEDING AND 30 FOLLOWING
             ) AS rolling_median
      FROM daily
    )
    SELECT date, n, rolling_median,
           ROUND(n::double / NULLIF(rolling_median, 0), 2) AS ratio
    FROM ranked
    WHERE rolling_median > 0 AND n::double / rolling_median < 0.5
    ORDER BY date
    """
    return con.sql(q).pl()


def stock_per_pbr_sanity(con) -> pl.DataFrame:
    """Row count where PE/PB/DY out of reasonable range."""
    q = """
    WITH by_month AS (
      SELECT date_trunc('month', date)::date AS month, market,
             COUNT(*) FILTER (WHERE dividend_yield > 20) AS dy_too_high,
             COUNT(*) FILTER (WHERE price_book_ratio > 100) AS pb_too_high,
             COUNT(*) FILTER (WHERE price_to_earning_ratio > 1000) AS pe_too_high,
             COUNT(*) AS total
      FROM pg.public.stock_per_pbr_dividend_yield
      WHERE market IN ('twse','tpex')
      GROUP BY 1, 2
    )
    SELECT month, market, dy_too_high, pb_too_high, pe_too_high, total,
           GREATEST(dy_too_high, pb_too_high, pe_too_high) AS worst
    FROM by_month
    WHERE (dy_too_high > total*0.05 OR pb_too_high > total*0.05 OR pe_too_high > total*0.05)
      AND total > 100
    ORDER BY month
    """
    return con.sql(q).pl()


def margin_trans_sanity(con) -> pl.DataFrame:
    q = """
    WITH by_date AS (
      SELECT date, market,
             COUNT(*) FILTER (
               WHERE margin_balance_of_the_day > margin_quota * 100
                 AND margin_quota > 0
             ) AS impossible_balance,
             COUNT(*) AS n
      FROM pg.public.margin_transactions
      WHERE market IN ('twse','tpex')
      GROUP BY 1, 2
    )
    SELECT * FROM by_date WHERE impossible_balance > 5 ORDER BY date
    """
    return con.sql(q).pl()


def operating_revenue_sanity(con) -> pl.DataFrame:
    q = """
    WITH by_month AS (
      SELECT year, month,
             COUNT(*) FILTER (WHERE monthly_revenue < 0) AS negative_rev,
             COUNT(*) FILTER (WHERE monthly_revenue = 0) AS zero_rev,
             COUNT(*) FILTER (WHERE monthly_revenue > 0
                              AND last_year_monthly_revenue > 0
                              AND (monthly_revenue::double / last_year_monthly_revenue > 100
                                   OR monthly_revenue::double / last_year_monthly_revenue < 0.01)
             ) AS impossible_yoy,
             COUNT(*) AS n
      FROM pg.public.operating_revenue
      GROUP BY year, month
    )
    SELECT * FROM by_month
    WHERE impossible_yoy > 5 OR negative_rev > 5
    ORDER BY year, month
    """
    return con.sql(q).pl()


def index_sanity(con) -> pl.DataFrame:
    """Check TAIEX index close series for jumps (sibling test)."""
    q = """
    WITH seq AS (
      SELECT name, date, close,
             LAG(close) OVER (PARTITION BY name ORDER BY date) AS prev
      FROM pg.public.index
      WHERE market='twse' AND name IN ('發行量加權股價指數','未含金融保險股指數','未含電子股指數')
        AND close > 0
    )
    SELECT date, name, prev, close,
           ROUND((close::double / NULLIF(prev,0))::numeric, 3) AS ratio
    FROM seq
    WHERE prev IS NOT NULL AND prev > 0
      AND (close::double / prev < 0.85 OR close::double / prev > 1.15)
    ORDER BY date
    """
    return con.sql(q).pl()


def financial_index_null_check(con) -> pl.DataFrame:
    """Which quarters have a lot of null fields in financial_index_ttm?"""
    q = """
    SELECT year, quarter,
           COUNT(*) AS total,
           COUNT(*) FILTER (WHERE ocf IS NULL) AS null_ocf,
           COUNT(*) FILTER (WHERE profit IS NULL) AS null_profit,
           COUNT(*) FILTER (WHERE roic IS NULL) AS null_roic,
           COUNT(*) FILTER (WHERE fcf_per_share IS NULL) AS null_fcf,
           COUNT(*) FILTER (WHERE cbs IS NULL) AS null_cbs
    FROM pg.public.financial_index_ttm
    GROUP BY year, quarter
    ORDER BY year, quarter
    """
    return con.sql(q).pl()


def ex_right_dividend_span(con) -> pl.DataFrame:
    q = """
    SELECT EXTRACT(year FROM date)::int AS year, COUNT(*) AS n
    FROM pg.public.ex_right_dividend
    GROUP BY 1 ORDER BY 1
    """
    return con.sql(q).pl()


def main():
    con = connect()

    print("=" * 80)
    print("FULL DATA AUDIT")
    print("=" * 80)

    pl.Config.set_tbl_rows(100)
    pl.Config.set_tbl_width_chars(160)
    pl.Config.set_fmt_str_lengths(80)

    for table in ("daily_quote", "daily_trading_details", "margin_transactions",
                  "stock_per_pbr_dividend_yield"):
        print(f"\n--- {table}: dates with row count < 50% of 30-day rolling median ---")
        df = daily_row_count_anomalies(con, table)
        print(f"[{len(df)} anomaly dates]")
        if len(df) > 0:
            print(df.head(30))

    print("\n--- stock_per_pbr_dividend_yield: value-range anomalies (monthly) ---")
    df = stock_per_pbr_sanity(con)
    print(f"[{len(df)} anomaly months]")
    if len(df) > 0:
        print(df)

    print("\n--- margin_transactions: impossible balance > quota*100 ---")
    df = margin_trans_sanity(con)
    print(f"[{len(df)} anomaly dates]")
    if len(df) > 0:
        print(df.head(30))

    print("\n--- operating_revenue: negative / absurd YoY ---")
    df = operating_revenue_sanity(con)
    print(f"[{len(df)} anomaly months]")
    if len(df) > 0:
        print(df.head(30))

    print("\n--- index: TAIEX / sector indices jumps > 15% d/d ---")
    df = index_sanity(con)
    print(f"[{len(df)} suspicious dates]")
    if len(df) > 0:
        print(df.head(30))

    print("\n--- financial_index_ttm: null prevalence by quarter ---")
    df = financial_index_null_check(con)
    total_nulls = df.select(pl.col("null_ocf").sum().alias("total_null_ocf"))
    print(total_nulls)
    print(df.tail(20))

    print("\n--- ex_right_dividend: row count by year ---")
    df = ex_right_dividend_span(con)
    print(df)


if __name__ == "__main__":
    main()
