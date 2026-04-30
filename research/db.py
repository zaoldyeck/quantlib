"""DuckDB connection helper that attaches the existing PostgreSQL quantlib DB.

Zero ETL: `con.sql("SELECT * FROM pg.public.daily_quote")` pulls rows directly
from Postgres into DuckDB's columnar engine. All joins / window functions /
aggregations happen in DuckDB (vectorized + multi-threaded).

Views expose BOTH TWSE + TPEx rows. Research scripts must apply explicit
`WHERE market='twse'` or `='tpex'` filters at query time.
"""
import os
import duckdb

DEFAULT_DSN = os.environ.get(
    "QL_PG_DSN",
    f"host=localhost port=5432 dbname=quantlib user={os.environ.get('USER', 'zaoldyeck')}"
)


CACHE_DB = os.path.join(os.path.dirname(__file__), "cache.duckdb")
RAW_QUARTERLY_PARQUET = os.path.join(os.path.dirname(__file__), "raw_quarterly.parquet")


def _register_raw_quarterly(con: duckdb.DuckDBPyConnection) -> None:
    """Register raw_quarterly first-principles factors as DuckDB view if parquet exists.

    Computed by `python research/strat_lab/raw_quarterly.py` from raw IS/BS/CF base tables.
    Replaces dependency on PG views (financial_index_quarterly, growth_analysis_ttm).
    """
    if os.path.exists(RAW_QUARTERLY_PARQUET):
        con.sql(
            f"CREATE OR REPLACE VIEW raw_quarterly AS "
            f"SELECT * FROM read_parquet('{RAW_QUARTERLY_PARQUET}')"
        )


def connect(dsn: str = DEFAULT_DSN, read_only: bool = True,
            use_cache: bool = True) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection. If use_cache=True and `research/cache.duckdb`
    exists, open it directly (millisecond queries). Otherwise attach live
    PostgreSQL as `pg` (slow — for ad-hoc cross-table queries only).

    Auto-registers `raw_quarterly` view from research/raw_quarterly.parquet
    (first-principles quality/growth factors derived from raw IS/BS/CF tables —
    NO PG VIEW dependency). Rebuild via `python research/strat_lab/raw_quarterly.py`.

    To rebuild cache: `uv run python research/cache_tables.py`
    """
    if use_cache and os.path.exists(CACHE_DB):
        # read_only=False so we can register temp in-memory frames; file itself is not modified
        con = duckdb.connect(CACHE_DB, read_only=False)
        con.sql("SET memory_limit = '8GB'")
        _register_raw_quarterly(con)
        return con

    con = duckdb.connect()
    con.sql("INSTALL postgres; LOAD postgres;")
    readonly = ", READ_ONLY" if read_only else ""
    con.sql(f"ATTACH '{dsn}' AS pg (TYPE postgres{readonly})")
    con.sql("SET memory_limit = '8GB'")
    # Register views matching cache table names (so same SQL works with/without cache).
    # stock_per_pbr in cache = stock_per_pbr_dividend_yield in pg.
    con.sql("CREATE OR REPLACE VIEW daily_quote AS "
            "SELECT market, date, company_code, "
            "       opening_price, highest_price, lowest_price, closing_price, "
            "       trade_volume, trade_value, "
            "       last_best_bid_price, last_best_ask_price "
            "FROM pg.public.daily_quote")
    con.sql("CREATE OR REPLACE VIEW stock_per_pbr AS "
            "SELECT market, date, company_code, price_book_ratio, "
            "       dividend_yield, price_to_earning_ratio "
            "FROM pg.public.stock_per_pbr_dividend_yield")
    # NOTE: growth_analysis_ttm view removed (PG VIEW with hand-written F-Score logic
    # we don't trust). Use first-principles `raw_quarterly` view instead, which provides:
    #   f_score_raw     — Piotroski F9 from concise_balance_sheet + concise_income_statement
    #                     + cash_flows_progressive
    #   gross_margin_q  — standalone quarterly gross margin (verified: TSMC 2024Q4 = 0.59)
    #   operating_margin_q, net_margin_q, roa_ttm, asset_turnover_ttm, current_ratio,
    #   lt_debt_ratio, cfo_ni_ratio_ttm
    # Built via `python research/strat_lab/raw_quarterly.py`.
    con.sql("CREATE OR REPLACE VIEW ex_right_dividend AS "
            "SELECT market, date, company_code, cash_dividend "
            "FROM pg.public.ex_right_dividend WHERE cash_dividend > 0")
    con.sql("CREATE OR REPLACE VIEW capital_reduction AS "
            "SELECT market, date, company_code, post_reduction_reference_price, "
            "       reason_for_capital_reduction "
            "FROM pg.public.capital_reduction")
    con.sql('CREATE OR REPLACE VIEW operating_revenue AS '
            'SELECT market, type, year, month, company_code, company_name, industry, '
            '       monthly_revenue, "monthly_revenue_compared_last_year(%))" AS monthly_revenue_yoy '
            'FROM pg.public.operating_revenue')
    con.sql("CREATE OR REPLACE VIEW daily_trading_details AS "
            "SELECT market, date, company_code, foreign_investors_difference, "
            "       securities_investment_trust_companies_difference AS trust_difference, "
            "       dealers_difference, total_difference "
            "FROM pg.public.daily_trading_details")
    con.sql("CREATE OR REPLACE VIEW margin_transactions AS "
            "SELECT market, date, company_code, "
            "       margin_balance_of_the_day AS margin_balance, "
            "       short_balance_of_the_day  AS short_balance, "
            "       margin_quota, short_quota "
            "FROM pg.public.margin_transactions")
    con.sql("CREATE OR REPLACE VIEW etf AS SELECT company_code FROM pg.public.etf")
    con.sql("CREATE OR REPLACE VIEW tdcc_shareholding AS "
            "SELECT data_date, company_code, holding_tier, num_holders, "
            "       num_shares, pct_of_outstanding "
            "FROM pg.public.tdcc_shareholding")
    con.sql("CREATE OR REPLACE VIEW sbl_borrowing AS "
            "SELECT market, date, company_code, prev_day_balance, daily_sold, "
            "       daily_returned, daily_adjustment, daily_balance, next_day_limit "
            "FROM pg.public.sbl_borrowing")
    con.sql("CREATE OR REPLACE VIEW foreign_holding_ratio AS "
            "SELECT market, date, company_code, outstanding_shares, "
            "       foreign_remaining_shares, foreign_held_shares, "
            "       foreign_remaining_ratio, foreign_held_ratio, foreign_limit_ratio "
            "FROM pg.public.foreign_holding_ratio")
    # Sprint B (MOPS structured filings) — parity with cache_tables.py
    con.sql("CREATE OR REPLACE VIEW treasury_stock_buyback AS "
            "SELECT market, announce_date, company_code, company_name, "
            "       planned_shares, price_low, price_high, period_start, period_end, "
            "       executed_shares, pct_of_capital "
            "FROM pg.public.treasury_stock_buyback")
    con.sql("CREATE OR REPLACE VIEW insider_holding AS "
            "SELECT market, report_date, declare_date, company_code, company_name, "
            "       reporter_title, reporter_name, transfer_method, transferee, "
            "       transfer_shares, max_intraday_shares, "
            "       current_shares_own, current_shares_trust, "
            "       planned_shares_own, planned_shares_trust "
            "FROM pg.public.insider_holding")
    # First-principles raw base tables (long-form line items)
    con.sql("CREATE OR REPLACE VIEW is_progressive_raw AS "
            "SELECT market, type, year, quarter, company_code, title, value "
            "FROM pg.public.concise_income_statement_progressive "
            "WHERE market IN ('twse','tpex') AND type='consolidated'")
    con.sql("CREATE OR REPLACE VIEW bs_concise_raw AS "
            "SELECT market, type, year, quarter, company_code, title, value "
            "FROM pg.public.concise_balance_sheet "
            "WHERE market IN ('twse','tpex') AND type='consolidated'")
    con.sql("CREATE OR REPLACE VIEW cf_progressive_raw AS "
            "SELECT market, year, quarter, company_code, title, value "
            "FROM pg.public.cash_flows_progressive WHERE market='tw'")
    _register_raw_quarterly(con)
    return con


if __name__ == "__main__":
    # Smoke test
    con = connect()
    print(con.sql("SELECT COUNT(*) FROM pg.public.daily_quote").pl())
