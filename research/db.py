"""DuckDB connection helper that attaches the existing PostgreSQL quantlib DB.

Zero ETL: `con.sql("SELECT * FROM pg.public.daily_quote")` pulls rows directly
from Postgres into DuckDB's columnar engine. All joins / window functions /
aggregations happen in DuckDB (vectorized + multi-threaded).

Views expose BOTH TWSE + TPEx rows. Research scripts must apply explicit
`WHERE market='twse'` or `='tpex'` filters at query time.
"""
import os
import duckdb

from research.industry_taxonomy import build_industry_taxonomy_pit
from research import paths

DEFAULT_DSN = os.environ.get(
    "QL_PG_DSN",
    f"host=localhost port=5432 dbname=quantlib user={os.environ.get('USER', 'zaoldyeck')}"
)


CACHE_DB = str(paths.CACHE_DB)
RAW_QUARTERLY_PARQUET = os.path.join(os.path.dirname(__file__), "raw_quarterly.parquet")


def _configure_connection(con: duckdb.DuckDBPyConnection) -> None:
    """Apply local performance defaults without changing query semantics."""
    threads = int(os.environ.get("QL_DUCKDB_THREADS", os.cpu_count() or 1))
    memory_limit = os.environ.get("QL_DUCKDB_MEMORY_LIMIT", "8GB")
    con.sql(f"SET memory_limit = '{memory_limit}'")
    con.sql(f"SET threads = {max(1, threads)}")
    try:
        con.sql("SET preserve_insertion_order = false")
    except duckdb.Error:
        pass


def _register_raw_quarterly(con: duckdb.DuckDBPyConnection) -> None:
    """Register raw_quarterly first-principles factors as a session-local view.

    Computed by `python research/strat_lab/raw_quarterly.py` from raw IS/BS/CF base tables.
    Replaces dependency on PG views (financial_index_quarterly, growth_analysis_ttm).

    This must stay TEMP so read-only cache connections remain process-parallel.
    """
    if os.path.exists(RAW_QUARTERLY_PARQUET):
        con.sql(
            f"CREATE OR REPLACE TEMP VIEW raw_quarterly AS "
            f"SELECT * FROM read_parquet('{RAW_QUARTERLY_PARQUET}')"
        )


def _register_industry_taxonomy(con: duckdb.DuckDBPyConnection) -> None:
    """Expose canonical PIT industry taxonomy on older caches or live PG views."""
    try:
        con.sql("SELECT 1 FROM industry_taxonomy_pit LIMIT 1")
        return
    except duckdb.Error:
        pass

    taxonomy = build_industry_taxonomy_pit(con)
    con.register("_industry_taxonomy_pit_df", taxonomy)
    con.sql(
        "CREATE OR REPLACE TEMP VIEW industry_taxonomy_pit AS "
        "SELECT * FROM _industry_taxonomy_pit_df"
    )


def connect(dsn: str = DEFAULT_DSN, read_only: bool = True,
            use_cache: bool = True,
            register_raw_quarterly: bool = True) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection. If use_cache=True and `var/cache/cache.duckdb`
    exists, open it directly (millisecond queries). Otherwise attach live
    PostgreSQL as `pg` (slow — for ad-hoc cross-table queries only).

    Auto-registers `raw_quarterly` view from research/raw_quarterly.parquet
    (first-principles quality/growth factors derived from raw IS/BS/CF tables —
    NO PG VIEW dependency). Rebuild via `python research/strat_lab/raw_quarterly.py`.

    To rebuild cache: `uv run python research/cache_tables.py`
    """
    if use_cache and os.path.exists(CACHE_DB):
        con = duckdb.connect(CACHE_DB, read_only=read_only)
        _configure_connection(con)
        if register_raw_quarterly:
            _register_raw_quarterly(con)
        _register_industry_taxonomy(con)
        return con

    con = duckdb.connect()
    con.sql("INSTALL postgres; LOAD postgres;")
    readonly = ", READ_ONLY" if read_only else ""
    con.sql(f"ATTACH '{dsn}' AS pg (TYPE postgres{readonly})")
    _configure_connection(con)
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
    con.sql('CREATE OR REPLACE VIEW market_index AS '
            'SELECT market, date, name, close, change, "change(%)" AS change_pct '
            'FROM pg.public."index"')
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
    con.sql("CREATE OR REPLACE VIEW taifex_futures_daily AS "
            "SELECT date, contract_code, contract_month, open, high, low, close, change, change_pct, "
            "       volume, settlement_price, open_interest, best_bid, best_ask, historical_high, historical_low, "
            "       trading_halt, trading_session, spread_single_volume "
            "FROM pg.public.taifex_futures_daily")
    con.sql("CREATE OR REPLACE VIEW taifex_futures_institutional AS "
            "SELECT date, contract_code, product_name, investor_type, long_volume, long_value_thousands, "
            "       short_volume, short_value_thousands, net_volume, net_value_thousands, "
            "       long_open_interest, long_oi_value_thousands, short_open_interest, short_oi_value_thousands, "
            "       net_open_interest, net_oi_value_thousands "
            "FROM pg.public.taifex_futures_institutional")
    con.sql("CREATE OR REPLACE VIEW taifex_futures_final_settlement AS "
            "SELECT date, contract_code, contract_month, final_settlement_price "
            "FROM pg.public.taifex_futures_final_settlement")
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
    _register_industry_taxonomy(con)
    return con


if __name__ == "__main__":
    # Smoke test
    con = connect()
    print(con.sql("SELECT COUNT(*) FROM pg.public.daily_quote").pl())
