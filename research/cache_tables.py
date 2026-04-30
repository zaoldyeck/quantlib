"""One-time copy: PostgreSQL → local DuckDB file. Fast subsequent queries.

Cache holds BOTH TWSE + TPEx rows (no market filter at cache level).
Research scripts must apply explicit `WHERE market='twse'` or `='tpex'` filters
at query time depending on universe choice. This is intentional: lifting the
market filter upstream lets any new strategy opt into either or both markets
without rebuilding cache.
"""
import duckdb
import os
import time

DB_PATH = "research/cache.duckdb"
PG_DSN = os.environ.get("QL_PG_DSN",
    f"host=localhost port=5432 dbname=quantlib user={os.environ.get('USER', 'zaoldyeck')}")

def main():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    con = duckdb.connect(DB_PATH)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")

    tables = [
        # daily_quote: pull OHLC + volume + spread for full talib indicator support.
        # Bid/ask spread (last_best_bid_price - last_best_ask_price) is a microstructure
        # liquidity proxy. Adds ~30MB to cache vs slim version.
        ("daily_quote",
         "SELECT market, date, company_code, "
         "       opening_price, highest_price, lowest_price, closing_price, "
         "       trade_volume, trade_value, "
         "       last_best_bid_price, last_best_ask_price "
         "FROM pg.public.daily_quote"),
        ("stock_per_pbr",        "SELECT market, date, company_code, price_book_ratio, dividend_yield, price_to_earning_ratio FROM pg.public.stock_per_pbr_dividend_yield"),
        # REMOVED: growth_analysis_ttm (was a PG VIEW with hand-written F-Score derivation
        # we couldn't fully verify). See research/strat_lab/raw_quarterly.py for the
        # first-principles replacement (Piotroski F9 from raw IS+BS+CF).
        ("ex_right_dividend",    "SELECT market, date, company_code, cash_dividend FROM pg.public.ex_right_dividend WHERE cash_dividend > 0"),
        ("capital_reduction",    "SELECT market, date, company_code, post_reduction_reference_price, reason_for_capital_reduction FROM pg.public.capital_reduction"),
        ("operating_revenue",    'SELECT market, type, year, month, company_code, company_name, industry, monthly_revenue, "monthly_revenue_compared_last_year(%))" AS monthly_revenue_yoy FROM pg.public.operating_revenue'),
        ("daily_trading_details", "SELECT market, date, company_code, foreign_investors_difference, securities_investment_trust_companies_difference AS trust_difference, dealers_difference, total_difference FROM pg.public.daily_trading_details"),
        ("margin_transactions",  "SELECT market, date, company_code, margin_balance_of_the_day AS margin_balance, short_balance_of_the_day AS short_balance, margin_quota, short_quota FROM pg.public.margin_transactions"),
        ("etf",                  "SELECT company_code FROM pg.public.etf"),
        # Sprint A additions (籌碼面 signal sources).
        # TDCC: weekly per-tier holder distribution (smart-money signal = tier 15 千張大戶).
        # SBL:  daily institutional securities-borrowing balance (squeeze signal).
        ("tdcc_shareholding",    "SELECT data_date, company_code, holding_tier, num_holders, num_shares, pct_of_outstanding FROM pg.public.tdcc_shareholding"),
        ("sbl_borrowing",        "SELECT market, date, company_code, prev_day_balance, daily_sold, daily_returned, daily_adjustment, daily_balance, next_day_limit FROM pg.public.sbl_borrowing"),
        # 外資及陸資持股比率 (snapshot, daily). foreign_held_ratio is the key signal for 外資接頂 detection.
        ("foreign_holding_ratio", "SELECT market, date, company_code, outstanding_shares, foreign_remaining_shares, foreign_held_shares, foreign_remaining_ratio, foreign_held_ratio, foreign_limit_ratio FROM pg.public.foreign_holding_ratio"),
        # Sprint B (MOPS structured filings). Event-style sparse data — typically <50K rows total.
        # 庫藏股: company-announced share buyback. +3-5% same-day signal; floor support indicator.
        ("treasury_stock_buyback", "SELECT market, announce_date, company_code, company_name, planned_shares, price_low, price_high, period_start, period_end, executed_shares, pct_of_capital FROM pg.public.treasury_stock_buyback"),
        # 內部人持股轉讓事前申報日報: forward signal — daily, transfer_method ∈ (鉅額逐筆 / 一般交易 / 贈與 / 信託 / 拍賣).
        # 「鉅額逐筆」+「一般交易」是真 sell signal (forward 5-30d -2~-5% CAR per TW academic literature).
        # 「信託」+「贈與」是 ownership re-allocation, transfer_shares=0, 較弱訊號.
        ("insider_holding", "SELECT market, report_date, declare_date, company_code, company_name, reporter_title, reporter_name, transfer_method, transferee, transfer_shares, max_intraday_shares, current_shares_own, current_shares_trust, planned_shares_own, planned_shares_trust FROM pg.public.insider_holding"),
        # REMOVED: financial_index_quarterly (was a PG VIEW with margin derivation
        # we couldn't fully verify). See research/strat_lab/raw_quarterly.py for
        # first-principles replacement (gross_margin_q, operating_margin_q, roa_ttm, etc.
        # from raw IS+BS+CF, validated TSMC 2024Q4 gross_margin = 0.5900).
        # First-principles RAW base tables for strat_lab. These are BASE TABLES (not views).
        # Filtered to twse+tpex (cash_flows uses 'tw'). Long-form: title is line item key.
        ("is_progressive_raw",
         "SELECT market, type, year, quarter, company_code, title, value "
         "FROM pg.public.concise_income_statement_progressive "
         "WHERE market IN ('twse','tpex') AND type='consolidated'"),
        ("bs_concise_raw",
         "SELECT market, type, year, quarter, company_code, title, value "
         "FROM pg.public.concise_balance_sheet "
         "WHERE market IN ('twse','tpex') AND type='consolidated'"),
        ("cf_progressive_raw",
         "SELECT market, year, quarter, company_code, title, value "
         "FROM pg.public.cash_flows_progressive WHERE market='tw'"),
    ]
    for name, sql in tables:
        t0 = time.time()
        con.sql(f"CREATE TABLE {name} AS {sql}")
        n = con.sql(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        print(f"  {name:25} {n:>10,} rows in {time.time()-t0:.1f}s")

    # Indexes for fast lookups
    con.sql("CREATE INDEX idx_dq_code_date ON daily_quote(company_code, date)")
    con.sql("CREATE INDEX idx_dq_date ON daily_quote(date)")
    con.sql("CREATE INDEX idx_pb_code_date ON stock_per_pbr(company_code, date)")
    # idx_ga_code_yq removed (growth_analysis_ttm no longer cached)
    con.sql("CREATE INDEX idx_cr_code_date ON capital_reduction(company_code, date)")
    con.sql("CREATE INDEX idx_or_code_year_month ON operating_revenue(company_code, year, month)")
    con.sql("CREATE INDEX idx_dtd_code_date ON daily_trading_details(company_code, date)")
    con.sql("CREATE INDEX idx_mt_code_date ON margin_transactions(company_code, date)")
    con.sql("CREATE INDEX idx_tdcc_code_date_tier ON tdcc_shareholding(company_code, data_date, holding_tier)")
    con.sql("CREATE INDEX idx_tdcc_date ON tdcc_shareholding(data_date)")
    con.sql("CREATE INDEX idx_sbl_code_date ON sbl_borrowing(company_code, date)")
    con.sql("CREATE INDEX idx_fhr_code_date ON foreign_holding_ratio(company_code, date)")
    con.sql("CREATE INDEX idx_tsb_code_date ON treasury_stock_buyback(company_code, announce_date)")
    con.sql("CREATE INDEX idx_ih_code_date ON insider_holding(company_code, report_date)")
    # idx_fiq_code_yq removed (financial_index_quarterly no longer cached)
    con.sql("CREATE INDEX idx_isp_code_yq ON is_progressive_raw(company_code, year, quarter)")
    con.sql("CREATE INDEX idx_bsr_code_yq ON bs_concise_raw(company_code, year, quarter)")
    con.sql("CREATE INDEX idx_cfr_code_yq ON cf_progressive_raw(company_code, year, quarter)")
    print(f"\n[done] cache at {DB_PATH}")

if __name__ == "__main__":
    main()
