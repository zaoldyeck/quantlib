"""One-time copy: PostgreSQL → local DuckDB file. Fast subsequent queries."""
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
        ("daily_quote",          "SELECT market, date, company_code, closing_price, trade_value FROM pg.public.daily_quote WHERE market='twse'"),
        ("stock_per_pbr",        "SELECT market, date, company_code, price_book_ratio, dividend_yield, price_to_earning_ratio FROM pg.public.stock_per_pbr_dividend_yield WHERE market='twse'"),
        ("growth_analysis_ttm",  "SELECT company_code, year, quarter, drop_score, f_score FROM pg.public.growth_analysis_ttm"),
        ("ex_right_dividend",    "SELECT market, date, company_code, cash_dividend FROM pg.public.ex_right_dividend WHERE market='twse' AND cash_dividend > 0"),
        ("etf",                  "SELECT company_code FROM pg.public.etf"),
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
    con.sql("CREATE INDEX idx_ga_code_yq ON growth_analysis_ttm(company_code, year, quarter)")
    print(f"\n[done] cache at {DB_PATH}")

if __name__ == "__main__":
    main()
