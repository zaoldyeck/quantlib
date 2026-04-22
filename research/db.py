"""DuckDB connection helper that attaches the existing PostgreSQL quantlib DB.

Zero ETL: `con.sql("SELECT * FROM pg.public.daily_quote")` pulls rows directly
from Postgres into DuckDB's columnar engine. All joins / window functions /
aggregations happen in DuckDB (vectorized + multi-threaded).
"""
import os
import duckdb

DEFAULT_DSN = os.environ.get(
    "QL_PG_DSN",
    f"host=localhost port=5432 dbname=quantlib user={os.environ.get('USER', 'zaoldyeck')}"
)


def connect(dsn: str = DEFAULT_DSN, read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection with Postgres extension loaded and quantlib attached as `pg`."""
    con = duckdb.connect()
    con.sql("INSTALL postgres; LOAD postgres;")
    readonly = ", READ_ONLY" if read_only else ""
    con.sql(f"ATTACH '{dsn}' AS pg (TYPE postgres{readonly})")
    # Use more threads; DuckDB defaults to physical cores, fine for a laptop
    con.sql("SET memory_limit = '8GB'")
    return con


if __name__ == "__main__":
    # Smoke test
    con = connect()
    print(con.sql("SELECT COUNT(*) FROM pg.public.daily_quote").pl())
