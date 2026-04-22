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


CACHE_DB = os.path.join(os.path.dirname(__file__), "cache.duckdb")


def connect(dsn: str = DEFAULT_DSN, read_only: bool = True,
            use_cache: bool = True) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection. If use_cache=True and `research/cache.duckdb`
    exists, open it directly (millisecond queries). Otherwise attach live
    PostgreSQL as `pg` (slow — for ad-hoc cross-table queries only).

    To rebuild cache: `uv run python research/cache_tables.py`
    """
    if use_cache and os.path.exists(CACHE_DB):
        # read_only=False so we can register temp in-memory frames; file itself is not modified
        con = duckdb.connect(CACHE_DB, read_only=False)
        con.sql("SET memory_limit = '8GB'")
        return con

    con = duckdb.connect()
    con.sql("INSTALL postgres; LOAD postgres;")
    readonly = ", READ_ONLY" if read_only else ""
    con.sql(f"ATTACH '{dsn}' AS pg (TYPE postgres{readonly})")
    con.sql("SET memory_limit = '8GB'")
    return con


if __name__ == "__main__":
    # Smoke test
    con = connect()
    print(con.sql("SELECT COUNT(*) FROM pg.public.daily_quote").pl())
