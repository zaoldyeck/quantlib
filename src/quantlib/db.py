"""DuckDB connection helper for the local cache (var/cache/cache.duckdb).

The cache is the **single structured source of truth**, rebuilt from the raw
archives under `data/` via `src/quantlib/crawl/rebuild.py`. PostgreSQL was retired
2026-07-23 — every consumer reads the cache directly (millisecond columnar
queries); there is no live-PG fallback.

Views expose BOTH TWSE + TPEx rows. Research scripts must apply explicit
`WHERE market='twse'` or `='tpex'` filters at query time.
"""
import os
import duckdb

from quantlib.industry_taxonomy import build_industry_taxonomy_pit
from quantlib import paths


CACHE_DB = str(paths.CACHE_DB)
RAW_QUARTERLY_PARQUET = str(paths.RAW_QUARTERLY)  # var/cache/(可重生產物,不在源碼樹)


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

    Computed by `python src/quantlib/strat_lab/raw_quarterly.py` from raw IS/BS/CF base tables.
    Provides f_score_raw, gross_margin_q, roa_ttm, cfo_ni_ratio_ttm, etc.

    This must stay TEMP so read-only cache connections remain process-parallel.
    """
    if os.path.exists(RAW_QUARTERLY_PARQUET):
        con.sql(
            f"CREATE OR REPLACE TEMP VIEW raw_quarterly AS "
            f"SELECT * FROM read_parquet('{RAW_QUARTERLY_PARQUET}')"
        )


def _register_industry_taxonomy(con: duckdb.DuckDBPyConnection) -> None:
    """Expose canonical PIT industry taxonomy on caches that lack the materialized table."""
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


def connect(cache_path: str = CACHE_DB, read_only: bool = True,
            register_raw_quarterly: bool = True) -> duckdb.DuckDBPyConnection:
    """Open the local DuckDB cache (millisecond queries).

    `cache_path` defaults to `var/cache/cache.duckdb`. Auto-registers the
    `raw_quarterly` view (first-principles quality/growth factors from raw
    IS/BS/CF) and `industry_taxonomy_pit` (PIT industry classification).

    Raises FileNotFoundError if the cache is missing — rebuild from the raw
    archives via `uv run --project . python -m quantlib.crawl.rebuild --all`.

    PG retired 2026-07-23: there is no `use_cache=False` live-PostgreSQL path.
    """
    if not os.path.exists(cache_path):
        raise FileNotFoundError(
            f"cache 不存在:{cache_path}。PostgreSQL 已退役(2026-07-23),"
            "請從 raw 重建:`uv run --project . python -m quantlib.crawl.rebuild --all`"
            "(季報鏈另跑 quantlib.crawl.rebuild_financials)。")
    con = duckdb.connect(cache_path, read_only=read_only)
    _configure_connection(con)
    if register_raw_quarterly:
        _register_raw_quarterly(con)
    _register_industry_taxonomy(con)
    return con


if __name__ == "__main__":
    # Smoke test
    con = connect()
    print(con.sql("SELECT COUNT(*) FROM daily_quote").pl())
