"""C-daily_trading_details / 步驟 2:日期覆蓋缺口。

「缺口」的定義:某市場在 daily_trading_details 起訖區間內、**其他日頻表證明當天
有開市**、但本表一列都沒有的日子。用「多表投票」而不是「星期幾」判休市——颱風假
無法從日曆推得(CLAUDE.md 鐵律)。

證人表(同一 cache 世代):daily_quote、stock_per_pbr、margin_transactions、
market_index、sbl_borrowing、foreign_holding_ratio。

用法:PYTHONPATH=<repo> uv run --project research python \
      docs/data_audit/scripts/C-daily_trading_details/02_gaps.py
"""
from __future__ import annotations

import duckdb

from research import paths

WITNESSES = [
    ("daily_quote", "market", "date"),
    ("stock_per_pbr", "market", "date"),
    ("margin_transactions", "market", "date"),
    ("market_index", "market", "date"),
    ("sbl_borrowing", "market", "date"),
    ("foreign_holding_ratio", "market", "date"),
]


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

    union = " UNION ALL ".join(
        f"SELECT {m} AS market, {d} AS date, '{t}' AS tbl, count(*) AS n "
        f"FROM {t} GROUP BY 1,2"
        for t, m, d in WITNESSES
    )
    con.sql(f"CREATE TEMP TABLE wit AS {union}")
    con.sql("""
        CREATE TEMP TABLE dtd AS
        SELECT market, date, count(*) AS n FROM daily_trading_details GROUP BY 1,2
    """)
    con.sql("""
        CREATE TEMP TABLE rng AS
        SELECT market, min(date) AS d0, max(date) AS d1 FROM dtd GROUP BY 1
    """)
    print("== dtd 起訖 ==")
    print(con.sql("SELECT * FROM rng ORDER BY 1").df().to_string(index=False))

    print("\n== 缺口:證人表有資料、dtd 沒有(限 dtd 起訖區間內)==")
    print(con.sql("""
        SELECT w.market, w.date,
               count(*) FILTER (WHERE w.n > 0)                       AS witnesses,
               string_agg(w.tbl || '=' || w.n, ' ' ORDER BY w.tbl)   AS detail
        FROM wit w JOIN rng r ON w.market = r.market
        LEFT JOIN dtd d ON d.market = w.market AND d.date = w.date
        WHERE d.market IS NULL AND w.date BETWEEN r.d0 AND r.d1 AND w.n > 0
        GROUP BY 1, 2 ORDER BY 1, 2
    """).df().to_string(index=False))

    print("\n== 反向:dtd 有資料、daily_quote 當天 0 列(幽靈日候選)==")
    print(con.sql("""
        SELECT d.market, d.date, d.n AS n_dtd,
               coalesce(max(w.n) FILTER (WHERE w.tbl='daily_quote'), 0) AS n_quote,
               string_agg(w.tbl || '=' || w.n, ' ' ORDER BY w.tbl)      AS other
        FROM dtd d LEFT JOIN wit w ON w.market = d.market AND w.date = d.date
        GROUP BY 1, 2, 3
        HAVING coalesce(max(w.n) FILTER (WHERE w.tbl='daily_quote'), 0) = 0
        ORDER BY 1, 2
    """).df().to_string(index=False))

    print("\n== dtd 起點之前,證人表已有幾個交易日(起點是否為端點限制)==")
    print(con.sql("""
        SELECT w.market, count(DISTINCT w.date) AS days_before, min(w.date) AS first_witness
        FROM wit w JOIN rng r ON w.market = r.market
        WHERE w.date < r.d0 AND w.n > 0 AND w.tbl = 'daily_quote'
        GROUP BY 1 ORDER BY 1
    """).df().to_string(index=False))


if __name__ == "__main__":
    main()
