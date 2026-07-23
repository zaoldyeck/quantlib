"""C-foreign_holding_ratio 稽核 02:cache vs PG 全表逐鍵逐欄值比對(非抽樣)。

鍵 = (market, date, company_code)(PG 唯一索引 idx_ForeignHoldingRatio_market_date_code)。
雙向 EXCEPT 找獨有鍵,共用鍵逐欄 IS DISTINCT FROM 計數。

Run: uv run --project research python docs/data_audit/scripts/C-foreign_holding_ratio/02_value_parity.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402

PG_DSN = "host=localhost port=5432 dbname=quantlib user=zaoldyeck"

COLS = [
    "outstanding_shares",
    "foreign_remaining_shares",
    "foreign_held_shares",
    "foreign_remaining_ratio",
    "foreign_held_ratio",
    "foreign_limit_ratio",
]


def main() -> None:
    t0 = time.time()
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")

    print("== 獨有鍵(雙向 EXCEPT)==")
    print(con.sql("""
      WITH c AS (SELECT market, date, company_code FROM foreign_holding_ratio),
           p AS (SELECT market, date, company_code FROM pg.public.foreign_holding_ratio)
      SELECT (SELECT COUNT(*) FROM (SELECT * FROM c EXCEPT SELECT * FROM p)) AS cache_only,
             (SELECT COUNT(*) FROM (SELECT * FROM p EXCEPT SELECT * FROM c)) AS pg_only
    """).df().to_string())

    diffs = ",\n             ".join(
        f"SUM(CASE WHEN c.{c} IS DISTINCT FROM p.{c} THEN 1 ELSE 0 END) AS d_{c}"
        for c in COLS
    )
    print("\n== 共用鍵逐欄不一致筆數 ==")
    print(con.sql(f"""
      SELECT COUNT(*) AS joined_rows,
             {diffs}
      FROM foreign_holding_ratio c
      JOIN pg.public.foreign_holding_ratio p
        ON c.market=p.market AND c.date=p.date AND c.company_code=p.company_code
    """).df().to_string())

    print(f"\n[elapsed] {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
