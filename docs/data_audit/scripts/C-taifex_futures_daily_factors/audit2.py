"""Follow-up probes for C-taifex_futures_daily_factors:
 (a) contract-format classes of the FRONT columns (is the front ever a spread/weekly?)
 (b) quantify the true-vs-stored next-term spread error on corrupted rows
 (c) the +7.32% spot-basis outlier on 2016-05-26 (market_index root or spot-CTE mis-pick?)
Run: PYTHONPATH=<repo> uv run --project research python .../audit2.py  (read-only)
"""
from __future__ import annotations

import duckdb
import pandas as pd

from research import paths

pd.set_option("display.width", 220)
pd.set_option("display.max_columns", 40)
con = duckdb.connect(str(paths.CACHE_DB), read_only=True)


def sec(t): print("\n" + "=" * 78 + f"\n{t}\n" + "=" * 78)


sec("(a) format classes of FRONT contract-month columns")
for col in ["tx_contract_month", "mtx_contract_month", "te_contract_month",
            "tf_contract_month", "tmf_contract_month", "tx_next_contract_month"]:
    r = con.execute(f"""
        SELECT
          count(*) FILTER (WHERE {col} IS NOT NULL) AS non_null,
          count(*) FILTER (WHERE {col} LIKE '%/%') AS has_slash_spread,
          count(*) FILTER (WHERE {col} LIKE '%W%') AS has_weekly,
          count(*) FILTER (WHERE regexp_full_match({col}, '\\d{{6}}')) AS pure_monthly
        FROM taifex_futures_daily_factors
    """).fetchone()
    print(f"  {col:24} non_null={r[0]:5}  spread(/)= {r[1]:4}  weekly(W)= {r[2]:4}  pure_6digit= {r[3]:5}")

sec("(b) true next-outright spread vs stored (spot-check corrupted rows)")
# recompute the correct next-month = smallest month_key strictly greater than front's,
# among PURE monthly outrights only.
print(con.execute("""
    WITH pure AS (
        SELECT date, product, contract_month, month_key, month_rank,
               COALESCE(settlement_price, final_settlement_price, close) AS px
        FROM taifex_futures_contract_rank
        WHERE product='TX' AND regexp_full_match(contract_month, '\\d{6}')
    ),
    front AS (SELECT * FROM pure WHERE month_rank=1),
    ranked AS (
        SELECT p.date, p.contract_month, p.month_key, p.px,
               row_number() OVER (PARTITION BY p.date ORDER BY p.month_key) AS rk
        FROM pure p
    ),
    real_next AS (
        SELECT r.date, r.contract_month AS true_next_cm, r.px AS true_next_px
        FROM ranked r WHERE r.rk=2
    )
    SELECT f.date,
           fr.contract_month AS front_cm, fr.px AS front_px,
           f.tx_next_contract_month AS stored_next_cm,
           f.tx_next_term_spread_pct AS stored_pct,
           rn.true_next_cm,
           round((rn.true_next_px/nullif(fr.px,0))-1.0, 6) AS true_pct
    FROM taifex_futures_daily_factors f
    JOIN front fr ON fr.date=f.date
    JOIN real_next rn ON rn.date=f.date
    WHERE f.tx_next_contract_month LIKE '%/%'
    ORDER BY f.date DESC LIMIT 8
""").df().to_string(index=False))

sec("(c) 2016-05-26 spot outlier: all TAIEX-name market_index rows that day")
print(con.execute("""
    SELECT market, name, close, "change(%)" FROM market_index
    WHERE date=DATE '2016-05-26' AND market='twse' AND name LIKE '%發行量加權股價指數%'
    ORDER BY name
""").df().to_string(index=False) if False else "")
# note: cache col renamed to change_pct
print(con.execute("""
    SELECT market, name, close, change_pct FROM market_index
    WHERE date=DATE '2016-05-26' AND name LIKE '%加權%'
    ORDER BY market, name
""").df().to_string(index=False))
print("\n  TX front settle that day, and TAIEX close on neighbouring days:")
print(con.execute("""
    SELECT date, close AS taiex_close FROM market_index
    WHERE market='twse' AND name='發行量加權股價指數' AND date BETWEEN DATE '2016-05-24' AND DATE '2016-05-30'
    ORDER BY date
""").df().to_string(index=False))
print(con.execute("""
    SELECT date, tx_contract_month, tx_close, tx_settlement_price, taiex_close, tx_spot_basis_pct
    FROM taifex_futures_daily_factors WHERE date BETWEEN DATE '2016-05-24' AND DATE '2016-05-30' ORDER BY date
""").df().to_string(index=False))

con.close()
print("\n[done]")
