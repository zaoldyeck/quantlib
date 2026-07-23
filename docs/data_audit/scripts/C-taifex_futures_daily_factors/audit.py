"""Data-correctness audit for cache-only derived table `taifex_futures_daily_factors`.

This table has NO PostgreSQL counterpart — it is COMPUTED in-cache by
`research/futures/taifex.py::build_taifex_futures_tables` from three cached base
tables: taifex_futures_contract_rank (<- taifex_futures_daily + _final_settlement),
market_index (TAIEX spot), taifex_futures_institutional.

So "cache vs PG consistency" is reframed as:
  (1) derivation correctness — recompute each factor independently from the base
      tables and diff against the stored column;
  (2) coverage gaps vs the TWSE trading calendar (inherited from base);
  (3) anomaly scan on the derived numeric columns.

Run:  uv run --project research python docs/data_audit/scripts/C-taifex_futures_daily_factors/audit.py
Needs: var/cache/cache.duckdb fresh (research/cache_tables.py). Read-only.
"""
from __future__ import annotations

import duckdb
import pandas as pd

from research import paths

pd.set_option("display.width", 220)
pd.set_option("display.max_columns", 40)

con = duckdb.connect(str(paths.CACHE_DB), read_only=True)


def section(t: str) -> None:
    print("\n" + "=" * 78 + f"\n{t}\n" + "=" * 78)


# ---------------------------------------------------------------------------
section("1. base table date ranges (source coverage that bounds the factors)")
for t in [
    "taifex_futures_daily",
    "taifex_futures_institutional",
    "taifex_futures_final_settlement",
    "taifex_futures_contract_rank",
    "market_index",
]:
    mn, mx, n = con.execute(f"SELECT min(date), max(date), count(*) FROM {t}").fetchone()
    print(f"  {t:35} {mn}..{mx}  n={n:,}")

# ---------------------------------------------------------------------------
section("2. BUG confirm: second-month picks calendar-SPREAD contract, not next outright")
# spread contracts carry contract_month like 'YYYYMM/YYYYMM'; regex ^\\d{6} extracts
# the front leg -> same month_key as the front outright -> string 'YYYYMM' sorts before
# 'YYYYMM/....' so outright keeps rank1 but the SPREAD steals rank2.
print("-- base TX contracts on 2024-08-30 (regular session) --")
print(
    con.execute(
        """
        SELECT contract_month, close, settlement_price, volume, open_interest
        FROM taifex_futures_daily
        WHERE date = DATE '2024-08-30' AND contract_code = 'TX' AND trading_session = '一般'
        ORDER BY contract_month
        """
    ).df().to_string()
)
print("\n-- contract_rank month_rank for TX 2024-08-30 --")
print(
    con.execute(
        """
        SELECT contract_month, month_key, month_rank, close, settlement_price
        FROM taifex_futures_contract_rank
        WHERE date = DATE '2024-08-30' AND product = 'TX'
        ORDER BY month_rank LIMIT 8
        """
    ).df().to_string()
)
print("\n-- stored factor row 2024-08-30 (next-term columns) --")
print(
    con.execute(
        """
        SELECT date, tx_contract_month, tx_next_contract_month,
               tx_settlement_price, tx_next_term_spread, tx_next_term_spread_pct
        FROM taifex_futures_daily_factors WHERE date = DATE '2024-08-30'
        """
    ).df().to_string()
)

# ---------------------------------------------------------------------------
section("3. scope of the spread-contract bug")
tot = con.execute("SELECT count(*) FROM taifex_futures_daily_factors").fetchone()[0]
slash = con.execute(
    "SELECT count(*) FROM taifex_futures_daily_factors WHERE tx_next_contract_month LIKE '%/%'"
).fetchone()[0]
big = con.execute(
    "SELECT count(*) FROM taifex_futures_daily_factors WHERE abs(tx_next_term_spread_pct) > 0.1"
).fetchone()[0]
mn_bad, mx_bad = con.execute(
    "SELECT min(date), max(date) FROM taifex_futures_daily_factors WHERE tx_next_contract_month LIKE '%/%'"
).fetchone()
print(f"  total rows                               = {tot:,}")
print(f"  rows where tx_next_contract_month has '/' = {slash:,}  ({100*slash/tot:.1f}%)  range {mn_bad}..{mx_bad}")
print(f"  rows where |tx_next_term_spread_pct|>0.10 = {big:,}")
print("  -> both sets are the SAME corrupted rows (spread price ~1pt vs index ~20000 => pct ~ -1).")
print("\n  yearly distribution of '/'-contract rows:")
print(
    con.execute(
        """
        SELECT CAST(year(date) AS INT) AS yr, count(*) AS n
        FROM taifex_futures_daily_factors WHERE tx_next_contract_month LIKE '%/%'
        GROUP BY 1 ORDER BY 1
        """
    ).df().to_string(index=False)
)

# ---------------------------------------------------------------------------
section("4. independent parity recompute of derived columns from base tables")
# Rebuild the factor logic straight from the cached base tables and diff against
# the stored table. Any mismatch => stored table drifted from its own source.
recompute = """
WITH front AS (SELECT * FROM taifex_futures_contract_rank WHERE month_rank = 1),
sm AS (SELECT * FROM taifex_futures_contract_rank WHERE month_rank = 2),
spot AS (
  SELECT date, close AS taiex_close FROM (
    SELECT date, close,
      row_number() OVER (PARTITION BY date ORDER BY CASE WHEN name='發行量加權股價指數' THEN 0 ELSE 1 END) rn
    FROM market_index
    WHERE market='twse' AND name LIKE '%發行量加權股價指數%' AND close IS NOT NULL
  ) WHERE rn=1
),
inst AS (
  SELECT date, contract_code AS product,
    sum(CASE WHEN investor_type='外資及陸資' THEN net_open_interest ELSE 0 END) AS foreign_net_oi
  FROM taifex_futures_institutional
  WHERE contract_code IN ('TX','MTX','TMF','TE','TF') GROUP BY date, contract_code
)
SELECT tx.date,
  tx.close AS tx_close,
  COALESCE(sm.settlement_price, sm.final_settlement_price, sm.close)
    - COALESCE(tx.settlement_price, tx.final_settlement_price, tx.close) AS tx_next_term_spread,
  spot.taiex_close,
  COALESCE(tx.settlement_price, tx.final_settlement_price, tx.close) - spot.taiex_close AS tx_spot_basis,
  inst_tx.foreign_net_oi AS foreign_tx_net_oi
FROM front tx
LEFT JOIN sm ON sm.date = tx.date AND sm.product = 'TX'
LEFT JOIN spot ON spot.date = tx.date
LEFT JOIN inst inst_tx ON inst_tx.date = tx.date AND inst_tx.product = 'TX'
WHERE tx.product = 'TX'
"""
con.execute(f"CREATE TEMP TABLE _rc AS {recompute}")
for col in ["tx_close", "tx_next_term_spread", "taiex_close", "tx_spot_basis", "foreign_tx_net_oi"]:
    diff = con.execute(
        f"""
        SELECT count(*) FROM taifex_futures_daily_factors f JOIN _rc r ON f.date = r.date
        WHERE f.{col} IS DISTINCT FROM r.{col}
        """
    ).fetchone()[0]
    print(f"  {col:22} mismatched rows vs independent recompute = {diff}")

# ---------------------------------------------------------------------------
section("5. anomaly scan: spot basis, mtx/tmf micro-spread")
print("  tx_spot_basis_pct extremes (should sit within ~ -3%..+2% for TAIEX):")
print(
    con.execute(
        """
        SELECT date, tx_close, taiex_close, tx_spot_basis, tx_spot_basis_pct
        FROM taifex_futures_daily_factors
        WHERE tx_spot_basis_pct IS NOT NULL AND abs(tx_spot_basis_pct) > 0.03
        ORDER BY abs(tx_spot_basis_pct) DESC LIMIT 12
        """
    ).df().to_string(index=False)
)
print("\n  tx_mtx_close_spread_pct extremes (MTX == same index as TX, should be ~0):")
print(
    con.execute(
        """
        SELECT date, tx_contract_month, mtx_contract_month, tx_close, mtx_close, tx_mtx_close_spread_pct
        FROM taifex_futures_daily_factors
        WHERE tx_mtx_close_spread_pct IS NOT NULL AND abs(tx_mtx_close_spread_pct) > 0.01
        ORDER BY abs(tx_mtx_close_spread_pct) DESC LIMIT 10
        """
    ).df().to_string(index=False)
)

# ---------------------------------------------------------------------------
section("6. hard-impossible value scans")
for label, cond in [
    ("negative/zero tx_close", "tx_close <= 0"),
    ("negative tx_volume", "tx_volume < 0"),
    ("negative tx_open_interest", "tx_open_interest < 0"),
    ("future date (> today)", "date > current_date"),
    ("taiex_close <= 0 (non-null)", "taiex_close IS NOT NULL AND taiex_close <= 0"),
]:
    n = con.execute(f"SELECT count(*) FROM taifex_futures_daily_factors WHERE {cond}").fetchone()[0]
    print(f"  {label:32} {n}")

# ---------------------------------------------------------------------------
section("7. coverage gaps vs base daily (both are TX-front driven)")
gap = con.execute(
    """
    WITH d AS (SELECT DISTINCT date FROM taifex_futures_daily WHERE contract_code='TX' AND trading_session='一般'),
    f AS (SELECT DISTINCT date FROM taifex_futures_daily_factors)
    SELECT (SELECT count(*) FROM d) AS base_tx_days,
           (SELECT count(*) FROM f) AS factor_days,
           (SELECT count(*) FROM d WHERE d.date NOT IN (SELECT date FROM f)) AS base_not_in_factor,
           (SELECT count(*) FROM f WHERE f.date NOT IN (SELECT date FROM d)) AS factor_not_in_base
    """
).df()
print(gap.to_string(index=False))
print("\n  largest calendar gaps inside the factor date series:")
print(
    con.execute(
        """
        WITH s AS (
          SELECT date, lag(date) OVER (ORDER BY date) AS prev FROM taifex_futures_daily_factors
        )
        SELECT prev AS gap_start, date AS gap_end, (date - prev) AS gap_days
        FROM s WHERE prev IS NOT NULL AND (date - prev) > 7
        ORDER BY gap_days DESC LIMIT 10
        """
    ).df().to_string(index=False)
)

con.close()
print("\n[done]")
