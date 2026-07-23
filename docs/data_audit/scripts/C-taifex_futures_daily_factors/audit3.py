"""Final checks: 3-date sample (factor row vs independently derived base values)
+ institutional net-OI range sanity. Read-only."""
from __future__ import annotations

import duckdb
import pandas as pd

from research import paths

pd.set_option("display.width", 240)
pd.set_option("display.max_columns", 60)
con = duckdb.connect(str(paths.CACHE_DB), read_only=True)


def sec(t): print("\n" + "=" * 78 + f"\n{t}\n" + "=" * 78)


sample_dates = ["2024-03-14", "2020-11-05", "2015-08-24"]  # 3 arbitrary trading days

sec("3-date sample: stored factor row vs base-derived front values")
for d in sample_dates:
    print(f"\n--- {d} ---")
    stored = con.execute(f"""
        SELECT tx_contract_month, tx_close, tx_settlement_price, tx_open_interest,
               mtx_contract_month, mtx_close, taiex_close, tx_spot_basis,
               foreign_tx_net_oi
        FROM taifex_futures_daily_factors WHERE date = DATE '{d}'
    """).df()
    print("stored :", stored.to_dict("records"))
    base = con.execute(f"""
        SELECT contract_month, close, settlement_price, open_interest
        FROM taifex_futures_contract_rank
        WHERE date = DATE '{d}' AND product='TX' AND month_rank=1
    """).df()
    print("base TX front :", base.to_dict("records"))
    base_mtx = con.execute(f"""
        SELECT contract_month, close FROM taifex_futures_contract_rank
        WHERE date = DATE '{d}' AND product='MTX' AND month_rank=1
    """).df()
    print("base MTX front:", base_mtx.to_dict("records"))
    spot = con.execute(f"""
        SELECT close FROM market_index
        WHERE date=DATE '{d}' AND market='twse' AND name='發行量加權股價指數'
    """).fetchall()
    print("base TAIEX close:", spot)
    foi = con.execute(f"""
        SELECT sum(net_open_interest) FROM taifex_futures_institutional
        WHERE date=DATE '{d}' AND contract_code='TX' AND investor_type='外資及陸資'
    """).fetchone()[0]
    print("base foreign TX net_oi:", foi)

sec("institutional net-OI / net-volume range sanity (post-2023-05-22 window)")
print(con.execute("""
    SELECT
      min(foreign_tx_net_oi) AS foi_min, max(foreign_tx_net_oi) AS foi_max,
      min(trust_tx_net_oi)   AS toi_min, max(trust_tx_net_oi)   AS toi_max,
      min(dealer_tx_net_oi)  AS doi_min, max(dealer_tx_net_oi)  AS doi_max,
      min(foreign_tx_net_volume) AS fv_min, max(foreign_tx_net_volume) AS fv_max
    FROM taifex_futures_daily_factors WHERE foreign_tx_net_oi IS NOT NULL
""").df().to_string(index=False))

sec("Lunar New Year gap check: are the ~12-day Jan/Feb gaps real closures?")
# is_trading_day sentinel calendar
print(con.execute("""
    WITH s AS (SELECT date, lag(date) OVER (ORDER BY date) prev FROM taifex_futures_daily_factors)
    SELECT prev AS gap_start, date AS gap_end, (date-prev) AS gap_days,
           month(prev) AS m
    FROM s WHERE (date-prev) > 7 ORDER BY gap_days DESC LIMIT 6
""").df().to_string(index=False))
print("  (61-day 2025-12-31->2026-03-02 = the base-table 2026 Jan/Feb crawl gap; rest are CNY month=1/2)")

con.close()
print("\n[done]")
