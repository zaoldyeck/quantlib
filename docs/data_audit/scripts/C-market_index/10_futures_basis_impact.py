"""C-market_index #10: 幽靈日的 TAIEX 進一步汙染 cache 內的衍生表 taifex_futures_daily_factors。

src/quantlib/futures/taifex.py:216-217 用 market_index 的 TAIEX 收盤算期貨基差:
    tx_spot_basis      = COALESCE(settlement, final_settlement, close) - taiex_close
    tx_spot_basis_pct  = COALESCE(...)/taiex_close - 1.0        # 小數,非百分比
TAIEX 錯 → 基差跟著錯,而且錯到不合物理(近月台指期對現貨溢價 7% 不可能)。

真值來源同 #9(TWSE FMTQIK 月報,2026-07-22 實抓)。
Run: PYTHONPATH=. uv run --project . python docs/data_audit/scripts/C-market_index/10_futures_basis_impact.py
"""
import duckdb
from research import paths

con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
TRUTH = {"2016-05-26": 8394.12, "2017-08-02": 10519.27, "2018-10-03": 10863.94,
         "2019-07-05": 10785.73, "2019-09-25": 10873.69}

q = ("SELECT date, taiex_close, tx_spot_basis, tx_spot_basis_pct "
     "FROM taifex_futures_daily_factors WHERE date IN ("
     + ",".join(f"DATE '{d}'" for d in TRUTH) + ") ORDER BY date")
print(f"{'日期':12} {'期貨結算價':>10} {'TAIEX(存)':>11} {'TAIEX(真)':>11} "
      f"{'基差(存)':>10} {'基差%(存)':>10} {'基差(真)':>10} {'基差%(真)':>10}")
print("-" * 100)
for d, tc, b, bp in con.sql(q).fetchall():
    fut = tc + b                      # 反推當時用的期貨結算價
    t = TRUTH[str(d)]
    tb = fut - t
    print(f"{str(d):12} {fut:>10.2f} {tc:>11.2f} {t:>11.2f} "
          f"{b:>10.2f} {bp*100:>9.2f}% {tb:>10.2f} {tb/t*100:>9.2f}%")

print("\n  近月台指期對現貨的正常基差在 ±1% 內;上表『存』欄有 3 天基差正負號與真值相反,")
print("  2016-05-26 更被算成 +7.3% 溢價(物理上不可能),全部源自那天的 TAIEX 是 2016-01-18 的複本。")
print("\n  註:taifex_futures_daily_factors 是 cache_tables.py 內 build_taifex_futures_tables() 現算的衍生表,")
print("      不是 PG 的複本 —— 也就是說 market_index 的髒污會在每次重建 cache 時被『二次加工』擴散。")
