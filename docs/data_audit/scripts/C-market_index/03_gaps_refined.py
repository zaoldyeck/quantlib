"""C-market_index #3: 用「同市場 daily_quote 有 ≥100 檔成交」當交易日曆重算缺口。

為什麼不用 research.data_calendar.is_trading_day:它(1)只讀 TWSE 的 sentinel、
(2)週末一律判非交易日 → 會漏掉**週六補行交易日**(台股有,例如 2009-12-12)。
本腳本改用市場自己的報價表當日曆,再把每個缺日的原始檔大小/存在與否標出來,
並對每個缺日列出「其他 6 張表各有幾列」以區分 market_index 缺 vs 別表缺。

Run: PYTHONPATH=. uv run --project research python docs/data_audit/scripts/C-market_index/03_gaps_refined.py
"""
from pathlib import Path

import duckdb
from research import paths
from research.data_calendar import is_trading_day

ROOT = Path(__file__).resolve().parents[4]
con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

OTHER = ["daily_trading_details", "stock_per_pbr", "margin_transactions",
         "sbl_borrowing", "foreign_holding_ratio"]

for mkt in ("twse", "tpex"):
    lo, hi = con.sql(f"SELECT min(date), max(date) FROM market_index "
                     f"WHERE market='{mkt}'").fetchone()
    print(f"\n{'='*70}\n[{mkt}] market_index 覆蓋 {lo} → {hi}")
    q = f"""
    WITH cal AS (SELECT date, count(*) n_stk FROM daily_quote
                 WHERE market='{mkt}' AND date BETWEEN DATE '{lo}' AND DATE '{hi}'
                 GROUP BY 1 HAVING count(*) >= 100),
         mi AS (SELECT date, count(*) n FROM market_index WHERE market='{mkt}' GROUP BY 1)
    SELECT cal.date, cal.n_stk FROM cal LEFT JOIN mi ON mi.date=cal.date
    WHERE mi.date IS NULL ORDER BY 1"""
    miss = con.sql(q).fetchall()
    print(f"  以 daily_quote(≥100 檔)為日曆:{con.sql(q.replace('SELECT cal.date, cal.n_stk', 'SELECT count(*)').split('ORDER BY')[0]).fetchone()[0]} 個交易日缺 market_index")
    for d, n_stk in miss:
        f = ROOT / "data" / "index" / mkt / str(d.year) / f"{d.year}_{d.month}_{d.day}.csv"
        sz = f.stat().st_size if f.exists() else "NO_FILE"
        wit = []
        for t in OTHER:
            c = con.sql(f"SELECT count(*) FROM {t} WHERE market='{mkt}' AND date=DATE '{d}'").fetchone()[0]
            if c:
                wit.append(f"{t}={c}")
        print(f"    {d} ({d.strftime('%a')})  daily_quote={n_stk}  raw={sz}  is_trading_day={is_trading_day(d)}")
        print(f"        其他表: {', '.join(wit) if wit else '(全無)'}")

    # 反向:market_index 有,daily_quote 該市場當天 <100 檔
    q2 = f"""
    WITH cal AS (SELECT date, count(*) n_stk FROM daily_quote WHERE market='{mkt}' GROUP BY 1),
         mi AS (SELECT date, count(*) n FROM market_index WHERE market='{mkt}' GROUP BY 1)
    SELECT mi.date, mi.n, coalesce(cal.n_stk,0) FROM mi LEFT JOIN cal ON cal.date=mi.date
    WHERE coalesce(cal.n_stk,0) < 100 ORDER BY 1"""
    extra = con.sql(q2).fetchall()
    print(f"  反向(market_index 有、daily_quote <100 檔): {len(extra)} 天")
    for d, n, ns in extra:
        print(f"    {d} ({d.strftime('%a')})  market_index={n} 列  daily_quote={ns} 檔")
