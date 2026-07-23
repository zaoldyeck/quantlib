"""C-tdcc_shareholding 稽核 ②:覆蓋缺口(週頻)+ 異常值掃描。

TDCC 集保股權分散表是**週頻**:endpoint 無日期參數,只回「當週最新」快照,
資料日期 = 每週最後一個營業日(遇假日不必然週五)。故覆蓋缺口的單位是「週」,
不是「日」。

問題 A(缺口):在已收集的日期區間內,有沒有整週的快照漏掉?漏掉的是
  「休市週」(該週根本沒有最後營業日的新快照)還是「真的漏抓」?
問題 B(異常值):數值欄有沒有不可能的值(負人數/負股數、比例超出 0~100、
  級距超出 1~17、日期在未來、合計級距比例 != 100)?

判缺口的錨:TWSE daily_quote 的實際交易日曆(含颱風假,靠 0-byte sentinel,
不能從星期幾推)。每個 ISO 週取「該週最後一個 TWSE 交易日」= 該週應有的
TDCC 資料日期,與實際捕捉到的 data_date 集合比對。**只看已收集區間的內部缺口**
(第一個 ~ 最後一個捕捉週之間);更早的週屬歷史未回補(Task #20),非營運漏抓。

cache 依賴:需 var/cache/cache.duckdb 最新。PG 需 localhost:5432/quantlib。
執行:PYTHONPATH=<repo> uv run --project . python \
        docs/data_audit/scripts/C-tdcc_shareholding/02_gaps_anomaly.py
"""
from __future__ import annotations

import datetime as dt
import os

import duckdb

from research import paths

PG_DSN = os.environ.get(
    "QL_PG_DSN",
    f"host=localhost port=5432 dbname=quantlib user={os.environ.get('USER', 'zaoldyeck')}",
)


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")

    # 捕捉到的 TDCC 資料日期
    got = [r[0] for r in con.sql(
        "SELECT DISTINCT data_date FROM tdcc_shareholding ORDER BY data_date"
    ).fetchall()]
    lo, hi = got[0], got[-1]
    print("=" * 70)
    print(f"捕捉到 {len(got)} 個週快照,區間 {lo} .. {hi}")
    for d in got:
        print(f"  {d} ({d:%a})")

    print("=" * 70)
    print("A. 覆蓋缺口 — 用 TWSE 交易日曆推每週應有的資料日期")
    # 每個 ISO 週的最後一個 TWSE 交易日(從 daily_quote 拿真實交易日曆)
    wk = con.sql(
        "SELECT date_part('isoyear', date) iy, date_part('week', date) iw, "
        "       max(date) last_td, count(DISTINCT date) n_td "
        "FROM pg.public.daily_quote "
        f"WHERE market='twse' AND date BETWEEN DATE '{lo}' - INTERVAL 7 DAY "
        f"    AND DATE '{hi}' + INTERVAL 7 DAY "
        "GROUP BY iy, iw ORDER BY iy, iw"
    ).fetchall()
    got_set = set(got)
    # 只看已收集區間內的週:該週最後交易日落在 [lo, hi]
    expected = [row[2] for row in wk if lo <= row[2] <= hi]
    missing = [d for d in expected if d not in got_set]
    print(f"  區間內應有週快照 {len(expected)} 個,捕捉 {len(got)} 個,缺 {len(missing)} 個")
    for row in wk:
        last_td, n_td = row[2], row[3]
        if not (lo <= last_td <= hi):
            continue
        mark = "OK " if last_td in got_set else "MISS"
        # last_td 的星期幾;週四=該週五為假日(holiday-shifted);週五=正常
        print(f"  [{mark}] 週末營業日={last_td} ({last_td:%a})  該週交易日數={n_td}")

    print("  缺漏週(真漏抓,非休市):", [str(d) for d in missing])
    # 佐證:缺漏週的週五/最後營業日在 daily_quote 有資料(是真交易日)
    for d in missing:
        n = con.sql(
            f"SELECT count(*) FROM pg.public.daily_quote WHERE market='twse' AND date='{d}'"
        ).fetchone()[0]
        print(f"    {d}: daily_quote twse rows={n} → {'真交易日(確認漏抓)' if n else '非交易日'}")

    print("=" * 70)
    print("B. 異常值掃描(cache)")
    checks = {
        "num_holders < 0": "SELECT count(*) FROM tdcc_shareholding WHERE num_holders < 0",
        "num_shares < 0": "SELECT count(*) FROM tdcc_shareholding WHERE num_shares < 0",
        "pct < 0": "SELECT count(*) FROM tdcc_shareholding WHERE pct_of_outstanding < 0",
        "pct > 100": "SELECT count(*) FROM tdcc_shareholding WHERE pct_of_outstanding > 100",
        "holding_tier NOT IN 1..17": "SELECT count(*) FROM tdcc_shareholding WHERE holding_tier NOT BETWEEN 1 AND 17",
        "data_date in future": f"SELECT count(*) FROM tdcc_shareholding WHERE data_date > DATE '{dt.date.today()}'",
        "any NULL": "SELECT count(*) FROM tdcc_shareholding WHERE data_date IS NULL OR company_code IS NULL "
                    "OR holding_tier IS NULL OR num_holders IS NULL OR num_shares IS NULL OR pct_of_outstanding IS NULL",
        "dup (date,code,tier)": "SELECT count(*) FROM (SELECT data_date, company_code, holding_tier "
                                "FROM tdcc_shareholding GROUP BY 1,2,3 HAVING count(*)>1)",
        "num_shares>0 but num_holders=0": "SELECT count(*) FROM tdcc_shareholding WHERE num_shares>0 AND num_holders=0",
    }
    for name, q in checks.items():
        print(f"  {name:35}: {con.sql(q).fetchone()[0]}")

    print("  值域(pct_of_outstanding):",
          con.sql("SELECT min(pct_of_outstanding), max(pct_of_outstanding) FROM tdcc_shareholding").fetchone())
    print("  級距值集:", sorted(r[0] for r in con.sql(
        "SELECT DISTINCT holding_tier FROM tdcc_shareholding").fetchall()))

    print("  tier 17(合計)每檔 pct 應=100:")
    r = con.sql(
        "SELECT count(*) total, count(*) FILTER (WHERE abs(pct_of_outstanding-100) > 0.01) off100, "
        "       min(pct_of_outstanding), max(pct_of_outstanding) "
        "FROM tdcc_shareholding WHERE holding_tier=17"
    ).fetchone()
    print(f"    tier17 rows={r[0]} 偏離100(>0.01)={r[1]} min={r[2]} max={r[3]}")

    # 內部一致性:合計人數(tier17) == sum(tier 1..16) 人數(A-dim 已驗,C 端複核)
    inc = con.sql(
        "WITH t AS (SELECT data_date, company_code, "
        "     sum(num_holders) FILTER (WHERE holding_tier BETWEEN 1 AND 16) AS s16, "
        "     sum(num_holders) FILTER (WHERE holding_tier=17) AS t17 "
        "   FROM tdcc_shareholding GROUP BY 1,2) "
        "SELECT count(*) FROM t WHERE s16 IS NOT NULL AND t17 IS NOT NULL AND s16 <> t17"
    ).fetchone()[0]
    print(f"  合計人數 != sum(tier1..16) 人數 的 (date,code) 數:{inc}")

    con.close()


if __name__ == "__main__":
    main()
