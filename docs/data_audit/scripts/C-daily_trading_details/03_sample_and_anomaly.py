"""C-daily_trading_details / 步驟 3:抽樣逐欄比對 + 異常值掃描。

(1) 抽樣:3 個日期 × 5 檔股票,cache 與 PostgreSQL 每一欄逐格比對(pandas equals)。
(2) 異常值:對 cache 全表掃「不可能的值」——重複主鍵、未來日期、代號格式、
    三大法人恆等式破裂、int32 邊界、全零列、正負號分布。

用法:PYTHONPATH=<repo> uv run --project . python \
      docs/data_audit/scripts/C-daily_trading_details/03_sample_and_anomaly.py
"""
from __future__ import annotations

import os

import duckdb
import pandas as pd

from research import paths

PG_DSN = os.environ.get(
    "QL_PG_DSN",
    f"host=localhost port=5432 dbname=quantlib user={os.environ.get('USER', 'zaoldyeck')}",
)

SAMPLES = [
    ("twse", "2013-06-14", ["2330", "2317", "1101", "3008", "2412"]),
    ("tpex", "2019-11-07", ["6488", "3105", "5483", "8069", "4966"]),
    ("twse", "2026-07-17", ["2330", "2317", "1101", "3008", "6488"]),
]


def main() -> None:
    con = duckdb.connect()
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    con.sql(f"ATTACH '{paths.CACHE_DB}' AS ca (READ_ONLY)")

    print("== (1) 抽樣逐欄比對 ==")
    for mkt, d, codes in SAMPLES:
        lst = ", ".join(f"'{c}'" for c in codes)
        pg = con.sql(f"""
            SELECT market, date, company_code,
                   foreign_investors_difference,
                   securities_investment_trust_companies_difference AS trust_difference,
                   dealers_difference, total_difference
            FROM pg.public.daily_trading_details
            WHERE market='{mkt}' AND date=DATE '{d}' AND company_code IN ({lst})
            ORDER BY company_code
        """).df()
        ca = con.sql(f"""
            SELECT market, date, company_code,
                   foreign_investors_difference, trust_difference,
                   dealers_difference, total_difference
            FROM ca.daily_trading_details
            WHERE market='{mkt}' AND date=DATE '{d}' AND company_code IN ({lst})
            ORDER BY company_code
        """).df()
        same = pg.reset_index(drop=True).equals(ca.reset_index(drop=True))
        print(f"  {mkt} {d}: pg={len(pg)} cache={len(ca)} equals={same}")
        if not same:
            print(pg.to_string(index=False))
            print(ca.to_string(index=False))
        else:
            print(ca.to_string(index=False))

    print("\n== (2) 異常值掃描(cache 全表)==")
    checks = {
        "總列數": "SELECT count(*) FROM ca.daily_trading_details",
        "重複 (market,date,code)": """
            SELECT count(*) FROM (SELECT market,date,company_code FROM ca.daily_trading_details
            GROUP BY 1,2,3 HAVING count(*)>1)""",
        "date 在未來(> today)": "SELECT count(*) FROM ca.daily_trading_details WHERE date > current_date",
        "market 非 twse/tpex": "SELECT count(*) FROM ca.daily_trading_details WHERE market NOT IN ('twse','tpex')",
        "company_code 非英數": "SELECT count(*) FROM ca.daily_trading_details WHERE NOT regexp_matches(company_code,'^[0-9A-Za-z]+$')",
        "任一欄為 NULL": """SELECT count(*) FROM ca.daily_trading_details WHERE
            foreign_investors_difference IS NULL OR trust_difference IS NULL
            OR dealers_difference IS NULL OR total_difference IS NULL""",
        # NOTE: 必須先轉 BIGINT——cache 欄位是 INTEGER(int32),直接相加會在
        # 大額日溢位丟 OutOfRangeException(實測 -778928308 + -1946343918)。
        "恆等式破裂 total<>fx+trust+dealers": """SELECT count(*) FROM ca.daily_trading_details
            WHERE total_difference::BIGINT <> foreign_investors_difference::BIGINT
                  + trust_difference::BIGINT + dealers_difference::BIGINT""",
        "三欄相加超出 int32 範圍(cache 直接相加會炸)": """
            SELECT count(*) FROM ca.daily_trading_details
            WHERE abs(foreign_investors_difference::BIGINT + trust_difference::BIGINT
                      + dealers_difference::BIGINT) > 2147483647""",
        "四欄全為 0": """SELECT count(*) FROM ca.daily_trading_details WHERE
            foreign_investors_difference=0 AND trust_difference=0 AND dealers_difference=0 AND total_difference=0""",
        "|值| 觸 int32 上界 (>2.1e9)": """SELECT count(*) FROM ca.daily_trading_details WHERE
            greatest(abs(foreign_investors_difference),abs(trust_difference),
                     abs(dealers_difference),abs(total_difference)) > 2100000000""",
    }
    rows = [(k, con.sql(q).fetchone()[0]) for k, q in checks.items()]
    print(pd.DataFrame(rows, columns=["檢查", "筆數"]).to_string(index=False))

    print("\n== (3) dealers_difference 正負號分布(分市場 × 世代)==")
    print(con.sql("""
        SELECT market,
               CASE WHEN market='twse' AND date <= DATE '2014-11-28' THEN 'twse 13欄世代'
                    WHEN market='tpex' AND date <= DATE '2014-11-28' THEN 'tpex 12/16欄世代'
                    ELSE '2014-12-01 之後' END AS era,
               count(*) AS n,
               count(*) FILTER (WHERE dealers_difference < 0) AS neg,
               min(dealers_difference) AS mn, max(dealers_difference) AS mx
        FROM ca.daily_trading_details GROUP BY 1,2 ORDER BY 1,2
    """).df().to_string(index=False))

    print("\n== (4) 恆等式破裂逐日 ==")
    print(con.sql("""
        SELECT market, date, count(*) AS n_bad
        FROM ca.daily_trading_details
        WHERE total_difference::BIGINT <> foreign_investors_difference::BIGINT
              + trust_difference::BIGINT + dealers_difference::BIGINT
        GROUP BY 1,2 ORDER BY 3 DESC LIMIT 20
    """).df().to_string(index=False))


if __name__ == "__main__":
    main()
