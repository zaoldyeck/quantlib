"""C-ex_right_dividend 稽核 02:逐月分佈 + 季節性缺口偵測。

台股除權息高度季節化(6~9 月為旺季),所以「某年比往年少」必須看逐月才知道是
少在旺季(=漏抓)還是少在淡季(=正常波動)。本腳本以 PG 為準(cache 只是它的
子集 + Python 爬蟲增量),輸出 market x year x month 的矩陣與同月歷史中位數比。

Run: uv run --project . python docs/data_audit/scripts/C-ex_right_dividend/02_monthly_profile.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402

PG_DSN = "host=localhost port=5432 dbname=quantlib user=zaoldyeck"


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    con.sql("""
      CREATE OR REPLACE TEMP VIEW e AS
      SELECT market, date, company_code, cash_dividend, right_or_dividend,
             closing_price_before_ex_right_ex_dividend AS pre_close,
             ex_right_ex_dividend_reference_price AS ref_px
      FROM pg.public.ex_right_dividend
    """)

    for mkt in ("twse", "tpex"):
        print(f"\n===== {mkt}:逐年 x 逐月筆數(PG 全量)=====")
        print(con.sql(f"""
          PIVOT (SELECT year(date) y, month(date) m, COUNT(*) n
                 FROM e WHERE market='{mkt}' GROUP BY 1,2)
          ON m IN (1,2,3,4,5,6,7,8,9,10,11,12) USING first(n) GROUP BY y ORDER BY y
        """).df().to_string())

    print("\n===== 旺季(6~9 月)逐年合計 =====")
    print(con.sql("""
      SELECT year(date) y, market, COUNT(*) n
      FROM e WHERE month(date) BETWEEN 6 AND 9
      GROUP BY 1,2 ORDER BY 1,2
    """).df().pivot(index="y", columns="market", values="n").to_string())

    print("\n===== 每月相對同月歷史中位數的比值(<0.6 = 可疑缺漏)=====")
    print(con.sql("""
      WITH mm AS (SELECT market, year(date) y, month(date) m, COUNT(*) n
                  FROM e GROUP BY 1,2,3),
           med AS (SELECT market, m, median(n) med_n FROM mm
                   WHERE y BETWEEN 2010 AND 2023 GROUP BY 1,2)
      SELECT mm.market, mm.y, mm.m, mm.n, med.med_n,
             round(mm.n / nullif(med.med_n,0), 2) ratio
      FROM mm JOIN med USING (market, m)
      WHERE mm.y >= 2015 AND mm.n / nullif(med.med_n,0) < 0.6
      ORDER BY 1,2,3
    """).df().to_string())

    print("\n===== 全史「整月零筆」的 (market, year, month) =====")
    print(con.sql("""
      WITH span AS (SELECT market, min(date) mn, max(date) mx FROM e GROUP BY 1),
           cal AS (SELECT s.market, y.y, m.m
                   FROM span s,
                        (SELECT unnest(range(2003, 2027)) y) y,
                        (SELECT unnest(range(1, 13)) m) m
                   WHERE make_date(y.y, m.m, 1) BETWEEN date_trunc('month', s.mn)
                                                    AND date_trunc('month', s.mx)),
           mm AS (SELECT market, year(date) y, month(date) m, COUNT(*) n
                  FROM e GROUP BY 1,2,3)
      SELECT cal.market, cal.y, cal.m
      FROM cal LEFT JOIN mm USING (market, y, m)
      WHERE mm.n IS NULL ORDER BY 1,2,3
    """).df().to_string())


if __name__ == "__main__":
    main()
