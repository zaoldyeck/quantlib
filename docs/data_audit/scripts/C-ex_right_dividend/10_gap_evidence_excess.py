"""C-ex_right_dividend 稽核 10:用「超額報酬」證明 twse 2024 除息事件真的發生過。

09 腳本的原始跌幅證據被 2024-08-05 全球股災汙染(當日加權指數 −8.35%),所以
改用「個股報酬 − 大盤報酬」的最負一天當訊號:除息造成的跳空是純個股事件,
會在超額報酬上留下缺口;股災是共同因子,會被大盤扣掉。

三組對照:
  miss  = 2023、2025 有除息,2024 沒有(疑似漏抓)
  have  = 2024 有除息紀錄(應該看到同樣的超額跳空)
  never = 2022~2026 完全沒有任何除息紀錄(不配息公司;超額跳空應明顯較小)

Run: uv run --project research python docs/data_audit/scripts/C-ex_right_dividend/10_gap_evidence_excess.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402

WIN = ("2024-06-01", "2024-09-30")


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

    con.sql(f"""
      CREATE OR REPLACE TEMP VIEW mkt AS
      SELECT date, close / lag(close) OVER (ORDER BY date) - 1 AS mret
      FROM market_index
      WHERE market='twse' AND name='發行量加權股價指數'
        AND date BETWEEN DATE '{WIN[0]}' - INTERVAL 10 DAY AND DATE '{WIN[1]}'
    """)
    print("大盤基準列數:", con.sql("SELECT COUNT(*) FROM mkt WHERE mret IS NOT NULL").fetchone())

    con.sql(f"""
      CREATE OR REPLACE TEMP VIEW ex AS
      WITH px AS (
        SELECT market, company_code, date, closing_price,
               lag(closing_price) OVER (PARTITION BY market, company_code ORDER BY date) prev
        FROM daily_quote
        WHERE market='twse' AND date BETWEEN DATE '{WIN[0]}' AND DATE '{WIN[1]}')
      SELECT px.company_code, px.date, px.prev, px.closing_price,
             px.closing_price/px.prev - 1 AS ret, mkt.mret,
             (px.closing_price/px.prev - 1) - mkt.mret AS excess
      FROM px JOIN mkt USING (date)
      WHERE px.prev > 0
    """)

    con.sql("""
      CREATE OR REPLACE TEMP VIEW grp AS
      WITH y AS (SELECT company_code, year(date) y FROM ex_right_dividend
                 WHERE market='twse' AND cash_dividend > 0 GROUP BY 1,2),
      c23 AS (SELECT company_code FROM y WHERE y=2023),
      c24 AS (SELECT company_code FROM y WHERE y=2024),
      c25 AS (SELECT company_code FROM y WHERE y=2025),
      anyy AS (SELECT DISTINCT company_code FROM y WHERE y BETWEEN 2022 AND 2026),
      universe AS (SELECT DISTINCT company_code FROM ex)
      SELECT u.company_code,
             CASE WHEN u.company_code IN (SELECT company_code FROM c23)
                   AND u.company_code IN (SELECT company_code FROM c25)
                   AND u.company_code NOT IN (SELECT company_code FROM c24) THEN 'miss'
                  WHEN u.company_code IN (SELECT company_code FROM c24) THEN 'have'
                  WHEN u.company_code NOT IN (SELECT company_code FROM anyy) THEN 'never'
                  ELSE 'other' END AS grp
      FROM universe u
    """)

    print("\n== 各組最負超額報酬(每檔取窗內最小 excess)==")
    print(con.sql("""
      WITH w AS (
        SELECT ex.company_code, g.grp, ex.date, ex.excess,
               row_number() OVER (PARTITION BY ex.company_code ORDER BY ex.excess) rn
        FROM ex JOIN grp g USING (company_code))
      SELECT grp, COUNT(*) n,
             round(median(excess), 4) med_min_excess,
             round(quantile_cont(excess, 0.25), 4) q25,
             round(quantile_cont(excess, 0.75), 4) q75,
             COUNT(*) FILTER (WHERE excess <= -0.03) n_le_3pct,
             COUNT(*) FILTER (WHERE excess <= -0.05) n_le_5pct
      FROM w WHERE rn=1 GROUP BY 1 ORDER BY 1
    """).df().to_string())

    print("\n== 各組『最負超額日』的日期分佈(前 12 名)==")
    print(con.sql("""
      WITH w AS (
        SELECT ex.company_code, g.grp, ex.date, ex.excess,
               row_number() OVER (PARTITION BY ex.company_code ORDER BY ex.excess) rn
        FROM ex JOIN grp g USING (company_code))
      SELECT grp, date, COUNT(*) n FROM w WHERE rn=1 AND grp IN ('miss','have','never')
      GROUP BY 1,2 QUALIFY row_number() OVER (PARTITION BY grp ORDER BY COUNT(*) DESC) <= 12
      ORDER BY grp, n DESC
    """).df().to_string())

    print("\n== miss 組逐檔樣本(前 15):最負超額日 + 2023/2025 股利 ==")
    print(con.sql("""
      WITH w AS (
        SELECT ex.company_code, g.grp, ex.date, ex.prev, ex.closing_price,
               ex.ret, ex.mret, ex.excess,
               row_number() OVER (PARTITION BY ex.company_code ORDER BY ex.excess) rn
        FROM ex JOIN grp g USING (company_code) WHERE g.grp='miss')
      SELECT w.company_code, w.date, w.prev, w.closing_price,
             round(w.ret,4) ret, round(w.mret,4) mret, round(w.excess,4) excess,
             e23.cash_dividend v2023, e25.cash_dividend v2025,
             round(coalesce(e23.cash_dividend,0)/w.prev, 4) AS yield_if_2023_div
      FROM w
      LEFT JOIN ex_right_dividend e23 ON e23.market='twse'
             AND e23.company_code=w.company_code AND year(e23.date)=2023
      LEFT JOIN ex_right_dividend e25 ON e25.market='twse'
             AND e25.company_code=w.company_code AND year(e25.date)=2025
      WHERE w.rn=1 ORDER BY w.company_code LIMIT 15
    """).df().to_string())


if __name__ == "__main__":
    main()
