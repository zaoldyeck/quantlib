"""C-capital_reduction 稽核 11:指定的隨機抽樣逐欄比對 + 新鮮度 + 材料性。

1. 隨機挑 3 個「有資料的日期」x 每天最多 5 檔,cache vs PG 逐欄比對(seed 20260722)。
2. 新鮮度:cache / PG 最新事件日、資料齊備日對照。
3. 材料性:被 prices.py 護欄丟掉的 15 檔,在事件前的日均成交金額(ADV),
   用來判斷這些假報酬會不會真的進到策略池(Universe 門檻 ADV >= NT$50M)。

Run: uv run --project . python docs/data_audit/scripts/C-capital_reduction/11_sample_and_freshness.py
"""
from __future__ import annotations

import random
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

    # 挑「當天至少 3 檔」的日期,才有辦法做到 3 日 x 5 檔的規格
    dates = [r[0] for r in con.sql(
        "SELECT date FROM capital_reduction GROUP BY date HAVING COUNT(*) >= 3 "
        "ORDER BY date").fetchall()]
    rng = random.Random(20260722)
    picks = rng.sample(dates, 3)
    print("== 抽樣日期(seed 20260722,每天 >=3 檔)==", picks)

    total = same = 0
    for d in picks:
        codes = [r[0] for r in con.sql(
            f"SELECT company_code FROM capital_reduction WHERE date=DATE '{d}' "
            f"ORDER BY company_code").fetchall()][:5]
        for code in codes:
            c = con.sql(f"""SELECT market,date,company_code,post_reduction_reference_price,
                                   reason_for_capital_reduction
                            FROM capital_reduction
                            WHERE date=DATE '{d}' AND company_code='{code}'""").fetchall()
            p = con.sql(f"""SELECT market,date,company_code,post_reduction_reference_price,
                                   reason_for_capital_reduction
                            FROM pg.public.capital_reduction
                            WHERE date=DATE '{d}' AND company_code='{code}'""").fetchall()
            total += 1
            ok = sorted(c) == sorted(p)
            same += ok
            print(f"  {d} {code}: {'SAME' if ok else 'DIFF'}  cache={c}  pg={p}")
    print(f"  → {same}/{total} 完全相同")

    print("\n== 新鮮度 ==")
    print(con.sql("""
      SELECT 'cache' AS src, MAX(date) AS max_event_date, COUNT(*) AS n FROM capital_reduction
      UNION ALL
      SELECT 'pg', MAX(date), COUNT(*) FROM pg.public.capital_reduction
    """).df().to_string())

    print("\n== 材料性:被 prices.py 護欄丟掉的事件,事件前 60 個交易日 ADV(NT$) ==")
    print(con.sql("""
      WITH bad AS (
        SELECT market,date,company_code,company_name,
               post_reduction_reference_price/closing_price_on_the_last_trading_date AS f
        FROM pg.public.capital_reduction
        WHERE post_reduction_reference_price/closing_price_on_the_last_trading_date
              NOT BETWEEN 0.05 AND 5.0)
      SELECT b.market, b.date, b.company_code, b.company_name, ROUND(b.f,2) AS f,
             ROUND((SELECT AVG(q.trade_value) FROM daily_quote q
                    WHERE q.market=b.market AND q.company_code=b.company_code
                      AND q.date < b.date AND q.date >= b.date - INTERVAL 120 DAY
                      AND q.trade_value > 0)) AS adv_ntd
      FROM bad b ORDER BY adv_ntd DESC NULLS LAST
    """).df().to_string())


if __name__ == "__main__":
    main()
