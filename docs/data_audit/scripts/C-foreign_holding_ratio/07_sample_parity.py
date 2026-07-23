"""C-foreign_holding_ratio 稽核 07:隨機抽樣逐欄比對(3 日 x 5 檔 x 2 market)+ 最新日健檢。

Run: uv run --project research python docs/data_audit/scripts/C-foreign_holding_ratio/07_sample_parity.py
"""
from __future__ import annotations

import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402

PG_DSN = "host=localhost port=5432 dbname=quantlib user=zaoldyeck"
SEED = 20260722
COLS = ["outstanding_shares", "foreign_remaining_shares", "foreign_held_shares",
        "foreign_remaining_ratio", "foreign_held_ratio", "foreign_limit_ratio"]


def main() -> None:
    rnd = random.Random(SEED)
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")

    total = same = 0
    for market in ("twse", "tpex"):
        dates = [r[0] for r in con.execute(
            "SELECT DISTINCT date FROM foreign_holding_ratio WHERE market=? ORDER BY 1", [market]).fetchall()]
        picks = rnd.sample(dates, 3)
        for d in picks:
            codes = [r[0] for r in con.execute(
                "SELECT company_code FROM foreign_holding_ratio WHERE market=? AND date=? ORDER BY company_code",
                [market, d]).fetchall()]
            for code in rnd.sample(codes, min(5, len(codes))):
                c = con.execute(
                    f"SELECT {','.join(COLS)} FROM foreign_holding_ratio "
                    "WHERE market=? AND date=? AND company_code=?", [market, d, code]).fetchone()
                p = con.execute(
                    f"SELECT {','.join(COLS)} FROM pg.public.foreign_holding_ratio "
                    "WHERE market=? AND date=? AND company_code=?", [market, d, code]).fetchone()
                total += 1
                ok = c == p
                same += ok
                print(f"  {market} {d} {code:>7}  {'SAME' if ok else 'DIFF'}  cache={c}  pg={p}")
    print(f"\n抽樣結果:{same}/{total} 逐欄完全相同(seed={SEED})")

    print("\n== 最新 5 個交易日健檢(逐 market)==")
    print(con.sql("""
      WITH d AS (SELECT market, date, COUNT(*) n,
                        quantile_cont(foreign_held_ratio,0.5) med_ratio,
                        SUM(foreign_held_shares) tot_held
                 FROM foreign_holding_ratio GROUP BY 1,2)
      SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY market ORDER BY date DESC) rk FROM d
      ) WHERE rk <= 5 ORDER BY market, date
    """).df().to_string())


if __name__ == "__main__":
    main()
