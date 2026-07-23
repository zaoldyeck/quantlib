"""C-daily_trading_details / 步驟 4:Python 直寫路徑會少寫幾列?

cache 的最新一天只存在於 cache(由 src/quantlib/crawl/ 直寫,不經 PG、不落原始檔),
所以「那一天的 cache 可不可信」只能靠同一支解析器在**有原始檔的日子**上的表現來推。

做法:對每個有原始檔的 twse/tpex 日,用 quantlib.crawl.sources.daily_trading_details._parse
重解析,和 cache 該日的列數比。排除 A 維已認定的 23 個「整日內容被複製自別天」的
污染日(那些日子 cache 本來就不是原始檔的內容,比了沒意義)。

用法:PYTHONPATH=<repo> uv run --project . python \
      docs/data_audit/scripts/C-daily_trading_details/04_python_writer_drop.py
"""
from __future__ import annotations

import re
from datetime import date as Date
from pathlib import Path

import duckdb

from research import paths
from quantlib.crawl.sources import daily_trading_details as dtd

_REPLAY_SQL = """
WITH agg AS (
  SELECT market, date, count(*) n,
         sum(foreign_investors_difference::HUGEINT) f,
         sum(trust_difference::HUGEINT) t,
         sum(dealers_difference::HUGEINT) d,
         sum(total_difference::HUGEINT) tot
  FROM daily_trading_details GROUP BY 1,2),
dup AS (
  SELECT market, n, f, t, d, tot FROM agg
  GROUP BY 1,2,3,4,5,6 HAVING count(*) > 1)
SELECT DISTINCT a.market, a.date FROM agg a JOIN dup USING (market, n, f, t, d, tot)
"""


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    # 內容指紋撞號的日子:cache 內容 = 別天的資料(A 維已認定的重放污染),
    # 拿它和自己的原始檔比列數沒有意義,先排除。
    replay = {(m, d.isoformat()) for m, d in con.sql(_REPLAY_SQL).fetchall()}
    print(f"排除內容重放日 {len(replay)} 個")
    total_days = 0
    bad_days: list[tuple[str, str, int, int]] = []
    for mkt in ("twse", "tpex"):
        for year_dir in sorted(Path(f"data/daily_trading_details/{mkt}").iterdir()):
            if not year_dir.is_dir():
                continue
            for p in sorted(year_dir.glob("*.csv")):
                if p.stat().st_size < 1000:
                    continue  # sentinel / 空回應
                m = re.match(r"(\d+)_(\d+)_(\d+)\.csv", p.name)
                if not m:
                    continue
                d = Date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                if (mkt, d.isoformat()) in replay:
                    continue
                try:
                    df = dtd._parse(p.read_bytes().decode("big5hkscs"), d, mkt)
                except Exception:
                    continue  # 舊世代欄位守門會擋下,不是本題目標
                if df is None:
                    continue
                n_cache = con.sql(
                    f"SELECT count(*) FROM daily_trading_details "
                    f"WHERE market='{mkt}' AND date=DATE '{d}'"
                ).fetchone()[0]
                if n_cache == 0:
                    continue
                total_days += 1
                if df.height != n_cache:
                    bad_days.append((mkt, d.isoformat(), df.height, n_cache))

    print(f"可比對日數(解析器吃得下的世代):{total_days}")
    print(f"列數不符日數:{len(bad_days)},累計少寫 {sum(c - p for _, _, p, c in bad_days)} 列")
    for mkt, d, npy, nca in bad_days:
        print(f"  {mkt} {d}: python={npy} cache={nca} 少寫={nca - npy}")


if __name__ == "__main__":
    main()
