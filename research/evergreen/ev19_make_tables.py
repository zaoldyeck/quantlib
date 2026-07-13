"""EV19 增強表生成:讀 ev17_tables(月初+籌碼),每行追加估值+財報欄。

追加欄(asof = 表日,PIT):
  |PB位x PE位x 殖x|益a→b
  - PB位/PE位 = PB/PE 三年(756 交易日)分位%(高=貴)
  - 殖 = 殖利率%
  - 益a→b = 前一已公佈季→最新已公佈季的累計營益率%(拐點視覺化)
輸出 ev19_tables/。依 EV18 歸因證據(dim_verdict):估值分位=偽形排除
最強;營益率拐點=與營收正交的品質 MVP。

需要 cache 最新。Run:
  uv run --project research python -m research.evergreen.ev19_make_tables 2023-02-01 ...
"""
from __future__ import annotations

import os
import sys
from datetime import date as Date

import duckdb

from research.evergreen.ev18_make_packs import fin_asof

SRC = "research/evergreen/data/ev17_tables"
OUT = "research/evergreen/data/ev19_tables"


def valuation_asof(raw: duckdb.DuckDBPyConnection, asof: str) -> dict[str, str]:
    q = raw.sql(f"""
    WITH h AS (
        SELECT company_code, date, price_to_earning_ratio pe,
               price_book_ratio pb, dividend_yield dy
        FROM stock_per_pbr
        WHERE date <= DATE '{asof}' AND date > DATE '{asof}' - INTERVAL 1100 DAY
    ),
    cur AS (
        SELECT company_code, pe, pb, dy FROM h
        QUALIFY row_number() OVER (PARTITION BY company_code ORDER BY date DESC) = 1
    )
    SELECT h.company_code,
           any_value(cur.pb), any_value(cur.pe), any_value(cur.dy),
           100.0*sum(CASE WHEN h.pb <= cur.pb THEN 1 END)/nullif(count(h.pb),0),
           100.0*sum(CASE WHEN h.pe <= cur.pe THEN 1 END)/nullif(count(h.pe),0)
    FROM h JOIN cur USING (company_code)
    GROUP BY h.company_code
    """).fetchall()

    def n(v):
        return "na" if v is None else f"{v:.0f}"

    def n1(v):
        return "na" if v is None else f"{v:.1f}"

    return {r[0]: f"PB位{n(r[4])} PE位{n(r[5])} 殖{n1(r[3])}" for r in q}


def margins_asof(raw: duckdb.DuckDBPyConnection, asof: str) -> dict[str, str]:
    qs = fin_asof(Date.fromisoformat(asof))[-2:]
    if len(qs) < 2:
        return {}
    res: dict[str, list] = {}
    for i, (y, q) in enumerate(qs):
        rows = raw.execute("""
        SELECT company_code,
          100.0*max(CASE WHEN title LIKE '營業利益%' THEN value END)
            / nullif(max(CASE WHEN title IN ('營業收入','營業收入合計') THEN value END),0)
        FROM is_progressive_raw WHERE year = ? AND quarter = ?
        GROUP BY company_code""", [y, q]).fetchall()
        for code, om in rows:
            res.setdefault(code, [None, None])[i] = om

    def n1(v):
        return "na" if v is None else f"{v:.1f}"

    return {c: f"益{n1(v[0])}→{n1(v[1])}" for c, v in res.items()}


def main() -> None:
    tags = sys.argv[1:]
    if not tags:
        raise SystemExit("usage: ev19_make_tables.py YYYY-MM-DD ...")
    raw = duckdb.connect("research/cache.duckdb", read_only=True)
    os.makedirs(OUT, exist_ok=True)
    for tag in tags:
        src = open(f"{SRC}/{tag}.txt").read().splitlines()
        val = valuation_asof(raw, tag)
        mar = margins_asof(raw, tag)
        header = (src[0] + "|PB位 PE位 殖(PB/PE 三年分位%,高=貴;殖利率%)"
                  "|益前→新(前一→最新已公佈季累計營益率%)")
        lines = [header]
        for line in src[1:]:
            code = line.split()[0]
            lines.append(f"{line}|{val.get(code, 'PB位na PE位na 殖na')}"
                         f"|{mar.get(code, '益na→na')}")
        path = f"{OUT}/{tag}.txt"
        open(path, "w").write("\n".join(lines) + "\n")
        print(f"{path}  {os.path.getsize(path)//1024}KB  {len(lines)-1} 檔")


if __name__ == "__main__":
    main()
