"""C-capital_reduction 稽核 02:原始 CSV(全史)vs PostgreSQL 逐欄比對。

capital_reduction 的爬蟲是「區間下載」——每個檔案是一段日期區間的全量傾印
(Task.pullCapitalReduction:strDate = 既有檔名最大值+1,endDate = 昨天;
檔名取 endDate)。所以「缺漏」不能用逐日檔案在不在來判,只能把所有檔案 union
起來,和 DB 對帳。

本腳本重現 TradingReader.readCapitalReduction 的解析規則(TWSE 12 欄 / TPEx 10 欄,
去空白與逗號),把全部原始檔解析成 (market, date, company_code) → 11 欄,再與 PG
雙向比對。

Run: uv run --project research python docs/data_audit/scripts/C-capital_reduction/02_raw_vs_db.py
"""
from __future__ import annotations

import csv
import io
import re
import sys
from collections import defaultdict
from datetime import date as Date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402

ROOT = Path(__file__).resolve().parents[4]
RAW = ROOT / "data" / "capital_reduction"
PG_DSN = "host=localhost port=5432 dbname=quantlib user=zaoldyeck"

_SLASH = re.compile(r"^(\d+)/(\d+)/(\d+)$")
_MINGUO7 = re.compile(r"^\d{7}$")


def _rows(path: Path) -> list[list[str]]:
    raw = path.read_bytes()
    if not raw:
        return []
    text = raw.decode("big5hkscs", errors="replace")
    return list(csv.reader(io.StringIO(text)))


def _clean(r: list[str]) -> list[str]:
    return [x.replace(" ", "").replace(",", "") for x in r]


def parse_twse(path: Path) -> list[tuple]:
    out = []
    for r in _rows(path):
        if len(r) != 12 or r[0] == "恢復買賣日期":
            continue
        c = _clean(r)
        m = _SLASH.match(c[0])
        if not m:
            continue
        d = Date(int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3)))
        out.append(("twse", d, c[1], c[2], c[3], c[4], c[5], c[6], c[7], c[8], c[9]))
    return out


def parse_tpex(path: Path) -> list[tuple]:
    out = []
    for r in _rows(path):
        if len(r) != 10 or r[0].strip().startswith("恢復買賣日期"):
            continue
        c = _clean(r)
        if not _MINGUO7.match(c[0]):
            continue
        d = Date(int(c[0][:3]) + 1911, int(c[0][3:5]), int(c[0][5:7]))
        out.append(("tpex", d, c[1], c[2], c[3], c[4], c[5], c[6], c[7], c[8], c[9]))
    return out


def _f(s: str) -> float | None:
    try:
        return float(s)
    except ValueError:
        return None


def main() -> None:
    per_key: dict[tuple, list[tuple]] = defaultdict(list)
    file_stats = []
    for market, parser in (("twse", parse_twse), ("tpex", parse_tpex)):
        files = sorted((RAW / market).rglob("*.csv"))
        n_empty = 0
        n_rows = 0
        for p in files:
            if p.stat().st_size == 0:
                n_empty += 1
                continue
            recs = parser(p)
            n_rows += len(recs)
            for t in recs:
                per_key[(t[0], t[1], t[2])].append((p.name, t))
        file_stats.append((market, len(files), n_empty, n_rows))

    print("== 原始檔盤點 ==")
    for market, nf, ne, nr in file_stats:
        print(f"  {market}: {nf} 檔(0-byte {ne}),解析出 {nr} 列(含跨檔重複)")
    print(f"  唯一鍵 (market,date,code) = {len(per_key)}")

    # 跨檔同鍵值衝突(區間傾印會重複出現同一筆,值必須一致)
    conflicts = []
    for k, lst in per_key.items():
        vals = {t[1] for t in lst}
        if len(vals) > 1:
            conflicts.append((k, lst))
    print(f"\n== 跨檔同鍵值衝突:{len(conflicts)} 個 ==")
    for k, lst in conflicts[:10]:
        print("  ", k)
        for name, t in lst:
            print("     ", name, t)

    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    pg = con.sql("""
        SELECT market, date, company_code, company_name,
               closing_price_on_the_last_trading_date, post_reduction_reference_price,
               limit_up, limit_down, opening_reference_price,
               ex_right_reference_price, reason_for_capital_reduction
        FROM pg.public.capital_reduction
    """).fetchall()
    pg_map = {(r[0], r[1], r[2]): r for r in pg}
    print(f"\n== PG 列數 {len(pg)},唯一鍵 {len(pg_map)} ==")

    raw_keys = set(per_key)
    pg_keys = set(pg_map)
    only_raw = sorted(raw_keys - pg_keys)
    only_pg = sorted(pg_keys - raw_keys)
    print(f"\n== 原始檔有 / PG 無:{len(only_raw)} ==")
    for k in only_raw[:50]:
        print("  ", k, per_key[k][0][0], per_key[k][0][1][3:])
    print(f"\n== PG 有 / 原始檔無:{len(only_pg)} ==")
    for k in only_pg[:50]:
        print("  ", k, pg_map[k])

    # 逐欄比對
    labels = ["company_name", "close_last_trading", "post_ref", "limit_up",
              "limit_down", "opening_ref", "ex_right_ref", "reason"]
    bad = defaultdict(list)
    for k in sorted(raw_keys & pg_keys):
        _, t = per_key[k][0]
        p = pg_map[k]
        raw_vals = [t[3], _f(t[4]), _f(t[5]), _f(t[6]), _f(t[7]), _f(t[8]),
                    (None if t[9] == "--" else _f(t[9])), t[10]]
        pg_vals = [p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10]]
        for lab, rv, pv in zip(labels, raw_vals, pg_vals):
            if isinstance(rv, float) and isinstance(pv, float):
                same = abs(rv - pv) < 1e-9
            else:
                same = rv == pv
            if not same:
                bad[lab].append((k, rv, pv))

    print("\n== 共用鍵逐欄差異 ==")
    for lab in labels:
        lst = bad[lab]
        print(f"  {lab:22} {len(lst)}")
        for item in lst[:5]:
            print("     ", item)


if __name__ == "__main__":
    main()
