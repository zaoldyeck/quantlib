"""C-capital_reduction 稽核 04:每一筆減資事件是「哪個原始檔第一次抓到的」。

區間下載的表看不出逐日缺漏,但可以看「抓到的延遲」:
  lag = 第一個含有該筆的檔案的檔名日期 - 事件日期
routine 增量抓取正常時 lag 應該只有幾天;lag 大到以年計 = 當時的增量抓取回空,
資料是後來某次全量傾印才補回來的(= 增量管線壞了但無聲)。

Run: uv run --project research python docs/data_audit/scripts/C-capital_reduction/04_provenance.py
"""
from __future__ import annotations

import sys
from collections import defaultdict
from datetime import date as Date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

spec_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(spec_dir))
from importlib import import_module  # noqa: E402

mod = import_module("02_raw_vs_db")

ROOT = Path(__file__).resolve().parents[4]
RAW = ROOT / "data" / "capital_reduction"


def file_date(name: str) -> Date:
    y, m, d = name.removesuffix(".csv").split("_")
    return Date(int(y), int(m), int(d))


def main() -> None:
    per_key: dict[tuple, list[tuple[Date, str]]] = defaultdict(list)
    for market, parser in (("twse", mod.parse_twse), ("tpex", mod.parse_tpex)):
        for p in sorted((RAW / market).rglob("*.csv")):
            if p.stat().st_size == 0:
                continue
            fd = file_date(p.name)
            for t in parser(p):
                per_key[(t[0], t[1], t[2])].append((fd, p.name))

    rows = []
    for k, lst in per_key.items():
        lst.sort()
        first_fd, first_name = lst[0]
        rows.append((k[0], k[1], k[2], first_fd, first_name, (first_fd - k[1]).days, len(lst)))
    rows.sort(key=lambda r: (r[0], r[1]))

    print("== 每個 market 的第一次抓到延遲(天)分佈 ==")
    for market in ("twse", "tpex"):
        lags = sorted(r[5] for r in rows if r[0] == market)
        n = len(lags)
        def q(p):
            return lags[min(n - 1, int(p * n))]
        print(f"  {market}: n={n} min={lags[0]} p50={q(.5)} p90={q(.9)} max={lags[-1]}")

    print("\n== 只出現在 2026_5_20.csv(意外全量傾印)的列 ==")
    only_dump = [r for r in rows if r[6] == 1 and r[4] == "2026_5_20.csv"]
    print(f"  {len(only_dump)} 列")
    for r in sorted(only_dump)[-30:]:
        print("   ", r[0], r[1], r[2], "lag", r[5])
    if only_dump:
        print("  日期範圍:", min(r[1] for r in only_dump), "~", max(r[1] for r in only_dump))

    print("\n== 只出現在 2020_7_10.csv(初次全量)的列數 ==")
    only_first = [r for r in rows if r[6] == 1 and r[4] == "2020_7_10.csv"]
    print(f"  {len(only_first)} 列;日期範圍",
          min((r[1] for r in only_first), default=None), "~",
          max((r[1] for r in only_first), default=None))

    print("\n== 增量檔(非兩個全量傾印)第一次抓到的列 ==")
    inc = [r for r in rows if r[4] not in ("2020_7_10.csv", "2026_5_20.csv")]
    print(f"  {len(inc)} 列;日期範圍",
          min((r[1] for r in inc), default=None), "~",
          max((r[1] for r in inc), default=None))
    by_year = defaultdict(int)
    for r in inc:
        by_year[(r[0], r[1].year)] += 1
    tot_by_year = defaultdict(int)
    for r in rows:
        tot_by_year[(r[0], r[1].year)] += 1
    print("\n  逐年:market year 增量抓到 / 總數")
    for k in sorted(tot_by_year):
        print(f"    {k[0]} {k[1]}  {by_year.get(k,0):>3} / {tot_by_year[k]:>3}")

    print("\n== lag > 365 天的列(增量當時漏抓,後來補回)==")
    late = [r for r in rows if r[5] > 365]
    print(f"  {len(late)} 列")
    for r in sorted(late, key=lambda x: x[1])[-20:]:
        print("   ", r[0], r[1], r[2], "first_file", r[4], "lag", r[5])


if __name__ == "__main__":
    main()
