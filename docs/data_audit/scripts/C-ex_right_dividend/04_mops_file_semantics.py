"""C-ex_right_dividend 稽核 04:MOPS 月報檔的查詢語義 —— 月份參數是「公告日期」還是「除權息日」?

這決定了 2024-07 起才開始抓 MOPS(Task.pullExRightDividend:357-358 firstYear=2024/
firstMonth=7)會不會造成永久缺口:若月份 = 公告日期,則「6 月公告、7 月除息」的
事件落在不存在的 2024_6.csv 裡,永遠補不回來。

同時輸出每個檔案「解析後應產生幾列」,供與 PG 實際列數對帳。

Run: uv run --project research python docs/data_audit/scripts/C-ex_right_dividend/04_mops_file_semantics.py
"""
from __future__ import annotations

import csv
import io
import sys
from collections import Counter
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

ROOT = Path(__file__).resolve().parents[4]
DATA = ROOT / "data" / "ex_right_dividend"

# MOPS t108sb27 欄位索引(與 TradingReader.parseMopsRows 一致)
I_CODE, I_NAME = 0, 1
I_STOCK_SURPLUS, I_STOCK_CAPITAL, I_EX_RIGHT = 4, 5, 6
I_CASH_SURPLUS, I_CASH_STATUTORY, I_CASH_PREF, I_EX_DIV = 7, 8, 9, 10
I_ANNOUNCE = 16


def _d(v: str) -> float:
    try:
        return float(v.replace(",", "").strip())
    except (ValueError, AttributeError):
        return 0.0


def _slash(s: str) -> date | None:
    s = (s or "").strip()
    try:
        y, m, d = s.split("/")
        return date(int(y), int(m), int(d))
    except (ValueError, TypeError):
        return None


def read_mops(path: Path) -> list[list[str]]:
    raw = path.read_bytes().decode("big5-hkscs", errors="replace")
    return [r for r in csv.reader(io.StringIO(raw))]


def parse_rows(rows: list[list[str]], market: str) -> list[dict]:
    """複製 parseMopsRows 的邏輯(含 r.size >= 17 的過濾)。"""
    out = []
    for r in rows:
        if len(r) < 17 or not r[I_CODE].strip() or r[I_CODE].strip() == "公司代號":
            continue
        code = r[I_CODE].strip()
        total_stock = _d(r[I_STOCK_SURPLUS]) + _d(r[I_STOCK_CAPITAL])
        total_cash = _d(r[I_CASH_SURPLUS]) + _d(r[I_CASH_STATUTORY]) + _d(r[I_CASH_PREF])
        ex_right, ex_div = _slash(r[I_EX_RIGHT]), _slash(r[I_EX_DIV])
        ann = _slash(r[I_ANNOUNCE]) if len(r) > I_ANNOUNCE else None
        if total_cash > 0 and ex_div:
            out.append({"market": market, "date": ex_div, "code": code,
                        "cash": total_cash, "kind": "息", "ann": ann})
        if total_stock > 0 and ex_right:
            out.append({"market": market, "date": ex_right, "code": code,
                        "cash": 0.0, "kind": "權", "ann": ann})
    return out


def main() -> None:
    print("== 每個 MOPS 月報檔:原始列數 / 解析出的列數 / 公告日期月份分佈 / 除權息日月份分佈 ==")
    for market in ("twse", "tpex"):
        for year_dir in sorted((DATA / market).iterdir()):
            if not year_dir.is_dir():
                continue
            for f in sorted(year_dir.glob("*.csv")):
                # 只看月報檔 YYYY_M.csv(legacy 是 YYYY_M_D.csv)
                if f.stem.count("_") != 1:
                    continue
                rows = read_mops(f)
                parsed = parse_rows(rows, market)
                short = sum(1 for r in rows if r and len(r) < 17 and r[0].strip()
                            and r[0].strip() != "公司代號")
                ann_months = Counter(f"{r['ann'].year}-{r['ann'].month:02d}"
                                     for r in parsed if r["ann"])
                ev_months = Counter(f"{r['date'].year}-{r['date'].month:02d}" for r in parsed)
                print(f"{market} {f.name:14} raw={len(rows):4} short_rows={short:3} "
                      f"parsed={len(parsed):4} ann={dict(sorted(ann_months.items()))} "
                      f"ev={dict(sorted(ev_months.items()))}")


if __name__ == "__main__":
    main()
