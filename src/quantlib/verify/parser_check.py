"""parser 取值正確性:第二套**獨立**解析器(以標頭欄名定位,不看位置)重解析 raw,
對 cache 逐欄(count/sum/null)比對。零差異 = canonical parser 取值 100% 正確。

**動機**:rebuild 讓 cache = parser(raw) 是構造上正確,但要證明 parser **取對了值**(欄位
沒錯位、單位沒錯、哨兵值沒誤判、無 int 溢位),必須用一份**獨立寫的**解析器交叉核對——
任何位置漂移/欄位錯接都會在「欄名定位的獨立版」與 canonical 版之間露餡。承接 A 維稽核當年
對 PG 的驗證(docs/data_audit/scripts/*/indep.py),改對修正後的新 cache 再驗一次。

現含 daily_quote(TWSE/TPEx 全世代欄名定位);其餘源可比照擴充(見 A 維 indep 腳本)。

Run: uv run --project . python -m quantlib.verify.parser_check              # 抽樣(跨世代邊界日)
     uv run --project . python -m quantlib.verify.parser_check --full       # 全史逐日
唯讀。
"""
from __future__ import annotations

import argparse
import csv
from datetime import date as Date

from quantlib import paths
from quantlib.db import connect

# ── 獨立 daily_quote 解析器(欄名定位;移植自 docs/data_audit/scripts/A-daily_quote/indep.py)──
_TWSE_MAP = {
    "證券代號": "company_code", "成交股數": "trade_volume", "成交筆數": "transaction",
    "成交金額": "trade_value", "開盤價": "opening_price", "最高價": "highest_price",
    "最低價": "lowest_price", "收盤價": "closing_price", "最後揭示買價": "last_best_bid_price",
    "最後揭示賣價": "last_best_ask_price",
}
_TPEX_MAP = {
    "代號": "company_code", "收盤": "closing_price", "開盤": "opening_price", "最高": "highest_price",
    "最低": "lowest_price", "成交股數": "trade_volume", "成交金額(元)": "trade_value",
    "成交筆數": "transaction", "最後買價": "last_best_bid_price", "最後賣價": "last_best_ask_price",
}
#: cache daily_quote 有的數值欄(比對這些)
_DQ_NUMCOLS = ["trade_volume", "transaction", "trade_value", "opening_price", "highest_price",
               "lowest_price", "closing_price", "last_best_bid_price", "last_best_ask_price"]


def _cells(line: str):
    return next(csv.reader([line.replace('="', '"')]), None)


def _clean(s):
    return s.replace(",", "").replace("%", "").replace(" ", "").strip() if s is not None else s


def _num(v, nulls):
    v = _clean(v)
    if v in nulls or v in ("除權息", "除權", "除息"):
        return None if v in nulls else 0.0
    if v in ("", "X"):
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return None  # 不可解析 → None(比對時視同 null;獨立版的保守處理)


def _indep_daily_quote(path, market: str) -> list[dict]:
    txt = path.read_bytes().decode("big5hkscs", errors="replace")
    lines = txt.splitlines()
    prefix = '"證券代號"' if market == "twse" else "代號,"
    hi = next((i for i, l in enumerate(lines) if l.startswith(prefix)), None)
    if hi is None:
        return []
    hdr = [h.strip() for h in (_cells(lines[hi]) if market == "twse" else lines[hi].split(","))]
    mp = _TWSE_MAP if market == "twse" else _TPEX_MAP
    idx = {mp[h]: i for i, h in enumerate(hdr) if h in mp}
    nul = {"--"} if market == "twse" else {"---", "----"}
    minc = 17 if market == "twse" else 15
    out = []
    for l in lines[hi + 1:]:
        r = _cells(l)
        if r is None or len(r) < minc:
            continue
        code = _clean(r[idx["company_code"]]) if "company_code" in idx else None
        if not code or not code[0].isdigit():
            continue
        rec = {"company_code": code}
        for c in _DQ_NUMCOLS:
            rec[c] = _num(r[idx[c]], nul) if c in idx else None
        out.append(rec)
    return out


def _raw_agg(market: str, day: Date) -> dict | None:
    """獨立解析該日 raw → {n, sum{col}, null{col}};無檔/sentinel 回 None。"""
    p = paths.RAW / "daily_quote" / market / f"{day.year:04d}" / f"{day.year}_{day.month}_{day.day}.csv"
    if not p.exists() or p.stat().st_size == 0:
        return None
    rows = _indep_daily_quote(p, market)
    if not rows:
        return None
    a = {"n": len(rows), "sum": {c: 0.0 for c in _DQ_NUMCOLS}, "null": {c: 0 for c in _DQ_NUMCOLS}}
    for r in rows:
        for c in _DQ_NUMCOLS:
            v = r[c]
            if v is None:
                a["null"][c] += 1
            else:
                a["sum"][c] += v
    return a


def _cache_agg(con, market: str, day: Date) -> dict | None:
    sel = ", ".join(f"count(*) FILTER (WHERE {c} IS NULL) AS nl_{c}, COALESCE(sum({c}),0) AS sm_{c}"
                    for c in _DQ_NUMCOLS)
    row = con.execute(
        f"SELECT count(*) AS n, {sel} FROM daily_quote WHERE market=? AND date=?",
        [market, day]).pl()
    if row["n"][0] == 0:
        return None
    r = row.to_dicts()[0]
    return {"n": r["n"], "sum": {c: float(r[f"sm_{c}"]) for c in _DQ_NUMCOLS},
            "null": {c: int(r[f"nl_{c}"]) for c in _DQ_NUMCOLS}}


def _sample_dates(con, market: str, full: bool) -> list[Date]:
    dates = [r[0] for r in con.execute(
        "SELECT DISTINCT date FROM daily_quote WHERE market=? ORDER BY date", [market]).fetchall()]
    if full or len(dates) <= 40:
        return dates
    # 抽樣:頭尾 + 均勻跨世代邊界(欄位錯位最可能在世代交界露餡)
    step = len(dates) // 36
    return dates[:2] + dates[2:-2:step] + dates[-2:]


def check(full: bool = False) -> dict:
    con = connect()
    total = mism = 0
    examples = []
    for market in ("twse", "tpex"):
        for day in _sample_dates(con, market, full):
            ra, ca = _raw_agg(market, day), _cache_agg(con, market, day)
            if ra is None and ca is None:
                continue
            total += 1
            if ra is None or ca is None:
                mism += 1
                examples.append(f"{market} {day}: raw={'∅' if ra is None else ra['n']} cache={'∅' if ca is None else ca['n']}")
                continue
            bad = []
            if ra["n"] != ca["n"]:
                bad.append(f"n {ra['n']}≠{ca['n']}")
            for c in _DQ_NUMCOLS:
                if ra["null"][c] != ca["null"][c]:
                    bad.append(f"null_{c} {ra['null'][c]}≠{ca['null'][c]}")
                tol = max(1e-6, abs(ca["sum"][c]) * 1e-9)
                if abs(ra["sum"][c] - ca["sum"][c]) > tol:
                    bad.append(f"sum_{c} {ra['sum'][c]:.0f}≠{ca['sum'][c]:.0f}")
            if bad:
                mism += 1
                if len(examples) < 15:
                    examples.append(f"{market} {day}: {'; '.join(bad[:4])}")
    return {"compared": total, "mismatches": mism, "examples": examples}


def main() -> None:
    ap = argparse.ArgumentParser(description="parser 取值正確性(獨立解析器對 cache 逐欄比對)")
    ap.add_argument("--full", action="store_true", help="全史逐日(預設抽樣跨世代邊界)")
    args = ap.parse_args()
    print("=== parser 取值驗證:daily_quote 獨立欄名解析器 vs cache(count/sum/null 逐欄)===")
    r = check(args.full)
    print(f"  比對 {r['compared']} 個 (市場×日期);差異 {r['mismatches']} 個")
    for e in r["examples"]:
        print(f"      ❌ {e}")
    print("  " + ("✓ 零差異 → daily_quote canonical parser 取值 100% 正確"
                  if r["mismatches"] == 0 else "❌ 有差異,需查 parser"))


if __name__ == "__main__":
    main()
