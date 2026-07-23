"""A-ex_right_dividend parse-correctness audit — independent reparser + parity.

Reparses raw ex_right_dividend CSVs WITHOUT calling TradingReader (the unit under
test), then compares column-by-column against PostgreSQL `ex_right_dividend`.
Also semantically cross-checks `closing_price_before_ex_right_ex_dividend`
against the real `daily_quote` close on the trading day before the ex-date.

Three raw formats exist (detected by filename, matching the reader):
  - legacy TWSE  YYYY_M_D.csv : 16-col, date '109年07月15日', cash<-col5(權值+息值)
  - legacy TPEx  YYYY_M_D.csv : 21/22-col, date '109/07/15', cash<-col7(權值+息值)
  - MOPS monthly YYYY_M.csv   : t108sb27, price cols unavailable -> reader stores 0

Run:  uv run --project . python \
        docs/data_audit/scripts/A-ex_right_dividend/01_reparse_parity.py
Needs: PostgreSQL (authoritative reader output). Does NOT need the DuckDB cache.
"""
from __future__ import annotations
import csv, io, re, sys, datetime as dt
from pathlib import Path
import duckdb

ROOT = Path("/Users/zaoldyeck/Documents/scala/quantlib")
RAW = ROOT / "data" / "ex_right_dividend"
DSN = "host=localhost port=5432 dbname=quantlib user=zaoldyeck"

TW_DATE = re.compile(r"(\d+)年(\d+)月(\d+)日")
MG_DATE = re.compile(r"(\d+)/(\d+)/(\d+)")
DAILY_FN = re.compile(r"^\d+_\d+_\d+\.csv$")
MONTHLY_FN = re.compile(r"^\d+_\d+\.csv$")


def num(s: str) -> float | None:
    s = s.replace(",", "").replace(" ", "").strip()
    if s == "" or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def read_cells(path: Path) -> list[list[str]]:
    # Big5-HKSCS decode (matches reader's "Big5-HKSCS"); standard csv respects quotes.
    raw = path.read_bytes().decode("big5hkscs", errors="replace")
    return list(csv.reader(io.StringIO(raw)))


def parse_twse_legacy(rows):
    out = []
    for r in rows:
        if len(r) != 16 or r[0] == "資料日期":
            continue
        m = TW_DATE.match(r[0].replace(" ", ""))
        if not m:
            continue
        y, mo, d = int(m[1]) + 1911, int(m[2]), int(m[3])
        date = dt.date(y, mo, d)
        out.append(dict(
            market="twse", date=date, code=r[1].replace(" ", ""),
            close_bef=num(r[3]), ref=num(r[4]), cash=num(r[5]),
            rd=r[6].replace(" ", ""), lim_up=num(r[7]), lim_dn=num(r[8]),
            open_ref=num(r[9]), exdiv_ref=num(r[10]),
        ))
    return out


def parse_tpex_legacy(rows):
    rd_map = {"除權": "權", "除息": "息", "除權息": "權息"}
    out = []
    for r in rows:
        if len(r) <= 20 or r[0] == "除權息日期":
            continue
        m = MG_DATE.match(r[0].replace(" ", ""))
        if not m:
            continue
        y, mo, d = int(m[1]) + 1911, int(m[2]), int(m[3])
        date = dt.date(y, mo, d)
        out.append(dict(
            market="tpex", date=date, code=r[1].replace(" ", ""),
            close_bef=num(r[3]), ref=num(r[4]), cash=num(r[7]),
            rd=rd_map.get(r[8].replace(" ", ""), r[8].replace(" ", "")),
            lim_up=num(r[9]), lim_dn=num(r[10]),
            open_ref=num(r[11]), exdiv_ref=num(r[12]),
        ))
    return out


def parse_mops(rows, market):
    out = []
    for r in rows:
        if len(r) < 17 or r[0] == "公司代號" or r[0].strip() == "":
            continue
        code, name = r[0].strip(), r[1].strip()
        stock = (num(r[4]) or 0) + (num(r[5]) or 0)
        ex_right = r[6].strip()
        cash = (num(r[7]) or 0) + (num(r[8]) or 0) + (num(r[9]) or 0)
        ex_div = r[10].strip()
        if cash > 0 and ex_div:
            try:
                date = dt.datetime.strptime(ex_div, "%Y/%m/%d").date()
                out.append(dict(market=market, date=date, code=code, rd="息",
                                close_bef=0.0, ref=0.0, cash=cash,
                                lim_up=0.0, lim_dn=0.0, open_ref=0.0, exdiv_ref=0.0))
            except ValueError:
                pass
        if stock > 0 and ex_right:
            try:
                date = dt.datetime.strptime(ex_right, "%Y/%m/%d").date()
                out.append(dict(market=market, date=date, code=code, rd="權",
                                close_bef=0.0, ref=0.0, cash=0.0,
                                lim_up=0.0, lim_dn=0.0, open_ref=0.0, exdiv_ref=0.0))
            except ValueError:
                pass
    return out


def parse_file(path: Path, market: str):
    rows = read_cells(path)
    if MONTHLY_FN.match(path.name):
        return parse_mops(rows, market)
    if market == "twse":
        return parse_twse_legacy(rows)
    return parse_tpex_legacy(rows)


SAMPLES = [
    ("twse", "2020/2020_7_15.csv"), ("tpex", "2020/2020_7_15.csv"),
    ("tpex", "2020/2020_7_10.csv"),                       # 22-col bulk 2011-2020
    ("twse", "2022/2022_4_27.csv"), ("tpex", "2023/2023_8_19.csv"),
    ("twse", "2026/2026_1.csv"), ("tpex", "2026/2026_1.csv"),   # MOPS monthly
    ("tpex", "2026/2026_1_21.csv"),                      # legacy range overlapping MOPS
]


def approx(a, b, tol=0.005):
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(a - b) <= tol + 1e-9


def main():
    con = duckdb.connect()
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{DSN}' AS pg (TYPE postgres, READ_ONLY);")

    print("=" * 70)
    print("PART 1 — independent reparse vs PG (per sampled file)")
    print("=" * 70)
    for market, rel in SAMPLES:
        path = RAW / market / rel
        if not path.exists():
            print(f"  [skip missing] {market}/{rel}")
            continue
        parsed = parse_file(path, market)
        # collapse to unique (market,date,code) as reader's distinctBy does
        by_key = {}
        for p in parsed:
            by_key.setdefault((p["market"], p["date"], p["code"]), p)
        dates = sorted({d for (_, d, _) in by_key})
        if not dates:
            print(f"  [no rows] {market}/{rel}")
            continue
        db = con.sql(f"""
            SELECT market, date, company_code AS code,
                   closing_price_before_ex_right_ex_dividend AS close_bef,
                   ex_right_ex_dividend_reference_price AS ref_price,
                   cash_dividend AS cash_amt,
                   right_or_dividend AS rd, limit_up AS lim_up, limit_down AS lim_dn,
                   opening_reference_price AS open_ref,
                   ex_dividend_reference_price AS exdiv_ref
            FROM pg.ex_right_dividend
            WHERE market='{market}' AND date BETWEEN DATE '{dates[0]}' AND DATE '{dates[-1]}'
        """).fetchall()
        cols = ["market", "date", "code", "close_bef", "ref", "cash", "rd",
                "lim_up", "lim_dn", "open_ref", "exdiv_ref"]
        dbmap = {(r[0], r[1], r[2]): dict(zip(cols, r)) for r in db}
        n_match = n_val_mismatch = n_only_raw = 0
        examples = []
        for key, p in by_key.items():
            if key not in dbmap:
                n_only_raw += 1
                if len(examples) < 4:
                    examples.append(f"    only-in-raw {key} rd={p['rd']} cash={p['cash']}")
                continue
            d = dbmap[key]
            diffs = []
            for f in ["close_bef", "ref", "cash", "lim_up", "lim_dn", "open_ref", "exdiv_ref"]:
                if not approx(p[f], d[f]):
                    diffs.append(f"{f}: raw={p[f]} db={d[f]}")
            if p["rd"] != d["rd"]:
                diffs.append(f"rd: raw={p['rd']} db={d['rd']}")
            if diffs:
                n_val_mismatch += 1
                if len(examples) < 8:
                    examples.append(f"    MISMATCH {key}: " + "; ".join(diffs))
            else:
                n_match += 1
        print(f"\n{market}/{rel}  raw_keys={len(by_key)} span={dates[0]}..{dates[-1]}")
        print(f"    match={n_match} val_mismatch={n_val_mismatch} only_in_raw={n_only_raw}")
        for e in examples:
            print(e)

    print("\n" + "=" * 70)
    print("PART 2 — semantic check: stored close_bef == real daily_quote close on")
    print("         the trading day BEFORE ex-date (legacy rows, non-zero prices)")
    print("=" * 70)
    q = con.sql("""
        WITH e AS (
          SELECT market, date, company_code,
                 closing_price_before_ex_right_ex_dividend AS pre
          FROM pg.ex_right_dividend
          WHERE closing_price_before_ex_right_ex_dividend > 0
            AND date BETWEEN DATE '2020-01-01' AND DATE '2024-06-30'
        ),
        pv AS (
          SELECT e.market, e.date, e.company_code, e.pre,
                 (SELECT q.closing_price FROM pg.daily_quote q
                   WHERE q.market=e.market AND q.company_code=e.company_code
                     AND q.date < e.date ORDER BY q.date DESC LIMIT 1) AS real_prev
          FROM e
        )
        SELECT market,
               COUNT(*) n,
               SUM(CASE WHEN real_prev IS NULL THEN 1 ELSE 0 END) no_quote,
               SUM(CASE WHEN real_prev IS NOT NULL
                         AND abs(pre-real_prev) <= 0.01*real_prev THEN 1 ELSE 0 END) within_1pct,
               SUM(CASE WHEN real_prev IS NOT NULL
                         AND abs(pre-real_prev) > 0.05*real_prev THEN 1 ELSE 0 END) off_gt5pct
        FROM pv GROUP BY market ORDER BY market
    """).fetchall()
    for r in q:
        print(f"  {r[0]}: n={r[1]} no_quote={r[2]} within_1pct={r[3]} off_gt5pct={r[4]}")

    print("\n" + "=" * 70)
    print("PART 3 — dual-source LOSS: tpex 2025-2026 events whose legacy range-file")
    print("         on disk has REAL prices but PG stored ZERO (MOPS won the race)")
    print("=" * 70)
    # reparse every tpex legacy range-file present in 2025-2026, build real-price map
    legacy_real = {}
    for yr in ("2025", "2026"):
        d = RAW / "tpex" / yr
        if not d.exists():
            continue
        for f in sorted(d.glob("*_*_*.csv")):
            for p in parse_tpex_legacy(read_cells(f)):
                if p["close_bef"] and p["close_bef"] > 0:
                    legacy_real[(p["date"], p["code"])] = (p["close_bef"], f.name)
    db = con.sql("""
        SELECT date, company_code,
               closing_price_before_ex_right_ex_dividend pre, cash_dividend, right_or_dividend
        FROM pg.ex_right_dividend
        WHERE market='tpex' AND date >= DATE '2025-01-01'
    """).fetchall()
    lost = []
    for date, code, pre, cash, rd in db:
        if (pre is None or pre == 0) and (date, code) in legacy_real:
            real, fn = legacy_real[(date, code)]
            lost.append((date, code, rd, cash, real, fn))
    print(f"  tpex 2025-2026 PG rows with zero close_bef that HAVE a real legacy price on disk: {len(lost)}")
    for row in lost[:15]:
        print(f"    {row[0]} {row[1]} rd={row[2]} db_cash={row[3]} db_close=0  legacy_close={row[4]} ({row[5]})")

    con.close()


if __name__ == "__main__":
    main()
