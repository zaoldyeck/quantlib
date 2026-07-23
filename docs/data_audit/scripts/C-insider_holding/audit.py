"""C-insider_holding cache audit — reproducible evidence.

Run:  PYTHONPATH=<repo> uv run --project research python docs/data_audit/scripts/C-insider_holding/audit.py
Needs: research/cache_tables.py current (cache built 2026-07-21); PostgreSQL up.

Answers dim-C questions for table `insider_holding`:
  1. schema parity cache vs PG
  2. row-count parity (total + per-year) + full bidirectional value diff (table is tiny, 771 rows)
  3. reader completeness: raw HTML valid rows == PG rows (no silent drops)
  4. coverage gaps vs trading calendar (the 19y void + 2026-window missed crawls)
  5. anomaly scan (impossible values, future dates, all-zero cols, declare_date semantics)
"""
from __future__ import annotations
import os, re, glob, subprocess
import duckdb
from bs4 import BeautifulSoup
from research import paths

DSN = f"host=localhost port=5432 dbname=quantlib user={os.environ.get('USER','zaoldyeck')}"
COLS = ("market, report_date, declare_date, company_code, company_name, reporter_title, "
        "reporter_name, transfer_method, transferee, transfer_shares, max_intraday_shares, "
        "current_shares_own, current_shares_trust, planned_shares_own, planned_shares_trust")
STOCK = re.compile(r'^[0-9][0-9A-Z]{3,}$')


def load_html(f: str) -> str:
    b = open(f, 'rb').read()
    try:
        s = b.decode('utf-8')
        if '�' in s:
            raise ValueError
    except Exception:
        s = b.decode('big5-hkscs', 'replace')
    return s


def rows_of(f: str):
    """Replicate reader/TradingReader.parseMopsHtml: every <td>.text of every <table tr>."""
    soup = BeautifulSoup(load_html(f), 'html.parser')
    out = []
    for tr in soup.select('table tr'):
        cells = [re.sub(r'\s+', ' ', td.get_text(separator=' ', strip=True)).strip()
                 for td in tr.find_all('td')]
        if cells:
            out.append(cells)
    return out


def main():
    ch = duckdb.connect()  # in-memory driver; ATTACH both sides read-only
    ch.sql(f"ATTACH '{paths.CACHE_DB}' AS c (READ_ONLY)")
    ch.sql("INSTALL postgres; LOAD postgres;")
    ch.sql(f"ATTACH '{DSN}' AS pg (TYPE postgres, READ_ONLY)")

    print("== 1. schema ==")
    print("cache:", ch.sql("PRAGMA table_info('c.insider_holding')").fetchall())

    print("\n== 2. counts + full value diff ==")
    cn = ch.sql("SELECT COUNT(*) FROM c.insider_holding").fetchone()[0]
    pn = ch.sql("SELECT COUNT(*) FROM pg.public.insider_holding").fetchone()[0]
    d1 = ch.sql(f"SELECT {COLS} FROM c.insider_holding EXCEPT SELECT {COLS} FROM pg.public.insider_holding").fetchall()
    d2 = ch.sql(f"SELECT {COLS} FROM pg.public.insider_holding EXCEPT SELECT {COLS} FROM c.insider_holding").fetchall()
    print(f"cache={cn} pg={pn}  cache_only={len(d1)} pg_only={len(d2)}")
    print("per-year:", ch.sql("SELECT EXTRACT(YEAR FROM report_date) y, COUNT(*) n, "
                              "COUNT(DISTINCT report_date) d FROM c.insider_holding GROUP BY 1 ORDER BY 1").fetchall())

    print("\n== 3. reader completeness (raw HTML valid rows vs PG) ==")
    pg_cnt = {}
    res = subprocess.run(["psql", "-h", "localhost", "-p", "5432", "-d", "quantlib", "-t", "-A",
                          "-F", "|", "-c",
                          "SELECT market, report_date, COUNT(*) FROM insider_holding GROUP BY 1,2"],
                         capture_output=True, text=True)
    for ln in res.stdout.strip().splitlines():
        p = [x.strip() for x in ln.split('|')]
        if len(p) == 3:
            pg_cnt[(p[0], p[1])] = int(p[2])
    tot_html = tot_pg = 0
    concat = []      # multi-value transfer_shares cells (the concatenation bug)
    trust_nz = 0     # raw rows with non-zero trust cols [11]/[13]
    for f in sorted(glob.glob("data/insider_holding/*/*/*.html")):
        if os.path.getsize(f) < 1024:
            continue
        mkt = "twse" if "/twse/" in f else "tpex"
        mm = re.search(r'(\d+)_(\d+)_(\d+)\.html$', os.path.basename(f))
        date = f"{int(mm[1]):04d}-{int(mm[2]):02d}-{int(mm[3]):02d}"
        seen = set()
        for r in rows_of(f):
            if len(r) >= 14 and STOCK.match(r[2]):
                key = (mkt, date, r[2], r[5], r[6], r[9])   # reader dedupe key
                if key not in seen:
                    seen.add(key)
                if r[11] not in ('', '0') or r[13] not in ('', '0'):
                    trust_nz += 1
                if ' ' in r[7].strip() and re.search(r'\d', r[7]):
                    concat.append((date, mkt, r[2], r[6], r[7]))
        tot_html += len(seen)
        tot_pg += pg_cnt.get((mkt, date), 0)
    print(f"raw-HTML valid+dedup rows={tot_html}  PG rows(matched files)={tot_pg}  (equal => no drops)")
    print(f"raw rows with NON-ZERO trust cols: {trust_nz}  (0 => all-zero trust is source truth)")
    print(f"transfer_shares concatenation rows (two space-sep values in cell [7]): {len(concat)}")
    for x in concat:
        print("   CONCAT", x)

    print("\n== 4. coverage gaps ==")
    print("2007 last:", ch.sql("SELECT MAX(report_date) FROM c.insider_holding WHERE report_date<'2010-01-01'").fetchone()[0],
          " 2026 first:", ch.sql("SELECT MIN(report_date) FROM c.insider_holding WHERE report_date>'2010-01-01'").fetchone()[0],
          " rows in void:", ch.sql("SELECT COUNT(*) FROM c.insider_holding WHERE report_date BETWEEN '2007-01-10' AND '2026-03-30'").fetchone()[0])
    def file_date(f: str) -> str:
        m = re.search(r'(\d+)_(\d+)_(\d+)\.html$', os.path.basename(f))
        return f"{int(m[1]):04d}-{int(m[2]):02d}-{int(m[3]):02d}"
    for mkt in ("twse", "tpex"):
        tdays = [r[0].isoformat() for r in ch.sql(
            f"SELECT DISTINCT date FROM c.daily_quote WHERE market='{mkt}' "
            f"AND date BETWEEN '2026-03-31' AND '2026-07-17' ORDER BY date").fetchall()]
        have = {file_date(f) for f in glob.glob(f"data/insider_holding/{mkt}/2026/*.html")}
        no_file = [d for d in tdays if d not in have]
        print(f"[{mkt}] trading_days={len(tdays)} NO_FILE={no_file}")

    print("\n== 5. anomaly scan ==")
    T = "c.insider_holding"
    print("markets:", ch.sql(f"SELECT market, COUNT(*) FROM {T} GROUP BY 1").fetchall())
    for col in ("transfer_shares", "max_intraday_shares", "current_shares_own",
                "current_shares_trust", "planned_shares_own", "planned_shares_trust"):
        mn, mx, neg = ch.sql(f"SELECT MIN({col}), MAX({col}), COUNT(*) FILTER(WHERE {col}<0) FROM {T}").fetchone()
        print(f"  {col:22} min={mn} max={mx} neg={neg}")
    print("future report_date:", ch.sql(f"SELECT COUNT(*) FROM {T} WHERE report_date>CURRENT_DATE").fetchone()[0])
    print("declare!=report:", ch.sql(f"SELECT COUNT(*) FROM {T} WHERE declare_date<>report_date").fetchone()[0])
    print("dup on unique key:", len(ch.sql(
        f"SELECT 1 FROM {T} GROUP BY market,report_date,company_code,reporter_name,transfer_method,transferee "
        f"HAVING COUNT(*)>1").fetchall()))
    print("transfer_shares=0 by method:", ch.sql(
        f"SELECT transfer_method, COUNT(*) FROM {T} WHERE transfer_shares=0 GROUP BY 1 ORDER BY 2 DESC").fetchall())


if __name__ == "__main__":
    main()
