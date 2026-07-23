"""A-sbl_borrowing ②:6,266 個原始檔 vs PostgreSQL 的全量逐欄比對(非抽樣)。

自己寫一套獨立解析器(TWSE Big5 CSV / TPEx JSON,只複製 QuantlibCSVReader 的兩條
特規,不呼叫受測程式),把每一列的 8 個欄位算出來,再與 PG `sbl_borrowing` 以
(market, date, company_code) 全外連接逐欄比對。順便量:

  * 只在原始檔 / 只在 DB 的列數(漏匯 vs 幽靈)
  * 檔內重複代號(reader 的 distinctBy 會靜靜吃掉)
  * 恆等式 前日餘額 + 當日賣出 - 當日還券 + 當日調整 == 當日餘額(驗欄位對應)
  * 檔名日期 vs 檔案內容的民國日期(TWSE 標題列 / TPEx `date` 欄)
  * TPEx `totalCount` vs 實際資料列數
  * 備註欄(schema 沒接)的非空比例與取值分佈

Run: PYTHONPATH=<repo> uv run --project research python docs/data_audit/scripts/A-sbl_borrowing/02_recon.py
依賴:PostgreSQL 在跑(唯讀);不依賴 cache。
"""
from __future__ import annotations

import collections
import csv
import datetime as dt
import json
import os
import re
from pathlib import Path

import duckdb
import polars as pl

from research import paths

RAW = paths.RAW / "sbl_borrowing"
FNAME = re.compile(r"^(\d{4})_(\d{1,2})_(\d{1,2})\.csv$")
CODE = re.compile(r"[0-9][0-9A-Z]*")          # reader 的 stockCode(大寫限定)
CODE_LAX = re.compile(r"[0-9][0-9A-Za-z]*")   # 放寬版,用來抓「被大寫限定擋掉」的列
TW_TITLE = re.compile(r"(\d{2,3})年(\d{2})月(\d{2})日")
PG_DSN = os.environ.get(
    "QL_PG_DSN",
    f"host=localhost port=5432 dbname=quantlib user={os.environ.get('USER', 'zaoldyeck')}")

NUM = ("prev", "sold", "ret", "adj", "bal", "limit")


def clean(s: str) -> str:
    return s.replace(",", "").replace("%", "").replace(" ", "").strip()


def twse_lines(path: Path):
    raw = path.read_bytes().decode("big5hkscs", errors="replace")
    for line in raw.splitlines():
        if '""' in line and ',""' not in line:
            continue
        if not line.strip():
            continue
        yield next(csv.reader([line.replace("=", "")]))


def parse(path: Path, market: str):
    """→ (rows, meta)。rows = list[dict];meta 裝版型/日期/備註等旁證。"""
    meta = {"content_date": None, "declared_n": None, "n_raw_rows": 0,
            "n_lax_only": 0, "n_short": 0, "notes": collections.Counter()}
    rows = []
    if market == "tpex":
        obj = json.loads(path.read_text("utf-8"))
        meta["content_date"] = obj.get("date")
        t = obj["tables"][0]
        meta["declared_n"] = t.get("totalCount")
        data = t.get("data", [])
    else:
        data = []
        for r in twse_lines(path):
            if meta["content_date"] is None and r and TW_TITLE.search(r[0]):
                m = TW_TITLE.search(r[0])
                meta["content_date"] = f"{int(m[1]) + 1911:04d}{m[2]}{m[3]}"
            data.append(r)
    for r in data:
        head = r[0].strip() if r else ""
        if not CODE_LAX.fullmatch(head):
            continue
        meta["n_raw_rows"] += 1
        if len(r) < 14:
            meta["n_short"] += 1
            continue
        if not CODE.fullmatch(r[0]):          # reader 用未 trim 的 head 比對
            meta["n_lax_only"] += 1
            continue
        v = [clean(x) for x in r]
        if len(v) >= 15:
            meta["notes"][v[14]] += 1
        try:
            rows.append({
                "company_code": v[0], "company_name": v[1],
                "prev": int(v[8]), "sold": int(v[9]), "ret": int(v[10]),
                "adj": int(v[11]), "bal": int(v[12]),
                "limit": int(v[13]) if v[13].lstrip("-").isdigit() else 0,
            })
        except ValueError:
            meta["n_short"] += 1
    return rows, meta


def main() -> None:
    recs, metas = [], []
    for market in ("twse", "tpex"):
        for path in sorted((RAW / market).rglob("*.csv")):
            m = FNAME.match(path.name)
            if not m or path.stat().st_size < 200:
                continue
            date = dt.date(int(m[1]), int(m[2]), int(m[3]))
            rows, meta = parse(path, market)
            dupes = len(rows) - len({r["company_code"] for r in rows})
            metas.append({"market": market, "date": date, "file": path.name,
                          "content_date": meta["content_date"],
                          "declared_n": meta["declared_n"], "n_parsed": len(rows),
                          "n_raw_rows": meta["n_raw_rows"], "n_short": meta["n_short"],
                          "n_lax_only": meta["n_lax_only"], "n_dup_code": dupes,
                          "notes_nonblank": sum(n for k, n in meta["notes"].items() if k),
                          "notes_total": sum(meta["notes"].values())})
            for r in rows:
                recs.append({"market": market, "date": date, **r})

    raw = pl.DataFrame(recs)
    meta = pl.DataFrame(metas, infer_schema_length=None).with_columns(
        pl.col("declared_n").cast(pl.Int64))
    print(f"== 獨立解析 {meta.height} 檔 / {raw.height} 列 ==")

    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{PG_DSN}' AS pg (TYPE POSTGRES, READ_ONLY)")
    con.register("raw", raw)
    con.register("meta", meta)
    con.execute("""CREATE TEMP TABLE db AS SELECT market, date, company_code, company_name,
                   prev_day_balance, daily_sold, daily_returned, daily_adjustment,
                   daily_balance, next_day_limit FROM pg.public.sbl_borrowing""")

    print("\n== 列數對照(逐市場)==")
    print(con.execute("""
        SELECT 'raw' src, market, count(*) n, count(DISTINCT date) d FROM raw GROUP BY 1,2
        UNION ALL SELECT 'db', market, count(*), count(DISTINCT date) FROM db GROUP BY 1,2
        ORDER BY 2,1""").pl())

    print("\n== 全外連接逐欄差異 ==")
    print(con.execute("""
        SELECT count(*) FILTER (r.company_code IS NULL) AS only_db,
               count(*) FILTER (d.company_code IS NULL) AS only_raw,
               count(*) FILTER (r.company_name IS DISTINCT FROM d.company_name) AS d_name,
               count(*) FILTER (r.prev  IS DISTINCT FROM d.prev_day_balance)  AS d_prev,
               count(*) FILTER (r.sold  IS DISTINCT FROM d.daily_sold)        AS d_sold,
               count(*) FILTER (r.ret   IS DISTINCT FROM d.daily_returned)    AS d_ret,
               count(*) FILTER (r.adj   IS DISTINCT FROM d.daily_adjustment)  AS d_adj,
               count(*) FILTER (r.bal   IS DISTINCT FROM d.daily_balance)     AS d_bal,
               count(*) FILTER (r."limit" IS DISTINCT FROM d.next_day_limit)  AS d_limit
        FROM raw r FULL JOIN db d USING (market, date, company_code)""").pl())

    for col, dbcol in [("prev", "prev_day_balance"), ("sold", "daily_sold"),
                       ("ret", "daily_returned"), ("adj", "daily_adjustment"),
                       ("bal", "daily_balance"), ("limit", "next_day_limit"),
                       ("company_name", "company_name")]:
        bad = con.execute(f"""SELECT market, date, company_code, r.{col} AS raw_v, d.{dbcol} AS db_v
                              FROM raw r JOIN db d USING (market, date, company_code)
                              WHERE r.{col} IS DISTINCT FROM d.{dbcol} LIMIT 8""").pl()
        if bad.height:
            print(f"\n-- 差異樣本 {col} --")
            print(bad)

    print("\n== 只在原始檔 / 只在 DB 的樣本 ==")
    print(con.execute("""SELECT CASE WHEN d.company_code IS NULL THEN 'only_raw' ELSE 'only_db' END side,
                                COALESCE(r.market,d.market) market, COALESCE(r.date,d.date) date,
                                COALESCE(r.company_code,d.company_code) code
                         FROM raw r FULL JOIN db d USING (market, date, company_code)
                         WHERE r.company_code IS NULL OR d.company_code IS NULL LIMIT 20""").pl())

    print("\n== 恆等式 prev + sold - ret + adj == bal ==")
    print(con.execute("""SELECT market, count(*) n,
                                count(*) FILTER (prev + sold - ret + adj <> bal) AS mismatch
                         FROM raw GROUP BY 1""").pl())
    print(con.execute("""SELECT market, date, company_code, prev, sold, ret, adj, bal
                         FROM raw WHERE prev + sold - ret + adj <> bal LIMIT 10""").pl())

    print("\n== 檔名日期 vs 內容日期 ==")
    print(con.execute("""SELECT * FROM meta
                         WHERE content_date IS NULL
                            OR content_date <> strftime(date, '%Y%m%d') LIMIT 20""").pl())

    print("\n== TPEx totalCount vs 實際列數 / 短列 / 大小寫被擋 / 檔內重複代號 ==")
    print(con.execute("""SELECT market,
                                count(*) FILTER (declared_n IS NOT NULL AND declared_n <> n_raw_rows) AS bad_count,
                                sum(n_short) AS n_short, sum(n_lax_only) AS lax_only,
                                sum(n_dup_code) AS dup_code,
                                sum(notes_nonblank) AS notes_nonblank, sum(notes_total) AS notes_total
                         FROM meta GROUP BY 1""").pl())

    out = paths.REPO / "docs/data_audit/scripts/A-sbl_borrowing/recon_meta.csv"
    meta.write_csv(out)
    print(f"\n[meta 落檔] {out}")


if __name__ == "__main__":
    main()
