"""C-tdcc_shareholding 稽核 ①:cache(DuckDB) vs PostgreSQL 一致性 + schema + 抽樣。

問題:DuckDB cache 的 tdcc_shareholding 與 PG 是否一字不差?schema 有沒有漏欄/型別降級?

做法(全部可重跑,零抽樣假設):
  1. schema 對照:PG information_schema vs DuckDB DESCRIBE,逐欄名/型別。
  2. 投影 parity:cache_tables.py 的建表 SQL vs db.py 的 pg-attach view SQL,
     兩種存取模式的欄位投影必須一致。
  3. 全量指紋:對 12 個 data_date 各算 count + sum(hash(6 欄)::HUGEINT)
     + bit_xor(hash(6 欄)),cache 與 PG 三個指紋逐格相等 → 非抽樣的全體證明。
  4. 抽樣逐欄:3 個 data_date × 5 檔股票,pandas DataFrame.equals 全 True。

cache 依賴:需 var/cache/cache.duckdb 為最新(research/cache_tables.py 建);
           PG 需在 localhost:5432/quantlib。

執行:uv run --project research python docs/data_audit/scripts/C-tdcc_shareholding/01_parity_schema_sample.py
"""
from __future__ import annotations

import os

import duckdb
import pandas as pd

from research import paths

PG_DSN = os.environ.get(
    "QL_PG_DSN",
    f"host=localhost port=5432 dbname=quantlib user={os.environ.get('USER', 'zaoldyeck')}",
)
COLS = ["data_date", "company_code", "holding_tier",
        "num_holders", "num_shares", "pct_of_outstanding"]


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")

    print("=" * 70)
    print("① SCHEMA 對照")
    pg_schema = con.sql(
        "SELECT column_name, data_type FROM pg.information_schema.columns "
        "WHERE table_name='tdcc_shareholding' ORDER BY ordinal_position"
    ).fetchall()
    ca_schema = con.sql("DESCRIBE tdcc_shareholding").fetchall()
    print("  PG   :", pg_schema)
    print("  cache:", [(c[0], c[1]) for c in ca_schema])
    pg_cols = {r[0] for r in pg_schema}
    ca_cols = {r[0] for r in ca_schema}
    print("  只在 PG(cache 丟掉):", pg_cols - ca_cols)
    print("  只在 cache(多出來):", ca_cols - pg_cols)

    print("=" * 70)
    print("② 整表 count + 日期範圍")
    for tag, tbl in (("PG", "pg.public.tdcc_shareholding"), ("cache", "tdcc_shareholding")):
        r = con.sql(
            f"SELECT count(*), count(DISTINCT data_date), min(data_date), max(data_date) FROM {tbl}"
        ).fetchone()
        print(f"  {tag:5}: rows={r[0]:,} dates={r[1]} range={r[2]}..{r[3]}")

    print("=" * 70)
    print("③ 全量指紋:每個 data_date 的 count + sum(hash) + bit_xor(hash),cache vs PG")
    hash_expr = "hash(" + " || '|' || ".join(f"CAST({c} AS VARCHAR)" for c in COLS) + ")"
    fp_sql = (
        "SELECT data_date, count(*) n, "
        f"       sum({hash_expr}::HUGEINT) s, bit_xor({hash_expr}) x "
        "FROM {tbl} GROUP BY data_date ORDER BY data_date"
    )
    pg_fp = con.sql(fp_sql.format(tbl="pg.public.tdcc_shareholding")).fetchall()
    ca_fp = con.sql(fp_sql.format(tbl="tdcc_shareholding")).fetchall()
    mism = [(p, c) for p, c in zip(pg_fp, ca_fp) if p != c]
    extra_pg = {r[0] for r in pg_fp} - {r[0] for r in ca_fp}
    extra_ca = {r[0] for r in ca_fp} - {r[0] for r in pg_fp}
    print(f"  比對 {len(pg_fp)} 個 data_date;mismatch={len(mism)} only_pg={extra_pg} only_cache={extra_ca}")
    for p, c in mism:
        print("   MISMATCH pg=", p, " cache=", c)
    print("  RESULT:", "PASS ✅" if (not mism and not extra_pg and not extra_ca) else "FAIL ❌")

    print("=" * 70)
    print("④ 抽樣逐欄:3 data_date × 5 codes,DataFrame.equals")
    samples = [
        ("2026-04-17", ["0050", "1101", "2317", "2330", "2412"]),
        ("2026-06-18", ["2454", "2603", "3008", "6488", "8069"]),
        ("2026-07-17", ["1216", "2308", "2882", "3711", "5871"]),
    ]
    all_ok = True
    for d, codes in samples:
        clist = ",".join(f"'{c}'" for c in codes)
        q = (
            "SELECT " + ",".join(COLS) + " FROM {tbl} "
            f"WHERE data_date='{d}' AND company_code IN ({clist}) "
            "ORDER BY company_code, holding_tier"
        )
        pg_df = con.sql(q.format(tbl="pg.public.tdcc_shareholding")).df()
        ca_df = con.sql(q.format(tbl="tdcc_shareholding")).df()
        eq = pg_df.equals(ca_df)
        all_ok &= eq
        print(f"  {d} × {len(codes)} codes: rows={len(pg_df)} equals={eq}")
    print("  RESULT:", "PASS ✅" if all_ok else "FAIL ❌")

    con.close()


if __name__ == "__main__":
    main()
