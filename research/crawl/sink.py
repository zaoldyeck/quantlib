"""DuckDB 增量 upsert sink(單一 writer;刪匹配 key + 插入,idempotent)。

與 `cache_tables.py`(全砍重建)相反:只前進當日/當月新資料,不動歷史。寫入期
獨占 cache.duckdb(read_write);07:20 爬完即 close,決策/執行才開 read_only,
序列化無鎖爭用。df 欄位須與目標表完全一致(名稱),INSERT 以表欄順序對齊。
"""
from __future__ import annotations

import os
from datetime import date as Date

import duckdb
import polars as pl
from research import paths

CACHE_DB = str(paths.CACHE_DB)


class Sink:
    def __init__(self, path: str = CACHE_DB):
        self.con = duckdb.connect(path, read_only=False)

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> "Sink":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _table_cols(self, table: str) -> list[str]:
        return [c[0] for c in self.con.execute(f"DESCRIBE {table}").fetchall()]

    def upsert(self, table: str, df: pl.DataFrame, key_cols: list[str]) -> int:
        """刪除 table 中「key 落在 df 內」的列,再插入 df。回插入列數。

        空 df 不動表(交由呼叫端決定是否寫 sentinel)。df 欄位須為 table 全欄。
        """
        cols = self._table_cols(table)
        missing = [c for c in cols if c not in df.columns]
        if missing:
            raise ValueError(f"{table}: df 缺欄 {missing}(需與 cache 表同構)")
        df = df.select(cols)  # 對齊表欄順序
        if df.is_empty():
            return 0
        keys = df.select(key_cols).unique()
        self.con.register("_new", df)
        self.con.register("_keys", keys)
        try:
            pred = " AND ".join(f"t.{k} = _keys.{k}" for k in key_cols)
            self.con.execute(
                f"DELETE FROM {table} t WHERE EXISTS (SELECT 1 FROM _keys WHERE {pred})")
            collist = ",".join(cols)
            self.con.execute(f"INSERT INTO {table} ({collist}) SELECT {collist} FROM _new")
        finally:
            self.con.unregister("_new")
            self.con.unregister("_keys")
        return df.height

    def upsert_day(self, table: str, market: str, day: Date, df: pl.DataFrame) -> int:
        """日頻表便捷:以 (market, date) 為批次 key(刪整日 + 插入)。"""
        return self.upsert(table, df, ["market", "date"])

    def has_day(self, table: str, market: str, day: Date) -> bool:
        """該 (market, date) 是否已在表中(避免重抓)。"""
        n = self.con.execute(
            f"SELECT count(*) FROM {table} WHERE market = ? AND date = ?",
            [market, day]).fetchone()[0]
        return n > 0
