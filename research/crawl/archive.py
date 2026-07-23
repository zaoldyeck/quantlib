"""原始檔封存:每個爬蟲抓下來的 raw 一定先落地 `data/`,才 parse 進 cache。

**使用者鐵律(2026-07-23)**:「爬蟲下來的檔案一定要保留原始檔,不能只有
cache.duckdb,發生任何意外才有辦法復原整個歷史資料——這些資料是最寶貴的。」

## 為什麼這是第一級基礎設施

`data/`(paths.RAW)是**不可重生的事實地基**:重爬要數週,且部分歷史端點已不再
供應舊資料。cache.duckdb 只是它的**衍生查詢層**,隨時可從 raw 重建。所以:

  抓取 → **先原子落地 raw 到 data/** → 再 parse → 寫 cache

順序不可顛倒。舊的 Python 爬蟲(daily_quote/dtd/stock_per_pbr)抓完直接 parse、
raw 丟掉——原始檔停在 2026-07-09,之後每天都在流失最寶貴的資料。本模組堵死它。

## 好處(除了防災)

- **parse 的對象 = 已封存的 raw**:所見即所存,不會有「入庫的和封存的不一致」。
- **cache 全可重建**:`rebuild_cache_from_raw` 重讀 data/ 全部 raw → cache,
  取代靠 PostgreSQL 重建的舊 cache_tables.py(PG 退役後的災難復原路徑)。
- **檔名/目錄統一**:所有源一律 `data/<source>/<market>/<year>/<year>_<m>_<d>.<ext>`
  (沿用 Scala 時代慣例,新舊一致)。
"""
from __future__ import annotations

import os
from datetime import date as Date
from pathlib import Path

from research import paths


def raw_path(source: str, market: str, day: Date, ext: str = "csv") -> Path:
    """封存路徑:data/<source>/<market>/<year>/<year>_<m>_<d>.<ext>(沿用既有慣例)。"""
    return (paths.RAW / source / market / f"{day.year:04d}"
            / f"{day.year:04d}_{day.month}_{day.day}.{ext}")


def save_raw(source: str, market: str, day: Date, content: bytes | str,
             ext: str = "csv") -> Path:
    """把抓到的 raw **原子**落地(tmp → os.replace)。回傳落地路徑。

    - content 是抓到的原始位元/文字,**不做任何清洗**(封存 = 原樣)。
    - 原子換名:斷網/當機不留半檔。
    - 目錄自動建立。
    """
    p = raw_path(source, market, day, ext)
    p.parent.mkdir(parents=True, exist_ok=True)
    data = content.encode("utf-8") if isinstance(content, str) else content
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, p)
    return p


def save_sentinel(source: str, market: str, day: Date) -> Path:
    """休市日 0-byte 哨兵(**只在已有正向休市證據時呼叫**——不是抓不到就寫)。

    哨兵同時是休市日曆(research/data_calendar.is_trading_day 讀它)。誤寫 = 把真
    交易日永久當假日(稽核 FC3 事故)。故寫入條件由呼叫端嚴格把關,本函式只負責
    原子落地空檔。
    """
    return save_raw(source, market, day, b"", ext="csv")


def has_raw(source: str, market: str, day: Date, ext: str = "csv") -> bool:
    """該日原始檔是否已封存(含 0-byte 哨兵)。"""
    return raw_path(source, market, day, ext).exists()
