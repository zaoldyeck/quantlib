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


def save_raw_bytes_at(path: Path, content: bytes) -> Path:
    """**原子落地的唯一真源**:把 bytes 以 tmp → os.replace 寫到任意 `path`。

    封存原子性(斷網/當機不留半檔)在此一處實作,`save_raw` / `save_raw_named` 皆
    委派本函式;路徑慣例不吃 `<source>/<market>/<year>` 版型的源(如 TAIFEX 期貨日檔
    的 `data/taifex/futures_daily/<year>_fut.csv` 年檔 + `<year>/<year>_<m>.csv` 月檔)
    自算 path 後直呼本函式,仍走同一條原子鐵律。

    **只收 bytes**(封存 = 位元保真);str 一律由呼叫端明確決定編碼後再傳。
    """
    if isinstance(content, str):
        raise TypeError(
            "save_raw_bytes_at 只收 bytes(封存位元保真);str 請由呼叫端明確 encode 後再傳")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(content)
    os.replace(tmp, path)
    return path


def save_raw(source: str, market: str, day: Date, content: bytes,
             ext: str = "csv") -> Path:
    """把抓到的 raw **原子**落地(tmp → os.replace)。回傳落地路徑。

    - **content 必須是伺服器原樣的位元組(bytes)**,不是已解碼的 str。封存的意義
      是**位元保真**:發生意外時能用原始編碼(Big5-HKSCS 等)重新解析。若傳入已
      解碼的 str 再 encode 成 UTF-8,存下來的就不是原始檔了(原編碼資訊遺失、
      用原編碼讀回會炸)——故本函式**拒絕 str**,強制呼叫端傳原始 bytes。
    - 不做任何清洗(封存 = 原樣);原子換名(斷網/當機不留半檔);目錄自動建立。
    """
    if isinstance(content, str):
        raise TypeError(
            "save_raw 只收伺服器原樣 bytes,不收已解碼的 str——傳 http.fetch_bytes() "
            "的結果,不要傳 fetch_text()(封存必須位元保真,見 docstring)")
    return save_raw_bytes_at(raw_path(source, market, day, ext), content)


def raw_named_path(source: str, market: str, year: int, filename: str,
                   subdir: bool = True) -> Path:
    """季頻/多檔源的封存路徑。

    - `subdir=True`(預設):`data/<source>/<market>/<year>/<filename>`——季頻財報
      一期會回多張表(MOPS t163sb05 依產業模板切成 `<year>_<quarter>_a_c_<idx>.csv`),
      檔名不是年月日格式,故走這條;`filename` 由呼叫端依 Scala 時代慣例組好。
    - `subdir=False`:`data/<source>/<market>/<filename>`——供 `financial_analysis`
      這類**既有封存採扁平佈局**的源:76 檔歷史封存在「pre year-routing」時代就落地為
      `data/financial_analysis/<market>/<year>_<a|b>.csv`(扁平),Scala reader 以
      `deepFiles` 遞迴掃描故位置無所謂。fetch **覆寫同一扁平路徑**保單一副本、
      idempotent;若改寫 year 子目錄會與既有扁平檔並存 → 重建掃描重複讀入同一年。

    日頻源用 `raw_path`(檔名 = 年_月_日)。
    """
    base = paths.RAW / source / market
    if subdir:
        base = base / f"{year:04d}"
    return base / filename


def save_raw_named(source: str, market: str, year: int, filename: str,
                   content: bytes | str, subdir: bool = True) -> Path:
    """季頻/多檔源:把抓到的 raw **原子**落地到 `raw_named_path`。回傳落地路徑。

    與 `save_raw` 同一條原子換名鐵律(tmp → os.replace,斷網不留半檔),只是檔名
    由呼叫端指定(季頻多表無「年_月_日」語義)。**封存 = 原樣**,不做任何清洗。
    `subdir=False` 供既有扁平佈局的源(見 `raw_named_path`)。
    """
    data = content.encode("utf-8") if isinstance(content, str) else content
    return save_raw_bytes_at(
        raw_named_path(source, market, year, filename, subdir=subdir), data)


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
