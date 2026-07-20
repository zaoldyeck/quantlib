"""CSV/日期解析共用工具(對齊 Scala QuantlibCSVReader + Reader 的清洗語義)。

要點:
- TWSE CSV 用 `="2330"` 的 Excel 護甲鎖前導零 → 解析前把 `="` 換成 `"` 還原成正常
  quoted 欄位(csv 模組即可正確去引號)。
- 儲存格清洗:去空白、逗號、`%`(對齊 Reader 的 `.replace(" ","").replace(",","")`)。
- 日期:TWSE 端點用西元 `yyyymmdd`;TPEx 用民國 `y/MM/dd`。
"""
from __future__ import annotations

import csv
import io
from datetime import date as Date

TWSE_ENC = "Big5-HKSCS"


def parse_csv(text: str) -> list[list[str]]:
    """Big5 CSV 文字 → rows。先還原 TWSE 的 `="..."` 護甲再交給 csv 模組。"""
    return list(csv.reader(io.StringIO(text.replace('="', '"'))))


def clean(s: str) -> str:
    """對齊 Reader:去空白/逗號/百分號。"""
    return s.replace(",", "").replace("%", "").replace(" ", "").strip()


def twse_date(d: Date) -> str:
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"


def minguo_slash(d: Date) -> str:
    """民國 y/MM/dd(TPEx 日頻端點格式)。"""
    return f"{d.year - 1911}/{d.month:02d}/{d.day:02d}"


def parse_minguo_slash(s: str) -> Date | None:
    """民國 'yyy/MM/dd' → 西元 date;不可解析回 None。"""
    s = s.strip()
    if not s:
        return None
    try:
        y, m, d = s.split("/")
        return Date(int(y) + 1911, int(m), int(d))
    except (ValueError, TypeError):
        return None


def find_header(rows: list[list[str]], first_col_marker: str) -> int:
    """回傳第一欄等於 marker 的 header 列 index;找不到回 -1(視為無資料)。"""
    for i, r in enumerate(rows):
        if r and r[0].strip().replace('"', "") == first_col_marker:
            return i
    return -1


class SchemaDrift(RuntimeError):
    """欄位佈局與預期不符——寧可 fail-loud 也不靜默錯位(TWSE 悄悄加欄地雷)。"""
