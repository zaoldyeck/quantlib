"""C-capital_reduction 稽核 03:區間下載的「視窗覆蓋」檢查。

這張表不是逐日檔,而是「一個檔 = 一段日期區間的全量傾印」。所以覆蓋缺口要看
每個原始檔標題回報的區間 [strDate, endDate] 有沒有把時間軸鋪滿。

TWSE 標題:「115年06月29日 至 115年07月18日 股票減資恢復買賣參考價格」
TPEx 標題:「恢復買賣日期:102/01/02至109/07/10」

Run: uv run --project . python docs/data_audit/scripts/C-capital_reduction/03_window_coverage.py
"""
from __future__ import annotations

import re
import sys
from datetime import date as Date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
RAW = ROOT / "data" / "capital_reduction"

_TWSE_HDR = re.compile(r"(-?\d+)年(\d+)月(\d+)日\s*至\s*(-?\d+)年(\d+)月(\d+)日")
_TPEX_HDR = re.compile(r"恢復買賣日期[::]\s*(\d+)/(\d+)/(\d+)至(\d+)/(\d+)/(\d+)")


def _minguo(y: int, m: int, d: int) -> Date | None:
    try:
        return Date(y + 1911, m, d)
    except ValueError:
        return None


def windows(market: str) -> list[tuple[str, Date | None, Date | None, int, int]]:
    out = []
    for p in sorted((RAW / market).rglob("*.csv")):
        size = p.stat().st_size
        if size == 0:
            out.append((p.name, None, None, size, 0))
            continue
        text = p.read_bytes().decode("big5hkscs", errors="replace")
        head = "\n".join(text.splitlines()[:3])
        rx = _TWSE_HDR if market == "twse" else _TPEX_HDR
        m = rx.search(head)
        if not m:
            out.append((p.name, None, None, size, -1))
            continue
        s = _minguo(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        e = _minguo(int(m.group(4)), int(m.group(5)), int(m.group(6)))
        out.append((p.name, s, e, size, 1))
    return out


def file_date(name: str) -> Date:
    y, m, d = name.removesuffix(".csv").split("_")
    return Date(int(y), int(m), int(d))


def main() -> None:
    for market in ("twse", "tpex"):
        ws = windows(market)
        print(f"\n===== {market}:{len(ws)} 檔 =====")
        bad_hdr = [w for w in ws if w[4] == -1]
        empt = [w for w in ws if w[4] == 0]
        print(f"  0-byte {len(empt)},標題無法解析 {len(bad_hdr)}")
        for w in bad_hdr[:10]:
            print("    無標題:", w[0], w[3], "bytes")

        # 標題回報的 endDate 是否等於檔名日期(檔名 = 請求 endDate)
        mism = []
        for name, s, e, size, ok in ws:
            if ok != 1:
                continue
            fd = file_date(name)
            if e != fd:
                mism.append((name, s, e, fd, size))
        print(f"  標題 endDate ≠ 檔名日期:{len(mism)}")
        for m in mism[:20]:
            print("    ", m)

        # 覆蓋鋪滿檢查:把所有 [s,e] 併起來,看有無空洞
        iv = sorted((s, e) for name, s, e, size, ok in ws if ok == 1 and s and e)
        merged: list[list[Date]] = []
        for s, e in iv:
            if merged and s <= merged[-1][1] + timedelta(days=1):
                merged[-1][1] = max(merged[-1][1], e)
            else:
                merged.append([s, e])
        print(f"  合併後覆蓋區段 {len(merged)}:")
        for s, e in merged:
            print(f"    {s} ~ {e}")

        # 0-byte 檔案的檔名日期是否落在某個覆蓋區段內(落在內 = 那天已被別的區間覆蓋)
        uncovered = []
        for name, s, e, size, ok in ws:
            if ok != 0:
                continue
            fd = file_date(name)
            if not any(a <= fd <= b for a, b in merged):
                uncovered.append((name, fd))
        print(f"  0-byte 且不在任何覆蓋區段內:{len(uncovered)} → {uncovered}")


if __name__ == "__main__":
    main()
