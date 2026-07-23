"""C-capital_reduction 稽核 05:請求區間 vs 回應區間逐檔對照(TWSE 漏抓根因)。

Task.pullCapitalReduction 的請求區間 = (既有檔名最大值 + 1, 昨天),檔名取 endDate。
所以「請求 strDate」可由排序後的前一個檔名日期 + 1 重建。把它和回應標題回報的區間
並排,就能看出交易所有沒有忠實回覆整段區間。

Run: uv run --project research python docs/data_audit/scripts/C-capital_reduction/05_requested_vs_returned_window.py
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
_TWSE_ROW = re.compile(r'^"(\d+)/(\d+)/(\d+)"')
_TPEX_ROW = re.compile(r'^"(\d{7})"')


def file_date(name: str) -> Date:
    y, m, d = name.removesuffix(".csv").split("_")
    return Date(int(y), int(m), int(d))


def _mg(y: int, m: int, d: int) -> Date | None:
    try:
        return Date(y + 1911, m, d)
    except ValueError:
        return None


def main() -> None:
    # 兩市場共用同一個 existFiles 集合(Setting.getDatesOfExistFiles = 各市場交集)
    for market in ("twse", "tpex"):
        files = sorted((RAW / market).rglob("*.csv"), key=lambda p: file_date(p.name))
        print(f"\n===== {market} =====")
        print(f"{'file':16} {'req_start':>11} {'req_end':>11} {'ret_start':>11} {'ret_end':>11} "
              f"{'rows':>5} {'bytes':>7}  note")
        prev: Date | None = None
        for p in files:
            fd = file_date(p.name)
            req_s = (prev + timedelta(days=1)) if prev else Date(2011, 1, 1) if market == "twse" else Date(2013, 1, 2)
            size = p.stat().st_size
            ret_s = ret_e = None
            nrow = 0
            if size:
                text = p.read_bytes().decode("big5hkscs", errors="replace")
                lines = text.splitlines()
                rx = _TWSE_HDR if market == "twse" else _TPEX_HDR
                m = rx.search("\n".join(lines[:3]))
                if m:
                    ret_s = _mg(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                    ret_e = _mg(int(m.group(4)), int(m.group(5)), int(m.group(6)))
                rrx = _TWSE_ROW if market == "twse" else _TPEX_ROW
                nrow = sum(1 for ln in lines if rrx.match(ln))
            note = ""
            if size <= 2:
                note = "EMPTY-RESPONSE"
            elif ret_s is None:
                note = "NO-HEADER"
            elif ret_s > req_s:
                note = f"!! 回應起點晚於請求起點 {(ret_s - req_s).days} 天"
            print(f"{p.name:16} {str(req_s):>11} {str(fd):>11} {str(ret_s):>11} {str(ret_e):>11} "
                  f"{nrow:>5} {size:>7}  {note}")
            prev = fd


if __name__ == "__main__":
    main()
