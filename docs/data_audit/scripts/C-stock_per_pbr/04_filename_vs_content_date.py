"""C-stock_per_pbr 稽核 04:原始 CSV 檔名日期 vs 檔頭內容日期(全史掃描)。

TWSE BWIBBU_d 對「非交易日」的查詢會回最近一份可用資料(而非空),爬蟲卻以
「請求日」為檔名存檔、Reader 又以檔名日期入庫 → 產生「幽靈日期 + 內容錯位」。
本腳本把每個原始檔第一行的民國日期解析出來,與檔名日期比對。

Run: uv run --project . python docs/data_audit/scripts/C-stock_per_pbr/04_filename_vs_content_date.py
"""
from __future__ import annotations

import re
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

ROOT = Path(__file__).resolve().parents[4] / "data" / "stock_per_pbr_dividend_yield"
# TWSE: "106年12月18日 個股日本益比..."; TPEx: "上櫃股票個股本益比... 資料日期:106/12/18"
_ROC = re.compile(r"(\d{2,3})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日")
_SLASH = re.compile(r"(\d{2,3})/(\d{1,2})/(\d{1,2})")


def head_date(p: Path) -> date | None:
    # 只讀前 400 bytes 會把 Big5 多字元切半 → 一律 errors="replace",
    # 標題行在最前面,替換字元不影響日期解析。
    raw = p.read_bytes()[:400]
    txt = raw.decode("big5-hkscs", errors="replace")
    if "\ufffd" in txt[:40] or "年" not in txt:
        txt2 = raw.decode("utf-8", errors="replace")
        if "年" in txt2 or "/" in txt2:
            txt = txt2 if "年" in txt2 else txt
    m = _ROC.search(txt) or _SLASH.search(txt)
    if not m:
        return None
    y, mo, d = (int(x) for x in m.groups())
    try:
        return date(y + 1911, mo, d)
    except ValueError:
        return None


def main() -> None:
    for mkt in ("twse", "tpex"):
        files = sorted((ROOT / mkt).rglob("*.csv"))
        bad, nohdr, empty = [], [], 0
        for p in files:
            if p.stat().st_size == 0:
                empty += 1
                continue
            y, mo, d = (int(x) for x in p.stem.split("_"))
            fn = date(y, mo, d)
            hd = head_date(p)
            if hd is None:
                nohdr.append(p.name)
            elif hd != fn:
                bad.append((fn, hd, p.stat().st_size, str(p.relative_to(ROOT))))
        print(f"== {mkt}: {len(files)} 檔(0-byte {empty}),檔名≠內容 {len(bad)},無日期表頭 {len(nohdr)} ==")
        for fn, hd, sz, rel in bad:
            print(f"   檔名 {fn}  內容 {hd}  size={sz}  {rel}")
        if nohdr:
            print(f"   無表頭樣本: {nohdr[:10]}")
        print()


if __name__ == "__main__":
    main()
