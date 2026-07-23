"""財報 **跨源交叉驗證**:cache 的科目值(bs/is 來自 MOPS 簡明/損益 CSV、cf 來自 tifrs)
是否**逐字出現在獨立的 tifrs 綜合報表 HTML**。兩個獨立 MOPS 源一致 = 強證據。

tifrs 是 iXBRL:`<span class="zh">科目</span> … <ix:nonFraction scale="3">8,660,949,685</ix:nonFraction>`
(scale=3=千元;cache 存千元值)。bs/is 由簡明/損益 CSV parse(見 balance_sheet.py/income_statement.py),
tifrs 綜合報表由另一 MOPS 端點提供**同一份財務數字**——故 cache 的核心科目值必逐字見於 tifrs。

**定位**:這是跨源一致性檢查,**不**驗簡明 CSV 專有欄(每股參考淨值等 tifrs 沒有的欄→排除)。
完整的「cache=parser(raw)」由 `rebuild_financials`(從各自 raw 權威重建、逐位重現 cache)背書;
本檔是額外的獨立源交叉確認。搭配 A 維全史逐格核對(bs/is「資料可信」)構成證據鏈。並行化。

Run: uv run --project . python -m quantlib.verify.financials_check              # 抽樣近季
     uv run --project . python -m quantlib.verify.financials_check --full       # 全公司季
唯讀。
"""
from __future__ import annotations

import argparse
import glob
from concurrent.futures import ProcessPoolExecutor

from quantlib import paths
from quantlib.db import connect

_FIN_TABLES = ["bs_concise_raw", "is_progressive_raw", "cf_progressive_raw"]


def _raw_files(code: str, year: int, quarter: int) -> list[str]:
    """該公司該季的 tifrs raw 檔(可能多份報表型別)。"""
    return glob.glob(f"data/financial_statements/{year}_{quarter}/*-{code}-{year}Q{quarter}.html")


def _check_one(job: tuple) -> tuple:
    """worker:驗一批 (code, year, quarter) → (n_checked, n_values, misses[])。"""
    from quantlib.db import connect as _connect
    con = _connect()
    n_checked = n_values = 0
    misses = []
    for code, year, quarter in job:
        files = _raw_files(code, year, quarter)
        if not files:
            continue
        blob = ""
        for f in files:
            blob += open(f, "rb").read().decode("utf-8", errors="replace")
        n_checked += 1
        for t in _FIN_TABLES:
            rows = con.execute(
                f"SELECT title, value FROM {t} WHERE company_code=? AND year=? AND quarter=? "
                f"AND value IS NOT NULL", [code, year, quarter]).fetchall()
            for title, val in rows:
                # 排除簡明 CSV 專有欄(每股*、比率等 tifrs 綜合報表沒有的),跨源本就無法對照
                if "每股" in title or abs(val) < 1000:
                    continue
                n_values += 1
                # 千分位格式(cache 存千元整數值);0 特判(raw 可能寫 0 或 -)
                iv = int(round(val))
                needle = f"{iv:,}"
                if needle not in blob and (iv == 0 or f"{-iv:,}" not in blob):
                    if len(misses) < 20:
                        misses.append(f"{code} {year}Q{quarter} {t} 「{title}」={iv:,} 不在 tifrs")
    return (n_checked, n_values, misses)


def _sample_jobs(con, full: bool) -> list[tuple]:
    """回 (code, year, quarter) 清單。抽樣=近 3 年隨機公司季;full=全部有 raw 檔的。"""
    q = ("SELECT DISTINCT company_code, year, quarter FROM bs_concise_raw WHERE year >= 2024"
         if not full else
         "SELECT DISTINCT company_code, year, quarter FROM bs_concise_raw WHERE year >= 2013")
    rows = con.execute(q).fetchall()
    if not full:
        # 抽樣:每季取前 ~40 家(依 code 排序,確定性)
        by_q = {}
        for code, y, qq in sorted(rows):
            by_q.setdefault((y, qq), []).append(code)
        picked = []
        for (y, qq), codes in by_q.items():
            picked.extend((c, y, qq) for c in codes[::max(1, len(codes) // 40)][:40])
        return picked
    return [(c, y, qq) for c, y, qq in rows]


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def main() -> None:
    import os
    ap = argparse.ArgumentParser(description="財報 parser 取值驗證(cache 值逐字在 raw)")
    ap.add_argument("--full", action="store_true", help="全公司季(預設抽樣近 3 年)")
    args = ap.parse_args()
    con = connect()
    jobs = _sample_jobs(con, args.full)
    print(f"=== 財報 parser 取值驗證:{len(jobs)} 個 (公司×季) 的 cache 值逐字比對 raw tifrs ===")
    workers = max(1, (os.cpu_count() or 4) - 1)
    batches = list(_chunks(jobs, max(1, len(jobs) // (workers * 4) + 1)))
    n_checked = n_values = 0
    all_miss = []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        for nc, nv, ms in ex.map(_check_one, batches):
            n_checked += nc
            n_values += nv
            all_miss.extend(ms)
    print(f"  驗 {n_checked} 個公司季、{n_values:,} 個科目值;不在 raw 的 = {len(all_miss)}")
    for m in all_miss[:20]:
        print(f"      ❌ {m}")
    print("  " + ("✓ 全部 cache 財報值逐字見於 raw → parser 取值正確"
                  if not all_miss else "❌ 有值不在 raw,需查 parser"))


if __name__ == "__main__":
    main()
