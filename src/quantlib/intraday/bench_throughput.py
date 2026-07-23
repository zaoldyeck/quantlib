"""日內回補的吞吐實測:平行到底安不安全、能快多少?(決定 pull_kbars 的參數)

**為什麼要實測而不是拍板**:官方文件寫了頻率上限(50 次/5 秒),但沒說 client
是不是執行緒安全——1.3.1 的 release note 才剛修過「race condition in contracts」。
在沒有文件背書的地方,唯一能拿來當證據的是**自己跑出來的逐位比對**:同一批
chunk 先序列抓一次、再平行抓一次,兩邊的資料指紋必須完全一致才准開平行。

量測三件事:
  1. `usage()` 與 `kbars()` 各自的往返延遲(決定「每格都查額度」值不值得)
  2. 序列 vs 平行的實際吞吐(格/秒)
  3. **平行結果 vs 序列結果的逐位一致性**(不一致 → 一律退回序列)

額度成本:2 × N 格 ≈ 1 MB(相對每日 2 GB 可忽略)。

Run: uv run --project . python -m quantlib.intraday.bench_throughput
     uv run --project . python -m quantlib.intraday.bench_throughput --workers 8 --n 16
"""
from __future__ import annotations

import argparse
import hashlib
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date as Date

import polars as pl

from quantlib.intraday.pull_kbars import _login, _months, _remaining, _to_frame
from quantlib.intraday.ratelimit import MARKET_CALLS, MARKET_WINDOW, RateLimiter

#: 測試用個股(流動性高低都取,讓延遲樣本涵蓋大小 payload)
CODES = ["2330", "2317", "2454", "2308", "1101", "2412", "1216", "2882"]


def _fingerprint(df: pl.DataFrame | None) -> str:
    """資料指紋:行數 + 內容 hash。平行與序列必須完全相同。"""
    if df is None or df.is_empty():
        return "empty"
    h = hashlib.sha256(df.sort("dt").write_ipc(None).getvalue()).hexdigest()[:16]
    return f"{df.height}:{h}"


def _fetch(api, code: str, s: Date, e: Date) -> pl.DataFrame | None:
    kb = api.kbars(contract=api.Contracts.Stocks[code],
                   start=s.isoformat(), end=e.isoformat())
    return _to_frame(kb)


def main() -> None:
    ap = argparse.ArgumentParser(description="日內回補吞吐與平行安全性實測")
    ap.add_argument("--n", type=int, default=8, help="測試格數(每格 = 一檔一個月)")
    ap.add_argument("--workers", type=int, default=6, help="平行 worker 數")
    args = ap.parse_args()

    api = _login()
    rem0 = _remaining(api)
    print(f"[bench] 登入 OK;剩餘額度 {(rem0 or 0)/1e6:.0f} MB")

    # 取最近一個完整月當測試窗(payload 大小接近真實回補)
    today = Date.today()
    m0 = today.replace(day=1)
    tag, s, e = _months(m0.replace(month=m0.month - 1 or 12,
                                   year=m0.year - (1 if m0.month == 1 else 0)), m0)[0]
    jobs = [(CODES[i % len(CODES)], s, e) for i in range(args.n)]
    print(f"[bench] 測試窗 {tag}({s}→{e});{len(jobs)} 格,{args.workers} workers")

    # ① usage() 延遲
    t = []
    for _ in range(5):
        t0 = time.perf_counter(); _remaining(api); t.append(time.perf_counter() - t0)
    print(f"① usage() 延遲 中位數 {statistics.median(t)*1000:.0f} ms")

    # ② 序列
    lim = RateLimiter()
    seq_fp, t0 = [], time.perf_counter()
    lat = []
    for code, a, b in jobs:
        lim.acquire()
        t1 = time.perf_counter()
        seq_fp.append(_fingerprint(_fetch(api, code, a, b)))
        lat.append(time.perf_counter() - t1)
    seq_s = time.perf_counter() - t0
    print(f"② kbars() 延遲 中位數 {statistics.median(lat)*1000:.0f} ms;"
          f"序列 {len(jobs)} 格 {seq_s:.1f}s = {len(jobs)/seq_s:.2f} 格/秒")

    # ③ 平行(共用同一個 limiter)
    lim2 = RateLimiter()

    def one(job):
        lim2.acquire()
        return _fingerprint(_fetch(api, *job))

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        par_fp = list(ex.map(one, jobs))
    par_s = time.perf_counter() - t0
    print(f"③ 平行 {len(jobs)} 格 {par_s:.1f}s = {len(jobs)/par_s:.2f} 格/秒"
          f"(加速 {seq_s/par_s:.1f}x)")

    # ④ 逐位一致性——這一關沒過就不准開平行
    bad = [(j[0], a, b) for j, a, b in zip(jobs, seq_fp, par_fp) if a != b]
    if bad:
        print(f"✗ 平行結果與序列不一致 {len(bad)} 格 → **禁止平行**:{bad[:3]}")
    else:
        print(f"✓ 平行結果與序列逐位一致({len(jobs)}/{len(jobs)} 格)")

    used = (rem0 or 0) - (_remaining(api) or 0)
    cap = MARKET_CALLS / MARKET_WINDOW
    print(f"\n[bench] 本次耗用 {used/1e6:.1f} MB;官方上限 {cap:.0f} 次/秒,"
          f"限流器設定 {lim.per_second:.1f} 次/秒")
    print(f"[bench] 建議:workers={args.workers} 時實測 {len(jobs)/par_s:.2f} 格/秒 → "
          f"每日 2 GB 額度約可抓 {2e9/max(used/(2*len(jobs)),1):,.0f} 格")
    api.logout()


if __name__ == "__main__":
    main()
