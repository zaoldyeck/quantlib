"""M03:refit 時點精修(回應使用者 2026-07-21 三問)。

(A) 現行 S 是否 = 去年 12 月(Dec-2025)的三年 refit?各年 12 月 refit 選到什麼
    config(穩定性)——直接檢驗「12 月更新版」與現役 S 是否同一支。
(B) 「12 月的什麼時候」——台股月營收 ~每月 10 日公布,故 12 月 refit 在**月報前
    (day ≤10)vs 月報後(day ≥11)**用到的訓練資料差一個月營收。用日級 refit 點測。

reuse m01 config NAV(2007→2026)+ m02 helpers。判準/極限同 M02。
Run: uv run --project research python -m research.apex.experiments.m03_refit_timing
"""
from __future__ import annotations

import time
from datetime import date as Date

import numpy as np
import polars as pl

from research.apex import data
from research.apex.experiments.m01_window_length import GRID, prep, run_config, seg
from research.apex.experiments.m02_refit_frequency import (
    BOOT_SEED, SWITCH_COST, _add_months, _metrics, _rets, _sub_years)

C = "company_code"


def cfg_name(i: int) -> str:
    c = GRID[i]
    return f"{c['axes']}-n{c['n']}-t{int(c['trail']*100)}-adv{int(c['adv']/1e6)}"


def pick_at(navs, refit_date: Date, kpi: str = "cagr") -> int | None:
    """trailing 3 年窗 [refit-3y, refit) 的最優 config index。"""
    a = _sub_years(refit_date, 3)
    tr = [seg(nv, a, refit_date) for nv in navs]
    if any(x is None for x in tr):
        return None
    return int(np.argmax([x[kpi] for x in tr]))


def refit_sim_day(navs, data_end, month: int, day: int, kpi: str = "cagr",
                  first_year: int = 2010) -> dict:
    """每年在 (month, day) refit 一次、串接連續策略(日級版 refit_sim)。"""
    points, y = [], first_year
    while Date(y, month, day) < data_end:
        points.append(Date(y, month, day))
        y += 1
    segs, prev, nsw = [], None, 0
    for i, tp in enumerate(points):
        b = points[i + 1] if i + 1 < len(points) else data_end
        tr = [seg(nv, _sub_years(tp, 3), tp) for nv in navs]
        if any(x is None for x in tr):
            continue
        pick = int(np.argmax([x[kpi] for x in tr]))
        sr = _rets(navs[pick], tp, b)
        if prev is not None and pick != prev and len(sr):
            nsw += 1
            d0 = sr["date"][0]
            sr = sr.with_columns(pl.when(pl.col("date") == d0)
                                 .then(pl.col("ret") - SWITCH_COST)
                                 .otherwise(pl.col("ret")).alias("ret"))
        segs.append(sr)
        prev = pick
    if not segs:
        return {}
    m = _metrics(pl.concat(segs).sort("date"), np.random.default_rng(BOOT_SEED))
    m["switches"] = nsw
    return m


def main() -> None:
    t0 = time.time()
    con = data.connect()
    latest = data.latest_date(con).isoformat()
    panel, feat = prep(con, prep_start="2006-06-01", end=latest)
    elig_map = {adv: (data.eligibility(panel, min_adv=adv)
                      .filter(pl.col("eligible")).select(["date", C]))
                for adv in [5e6, 20e6]}
    print(f"prep {time.time()-t0:.0f}s;跑 {len(GRID)} config NAV(2007→{latest})…")
    navs = [run_config(panel, feat, elig_map, c, sim_start="2007-01-02") for c in GRID]
    data_end = navs[0]["date"][-1]
    print(f"NAV done {time.time()-t0:.0f}s(S = ax6-n5-t35-adv5)\n")

    print("=== (A) 各年『12/1』trailing-3yr 選到的 config(檢驗 Dec-2025 = S? + 穩定性)===")
    for y in range(2010, 2026):
        p = pick_at(navs, Date(y, 12, 1))
        star = "  ← 去年 12 月(現役應對照此)" if y == 2025 else ""
        print(f"  Dec-{y}-01  trailing {y-3}-12→{y}-12  →  {cfg_name(p) if p is not None else '—'}{star}")
    pj = pick_at(navs, Date(2026, 7, 1))
    print(f"  [實際現役] Jul-2026 trailing 2023-07→2026-07 → {cfg_name(pj) if pj is not None else '—'}")

    print("\n=== (B) 12 月 refit 的日內時點(月營收 ~10 日公布;月報前 vs 後)+ 1 月對照 ===")
    rows = []
    for mo, label in [(12, "12月"), (1, "1月")]:
        for d in [1, 6, 11, 16, 21]:
            m = refit_sim_day(navs, data_end, mo, d)
            if m:
                rows.append({"月": label, "日(vs月報~10)": d, "CAGR": m["cagr"],
                             "P5": m["p5"], "switches": m["switches"]})
    with pl.Config(float_precision=3, tbl_rows=12):
        print(pl.DataFrame(rows))
    print(f"\ntotal {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
