"""「近 N 年 refit vs 全史固定」walk-forward 對決——回答:regime 不同,該不該用近期資料重選參數?

使用者質疑(2026-07-24):「為什麼不能用最近幾年資料研發最強策略?每個 regime 又不一樣。」
先前 D1 結論(用全史、不縮窗)的證據只到「edge 跨期 IC 同號」——**那不等於「近期 refit 一定更差」**。
本檔用**真樣本外對決**直接測,取代論述。

## 設計(嚴格 walk-forward)
1. 參數網格:S 的四個有學理出處的軸(非無腦 sweep)——
   trail{.25,.35,.45} × loser_time{10,15,20} × time_stop{25,30,35} × slots{3,5,8} = 81 格。
2. 每格跑一次**全跨度** NAV(並行);之後所有 train/OOS 段用切片評估(walk-forward 文獻標準做法;
   路徑相依的近似誤差在年級窗上有限——誠實聲明)。
3. 對每個 OOS 年 y(2017…2026)× 每個回看窗長 N ∈ {1,2,3,5,全史}:
   - train = [y−N, y) 段,依 **Sortino**(D2 主尺)選 train 最佳參數;
   - 記該參數在 **y 年(從未參與選擇)** 的報酬 = 真 OOS。
4. 對照:canonical 固定參數(trail .35/lt 15/ts 30/slots 5)在同樣 y 年的報酬。
5. 彙總每個窗長:pooled OOS 幾何年化、勝過 canonical 的年數、平均超額。
   另報 **train Sharpe/Sortino ↔ OOS 報酬的相關**(3.1 的完整 S 參數空間版)。

## 判讀
- 若某窗長的 pooled OOS 顯著 > canonical → **regime-adaptive refit 有效**,使用者的直覺對,
  應改制度為滾動 refit;
- 若各窗長 ≈ 或 < canonical、且 train↔OOS 相關 ≤ 0 → **refit 在擬合已過去的 regime**,
  固定參數是對的(D1 成立)。

Run: uv run --project . python -m quantlib.strat_lab.window_length_wf
依賴 cache:是(乾淨世代)。
"""
from __future__ import annotations

import itertools
import os
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.engine import ExitSpec, PortSpec
from quantlib.apex.strategy_s import DS, prep_cached, run_s_full

#: 四軸網格(每軸都有出處:trail/時間止損為 S 規格軸,slots 為組合集中度軸)
TRAILS = (0.25, 0.35, 0.45)
LOSER_TS = (10, 15, 20)
TIME_TS = (25, 30, 35)
SLOTS = (3, 5, 8)
CANON = (0.35, 15, 30, 5)

OOS_YEARS = tuple(range(2017, 2027))
WINDOWS = (1, 2, 3, 5, 99)  # 99 = 全史(擴張窗)


def _grid() -> list[tuple]:
    return list(itertools.product(TRAILS, LOSER_TS, TIME_TS, SLOTS))


def _run_cell(cell: tuple) -> tuple[tuple, list, list]:
    """跑一格參數的全跨度 NAV → (cell, dates, navs)。worker 自行讀 prep 快取。"""
    tr, lt, ts, ns = cell
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    nav, _ = run_s_full(
        panel, feat, elig, DS,
        _exit_spec=ExitSpec(trailing_stop=tr, time_stop=ts, loser_time_stop=lt),
        _port_spec=PortSpec(n_slots=ns, max_new_per_day=2),
    )
    nav = nav.sort("date")
    return cell, nav["date"].to_list(), nav["nav"].to_list()


def _seg_stats(dates: list, navs: list, lo, hi) -> dict | None:
    """[lo, hi) 段的報酬與 Sortino(日報酬)。"""
    idx = [i for i, d in enumerate(dates) if lo <= d < hi]
    if len(idx) < 40:
        return None
    v = np.array([navs[i] for i in idx])
    r = v[1:] / v[:-1] - 1
    if len(r) < 30 or v[0] <= 0:
        return None
    down = r[r < 0]
    sortino = (r.mean() / down.std() * np.sqrt(252)) if len(down) > 2 and down.std() > 0 else 0.0
    return {"ret": float(v[-1] / v[0] - 1), "sortino": float(sortino)}


def main() -> None:
    import datetime as dt
    grid = _grid()
    n_workers = max(1, (os.cpu_count() or 4) - 2)
    print(f"[wf] 跑 {len(grid)} 格參數的全跨度 NAV(並行 {n_workers})…", flush=True)
    curves: dict[tuple, tuple] = {}
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        for cell, dates, navs in ex.map(_run_cell, grid):
            curves[cell] = (dates, navs)
    print(f"[wf] 完成 {len(curves)} 格。開始 walk-forward 評估…\n", flush=True)

    rows = []
    corr_pairs = []          # (train sortino, oos ret) 全配對,測 3.1 的完整版
    for y in OOS_YEARS:
        oos_lo, oos_hi = dt.date(y, 1, 1), dt.date(y + 1, 1, 1)
        canon_oos = _seg_stats(*curves[CANON], oos_lo, oos_hi)
        if canon_oos is None:
            continue
        for N in WINDOWS:
            tr_lo = dt.date(max(2015, y - N), 1, 1) if N < 99 else dt.date(2015, 1, 1)
            tr_hi = oos_lo
            best, best_s = None, -1e9
            for cell, (dates, navs) in curves.items():
                s = _seg_stats(dates, navs, tr_lo, tr_hi)
                if s is None:
                    continue
                if N == WINDOWS[0]:  # 只需收集一次相關樣本(用最短窗避免重複)
                    o = _seg_stats(dates, navs, oos_lo, oos_hi)
                    if o:
                        corr_pairs.append((s["sortino"], o["ret"]))
                if s["sortino"] > best_s:
                    best, best_s = cell, s["sortino"]
            if best is None:
                continue
            o = _seg_stats(*curves[best], oos_lo, oos_hi)
            if o is None:
                continue
            rows.append({"year": y, "window": N, "picked": str(best),
                         "oos_ret": o["ret"], "canon_ret": canon_oos["ret"],
                         "excess": o["ret"] - canon_oos["ret"]})

    df = pl.DataFrame(rows)
    print("=== 各回看窗長的真樣本外表現(vs canonical 固定參數)===")
    print(f"  {'窗長':>8}{'pooled 年化':>13}{'canonical':>12}{'勝年數':>9}{'平均超額':>11}")
    for N in WINDOWS:
        g = df.filter(pl.col("window") == N)
        if g.is_empty():
            continue
        gm = float(np.prod([1 + r for r in g["oos_ret"]]) ** (1 / g.height) - 1)
        cm = float(np.prod([1 + r for r in g["canon_ret"]]) ** (1 / g.height) - 1)
        wins = int((g["excess"] > 0).sum())
        lab = "全史" if N == 99 else f"{N} 年"
        print(f"  {lab:>8}{gm:>+12.1%}{cm:>+12.1%}{wins:>6}/{g.height}{g['excess'].mean():>+10.1%}")

    print("\n=== 逐年 OOS 報酬(refit 選出的參數 vs canonical)===")
    print(f"  {'年':>6}" + "".join(f"{('全史' if N==99 else f'{N}y'):>9}" for N in WINDOWS) + f"{'canonical':>11}")
    for y in OOS_YEARS:
        g = df.filter(pl.col("year") == y)
        if g.is_empty():
            continue
        line = f"  {y:>6}"
        for N in WINDOWS:
            r = g.filter(pl.col("window") == N)
            line += f"{r['oos_ret'][0]:>+8.0%} " if r.height else f"{'--':>9}"
        line += f"{g['canon_ret'][0]:>+10.0%}"
        print(line)

    if len(corr_pairs) > 30:
        a = np.array([p[0] for p in corr_pairs]); b = np.array([p[1] for p in corr_pairs])
        print(f"\n=== train Sortino ↔ OOS 報酬 相關(n={len(corr_pairs)},完整 S 參數空間)===")
        print(f"  Pearson {np.corrcoef(a, b)[0,1]:+.3f}"
              f"  (3.1 曾在動能因子上量到 -0.6~-0.8;>0 = 選參有效、<0 = 擬合雜訊)")
    print("\n  判讀:某窗長 pooled 年化顯著 > canonical 且勝年數過半 → regime-adaptive refit 成立;"
          "否則固定參數(D1)成立。")


if __name__ == "__main__":
    main()
