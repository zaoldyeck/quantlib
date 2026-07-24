"""啟動時機敏感度:三策略(S/Evergreen/Serenity)隨時啟動都能賺嗎?要不要等月報?

使用者實戰問題:「現在購買標的,還是等下個月報發佈後再跑策略選標的?」
方法:對每策略掃一組錯開的**啟動日**(近 N 年每月多個),各自跑到共同終點,量:
- 前 63 交易日(≈3 個月)報酬——啟動期敏感度(等不等月報差多少)
- 全窗 CAGR——長期是否收斂(晚點進場長期有沒有差)
S 另做「啟動日距最近月報公布潮(每月 10 日)的天數」分組——直接回答等不等。

S/Evergreen 用引擎內迴圈(快);Serenity 引擎重(subprocess),用 --serenity 旗標另跑
12 個代表起點。

Run: uv run --project . python -m quantlib.strat_lab.entry_timing            # S + Evergreen
     uv run --project . python -m quantlib.strat_lab.entry_timing --serenity # Serenity(慢)
依賴 cache:是。
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import date as Date, timedelta

import numpy as np
import polars as pl

from quantlib import paths
from quantlib.apex import data
from quantlib.apex.strategy_s import DS, prep_cached, run_s

_END = "2026-07-09"


def _starts(first_year: int = 2023, per_month: tuple[int, ...] = (1, 11, 21)) -> list[str]:
    out = []
    d0 = Date(first_year, 7, 1)
    d1 = Date.fromisoformat(_END) - timedelta(days=200)  # 至少留 ~9 月窗
    cur = d0
    while cur <= d1:
        for dd in per_month:
            try:
                s = Date(cur.year, cur.month, dd)
            except ValueError:
                continue
            if d0 <= s <= d1:
                out.append(s.isoformat())
        cur = (cur.replace(day=1) + timedelta(days=32)).replace(day=1)
    return sorted(set(out))


def _report(tag: str, rows: list[dict]) -> None:
    df = pl.DataFrame(rows)
    e = df["early_ret"]
    c = df["cagr"]
    print(f"\n=== {tag}({df.height} 個啟動日)===")
    print(f"  前 3 月報酬:P10 {e.quantile(0.1):+.1%}  中位 {e.median():+.1%}  P90 {e.quantile(0.9):+.1%}"
          f"  <0 比例 {(e < 0).mean():.0%}")
    print(f"  全窗 CAGR :P10 {c.quantile(0.1):+.1%}  中位 {c.median():+.1%}  P90 {c.quantile(0.9):+.1%}")
    # 距月報公布潮(每月 10 日)天數分組:0-6 / 7-14 / 15+(下一個 10 日往回算)
    def dist(sd: str) -> int:
        d = Date.fromisoformat(sd)
        anchor = d.replace(day=10)
        if d < anchor:
            prev = (d.replace(day=1) - timedelta(days=1)).replace(day=10)
            return (d - prev).days
        return (d - anchor).days
    df = df.with_columns(pl.col("start").map_elements(dist, return_dtype=pl.Int64).alias("dd"))
    for lo, hi, lab in ((0, 6, "公布潮 0-6 天內啟動"), (7, 14, "7-14 天"), (15, 40, "15+ 天(等下輪前夕)")):
        g = df.filter((pl.col("dd") >= lo) & (pl.col("dd") <= hi))
        if g.height:
            print(f"  {lab:20}: 前 3 月中位 {g['early_ret'].median():+.1%}(n={g.height})")


def _nav_stats(nav: pl.DataFrame) -> dict:
    nav = nav.sort("date")
    v = nav["nav"].to_numpy()
    if len(v) < 70:
        return {}
    early = v[min(62, len(v) - 1)] / v[0] - 1
    yrs = len(v) / 252
    cagr = (v[-1] / v[0]) ** (1 / yrs) - 1
    return {"early_ret": float(early), "cagr": float(cagr)}


def run_s_scan() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    rows = []
    for s in _starts():
        st = _nav_stats(run_s(panel, feat, elig, s))
        if st:
            rows.append({"start": s, **st})
    _report("S(apex_revcycle;事件驅動日頻)", rows)


def run_eg_scan() -> None:
    from quantlib.evergreen import engine as E
    con = data.connect()
    doc = json.loads(E.LIVE_CONFIG.read_text())
    d = E.EvergreenData(con, _END)
    rows = []
    for s in _starts():
        nav = E.replay_nav(d, doc["config"], Date.fromisoformat(s), Date.fromisoformat(_END))
        st = _nav_stats(nav)
        if st:
            rows.append({"start": s, **st})
    _report("Evergreen(乾淨 refit cfg)", rows)


def run_se_scan() -> None:
    starts = _starts(first_year=2024, per_month=(1, 11, 21))[:15]
    rows = []
    for s in starts:
        label = f"et_{s.replace('-', '')}"
        cmd = ["uv", "run", "--project", ".", "python", "-m", "quantlib.serenity.engine",
               "--variants", "ev_v3_wf", "--ablate", "filters",
               "--fresh-bonus", "10", "--fresh-months", "12",
               "--start", s, "--end", _END,
               "--registry", "src/quantlib/serenity/wf/registry_wf.csv", "--label", label]
        r = subprocess.run(cmd, cwd=paths.REPO, capture_output=True, text=True, timeout=1800)
        if r.returncode != 0:
            print(f"  {s}: FAILED", flush=True)
            continue
        nav = pl.read_csv(paths.OUT_STRAT_LAB / f"{label}_ev_v3_wf_daily.csv")
        st = _nav_stats(nav)
        if st:
            rows.append({"start": s, **st})
        print(f"  {s} ✓", flush=True)
    _report("Serenity ev_v3_wf(機械 registry)", rows)


if __name__ == "__main__":
    if "--serenity" in sys.argv:
        run_se_scan()
    else:
        run_s_scan()
        run_eg_scan()
