"""EV53 附錄 — 316% CAGR 出處對帳(回應使用者:逐年 65-218% 怎會年化 316%)。

證明「逐年報酬」與「train 窗 CAGR」量的是不同切法,定位 316% 的爆發來源
(跨曆年邊界的猛烈區段),並掃資料假突刺(單日暴跳=分割/還原 bug)排除
數字造假。純量測,不改策略。

Run: uv run --project research python -m research.evergreen.ev53_cagr_reconcile
依賴 cache: 是
"""
from __future__ import annotations

from datetime import date as Date

import numpy as np
import polars as pl

from research.evergreen.ev43_live_refit import LabL
from research.evergreen.ev53_mdd_cap import LIVE, build_sc, nav_of

FULL0, FULL1 = Date(2022, 7, 11), Date(2026, 7, 9)  # registry 全span
TR0, TR1 = Date(2023, 7, 11), Date(2026, 7, 9)       # ev43 refit train 窗(316% 出處)


def _cagr(v0, v1, days):
    return (v1 / v0) ** (365.25 / max(days, 1)) - 1


def _year_rets(nav):
    v = nav["nav"].to_numpy()
    years = nav["date"].dt.year().to_numpy()
    out = []
    for y in np.unique(years):
        idx = np.where(years == y)[0]
        prev = v[idx[0] - 1] if idx[0] > 0 else v[idx[0]]
        out.append((int(y), v[idx[-1]] / prev - 1))
    return out


def main() -> None:
    lab = LabL()
    sc, pf = build_sc(lab, LIVE["gate"], LIVE["score"], LIVE["pool_months"],
                      LIVE["n_slots"])

    # ① 重現 316%:與 ev43 同法——simulate 從 TR0 起(fresh)
    tw = nav_of(lab, sc, pf, TR0, TR1, trail=LIVE["trail"], lts=LIVE["lts"],
                n_slots=LIVE["n_slots"], abs_stop=LIVE["abs_stop"])
    tv = tw["nav"].to_numpy()
    days = (tw["date"][-1] - tw["date"][0]).days
    print(f"① train 窗 {TR0}~{TR1}(fresh 起跑,與 live_config 同法)")
    print(f"   {len(tv)} 日 ≈ {days / 365.25:.2f} 年;總報酬 {tv[-1] / tv[0]:.1f}x;"
          f"CAGR {_cagr(tv[0], tv[-1], days):.1%}  ← live_config 的 316%")

    print("\n② 同一條淨值的『年中→年中』逐年切片(train 窗內 3 段):")
    for a, b in [(TR0, Date(2024, 7, 10)), (Date(2024, 7, 11), Date(2025, 7, 10)),
                 (Date(2025, 7, 11), TR1)]:
        seg = tw.filter((pl.col("date") >= a) & (pl.col("date") <= b))
        sv = seg["nav"].to_numpy()
        print(f"   {a}~{b}: {sv[-1] / sv[0]:5.2f}x  年化 {_cagr(sv[0], sv[-1], (seg['date'][-1] - seg['date'][0]).days):+7.0%}")

    print("\n③ 同一條淨值的『曆年』報酬(Jan-Dec;首尾部分年)——你看到的視角:")
    for y, r in _year_rets(tw):
        print(f"   {y}: {r:+.1%}")

    # ④ 資料假突刺掃描:單日 NAV 報酬異常 = 分割/還原 bug 的訊號
    r = tv[1:] / tv[:-1] - 1
    order = np.argsort(r)[::-1][:6]
    print("\n④ 最大單日 NAV 跳動(掃資料假突刺;>25%/日 = 需查該檔分割還原):")
    for i in order:
        print(f"   {tw['date'][int(i) + 1]}: {r[int(i)]:+.1%}")

    # ⑤ 全 span 曆年(2022 起)——對使用者列的 5 個數字
    full = nav_of(lab, sc, pf, FULL0, FULL1, trail=LIVE["trail"], lts=LIVE["lts"],
                  n_slots=LIVE["n_slots"], abs_stop=LIVE["abs_stop"])
    fv = full["nav"].to_numpy()
    fdays = (full["date"][-1] - full["date"][0]).days
    print(f"\n⑤ 全 span {FULL0}~{FULL1}(fresh 2022 起)曆年報酬:")
    for y, rr in _year_rets(full):
        print(f"   {y}: {rr:+.1%}")
    print(f"   全 span 總報酬 {fv[-1] / fv[0]:.1f}x;CAGR {_cagr(fv[0], fv[-1], fdays):.1%}"
          f"(比 316% 低——2022-07~2023-07 沒有那段爆發年)")

    # ⑥ 三方對算:證明儀表板數字 = 掉了 gate 的降級版
    print("\n⑥ 三方對算(全 span 2022-07 起,曆年 + CAGR):")
    for label, gate in [("加 gate=inst5(真 live config)", "inst5"),
                        ("拿掉 gate(=儀表板 evergreen_nav 現狀)", "none")]:
        scg, pfg = build_sc(lab, gate, LIVE["score"], LIVE["pool_months"],
                            LIVE["n_slots"])
        n = nav_of(lab, scg, pfg, FULL0, FULL1, trail=LIVE["trail"],
                   lts=LIVE["lts"], n_slots=LIVE["n_slots"], abs_stop=LIVE["abs_stop"])
        nv = n["nav"].to_numpy()
        yr = " ".join(f"{y}:{r:+.0%}" for y, r in _year_rets(n))
        print(f"   {label}")
        print(f"     {yr}  | CAGR {_cagr(nv[0], nv[-1], (n['date'][-1]-n['date'][0]).days):.0%}")

    # 儀表板函式本尊(權威來源,確認使用者看到的數字)
    try:
        from research.tri.pnl_dashboard import evergreen_nav
        dn = evergreen_nav(FULL1)
        dv = dn.to_numpy()
        yrs = dn.index.year
        db_yr = []
        for y in sorted(set(yrs)):
            idx = np.where(yrs == y)[0]
            prev = dv[idx[0] - 1] if idx[0] > 0 else dv[idx[0]]
            db_yr.append((int(y), dv[idx[-1]] / prev - 1))
        print("   儀表板 evergreen_nav 本尊(使用者看到的):")
        print("     " + " ".join(f"{y}:{r:+.0%}" for y, r in db_yr)
              + f"  | CAGR {_cagr(dv[0], dv[-1], (dn.index[-1]-dn.index[0]).days):.0%}")
    except Exception as e:
        print(f"   (儀表板函式呼叫略過:{e})")


if __name__ == "__main__":
    main()
