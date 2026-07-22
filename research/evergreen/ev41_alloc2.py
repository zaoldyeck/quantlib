"""EV41 — 配置修正:置信度入權(修 EV35 單窗報酬優化的窗口偏差)。

EV35 的 S 權重 0 是「2025 起單窗報酬最優」的 artifact。本輪:
- EV 臂改用 refit 流程的真 OOS NAV(折1+折2 串接,非 in-sample v3.3 曲線)
- 對比配置變體(含 S>0),共同窗 2025-01-02~2026-07-03(Serenity 資料下限;
  窗口仍偏 Serenity 系,誠實標注)+ 披露各臂單獨數字
- 日再平衡,零成本假設(粗對比;實務月再平衡成本 <0.1%/月)

Run: uv run --project research python -m research.evergreen.ev41_alloc2
"""
from __future__ import annotations

from datetime import date as Date

import numpy as np
import polars as pl

from research.evergreen.ev36_walkforward import seg_kpi
from research.evergreen.ev38_exhaust import LabX
from research.evergreen.ev40_validation import oos_navs
from research import paths

W0, W1 = Date(2025, 1, 2), Date(2026, 7, 3)


def main() -> None:
    lab = LabX()
    n1, n2 = oos_navs(lab)  # 折1 / 折2 OOS NAV
    # 分段算日報酬(折界不跨算),再串接複利——避免銜接假跳點
    dates, rets = [], []
    for seg in (n1, n2):
        v = seg["nav"].to_numpy()
        ds = seg["date"].to_list()
        dates.extend(ds)
        rets.append(0.0)
        rets.extend((v[1:] / v[:-1] - 1).tolist())
    ev = pl.DataFrame({"date": dates, "EV": np.cumprod(1 + np.array(rets))})

    s = (pl.read_parquet("research/apex/ledger/curves/T0334.parquet")
         .select(["date", pl.col("nav").alias("S")]))
    ser = (pl.read_csv(
        f"{paths.OUT_STRAT_LAB}/abl_adv_l0_ev_v2_thesis_inst_daily.csv",
        schema_overrides={"date": pl.Date}).select(["date", pl.col("nav").alias("SER")]))
    j = (ev.join(s, on="date", how="inner").join(ser, on="date", how="inner")
         .filter((pl.col("date") >= W0) & (pl.col("date") <= W1)).sort("date"))
    R = {c: (j[c].to_numpy()[1:] / j[c].to_numpy()[:-1] - 1)
         for c in ("EV", "S", "SER")}
    print(f"共同窗 {j['date'][0]} ~ {j['date'][-1]}({len(j)} 交易日)")
    for c in ("SER", "EV", "S"):
        nav = pl.DataFrame({"date": j["date"], "nav": j[c]})
        k = seg_kpi(nav)
        print(f"  {c:4s} 單臂:CAGR {k['cagr']:7.1%}  MDD {k['mdd']:6.1%}  "
              f"Martin {k['martin']:6.1f}")
    corr = np.corrcoef([R["EV"], R["S"], R["SER"]])
    print(f"  相關:EV-SER {corr[0][2]:.2f}  EV-S {corr[0][1]:.2f}  "
          f"S-SER {corr[1][2]:.2f}")

    mixes = [
        ("EV35 版  SER75/EV25/S0", (0.00, 0.25, 0.75)),
        ("壓艙石版 SER60/EV20/S20", (0.20, 0.20, 0.60)),
        ("機構版  SER50/EV25/S25", (0.25, 0.25, 0.50)),
        ("等權    33/33/33",      (1 / 3, 1 / 3, 1 / 3)),
        ("純 Serenity",           (0.00, 0.00, 1.00)),
    ]
    print("\n配置對比(日再平衡):")
    for name, (ws, wev, wser) in mixes:
        pr = ws * R["S"] + wev * R["EV"] + wser * R["SER"]
        nav = pl.DataFrame({"date": j["date"][1:],
                            "nav": np.cumprod(1 + pr)})
        k = seg_kpi(nav)
        print(f"  {name:24s} CAGR {k['cagr']:7.1%}  MDD {k['mdd']:6.1%}  "
              f"Martin {k['martin']:6.1f}")
    print("\n(窗口 caveat:2025 起共同窗偏 Serenity 系地形;S 的價值是"
          "全天候壓艙——14.5 年 +74.8%/年、DSR 0.99、與兩者相關最低)")


if __name__ == "__main__":
    main()
