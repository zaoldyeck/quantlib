"""S01 — apex_revcycle_S 逐年 PnL 分佈診斷(離散度 vs 集中度)。

回答「S 每年賺賠不均勻是好是壞」:區分兩種不均勻——
  (a) 健康離散:edge 在多數年份都在,只是幅度大小年不同;
  (b) 脆弱集中:整條績效靠一兩個幸運年撐,剔掉就沒了。
量測:(1) 正報酬年數比例 (2) 最佳年佔複利對數財富比重 (3) 剔除最強
1/2 年後的幾何年均(edge 是否只靠幸運年)(4) 各年 vs 0050 對照
(壞年是否比大盤更抗跌)。純量測,不改策略一參一字。

S 規格單一真源 = chart_s_vs_benchmarks(5 席 20% 等權,STRATEGY.md)。
Run: uv run --project research python -m research.apex.experiments.s01_pnl_distribution
依賴 cache: 是(需最新)
"""
from __future__ import annotations

import numpy as np
import polars as pl

from research.apex import data
from research.apex.experiments.chart_s_vs_benchmarks import (DS, prep, run_s,
                                                             yearly_ret)


def intra_year_mdd(df: pl.DataFrame) -> dict[int, float]:
    """各年『年內』最大回撤(以年初重置的路徑計)。"""
    out: dict[int, float] = {}
    years = df["date"].dt.year().to_numpy()
    v = df["nav"].to_numpy()
    for y in np.unique(years):
        seg = v[np.where(years == y)[0]]
        out[int(y)] = float((seg / np.maximum.accumulate(seg) - 1).min())
    return out


def main() -> None:
    con = data.connect()
    de = data.latest_date(con).isoformat()  # cache 最新日,動態(禁寫死)
    panel, feat, elig = prep(con, de)
    s_nav = run_s(panel, feat, elig, DS)
    b0050 = (data.benchmark_nav(con, DS, de, code="0050").sort("date")
             .with_columns(pl.col("nav") / pl.col("nav").first()))
    yr = yearly_ret({"S": s_nav, "0050": b0050})
    mdd = intra_year_mdd(s_nav)

    years = sorted(yr)
    s_rets = {y: yr[y]["S"] for y in years}
    # 排除首尾殘年(2014 僅 11-12 月、2026 至 7/9)做分佈統計
    full_years = [y for y in years if y not in (2014, 2026)]

    print(f"apex_revcycle_S(5 席 × 20%)逐年 PnL —— 全史 {DS} ~ {de}")
    print(f"(2019-01~2025-06 為參數最佳化窗;其餘為窗外 evaluation-only)\n")
    print(f"{'年':>7} {'S 報酬':>9} {'年內MDD':>9} {'0050':>9} {'S超額':>9}")
    for y in years:
        s = s_rets[y]
        b = yr[y].get("0050")
        star = "*" if y in (2014, 2026) else ""
        rel = f"{s - b:+.1%}" if b is not None else "—"
        bshow = f"{b:+.1%}" if b is not None else "—"
        print(f"{str(y) + star:>7} {s:>+8.1%} {mdd[y]:>+8.1%} {bshow:>9} {rel:>9}")

    rr = np.array([s_rets[y] for y in full_years])
    logw = np.log1p(rr)
    n = len(rr)
    total_log = float(logw.sum())
    order = np.argsort(logw)[::-1]
    geomean = np.expm1(total_log / n)
    ex1 = np.expm1((total_log - logw[order[0]]) / (n - 1))
    ex2 = np.expm1((total_log - logw[order[:2]].sum()) / (n - 2))
    wi = int(np.argmin(rr))

    print(f"\n── 分佈診斷(完整年 {n} 年)──")
    print(f"  正報酬年       :{int((rr > 0).sum())}/{n} = {(rr > 0).mean():.0%}")
    print(f"  幾何年均        :{geomean:+.1%}")
    print(f"  最佳年 {full_years[order[0]]} 佔複利對數財富:{logw[order[0]] / total_log:.0%}")
    print(f"  最佳兩年佔複利對數財富      :{logw[order[:2]].sum() / total_log:.0%}")
    print(f"  剔最佳 1 年後幾何年均:{ex1:+.1%}   (edge 靠幸運年?)")
    print(f"  剔最佳 2 年後幾何年均:{ex2:+.1%}")
    print(f"  最差年          :{full_years[wi]} {rr[wi]:+.1%}"
          f"(同年 0050 {yr[full_years[wi]].get('0050', float('nan')):+.1%};"
          f"S {'贏' if rr[wi] > yr[full_years[wi]].get('0050', -9) else '輸'})")
    print(f"  年報酬標準差    :{rr.std(ddof=1):.1%}(離散度本身)")


if __name__ == "__main__":
    main()
