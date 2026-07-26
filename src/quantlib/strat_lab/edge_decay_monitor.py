"""Edge 衰減監控:S 的六因子逐年預測力有沒有在退化?(= 「何時該重新研發」的操作化判準)

## 為什麼要做(承接三層判決的唯一開口)
`regime_adaptive_verdict` 判定「依近期績效換策略/因子/參數」全數無效,但明確保留一個例外:
**edge 本身失效時該換,判準是 edge 的預測力衰減,不是近期報酬**。本檔把那句話變成可執行的監控。

同時,第 2 層(spread 版)有兩個年份重選版**贏過** S(2025 +67% vs +32%、2026 +67% vs +44%),
且近年選出的因子在變(開始選 mom_126_5/hvn_dist、少選 rev_yoy)——這**可能是雜訊,也可能是
真的衰減**。不預設答案,直接量。

## 量什麼
S 的六因子 + 重選版常客(range_pos_60/hvn_dist/rev_yoy),在 S 的池內逐年:
- h21 截面 IC(預測方向)
- top−bottom 五分位 fwd21 報酬差(可交易性)
並對「年 vs 指標」做線性趨勢檢定(斜率 + Pearson),看是否有**統計上的衰減**。

## 判讀
- 斜率顯著為負且近 3 年 spread 明顯低於前期 → edge 衰減,**該啟動重新研發**(此時使用者的
  「換」是對的,判準是因果不是績效);
- 無趨勢/波動但無方向 → 近年輸贏是雜訊,維持固定策略。

Run: uv run --project . python -m quantlib.strat_lab.edge_decay_monitor
依賴 cache:是(乾淨世代)。
"""
from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl

from quantlib.apex import data, factors
from quantlib.apex.strategy_s import WREL, prep_cached
from quantlib.strat_lab.rolling_refactor import _ic, _s_pool

WATCH = list(WREL) + ["range_pos_60", "hvn_dist", "rev_yoy"]
YEARS = tuple(range(2015, 2027))


def _trend(xs: list[int], ys: list[float]) -> tuple[float, float]:
    """(每年斜率, Pearson r);樣本不足回 (nan, nan)。"""
    x = np.array(xs, dtype=float)
    y = np.array(ys, dtype=float)
    ok = ~np.isnan(y)
    if ok.sum() < 5:
        return float("nan"), float("nan")
    x, y = x[ok], y[ok]
    slope = float(np.polyfit(x, y, 1)[0])
    r = float(np.corrcoef(x, y)[0, 1])
    return slope, r


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    fwd = factors.forward_returns(panel)
    pool = _s_pool(feat, elig)

    for metric, lab in (("ic", "h21 截面 IC"), ("spread", "top−bottom 五分位 fwd21 報酬差")):
        print(f"\n=== {lab} 逐年({'S 六因子' if metric=='ic' else '同左'} + 重選版常客)===")
        print(f"  {'因子':<18}" + "".join(f"{y % 100:>7}" for y in YEARS) + f"{'斜率/年':>10}{'r':>7}")
        for col in WATCH:
            vals = []
            for y in YEARS:
                v = _ic(pool, fwd, col, dt.date(y, 1, 1), dt.date(y + 1, 1, 1), metric)
                vals.append(v)
            slope, r = _trend(list(YEARS), vals)
            cells = "".join((f"{v:>+7.3f}" if metric == "ic" else f"{v:>+7.1%}")
                            if v == v else f"{'--':>7}" for v in vals)
            star = " ⚠衰減" if (r == r and r < -0.5 and slope < 0) else ""
            print(f"  {col:<18}{cells}{slope:>+10.4f}{r:>+7.2f}{star}")

    print("\n  判讀:斜率顯著為負 + r < −0.5 = 該因子預測力在退化;若 S 的核心因子(rev_yoy_accel/"
          "high_52w)出現衰減 → 啟動重新研發(此時『換』有因果依據);否則近年輸贏屬雜訊。")


if __name__ == "__main__":
    main()
