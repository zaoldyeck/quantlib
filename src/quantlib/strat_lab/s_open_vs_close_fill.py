"""買在開盤 vs 買在收盤,哪個好?——用逐筆價格漂移做高解析度估計。

## 為什麼需要這支(NAV 配對檢定不夠銳利)
`s_buy_limit.py` 第五段已測出兩者統計上打平(年化差 +0.01%,95% CI ±4.2%),但那個
CI 的半寬 4.2%/年,代表整條 NAV 的配對檢定**解析不出小於 ~4%/年的差異**——而執行層
的真實效應本來就是這個量級(0.1% 滑價 ≈ 1.2%/年)。

低雜訊的做法:**不比 NAV,直接比價格**。同一批進場日、同一批標的,開盤價與收盤價
是同一天同一檔的兩個數字,配對後把市場方向完全消掉,標準誤小一個量級。

    每年拖累 = E[(收盤/開盤 − 1)] × 每倉權重 × 每年進場筆數

正號 = 當天從開盤漲到收盤 → 買在開盤比較便宜。

## 界線
本估計只算「買進當天的價格差」,不含路徑效應(買貴一點 → 後續止損線、峰值錨都跟著
變)。路徑效應由 `s_buy_limit.py` 的引擎版涵蓋;兩者互補:這支給方向與量級,那支
給「算進所有連鎖效應後還剩多少」。

Run: uv run --project . python -m quantlib.strat_lab.s_open_vs_close_fill
依賴 cache: 是(prep_cached)。
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.engine import ExecSpec
from quantlib.apex.strategy_s import C, DS, prep_cached, run_s_full

W = 0.20          # 每倉目標權重(5 席等權)
YEARS = 11.74     # 2014-10-31 ~ 2026-07-24


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)

    # 真實進場清單(canonical S);entry_date 即成交日
    _, trades = run_s_full(panel, feat, elig, DS)
    ent = trades.select([C, "entry_date"]).unique()
    px = panel.select([C, "date", "open", "close", "high", "low"])
    j = (ent.join(px, left_on=[C, "entry_date"], right_on=[C, "date"], how="inner")
         .filter((pl.col("open") > 0) & (pl.col("close") > 0))
         .with_columns((pl.col("close") / pl.col("open") - 1.0).alias("o2c")))
    r = j["o2c"].to_numpy()
    n = len(r)
    m, sd = float(r.mean()), float(r.std(ddof=1))
    se = sd / np.sqrt(n)
    lo, hi = m - 1.96 * se, m + 1.96 * se
    rate = n / YEARS

    print(f"S 進場日的「開盤 → 收盤」漂移(配對,{n} 筆,{rate:.1f} 筆/年)")
    print(f"  平均 {m:+.3%}  中位 {np.median(r):+.3%}  標準差 {sd:.3%}")
    print(f"  95% CI [{lo:+.3%}, {hi:+.3%}]   收盤高於開盤的比例 {(r > 0).mean():.1%}")
    print()
    print("換算成年化拖累(每倉權重 20% × 每年進場筆數):")
    for lab, v in (("平均", m), ("CI 下界", lo), ("CI 上界", hi)):
        print(f"  {lab:>7}:{v * W * rate:+.2%}/年"
              f"({'買在開盤較優' if v > 0 else '買在收盤較優'})")
    print()
    half = (hi - m) * W * rate
    print(f"  解析度對照:s_buy_limit 的 NAV 配對檢定 CI 半寬 ±4.2%/年;本估計 "
          f"±{half:.2%}/年(僅 {4.2 / max(half * 100, 1e-9):.1f} 倍)")
    print("  ⚠ 原假設「比價格會銳利一個量級」**不成立**:逐筆開→收漂移的標準差高達 "
          f"{sd:.1%},樣本 {n} 筆壓不下來。兩種估計法都分辨不出這個量級的差異。")

    # 分年看穩定性(單一年份的極端值會不會撐起全部)
    print("\n逐年(避免結論被單一年份撐起):")
    yr = (j.with_columns(pl.col("entry_date").dt.year().alias("y"))
          .group_by("y").agg([pl.len().alias("n"), pl.col("o2c").mean().alias("m")])
          .sort("y"))
    for row in yr.iter_rows(named=True):
        bar = "▇" * max(int(abs(row["m"]) * 2000), 0)
        print(f"  {row['y']}  n={row['n']:>3}  {row['m']:+.3%}  {bar}")
    pos = (yr["m"] > 0).sum()
    print(f"  → {pos}/{yr.height} 個年份為正(開盤較便宜)")


if __name__ == "__main__":
    main()
