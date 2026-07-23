"""S(apex_revcycle_S)在**校正後 cache** 上的重驗證。

2026-07-23 全資料鏈從 raw 重建修正 162 parser bug(dtd 自營商錯位/Int32、除權息
FC1 參考價、FC8 產業別 PIT 等),回測基底已變。本腳本用**canonical 引擎**
`strategy_s.run_s`(唯一真源,n_slots=5)在校正 cache 上重跑全跨度 + 近窗 KPI +
block-bootstrap CI,對照 docs/REPORT.md 記錄值(在 buggy 資料上算),確認 S 是否
仍是有效 alpha(而非資料 bug 撐出來的)。

**與 Evergreen 重錄同理**:資料校正必然改變回測,策略 KPI 需在正確資料上重錄。

Run: uv run --project . python -m quantlib.apex.revalidate_corrected
依賴 cache: 是(需最新校正世代)。
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.metrics import perf_stats, yearly_table
from quantlib.apex.strategy_s import DS, prep, run_s
from quantlib.apex.validate import block_bootstrap_cagr

#: docs/REPORT.md 記錄值(buggy 資料;dev/val/holdout 為分窗回測,全跨度為 v3-n20 連續)
RECORDED = {
    "dev 2012-2023": {"cagr": 0.332, "sharpe": 1.65, "mdd": -0.266},
    "val 2024-2025H1": {"cagr": 0.262, "sharpe": 1.21, "mdd": -0.189},
    "holdout 2025H2+": {"cagr": 0.483, "sharpe": 1.51, "mdd": -0.157},
}

#: 分窗(對照 REPORT 邊界;連續 NAV 切片,非分窗回測——故 CAGR 會與 REPORT 略異)
WINDOWS = [
    ("dev 2012-2023", "2014-10-31", "2023-12-31"),
    ("val 2024-2025H1", "2024-01-01", "2025-06-30"),
    ("holdout 2025H2+", "2025-07-01", "2099-12-31"),
]


def _slice(nav: pl.DataFrame, lo: str, hi: str) -> pl.DataFrame:
    s = nav.filter((pl.col("date") >= pl.lit(lo).str.to_date())
                   & (pl.col("date") <= pl.lit(hi).str.to_date())).sort("date")
    if s.height < 2:
        return s
    return s.with_columns(pl.col("nav") / pl.col("nav").first())  # 各窗重歸一


def main() -> None:
    con = data.connect()
    end = con.execute("SELECT max(date) FROM daily_quote WHERE market='twse'").fetchone()[0]
    print(f"[revalidate] 校正 cache,資料截止 {end};canonical strategy_s(n_slots=5)全跨度重跑…\n")

    panel, feat, elig = prep(con, str(end))
    nav = run_s(panel, feat, elig, DS).sort("date")

    full = perf_stats(nav)
    print("=== 全跨度(校正資料)===")
    print(f"  {full['years']}y  CAGR {full['cagr']:+.1%}  Sharpe {full['sharpe']:.2f}  "
          f"MDD {full['mdd']:.1%}  Calmar {full['calmar']:.2f}  終值 {full['final_nav_ratio']:.1f}x")
    boot = block_bootstrap_cagr(nav)
    print(f"  block-bootstrap CAGR 95% CI [{boot['ci_lo']:+.1%}, {boot['ci_hi']:+.1%}] "
          f"中位 {boot['median']:+.1%};P(CAGR≤0) = {boot['p_neg']:.3f}")

    print("\n=== 分窗對照(校正 vs REPORT 記錄〔buggy〕;切片重歸一,與分窗回測略異)===")
    print(f"  {'窗口':<18}{'校正 CAGR':>12}{'記錄 CAGR':>12}{'校正 Sharpe':>13}{'校正 MDD':>11}")
    for name, lo, hi in WINDOWS:
        w = _slice(nav, lo, hi)
        if w.height < 2:
            print(f"  {name:<18}{'(無資料)':>12}")
            continue
        st = perf_stats(w)
        rec = RECORDED.get(name, {})
        print(f"  {name:<18}{st['cagr']:>+11.1%}{rec.get('cagr', float('nan')):>+11.1%}"
              f"{st['sharpe']:>13.2f}{st['mdd']:>+10.1%}")

    print("\n=== 逐年報酬(校正資料)===")
    yt = yearly_table(nav)
    with pl.Config(tbl_rows=20):
        print(yt)

    # 判準白話
    print("\n=== 結論 ===")
    strong = full["cagr"] > 0.15 and full["sharpe"] > 1.0 and boot["p_neg"] < 0.05
    print(f"  S 在校正資料上{'仍是有效 alpha' if strong else '需人工檢視(KPI 明顯弱化)'}:"
          f"全跨度 CAGR {full['cagr']:+.1%}、Sharpe {full['sharpe']:.2f}、"
          f"bootstrap 下界 {boot['ci_lo']:+.1%}、P(虧損) {boot['p_neg']:.3f}。")


if __name__ == "__main__":
    main()
