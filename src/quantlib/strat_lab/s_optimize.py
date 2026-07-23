"""Phase 3.4:S 建構塊剪枝實測——去掉 IC 檢驗證偽的弱因子,S 會不會更強?

依 first_principles_framework step ④:edge 檢驗(edge_accel_rel)發現 mom_126_5 在 h21 無訊號
(IC -0.002 t -0.2)、accel_rel 無加分。本檔 whole-strategy 實測剪枝——**reuse S 的 canonical
引擎 run_s**(monkeypatch WREL 換因子集,不重寫引擎、跑完還原),對照各變體全跨度 KPI + bootstrap
下界。若剪枝後 CAGR/Sharpe/下界不降反升 = 弱因子確為死重,S 可簡化強化。

**注意**:這是研究實驗,monkeypatch 後 restore;正式採用某變體須過框架 step ⑤ walk-forward/DSR/PBO
(本檔只是全跨度篩選,非出廠驗證)。全跨度 in-sample 好不代表 OOS 好——證偽用可,拍板要 OOS。

Run: uv run --project . python -m quantlib.strat_lab.s_optimize
依賴 cache:是(乾淨世代)。
"""
from __future__ import annotations

from quantlib.apex import data, strategy_s
from quantlib.apex.metrics import perf_stats
from quantlib.apex.strategy_s import DS, prep, run_s
from quantlib.apex.validate import block_bootstrap_cagr

_BASE = dict(strategy_s.WREL)
VARIANTS = {
    "baseline(6 因子,S 現行)": _BASE,
    "去 mom_126_5(h21 死重)": {k: v for k, v in _BASE.items() if k != "mom_126_5"},
    "去 mom_126_5+accel_rel(簡化)": {k: v for k, v in _BASE.items()
                                     if k not in ("mom_126_5", "accel_rel")},
    "只留 high_52w+rev_yoy_accel(最強雙因子)": {"high_52w": 1.0, "rev_yoy_accel": 1.0},
}


def main() -> None:
    con = data.connect()
    print("[s-opt] 組裝特徵(乾淨資料)…", flush=True)
    panel, feat, elig = prep(con)
    print(f"\n=== S 建構塊剪枝實測(全跨度,含成本;reuse run_s 引擎)===")
    print(f"  {'變體':<34}{'CAGR':>9}{'Sharpe':>8}{'MDD':>8}{'Calmar':>8}{'boot下界':>10}")
    try:
        for name, wrel in VARIANTS.items():
            strategy_s.WREL = wrel  # monkeypatch:reuse 引擎、換因子集
            nav = run_s(panel, feat, elig, DS).sort("date")
            st = perf_stats(nav)
            boot = block_bootstrap_cagr(nav)
            print(f"  {name:<34}{st['cagr']:>+8.1%}{st['sharpe']:>8.2f}{st['mdd']:>+7.1%}"
                  f"{st['calmar']:>8.2f}{boot['ci_lo']:>+9.1%}")
    finally:
        strategy_s.WREL = _BASE  # 還原,不留副作用
    print("\n  判準:剪枝後 CAGR/Sharpe/下界不降反升 = 弱因子為死重。全跨度僅篩選;拍板須 OOS walk-forward。")


if __name__ == "__main__":
    main()
