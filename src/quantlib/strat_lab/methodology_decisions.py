"""Phase 3.4 方法論拍板證據:Q1 回測用幾年歷史 + Q2 用什麼 KPI 比較策略。

使用者問:(Q1)最強策略該用過去幾年回測優化?(Q2)KPI 決定了嗎,Sortino 該取代 Sharpe 嗎?
這兩個是框架級決策,本檔產證據讓結論落地(非拍腦袋)。reuse prep_cached(秒回)+ factors +
metrics(已含 sortino)。

Q1 證據:edge(rev_yoy_accel/high_52w)在**早期 2014-2019 vs 近期 2020-2026** 的截面 IC——若跨期
穩定 = edge 是結構性的,用全史(最大統計力 + regime 覆蓋)最穩;若近期才有 = 才需縮窗。
Q2 證據:各變體的 Sharpe vs Sortino vs Calmar——長多偏正偏策略,Sharpe 懲罰上行波動不合理。

Run: uv run --project . python -m quantlib.strat_lab.methodology_decisions
"""
from __future__ import annotations

import polars as pl

from quantlib.apex import data, factors, strategy_s
from quantlib.apex.metrics import perf_stats
from quantlib.apex.strategy_s import C, DS, prep_cached, run_s


def _sub_ic(feat, fwd, elig, col, lo, hi):
    fac = (feat.select(["date", C, col]).rename({col: "value"})
           .filter((pl.col("date") >= pl.lit(lo).str.to_date())
                   & (pl.col("date") < pl.lit(hi).str.to_date())))
    r = factors.evaluate_factor(col, fac, fwd, elig, family="rev", batch="q1", log=False)
    h = r.get("h21") or {}
    return h.get("mean_ic"), h.get("t")


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    fwd = factors.forward_returns(panel)

    print("=== Q1:edge 跨期 IC 穩定性(h21;決定回測用幾年)===")
    print(f"  {'因子':<18}{'2014-2019 IC(t)':>20}{'2020-2026 IC(t)':>20}")
    for col in ["rev_yoy_accel", "high_52w", "close_pos_20"]:
        e_ic, e_t = _sub_ic(feat, fwd, elig, col, "2014-01-01", "2020-01-01")
        r_ic, r_t = _sub_ic(feat, fwd, elig, col, "2020-01-01", "2027-01-01")
        print(f"  {col:<18}{f'{e_ic:+.3f}({e_t:+.1f})':>20}{f'{r_ic:+.3f}({r_t:+.1f})':>20}")
    print("  → 若早期/近期 IC 同號同量 = edge 結構性、跨 regime 穩 → 用全史回測(最大統計力)最穩,\n"
          "    不縮窗;縮窗到「過去 N 年」反而丟證據 + 踩 3.1 的過擬合陷阱。")

    print("\n=== Q2:各變體 Sharpe vs Sortino vs Calmar(決定 KPI)===")
    print(f"  {'變體':<20}{'CAGR':>8}{'Sharpe':>8}{'Sortino':>9}{'Calmar':>8}{'MDD':>8}")
    _B = dict(strategy_s.WREL)
    variants = {"baseline6": _B, "-accelrel": {k: v for k, v in _B.items() if k != "accel_rel"}}
    try:
        for name, wrel in variants.items():
            strategy_s.WREL = wrel
            st = perf_stats(run_s(panel, feat, elig, DS).sort("date"))
            print(f"  {name:<20}{st['cagr']:>+7.1%}{st['sharpe']:>8.2f}{st['sortino']:>9.2f}"
                  f"{st['calmar']:>8.2f}{st['mdd']:>+7.1%}")
    finally:
        strategy_s.WREL = _B
    print("  → 長多偏正偏策略:Sortino(只罰下行波動)比 Sharpe 合理;但單一比率不夠,\n"
          "    要 Sortino(風險調整)+ Calmar(回撤,實際會痛的)+ DSR/PBO(真不真)+ 逐年下界(regime)。")


if __name__ == "__main__":
    main()
