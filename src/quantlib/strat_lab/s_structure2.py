"""Phase 3.4 goal-④ 補測:MAE/MFE 指向的 abs10/15 分離點 + 止盈 + 動態加碼(whole-strategy)。

s_maemfe 分佈證據:贏家 MAE P90=-8.8%、-10% 停損「贏家中槍 7.1% vs 輸家攔截 38.2%」分離度
最好(今晨 s_structure 只測了分離度最差的 abs20/25)。本檔 whole-strategy 補實測:
- abs_stop ∈ {10%, 12.5%, 15%}(反事實分離點附近)
- profit_take ∈ {40%, 60%}(止盈反事實:+40% 救 0 輸家、封頂 11% 贏家 → 預期負,實測釘死)
- pyramiding(引擎原生 pyramid_trigger/max/frac;F11 舊輪判噪音級,乾淨資料重驗)
KPI 依 D2:Sortino/Calmar/MDD/bootstrap 下界。reuse prep_cached + run_s_full 參數化。

Run: uv run --project . python -m quantlib.strat_lab.s_structure2
依賴 cache:是。
"""
from __future__ import annotations

from quantlib.apex import data
from quantlib.apex.engine import ExitSpec, PortSpec
from quantlib.apex.metrics import perf_stats
from quantlib.apex.strategy_s import DS, prep_cached, run_s_full
from quantlib.apex.validate import block_bootstrap_cagr

_BE = dict(trailing_stop=0.35, time_stop=30, loser_time_stop=15)
VARIANTS = {
    "canonical": (None, None),
    "abs 10%": (ExitSpec(**_BE, abs_stop=0.10), None),
    "abs 12.5%": (ExitSpec(**_BE, abs_stop=0.125), None),
    "abs 15%": (ExitSpec(**_BE, abs_stop=0.15), None),
    "profit_take 40%": (ExitSpec(**_BE, profit_take=0.40), None),
    "profit_take 60%": (ExitSpec(**_BE, profit_take=0.60), None),
    "pyramid t15 f50": (None, PortSpec(n_slots=5, max_new_per_day=2,
                                       pyramid_trigger=0.15, pyramid_max=1, pyramid_frac=0.5)),
    "pyramid t25 f50": (None, PortSpec(n_slots=5, max_new_per_day=2,
                                       pyramid_trigger=0.25, pyramid_max=1, pyramid_frac=0.5)),
}


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    print("=== S 交易系統補測(abs 分離點/止盈/加碼;全跨度)===")
    print(f"  {'變體':<18}{'CAGR':>8}{'Sortino':>9}{'Calmar':>8}{'MDD':>8}{'下界':>9}")
    for name, (es, ps) in VARIANTS.items():
        nav, _ = run_s_full(panel, feat, elig, DS, _exit_spec=es, _port_spec=ps)
        nav = nav.sort("date")
        st = perf_stats(nav)
        boot = block_bootstrap_cagr(nav)
        print(f"  {name:<18}{st['cagr']:>+7.1%}{st['sortino']:>9.2f}{st['calmar']:>8.2f}"
              f"{st['mdd']:>+7.1%}{boot['ci_lo']:>+8.1%}", flush=True)
    print("\n  判準(D2):任一變體 Sortino+Calmar+下界同時 ≥ canonical 才算改進;否則證偽落地。")


if __name__ == "__main__":
    main()
