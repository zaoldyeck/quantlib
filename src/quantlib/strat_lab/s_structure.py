"""Phase 3.4 path A:質疑 S 的結構/風控——加絕對停損能否壓 -34% MDD?

S 現行出場只有 trail 35% + time 30/15,**無絕對停損**。CLAUDE.md 策略設計原則明列「Absolute
stop -20%」為多條件 OR 出場的一環。這是**有原則的風控改動(非參數挖礦)**:單筆從進場跌 20%
就砍,理論上壓大額個股虧損 → 降 MDD。測各風控/結構變體,用 D2 的 KPI(Sortino/Calmar/MDD/下界)判。

reuse prep_cached(秒回)+ run_s_full(_exit_spec/_port_spec 參數化,預設 canonical)+ metrics/validate。
**這是研究,拍板改 canonical S 前須 ledger 預註冊 + 出廠閘門全驗。**

Run: uv run --project . python -m quantlib.strat_lab.s_structure
"""
from __future__ import annotations

from quantlib.apex import data
from quantlib.apex.engine import ExitSpec, PortSpec
from quantlib.apex.metrics import perf_stats
from quantlib.apex.strategy_s import DS, prep_cached, run_s_full
from quantlib.apex.validate import block_bootstrap_cagr

_BE = dict(trailing_stop=0.35, time_stop=30, loser_time_stop=15)
#: (exit_spec, port_spec);None = canonical 預設
VARIANTS = {
    "canonical(無abs)": (None, None),
    "+abs 20%": (ExitSpec(**_BE, abs_stop=0.20), None),
    "+abs 25%": (ExitSpec(**_BE, abs_stop=0.25), None),
    "trail 25%(緊)": (ExitSpec(trailing_stop=0.25, time_stop=30, loser_time_stop=15), None),
    "+abs20 +trail25": (ExitSpec(trailing_stop=0.25, time_stop=30, loser_time_stop=15, abs_stop=0.20), None),
    "slots 8": (None, PortSpec(n_slots=8, max_new_per_day=2)),
    "slots 10": (None, PortSpec(n_slots=10, max_new_per_day=2)),
}


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    print("=== S 結構/風控變體(全跨度;reuse run_s_full,參數化)===")
    print(f"  {'變體':<20}{'CAGR':>8}{'Sortino':>9}{'Calmar':>8}{'MDD':>8}{'下界':>9}")
    for name, (es, ps) in VARIANTS.items():
        nav, _ = run_s_full(panel, feat, elig, DS, _exit_spec=es, _port_spec=ps)
        nav = nav.sort("date")
        st = perf_stats(nav)
        boot = block_bootstrap_cagr(nav)
        print(f"  {name:<20}{st['cagr']:>+7.1%}{st['sortino']:>9.2f}{st['calmar']:>8.2f}"
              f"{st['mdd']:>+7.1%}{boot['ci_lo']:>+8.1%}")
    print("\n  判準(D2):壓 MDD 又不砍太多 CAGR → Sortino/Calmar 升 = 有原則的風控改善;"
          "拍板須 OOS + ledger。")


if __name__ == "__main__":
    main()
