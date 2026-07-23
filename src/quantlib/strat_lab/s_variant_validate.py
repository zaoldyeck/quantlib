"""Phase 3.4 step ⑤:S 變體 OOS-robustness 驗證(逐年一致性 + DSR + PBO)。

s_optimize 全跨度 in-sample 顯示 baseline CAGR 最高、簡化版 Calmar 最優。哪個真的更穩,要看
**樣本外 robustness**。S 變體是**固定因子集(無 fitted 參數)**,故 OOS 驗證 =
- **逐年報酬一致性**:各變體逐年 return,看優勢是全期一致還是靠幾年(regime 依賴)。
- **DSR(Bailey-López de Prado)**:最佳變體的 Sharpe 校正「試了 K 個變體」的多重比較膨脹。
- **PBO(CSCV)**:選 IS 最佳變體,OOS 落後中位的機率(過擬合機率;<0.5 才可信)。

變體集**證據導向**(來自 edge_accel_rel 的 IC 拆解,非 grid-search 掃參數,守框架「穩健選參非
最佳化」):baseline + 剪各弱因子的組合。reuse run_s 引擎(monkeypatch WREL,跑完還原)+
apex.validate(deflated_sharpe/pbo_cscv/sr_variance_from_curves)。

Run: uv run --project . python -m quantlib.strat_lab.s_variant_validate
依賴 cache:是。
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib.apex import data, strategy_s
from quantlib.apex.metrics import perf_stats
from quantlib.apex.strategy_s import DS, prep, run_s
from quantlib.apex.validate import (deflated_sharpe, pbo_cscv,
                                    sr_variance_from_curves)

_B = dict(strategy_s.WREL)
#: 證據導向變體(來自 IC 拆解:mom_126_5 h21 死重、accel_rel 無加分、close_pos_20 弱 spread)
VARIANTS = {
    "baseline6": _B,
    "-mom126": {k: v for k, v in _B.items() if k != "mom_126_5"},
    "-mom126-accelrel": {k: v for k, v in _B.items() if k not in ("mom_126_5", "accel_rel")},
    "-mom126-accelrel-clpos": {k: v for k, v in _B.items()
                               if k not in ("mom_126_5", "accel_rel", "close_pos_20")},
    "-accelrel": {k: v for k, v in _B.items() if k != "accel_rel"},
    "-revseq": {k: v for k, v in _B.items() if k != "rev_seq"},
}


def main() -> None:
    con = data.connect()
    print("[s-val] 組裝特徵…", flush=True)
    panel, feat, elig = prep(con)
    navs: dict[str, pl.DataFrame] = {}
    try:
        for name, wrel in VARIANTS.items():
            strategy_s.WREL = wrel
            navs[name] = run_s(panel, feat, elig, DS).sort("date")
    finally:
        strategy_s.WREL = _B

    # 全跨度 KPI
    print("\n=== 全跨度 KPI ===")
    print(f"  {'變體':<24}{'CAGR':>9}{'Sharpe':>8}{'MDD':>8}{'Calmar':>8}")
    stats = {}
    for name, nav in navs.items():
        st = perf_stats(nav)
        stats[name] = st
        print(f"  {name:<24}{st['cagr']:>+8.1%}{st['sharpe']:>8.2f}{st['mdd']:>+7.1%}{st['calmar']:>8.2f}")

    # 逐年報酬一致性
    print("\n=== 逐年報酬(OOS 一致性;看優勢是全期還是靠幾年)===")
    years = sorted({d.year for d in navs["baseline6"]["date"].to_list()})
    hdr = "  年份 " + "".join(f"{n[:10]:>12}" for n in VARIANTS)
    print(hdr)
    for y in years:
        row = f"  {y} "
        for name in VARIANTS:
            nav = navs[name].filter(pl.col("date").dt.year() == y)
            if nav.height >= 2:
                r = nav["nav"][-1] / nav["nav"][0] - 1
                row += f"{r:>+11.0%} "
            else:
                row += f"{'--':>12}"
        print(row)

    # DSR + PBO(以日報酬對齊)
    print("\n=== OOS robustness:DSR(多重比較校正)+ PBO(過擬合機率)===")
    curves = list(navs.values())
    sr_var = sr_variance_from_curves(curves)
    # 對齊日報酬成 T×K 矩陣
    rets = []
    common = None
    for nav in curves:
        s = nav.select(["date", "nav"]).with_columns(
            (pl.col("nav") / pl.col("nav").shift(1) - 1).alias("r")).drop_nulls()
        common = s.select("date") if common is None else common.join(s.select("date"), on="date", how="inner")
    for nav in curves:
        s = (nav.select(["date", "nav"]).with_columns(
            (pl.col("nav") / pl.col("nav").shift(1) - 1).alias("r")).drop_nulls()
            .join(common, on="date", how="semi").sort("date"))
        rets.append(s["r"].to_numpy())
    mat = np.column_stack(rets)  # T×K
    pbo = pbo_cscv(mat, s=16)
    best = max(stats, key=lambda n: stats[n]["sharpe"])
    dsr = deflated_sharpe(navs[best], n_trials=len(VARIANTS), sr_var_across_trials=sr_var)
    print(f"  IS 最佳(Sharpe):{best}")
    print(f"  DSR(最佳變體,校正 {len(VARIANTS)} 試):{dsr.get('dsr', dsr)}")
    print(f"  PBO(CSCV):ω = {pbo.get('pbo', pbo)}(<0.5 = IS 最佳者 OOS 多半仍領先,非過擬合)")
    print("\n  判讀:CAGR 最高者若 PBO 高/逐年靠幾年 = 過擬合;Calmar 高且逐年穩者更可託付。")


if __name__ == "__main__":
    main()
