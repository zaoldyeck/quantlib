"""Phase 3.4 edge 直接檢驗:S 的核心 edge「營收加速度相對產業(accel_rel)」有沒有訊號?

依 `docs/strategy_research/first_principles_framework.md` step ②:動手建策略前,**先直接驗 edge
本身**(IC / decile 單調性 / t 值),而非整包接受 S。並依 step ③ 拆建構塊——對照:
- `rev_yoy_accel`:原始營收年增加速度(未相對產業)——測「加速度」這個訊號本身。
- `accel_rel`:加速度**相對產業中位數**——測「相對產業」這個建構塊有沒有加分(S 用的)。
- `rev_seq`:近 3 月環比營收成長——另一營收動能建構塊。

thesis(edge 為什麼存在):某公司營收加速度贏過同業 = 基本面相對驚喜/景氣領先,市場對月營收
反應有延遲 → 加速度領先者未來報酬較高。第一性原理要問:這個延遲/驚喜真的有截面預測力嗎?

reuse:`strategy_s.prep`(因子組裝,乾淨資料)+ `apex.factors`(forward_returns + evaluate_factor
IC harness)。零重寫。

Run: uv run --project . python -m quantlib.strat_lab.edge_accel_rel
依賴 cache:是(乾淨世代)。
"""
from __future__ import annotations

from quantlib.apex import data, factors
from quantlib.apex.strategy_s import C, prep


def main() -> None:
    con = data.connect()
    print("[edge] 組裝 S 特徵(乾淨資料)…", flush=True)
    panel, feat, elig = prep(con)
    fwd = factors.forward_returns(panel)

    print("\n=== edge 直接檢驗:各營收建構塊的截面 IC / decile(20d 前瞻)===")
    print("  (IC>0 且 t>2 = 有截面訊號;decile spread 年化 = 多空報酬差;mono→1 = 單調)\n")
    cands = [
        ("rev_yoy_accel", "營收年增加速度(原始,未相對產業)"),
        ("accel_rel", "加速度相對產業中位數(S 用的 edge)"),
        ("rev_seq", "近 3 月環比營收成長"),
    ]
    for col, desc in cands:
        if col not in feat.columns:
            print(f"  · {col}: 特徵不存在,跳過")
            continue
        fac = feat.select(["date", C, col]).rename({col: "value"})
        r = factors.evaluate_factor(col, fac, fwd, elig, family="revenue",
                                    batch="phase3.4-edge", log=False)
        print(f"  {desc}")
        print(f"     {factors.fmt_factor(r)}\n")
    print("判讀:accel_rel vs rev_yoy_accel 的 IC/spread 差 = 「相對產業」這個建構塊的邊際貢獻;"
          "若無差,S 可簡化掉產業相對(奧坎剃刀)。")


if __name__ == "__main__":
    main()
