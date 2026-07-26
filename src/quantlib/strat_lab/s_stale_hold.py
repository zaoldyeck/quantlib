"""條件式訊號出場:「營收訊號過期,但價格動能仍強 → 續抱」——直攻 S 的右尾捕獲率。

## 為什麼是這一刀(來自 s_attribution 的診斷,非拍腦袋)
- S 的報酬 **94% 來自前 10% 的交易**(前 5% 佔 59%)= 極度右尾驅動;
- S 的出場 **70% 是 signal**(`rev_fresh_days ≥ 26`,即營收資訊過期),**無條件賣出**;
- 但「營收訊號過期」≠「這檔股票走完了」——若價格動能仍強,無條件賣 = **主動砍掉正在跑的右尾**;
- 而診斷也顯示 abs/trail/止盈全域無效的根因正是:它們幾乎不觸發(trail 3 次/time 5 次),
  真正決定 S 命運的是這道 signal 出場。**要動 S,就要動這一刀。**

## 假設
signal 出場加一道「動能仍強則續抱」條件:`stale = (rev_fresh_days ≥ 26) AND (動能弱)`。
動能強弱用 S 自己已驗證的價格因子(high_52w = 收盤/252 日最高;close_pos_20 = 20 日區間位置),
**不引入新資訊**(避免加軸的過擬合風險)——只是把既有因子從「選股用」延伸到「續抱用」。

## 變體
A. high_52w ≥ θ 則續抱,θ ∈ {0.85, 0.90, 0.95}
B. close_pos_20 ≥ θ 則續抱,θ ∈ {0.6, 0.7, 0.8}
C. 兩者皆須滿足(嚴格續抱)
D. 續抱但設上限:延長至多 N 日後仍強制出(N ∈ {20, 40})——防無限續抱
判準(D2):Sortino/Calmar/MDD/bootstrap 下界同時 ≥ canonical + 配對 block-bootstrap CI 不跨 0。

Run: uv run --project . python -m quantlib.strat_lab.s_stale_hold
依賴 cache:是(乾淨世代)。
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.metrics import perf_stats
from quantlib.apex.strategy_s import C, DS, prep_cached, run_s_full
from quantlib.apex.validate import block_bootstrap_cagr

_STALE = 26


def _stale_flags(feat: pl.DataFrame, cond: pl.Expr | None, cap_days: int | None = None) -> pl.DataFrame:
    """訊號出場旗標:rev_fresh_days ≥ 26 且(無續抱條件 or 續抱條件不成立)。
    cap_days:即使動能仍強,rev_fresh_days ≥ 26+cap 也強制出場(防無限續抱)。"""
    base = pl.col("rev_fresh_days") >= _STALE
    if cond is None:
        flag = base
    else:
        flag = base & ~cond          # 動能強(cond 真)則不出場
        if cap_days is not None:
            flag = flag | (pl.col("rev_fresh_days") >= _STALE + cap_days)
    return (feat.filter(flag).select(["date", C])
            .filter(pl.col("date") >= pl.lit(DS).str.to_date()))


def _paired(nav_a: pl.DataFrame, nav_b: pl.DataFrame, n_boot=4000, block=21, seed=42) -> dict:
    j = (nav_a.select(["date", pl.col("nav").alias("na")])
         .join(nav_b.select(["date", pl.col("nav").alias("nb")]), on="date", how="inner").sort("date")
         .with_columns((pl.col("na") / pl.col("na").shift(1)
                        - pl.col("nb") / pl.col("nb").shift(1)).alias("d")).drop_nulls())
    d = j["d"].to_numpy()
    T = len(d)
    rng = np.random.default_rng(seed)
    st = [np.concatenate([d[i:i + block] for i in rng.integers(0, T - block, T // block + 1)])[:T].mean() * 252
          for _ in range(n_boot)]
    lo, hi = np.percentile(st, [2.5, 97.5])
    return {"ann": float(d.mean() * 252), "lo": float(lo), "hi": float(hi),
            "p_le0": float(np.mean(np.array(st) <= 0))}


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)

    variants: dict[str, tuple] = {"canonical(無條件出場)": (None, None)}
    for th in (0.85, 0.90, 0.95):
        variants[f"A high52w≥{th}"] = (pl.col("high_52w") >= th, None)
    for th in (0.6, 0.7, 0.8):
        variants[f"B cpos20≥{th}"] = (pl.col("close_pos_20") >= th, None)
    variants["C 兩者(h≥.90 & c≥.7)"] = ((pl.col("high_52w") >= 0.90) & (pl.col("close_pos_20") >= 0.7), None)
    for cap in (20, 40):
        variants[f"D h≥.90 上限+{cap}日"] = (pl.col("high_52w") >= 0.90, cap)

    print("=== 條件式訊號出場(訊號過期但動能仍強則續抱)===")
    print(f"  {'變體':<24}{'CAGR':>9}{'Sortino':>9}{'Calmar':>8}{'MDD':>8}{'下界':>9}{'交易數':>7}")
    navs: dict[str, pl.DataFrame] = {}
    for name, (cond, cap) in variants.items():
        stale = _stale_flags(feat, cond, cap)
        nav, tr = _run_with_stale(panel, feat, elig, stale)
        nav = nav.sort("date")
        navs[name] = nav
        st = perf_stats(nav)
        boot = block_bootstrap_cagr(nav)
        print(f"  {name:<24}{st['cagr']:>+8.1%}{st['sortino']:>9.2f}{st['calmar']:>8.2f}"
              f"{st['mdd']:>+7.1%}{boot['ci_lo']:>+8.1%}{tr.height:>7}", flush=True)

    base = navs["canonical(無條件出場)"]
    print("\n=== 配對 block-bootstrap(變體 − canonical)===")
    for name, nav in navs.items():
        if name.startswith("canonical"):
            continue
        p = _paired(nav, base)
        sig = "✓顯著" if p["lo"] > 0 else ("✗跨0" if p["hi"] > 0 else "✗顯著劣")
        print(f"  {name:<24} 年化差 {p['ann']:>+7.1%}  CI [{p['lo']:>+6.1%},{p['hi']:>+6.1%}]"
              f"  P(≤0)={p['p_le0']:.3f}  {sig}")
    print("\n  判準:CI 下界 >0 且 D2 四指標同時 ≥ canonical 才算改進。")


def _run_with_stale(panel, feat, elig, stale):
    """用自訂 stale 旗標跑 S(其餘 canonical)——直接呼叫引擎,避免改 run_s_full 預設。"""
    from datetime import date as Date
    from quantlib.apex.assemble import entries_and_flags
    from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
    from quantlib.apex.strategy_s import WREL
    pool = feat.filter(pl.col("rev_fresh_days") <= 7)
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]), on=["date", C], how="semi")
          .drop_nulls(subset=list(WREL))
          .filter(pl.all_horizontal([pl.col(c).is_finite() for c in WREL]))
          .filter(pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").quantile(0.5).over("date")))
    expr = None
    for c_, wt in WREL.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = (df.with_columns(expr.alias("score")).select(["date", C, "score"])
          .filter(pl.col("date") >= pl.lit(DS).str.to_date()))
    entries, _ = entries_and_flags(sc, 5, 10**9)
    res = simulate(panel, entries, exit_flags=stale, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30, loser_time_stop=15),
                   start=Date.fromisoformat(DS))
    nav = (res.nav.select(["date", "nav"]).sort("date")
           .with_columns(pl.col("nav") / pl.col("nav").first()))
    return nav, res.trades


if __name__ == "__main__":
    main()
