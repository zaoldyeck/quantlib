"""動能衰竭提早出場:訊號還新鮮但價格已轉弱 → 提早釋放 slot,讓位給新候選。

## 為什麼是這一刀(承接兩個實測結果)
- `s_attribution` 診斷:**滿倉擋掉 16.8% 候選、節流擋掉 15.0%**(真漏損 32%);且未進場候選的
  後續報酬(fwd21 +1.50%)**不輸**有進場者(+0.97%)→ 格子的機會成本是真的。
- `s_stale_hold` 證偽「延後賣」(續抱全線劣化,配對顯著)→ 那反方向呢?**提早賣**能加快週轉、
  把格子讓給更新鮮的訊號。

## 與既有出場的區別(不是重測)
- `trail`(0.35)= 相對**持有期峰值**回撤;`loser_time`(15)= **水下**時的時間止損;
- 本檔測的是**絕對動能位置衰竭**:high_52w(收盤/252 日最高)或 close_pos_20 跌破門檻就出場,
  **不論賺賠、不論是否回撤自峰值**——語義完全不同(可能還在獲利但動能已死)。
- 也測**相對排名衰竭**:該股 high_52w 在當日全市場的百分位跌出 θ。

## 變體
A. high_52w < θ 出場,θ ∈ {0.75, 0.80, 0.85, 0.88}
B. close_pos_20 < θ 出場,θ ∈ {0.3, 0.4, 0.5}
C. high_52w 的當日全市場百分位 < θ 出場,θ ∈ {0.5, 0.7}
判準(D2)+ 配對 block-bootstrap;與 canonical 對照。

Run: uv run --project . python -m quantlib.strat_lab.s_momentum_exit
依賴 cache:是(乾淨世代)。
"""
from __future__ import annotations

import polars as pl

from quantlib.apex import data
from quantlib.apex.metrics import perf_stats
from quantlib.apex.strategy_s import C, DS, prep_cached
from quantlib.apex.validate import block_bootstrap_cagr
from quantlib.strat_lab.s_stale_hold import _paired, _run_with_stale

_STALE = 26


def _flags(feat: pl.DataFrame, extra: pl.Expr | None) -> pl.DataFrame:
    """canonical 訊號出場 OR 額外的動能衰竭條件。"""
    cond = pl.col("rev_fresh_days") >= _STALE
    if extra is not None:
        cond = cond | extra
    return (feat.filter(cond).select(["date", C])
            .filter(pl.col("date") >= pl.lit(DS).str.to_date()))


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    # 當日全市場 high_52w 百分位(相對排名衰竭用)
    feat = feat.with_columns(
        (pl.col("high_52w").rank() / pl.len()).over("date").alias("h52_pct"))

    variants: dict[str, pl.Expr | None] = {"canonical": None}
    for th in (0.75, 0.80, 0.85, 0.88):
        variants[f"A high52w<{th}"] = pl.col("high_52w") < th
    for th in (0.3, 0.4, 0.5):
        variants[f"B cpos20<{th}"] = pl.col("close_pos_20") < th
    for th in (0.5, 0.7):
        variants[f"C h52 百分位<{th}"] = pl.col("h52_pct") < th

    print("=== 動能衰竭提早出場(訊號仍新鮮但價格轉弱即釋放格子)===")
    print(f"  {'變體':<20}{'CAGR':>9}{'Sortino':>9}{'Calmar':>8}{'MDD':>8}{'下界':>9}{'交易數':>7}")
    navs = {}
    for name, extra in variants.items():
        nav, tr = _run_with_stale(panel, feat, elig, _flags(feat, extra))
        nav = nav.sort("date")
        navs[name] = nav
        st = perf_stats(nav)
        boot = block_bootstrap_cagr(nav)
        print(f"  {name:<20}{st['cagr']:>+8.1%}{st['sortino']:>9.2f}{st['calmar']:>8.2f}"
              f"{st['mdd']:>+7.1%}{boot['ci_lo']:>+8.1%}{tr.height:>7}", flush=True)

    print("\n=== 配對 block-bootstrap(變體 − canonical)===")
    base = navs["canonical"]
    for name, nav in navs.items():
        if name == "canonical":
            continue
        p = _paired(nav, base)
        sig = "✓顯著改進" if p["lo"] > 0 else ("✗跨0" if p["hi"] > 0 else "✗顯著劣")
        print(f"  {name:<20} 年化差 {p['ann']:>+7.1%}  CI [{p['lo']:>+6.1%},{p['hi']:>+6.1%}]"
              f"  P(≤0)={p['p_le0']:.3f}  {sig}")


if __name__ == "__main__":
    main()
