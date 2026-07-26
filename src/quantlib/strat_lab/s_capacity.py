"""S 的資金容量曲線:這個策略能裝多少錢?(報酬率之外,決定實際財富的另一半)

## 為什麼這比「再多 2% CAGR」重要
S 的報酬率已證到頂(400+ 變體零通過),但**實際賺到的錢 = 報酬率 × 可投入資金**。
目前 live 是每檔買 1 股的測試模式(NAV ≈ 2.5 萬)。若容量能到千萬級,那比再擠出 2pp 報酬
重要一個數量級;若容量只有百萬級,那就是真正的天花板所在——而**這個維度從來沒被測過**。

## 方法(市場衝擊 square-root law,有學理出處)
Almgren-Chriss / Torre 的平方根衝擊模型:單筆市價單的暫時性衝擊
    impact ≈ k × σ_daily × sqrt(Q / ADV)
其中 Q = 下單金額、ADV = 20 日中位成交值、σ_daily = 個股日波動、k ≈ 0.5~1.0(文獻常用值)。
本檔對每個資金規模,把 S 每筆進出場的 (Q/ADV) 算出來 → 轉成該筆的額外滑價 → 疊加到基準
滑價(0.1%)上重跑,得到「資金規模 → 淨 CAGR」的容量曲線。

**保守假設(全部往壞處estimate)**:
- 用進場當日的 ADV20(不是進場後的),流動性不因買盤放大;
- 買賣兩邊都吃衝擊;k 取 0.7(區間 0.5/1.0 另跑敏感度);
- 不假設拆單/演算法執行改善(實務上分批可降低衝擊 → 本估算是下界)。

## 判讀
曲線平坦處 = 容量充裕;開始明顯下彎處 = 該資金規模已觸及流動性天花板。

Run: uv run --project . python -m quantlib.strat_lab.s_capacity
依賴 cache:是(乾淨世代)。
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec
from quantlib.apex.metrics import perf_stats
from quantlib.apex.strategy_s import C, DS, prep_cached, run_s_full

CAPITALS = (1_000_000, 3_000_000, 10_000_000, 30_000_000, 100_000_000, 300_000_000)
K_IMPACT = 0.7          # 平方根衝擊係數(文獻 0.5~1.0;另跑敏感度)
BASE_SLIP = 0.001       # 現行基準滑價


def _impact_stats(panel: pl.DataFrame, trades: pl.DataFrame, capital: float,
                  n_slots: int = 5, k: float = K_IMPACT) -> dict:
    """每筆交易的 (Q/ADV) 與估計衝擊;回平均/中位/P90 額外滑價(單邊)。"""
    # 個股日波動(20 日)與 ADV20
    p = panel.sort([C, "date"]).with_columns([
        (pl.col("close") / pl.col("close").shift(1).over(C) - 1).alias("ret"),
    ])
    p = p.with_columns([
        pl.col("ret").rolling_std(20).over(C).alias("sig20"),
        pl.col("trade_value").cast(pl.Float64).rolling_median(20).over(C).alias("adv20"),
    ])
    closed = trades.filter(pl.col("exit_reason") != "open")
    j = (closed.select([C, "entry_date"])
         .join(p.select([C, pl.col("date").alias("entry_date"), "sig20", "adv20"]),
               on=[C, "entry_date"], how="left")
         .drop_nulls(subset=["adv20", "sig20"])
         .filter(pl.col("adv20") > 0))
    if j.height == 0:
        return {}
    q = capital / n_slots                      # 單筆下單金額(等權)
    frac = (q / j["adv20"]).to_numpy()         # Q/ADV
    sig = j["sig20"].to_numpy()
    imp = k * sig * np.sqrt(frac)              # 單邊暫時衝擊(比例)
    return {"n": j.height, "frac_med": float(np.median(frac)), "frac_p90": float(np.percentile(frac, 90)),
            "imp_med": float(np.median(imp)), "imp_mean": float(np.mean(imp)),
            "imp_p90": float(np.percentile(imp, 90))}


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    base_nav, base_tr = run_s_full(panel, feat, elig, DS)
    base = perf_stats(base_nav.sort("date"))
    print(f"=== S 資金容量曲線(平方根衝擊模型 k={K_IMPACT};基準 CAGR {base['cagr']:+.1%})===")
    print(f"  {'資金':>12}{'單筆金額':>12}{'Q/ADV 中位':>12}{'Q/ADV P90':>11}"
          f"{'額外滑價中位':>13}{'總滑價':>9}{'CAGR':>9}{'Sortino':>9}{'MDD':>8}")
    for cap in CAPITALS:
        st = _impact_stats(panel, base_tr, cap)
        if not st:
            continue
        # 用「平均衝擊」當該資金規模的等效滑價加成(保守:買賣各吃一次 → ExecSpec.slippage 單邊)
        slip = BASE_SLIP + st["imp_mean"]
        nav, _ = run_s_full(panel, feat, elig, DS,
                            _exec_spec=ExecSpec(slippage=slip),
                            _port_spec=PortSpec(n_slots=5, max_new_per_day=2, capital=float(cap)))
        s = perf_stats(nav.sort("date"))
        print(f"  {cap/1e6:>10.0f}M{cap/5/1e6:>11.1f}M{st['frac_med']:>11.2%}{st['frac_p90']:>10.1%}"
              f"{st['imp_med']:>12.2%}{slip:>8.2%}{s['cagr']:>+8.1%}{s['sortino']:>9.2f}{s['mdd']:>+7.1%}",
              flush=True)

    print("\n=== k 敏感度(資金 30M / 100M)===")
    for k in (0.5, 0.7, 1.0):
        line = f"  k={k}: "
        for cap in (30_000_000, 100_000_000):
            st = _impact_stats(panel, base_tr, cap, k=k)
            slip = BASE_SLIP + st["imp_mean"]
            nav, _ = run_s_full(panel, feat, elig, DS, _exec_spec=ExecSpec(slippage=slip),
                                _port_spec=PortSpec(n_slots=5, max_new_per_day=2, capital=float(cap)))
            line += f"{cap/1e6:.0f}M → CAGR {perf_stats(nav.sort('date'))['cagr']:+.1%}(滑價 {slip:.2%})  "
        print(line, flush=True)

    print("\n  判讀:曲線平坦 = 容量充裕;明顯下彎處 = 流動性天花板。")
    print("  保守性:未計拆單/演算法執行的改善(實務可降衝擊)→ 本估算為下界。")


if __name__ == "__main__":
    main()
