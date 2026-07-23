"""Goal 最後一塊:「去 accel_rel」候選的出廠閘門判定(配對檢定 + 權重擾動穩健性)。

s_variant_validate 已示:-accelrel 每指標略優(CAGR 82.3→83.9、Calmar 2.40→2.83)、DSR≈1,
但 PBO 0.92 警示「憑回測排名挑變體會過擬合」。本檔補足出廠判定的兩塊:
1. **配對 block-bootstrap**:兩 nav 高度相關,對「日報酬差 d_t = r_variant − r_canonical」做
   block bootstrap(block=21)→ 年化差異 CI。CI 下界 >0 = 優勢統計顯著;跨 0 = 噪音級。
   (對相關曲線,配對檢定的統計力遠高於各自 bootstrap 比較。)
2. **權重擾動 grid**(robustness,出廠閘門項):對 -accelrel 變體的 5 因子權重各 ±20% 擾動
   (逐一,10 組),看 CAGR spread——若 spread 大 = 對權重敏感 = 結構脆弱。

判定:配對 CI 下界 >0 且擾動 spread <15pp → 建議採用(canonical 變更仍須 ledger 預註冊,
使用者裁決);否則 → 定案「modest/噪音級,不足以改 canonical」,goal 該項證偽落地。

Run: uv run --project . python -m quantlib.strat_lab.s_accelrel_gate
依賴 cache:是。
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib.apex import data, strategy_s
from quantlib.apex.metrics import perf_stats
from quantlib.apex.strategy_s import DS, prep_cached, run_s

_BASE = dict(strategy_s.WREL)
_VAR = {k: v for k, v in _BASE.items() if k != "accel_rel"}


def _nav(panel, feat, elig, wrel):
    try:
        strategy_s.WREL = wrel
        return run_s(panel, feat, elig, DS).sort("date")
    finally:
        strategy_s.WREL = _BASE


def paired_boot(nav_a: pl.DataFrame, nav_b: pl.DataFrame, n_boot=4000, block=21, seed=42) -> dict:
    """年化(幾何)日報酬差的 block-bootstrap CI:d_t = r_a − r_b(a=變體, b=canonical)。"""
    j = (nav_a.select(["date", pl.col("nav").alias("na")])
         .join(nav_b.select(["date", pl.col("nav").alias("nb")]), on="date", how="inner")
         .sort("date")
         .with_columns((pl.col("na") / pl.col("na").shift(1) - 1
                        - (pl.col("nb") / pl.col("nb").shift(1) - 1)).alias("d"))
         .drop_nulls())
    d = j["d"].to_numpy()
    rng = np.random.default_rng(seed)
    T = len(d)
    nblk = T // block + 1
    stats = []
    for _ in range(n_boot):
        idx = rng.integers(0, T - block, nblk)
        sample = np.concatenate([d[i:i + block] for i in idx])[:T]
        stats.append(sample.mean() * 252)  # 年化算術日差(小量級下 ≈ 幾何差)
    lo, med, hi = np.percentile(stats, [2.5, 50, 97.5])
    return {"ann_diff": d.mean() * 252, "ci_lo": lo, "med": med, "ci_hi": hi,
            "p_le0": float(np.mean(np.array(stats) <= 0))}


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    nav_c = _nav(panel, feat, elig, _BASE)
    nav_v = _nav(panel, feat, elig, _VAR)

    print("=== 閘門 1:配對 block-bootstrap(-accelrel − canonical 的年化日報酬差)===")
    pb = paired_boot(nav_v, nav_c)
    print(f"  年化差 {pb['ann_diff']:+.1%};95% CI [{pb['ci_lo']:+.1%}, {pb['ci_hi']:+.1%}]"
          f";P(差≤0) = {pb['p_le0']:.3f}")
    sig = pb["ci_lo"] > 0
    print(f"  → {'✓ 優勢統計顯著' if sig else '✗ CI 跨 0 = 優勢屬噪音級'}\n")

    print("=== 閘門 2:權重擾動穩健性(-accelrel 5 因子各 ±20%,CAGR spread)===")
    cagrs = []
    for k in _VAR:
        for mult in (0.8, 1.2):
            w = dict(_VAR)
            w[k] = w[k] * mult
            st = perf_stats(_nav(panel, feat, elig, w))
            cagrs.append(st["cagr"])
            print(f"  {k}×{mult}: CAGR {st['cagr']:+.1%}", flush=True)
    spread = max(cagrs) - min(cagrs)
    print(f"  → spread {spread:.1%}({'✓ <15pp 穩健' if spread < 0.15 else '✗ ≥15pp 對權重敏感'})\n")

    base_v = perf_stats(nav_v)
    print("=== 判定 ===")
    if sig and spread < 0.15:
        print(f"  兩閘皆過:-accelrel(CAGR {base_v['cagr']:+.1%})為統計顯著且穩健的改進;"
              "canonical 變更仍須 ledger 預註冊,使用者裁決。")
    else:
        print("  未全過:『去 accel_rel』優勢不足出廠標準 → 定案 modest/噪音級,"
              "不改 canonical;goal 該項以此證據落地。")


if __name__ == "__main__":
    main()
