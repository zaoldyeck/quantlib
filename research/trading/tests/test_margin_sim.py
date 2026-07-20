"""margin_sim 分析解錨測試(money-path 守護:斷頭線/利息/摩擦數學必須對)。

Run: uv run --project research python -m research.trading.tests.test_margin_sim
     或 uv run --project research pytest research/trading/tests/test_margin_sim.py
"""
from __future__ import annotations

from datetime import date as Date, timedelta

import numpy as np
import polars as pl

from research.trading.margin_sim import (BUY_COST, FIN_RATE, FORCED_COST,
                                         SELL_COST, constant_leverage,
                                         vol_target_leverage)


def _nav(vals):
    d0 = Date(2024, 1, 1)
    return pl.DataFrame({"date": [d0 + timedelta(days=i) for i in range(len(vals))],
                         "nav": [float(x) for x in vals]})


def test_L1_is_identity() -> None:
    """L=1:無債無息無摩擦,輸出 = 輸入。"""
    nav = _nav([1.0, 1.05, 0.98, 1.10])
    out, forced = constant_leverage(nav, 1.0)
    assert forced == 0
    assert np.allclose(out["nav"].to_numpy(), nav["nav"].to_numpy(), atol=1e-12)


def test_2x_one_day_analytic() -> None:
    """2x 單日 +10%:eq ≈ (1−1×BUY_COST) × (1 + 2×10% − 日息貢獻)。"""
    nav = _nav([1.0, 1.1])
    out, forced = constant_leverage(nav, 2.0)
    eq0 = 1.0 - 1.0 * BUY_COST
    P1 = 2 * eq0 * 1.1
    D1 = eq0 * (1 + FIN_RATE / 252)
    assert forced == 0
    assert abs(out["nav"][-1] - (P1 - D1)) < 1e-12
    assert 1.19 < out["nav"][-1] < 1.20  # ≈ +20% 減微小摩擦/利息


def test_forced_liquidation_at_maintenance() -> None:
    """全額融資 L=2.5(自備 4 成):斷頭線 = 標的自進場 −22%(P/D<1.3)。
    路徑累計 −10%→−20%(維持率 ≈1.333 貼線未觸)→−30%(必觸,當日以 2.5x
    承受完才斷)→隔日 −10% 只以 1x 承受(斷頭後空手融資)。"""
    nav = _nav([1.0, 0.9, 0.8, 0.7, 0.63])
    out, forced = constant_leverage(nav, 2.5)
    v = out["nav"].to_numpy()
    assert forced == 1
    assert v[-1] > 0  # 斷頭賣的是融資部位,自備部位保留,未歸零
    # 觸線日(第 3 天)本身以全槓桿承受:跌幅遠大於標的 −12.5%
    assert v[3] / v[2] - 1 < -0.40
    # 斷頭後(第 4 天,−10%)只以 1x 承受
    assert abs(v[4] / v[3] - 1 - (0.63 / 0.7 - 1)) < 1e-9


def test_vol_target_cap_and_financing() -> None:
    """平靜市(σ→0):e=cap 滿槓桿;年化財務成本 ≈ (cap−1)×6.5%。"""
    n = 260
    vals = [1.0 * (1.0005 ** i) for i in range(n)]  # 恆定微漲,σ 極小
    out = vol_target_leverage(_nav(vals), sigma_tgt=0.20, lookback=20, cap=2.0)
    base_total = vals[-1] / vals[0] - 1
    lev_total = out["nav"][-1] / out["nav"][0] - 1
    # 2x 報酬 − 1x 融資息(~6.5%/年);寬鬆界:應明顯高於 1x、低於無息 2x
    assert lev_total > base_total * 1.5
    assert lev_total < base_total * 2.0


def main() -> None:
    for fn in (test_L1_is_identity, test_2x_one_day_analytic,
               test_forced_liquidation_at_maintenance,
               test_vol_target_cap_and_financing):
        fn()
        print(f"✓ {fn.__name__}")
    print("✓ margin_sim 分析解錨全過")


if __name__ == "__main__":
    main()
