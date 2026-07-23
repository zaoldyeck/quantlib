"""apex Phase 3 驗證統計 — 純函式(numpy only,無 scipy 依賴)。

包含:moving-block bootstrap CAGR CI、Deflated Sharpe Ratio(Bailey & López de
Prado 2014)、PBO/CSCV(Bailey et al. 2015)。permutation 與參數擾動因需引擎,
由 experiments/p01_battery 執行。
"""
from __future__ import annotations

import math
from itertools import combinations

import numpy as np
import polars as pl

TRADING_DAYS = 252


def daily_returns(nav: pl.DataFrame) -> np.ndarray:
    v = nav.sort("date")["nav"].to_numpy()
    return v[1:] / v[:-1] - 1.0


def block_bootstrap_cagr(
    nav: pl.DataFrame, n_boot: int = 2000, block: int = 21, seed: int = 42
) -> dict:
    """circular moving-block bootstrap → CAGR 分佈與 95% CI。"""
    r = daily_returns(nav)
    t = len(r)
    rng = np.random.default_rng(seed)
    n_blocks = math.ceil(t / block)
    starts = rng.integers(0, t, size=(n_boot, n_blocks))
    idx = (starts[:, :, None] + np.arange(block)[None, None, :]) % t
    samples = r[idx.reshape(n_boot, -1)[:, :t]]
    growth = np.prod(1.0 + samples, axis=1)
    cagr = growth ** (TRADING_DAYS / t) - 1.0
    return {
        "ci_lo": float(np.percentile(cagr, 2.5)),
        "ci_hi": float(np.percentile(cagr, 97.5)),
        "median": float(np.percentile(cagr, 50)),
        "p_neg": float((cagr <= 0).mean()),
    }


def deflated_sharpe(nav: pl.DataFrame, n_trials: int, sr_var_across_trials: float) -> dict:
    """DSR = P(true SR > 0 | 多重嘗試)。輸入日 NAV、campaign trial 數、
    各 trial 日 Sharpe 的變異數。"""
    r = daily_returns(nav)
    t = len(r)
    mu, sd = r.mean(), r.std(ddof=1)
    sr = mu / sd
    g3 = float(((r - mu) ** 3).mean() / sd**3)
    g4 = float(((r - mu) ** 4).mean() / sd**4)
    e = math.e
    gamma = 0.5772156649015329
    sr0 = math.sqrt(max(sr_var_across_trials, 1e-12)) * (
        (1 - gamma) * _phi_inv(1 - 1.0 / n_trials) + gamma * _phi_inv(1 - 1.0 / (n_trials * e))
    )
    denom = math.sqrt(max(1 - g3 * sr + (g4 - 1) / 4.0 * sr * sr, 1e-12))
    z = (sr - sr0) * math.sqrt(t - 1) / denom
    return {
        "sr_daily": sr, "sr_ann": sr * math.sqrt(TRADING_DAYS), "sr0_daily": sr0,
        "skew": g3, "kurt": g4, "z": z, "dsr": _phi(z), "n_trials": n_trials,
    }


def pbo_cscv(returns: np.ndarray, s: int = 16) -> dict:
    """CSCV PBO。returns: (T days × K configs)。回傳 ω = P(IS 最佳者 OOS 落後中位)。"""
    t, k = returns.shape
    edges = np.linspace(0, t, s + 1, dtype=int)
    blocks = [returns[edges[i]: edges[i + 1]] for i in range(s)]
    stats = np.array([[b[:, j].mean() for j in range(k)] for b in blocks])          # (s, k) mean
    stats2 = np.array([[b[:, j].var(ddof=1) for j in range(k)] for b in blocks])    # (s, k) var
    ns = np.array([len(b) for b in blocks], dtype=float)

    def sharpe_of(sel: tuple[int, ...]) -> np.ndarray:
        w = ns[list(sel)]
        m = np.average(stats[list(sel)], axis=0, weights=w)
        v = np.average(stats2[list(sel)] + stats[list(sel)] ** 2, axis=0, weights=w) - m**2
        return m / np.sqrt(np.maximum(v, 1e-18))

    below = 0
    total = 0
    all_ix = set(range(s))
    for is_sel in combinations(range(s), s // 2):
        oos_sel = tuple(sorted(all_ix - set(is_sel)))
        sr_is = sharpe_of(is_sel)
        sr_oos = sharpe_of(oos_sel)
        best = int(np.argmax(sr_is))
        oos_rank = (sr_oos < sr_oos[best]).sum() / (len(sr_oos) - 1 + 1e-12)
        below += oos_rank < 0.5
        total += 1
    return {"pbo": below / total, "n_combos": total, "n_configs": k}


def sr_variance_from_curves(curves: list[pl.DataFrame]) -> float:
    """各 trial 曲線的日 Sharpe 變異數(DSR 的 V[SR] 輸入)。"""
    srs = []
    for c in curves:
        r = daily_returns(c)
        if len(r) > 50 and r.std() > 0:
            srs.append(r.mean() / r.std(ddof=1))
    return float(np.var(srs, ddof=1)) if len(srs) > 2 else 0.0


# ── 標準常態 CDF 與反函數(Acklam 近似)─────────────────────────────────

def _phi(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


_A = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02,
      1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
_B = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02,
      6.680131188771972e01, -1.328068155288572e01]
_C = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00,
      -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
_D = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00,
      3.754408661907416e00]


def _phi_inv(p: float) -> float:
    if not 0.0 < p < 1.0:
        raise ValueError("p must be in (0,1)")
    pl_, ph = 0.02425, 1 - 0.02425
    if p < pl_:
        q = math.sqrt(-2 * math.log(p))
        return (((((_C[0] * q + _C[1]) * q + _C[2]) * q + _C[3]) * q + _C[4]) * q + _C[5]) / (
            (((_D[0] * q + _D[1]) * q + _D[2]) * q + _D[3]) * q + 1
        )
    if p > ph:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((_C[0] * q + _C[1]) * q + _C[2]) * q + _C[3]) * q + _C[4]) * q + _C[5]) / (
            (((_D[0] * q + _D[1]) * q + _D[2]) * q + _D[3]) * q + 1
        )
    q = p - 0.5
    r = q * q
    return (((((_A[0] * r + _A[1]) * r + _A[2]) * r + _A[3]) * r + _A[4]) * r + _A[5]) * q / (
        ((((_B[0] * r + _B[1]) * r + _B[2]) * r + _B[3]) * r + _B[4]) * r + 1
    )
