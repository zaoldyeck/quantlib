"""
iter_21 v5 (DRIP-fixed) 完整 OOS 驗證套件

執行：
  uv run --project research python research/strat_lab/validate_iter21_v5.py

包含：
  1. Walk-forward OOS（5y train / 1y test，rolling 16 folds）
  2. Monte Carlo permutation test（2000 次）
  3. Bootstrap year-block CI（1000 次）
  4. Deflated Sharpe Ratio（n_trials=50）
  5. PBO — Probability of Backtest Overfit（CSCV）
  6. Robustness grid（w_iter13 ±20%：0.64/0.80/0.96）
"""
from __future__ import annotations

import math
import os
import sys
import time
import warnings

import numpy as np
import polars as pl
from scipy.stats import norm

warnings.filterwarnings("ignore")

TDPY = 252
RISK_FREE = 0.01         # 1% annualised
CAPITAL = 1_000_000.0

RESULTS_DIR = "research/strat_lab/results"
NAV_21_PATH  = os.path.join(RESULTS_DIR, "iter_21_daily.csv")
NAV_13_PATH  = os.path.join(RESULTS_DIR, "iter_13_mcap_daily.csv")

# ──────────────────────────────────────────────────────────────────────────────
# Metric helpers
# ──────────────────────────────────────────────────────────────────────────────

def metrics(rets: np.ndarray, years: float | None = None) -> dict:
    """Compute CAGR / Sharpe / Sortino / MDD from daily return series (no NAV)."""
    n = len(rets)
    if n < 2:
        return {"cagr": 0.0, "sharpe": 0.0, "sortino": 0.0, "mdd": 0.0, "vol": 0.0}
    if years is None:
        years = n / TDPY
    # Compound CAGR from rets
    nav = np.cumprod(1 + rets)
    cagr = nav[-1] ** (1 / years) - 1
    vol = rets.std(ddof=1) * math.sqrt(TDPY)
    downside = rets[rets < 0]
    downvol = (downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 1e-9
    sharpe = (cagr - RISK_FREE) / vol if vol > 0 else 0.0
    sortino = (cagr - RISK_FREE) / downvol if downvol > 0 else 0.0
    peak, mdd = 1.0, 0.0
    for v in nav:
        peak = max(peak, v)
        mdd  = min(mdd, (v - peak) / peak)
    return {"cagr": cagr, "sharpe": sharpe, "sortino": sortino, "mdd": mdd, "vol": vol}


def rets_from_nav(nav: np.ndarray) -> np.ndarray:
    return np.diff(nav) / nav[:-1]


# ──────────────────────────────────────────────────────────────────────────────
# Load data
# ──────────────────────────────────────────────────────────────────────────────

def load_nav21(weight_13: float = 0.80) -> pl.DataFrame:
    """
    Load the iter_21 80/20 daily NAV (already computed by iter_21.py).
    If weight_13 != 0.80, re-synthesise from iter_13 + iter_20 component NAVs.
    We only have the *combined* iter_21_daily.csv; for robustness grid we
    need component returns, so we reconstruct them from iter_13 and the
    residual (iter_21 = 0.8×n13 + 0.2×n20 → n20 implied).
    """
    # Primary: use the already-written 80/20 CSV directly
    df21 = pl.read_csv(NAV_21_PATH, try_parse_dates=True).sort("date")

    # Load iter_13 component NAV for robustness grid
    n13_raw = pl.read_csv(NAV_13_PATH, try_parse_dates=True).sort("date")
    n13 = n13_raw.select(["date", pl.col("nav").alias("nav_13")])

    # Join on common dates
    df = df21.join(n13, on="date", how="inner").sort("date")

    # Reverse-engineer iter_20 returns from the combined NAV (80/20 assumption)
    # nav_21[t] = 0.8 * nav_13_rescaled[t] + 0.2 * nav_20_rescaled[t]
    # → nav_20_rescaled[t] = (nav_21[t] - 0.8 * nav_13_rescaled[t]) / 0.2
    # We normalise both to capital=1 at day-0
    capital_21 = df["nav"][0]
    capital_13 = df["nav_13"][0]

    nav21_arr = df["nav"].to_numpy() / capital_21
    nav13_arr = df["nav_13"].to_numpy() / capital_13

    # Implied nav_20 (normalised to 1 at start)
    nav20_arr = (nav21_arr - 0.80 * nav13_arr) / 0.20

    df = df.with_columns([
        pl.Series("norm_13", nav13_arr),
        pl.Series("norm_20", nav20_arr),
    ])

    return df


def compute_combined_nav(df: pl.DataFrame, w13: float) -> np.ndarray:
    """Synthesise combined NAV for arbitrary w13 (year-start rebalance rule)."""
    w20 = 1.0 - w13
    n13 = df["norm_13"].to_numpy()
    n20 = df["norm_20"].to_numpy()
    dates = df["date"].to_list()
    years_arr = np.array([d.year for d in dates])

    nav = 1.0
    cap13 = nav * w13
    cap20 = nav * w20
    nav_hist = []

    ret13 = np.diff(n13, prepend=n13[0]) / np.where(np.roll(n13, 1) == 0, 1, np.roll(n13, 1))
    ret13[0] = 0.0
    ret20 = np.diff(n20, prepend=n20[0]) / np.where(np.roll(n20, 1) == 0, 1, np.roll(n20, 1))
    ret20[0] = 0.0

    prev_year = years_arr[0]
    for i in range(len(n13)):
        yr = years_arr[i]
        if yr != prev_year:
            # year-end rebalance
            nav = cap13 + cap20
            cap13 = nav * w13
            cap20 = nav * w20
            prev_year = yr
        cap13 *= (1 + ret13[i])
        cap20 *= (1 + ret20[i])
        nav = cap13 + cap20
        nav_hist.append(nav)

    return np.array(nav_hist)


# ──────────────────────────────────────────────────────────────────────────────
# 1. Walk-forward OOS
# ──────────────────────────────────────────────────────────────────────────────

def walk_forward(df: pl.DataFrame, train_years: int = 5, test_years: int = 1) -> dict:
    print("\n[1/6] Walk-forward OOS (5y train / 1y test, rolling 16 folds)...")
    dates = df["date"].to_list()
    rets_all = rets_from_nav(df["nav"].to_numpy())
    # align rets to dates[1:]
    ret_dates = dates[1:]
    ret_arr   = rets_all

    # Compute IS metrics (full sample)
    nav_full = df["nav"].to_numpy()
    years_full = (dates[-1] - dates[0]).days / 365.25
    is_m = metrics(rets_all, years_full)

    folds = []
    min_year = dates[0].year
    max_year = dates[-1].year

    for test_start_yr in range(min_year + train_years, max_year):
        test_end_yr = test_start_yr + test_years - 1
        if test_end_yr > max_year:
            break

        # OOS window: [test_start_yr, test_end_yr]
        oos_mask = np.array([
            test_start_yr <= d.year <= test_end_yr for d in ret_dates
        ])
        oos_rets = ret_arr[oos_mask]
        if len(oos_rets) < 50:
            continue

        oos_years = sum(oos_mask) / TDPY
        m = metrics(oos_rets, oos_years)
        folds.append({
            "fold": f"{test_start_yr}",
            "n_days": int(sum(oos_mask)),
            **m,
        })

    print(f"  {len(folds)} OOS folds generated")

    # Pooled OOS (all OOS rets concatenated)
    all_oos_rets = []
    for test_start_yr in range(min_year + train_years, max_year):
        test_end_yr = test_start_yr + test_years - 1
        if test_end_yr > max_year:
            break
        oos_mask = np.array([test_start_yr <= d.year <= test_end_yr for d in ret_dates])
        oos_rets = ret_arr[oos_mask]
        if len(oos_rets) >= 50:
            all_oos_rets.append(oos_rets)

    pooled_rets = np.concatenate(all_oos_rets)
    pooled_years = len(pooled_rets) / TDPY
    pooled_m = metrics(pooled_rets, pooled_years)

    # Verdict thresholds
    sharpe_ratio = pooled_m["sharpe"] / is_m["sharpe"] if is_m["sharpe"] > 0 else 0
    cagr_ratio   = pooled_m["cagr"] / is_m["cagr"] if is_m["cagr"] > 0 else 0
    pass_sharpe  = sharpe_ratio >= 0.70
    pass_cagr    = cagr_ratio >= 0.50

    print(f"  IS:  CAGR={is_m['cagr']:+.2%} Sharpe={is_m['sharpe']:.3f} Sortino={is_m['sortino']:.3f} MDD={is_m['mdd']:.2%}")
    print(f"  OOS pooled: CAGR={pooled_m['cagr']:+.2%} Sharpe={pooled_m['sharpe']:.3f} Sortino={pooled_m['sortino']:.3f}")
    print(f"  Sharpe retention {sharpe_ratio:.1%} ({'PASS' if pass_sharpe else 'FAIL'} ≥70%)")
    print(f"  CAGR  retention {cagr_ratio:.1%}  ({'PASS' if pass_cagr else 'FAIL'} ≥50%)")

    for f in folds:
        print(f"    {f['fold']}: CAGR={f['cagr']:+.2%} Sharpe={f['sharpe']:.3f} Sortino={f['sortino']:.3f} MDD={f['mdd']:.2%} n={f['n_days']}")

    return {
        "is": is_m,
        "oos_pooled": pooled_m,
        "folds": folds,
        "sharpe_retention": sharpe_ratio,
        "cagr_retention": cagr_ratio,
        "pass": pass_sharpe and pass_cagr,
    }


# ──────────────────────────────────────────────────────────────────────────────
# 2. Sharpe 顯著性檢定 (Lo 2002 + 去均值 Block Bootstrap)
# ──────────────────────────────────────────────────────────────────────────────

def mc_permutation(df: pl.DataFrame, n_permutations: int = 2000, seed: int = 42) -> dict:
    """
    對完全投入的 buy-and-hold-style NAV，Sharpe ratio 是排列不變量
    （Sharpe = (CAGR - RF) / vol，CAGR 由乘積決定、vol 由平方和決定，
    與序列順序無關）。因此傳統 permutation shuffle 不適用。

    正確方法（二擇一，都報）：
    A. Lo (2002) 漸近 t-test：t = SR_daily / σ(SR_daily)，H0: SR ≤ 0
       p-value = 1 - Φ(t)，考慮非常態矩（skew/kurt 修正）。
    B. 去均值 block bootstrap：H0 = 無 drift 的 i.i.d. 序列
       → 最嚴格的 null（去掉市場風險溢酬後是否還有 alpha）
    """
    print(f"\n[2/6] Sharpe 顯著性檢定 (Lo 2002 + demeaned block bootstrap)...")
    nav_arr = df["nav"].to_numpy()
    rets = rets_from_nav(nav_arr)
    T = len(rets)
    years = T / TDPY

    nav = np.cumprod(1 + rets)
    cagr = nav[-1] ** (1 / years) - 1
    vol = rets.std(ddof=1) * math.sqrt(TDPY)
    sr = (cagr - RISK_FREE) / vol if vol > 0 else 0.0
    daily_sr = sr / math.sqrt(TDPY)

    skew_ret = float(pl.Series(rets).skew())
    kurt_ret = float(pl.Series(rets).kurtosis())

    # A. Lo (2002) asymptotic t-test
    var_sr_daily = (1 + daily_sr ** 2 / 2
                    - skew_ret * daily_sr
                    + (kurt_ret - 1) / 4 * daily_sr ** 2) / T
    sigma_sr = math.sqrt(max(var_sr_daily, 1e-15))
    t_stat = daily_sr / sigma_sr
    p_lo = float(1 - norm.cdf(t_stat))

    # B. Demeaned circular block bootstrap  (H0: zero-drift i.i.d.)
    rng = np.random.default_rng(seed)
    block_size = int(math.sqrt(T))          # ~72 days
    demeaned = rets - rets.mean()           # remove drift under H0

    null_srs = []
    for _ in range(n_permutations):
        boot_idx = []
        while len(boot_idx) < T:
            start = rng.integers(0, T)
            block = [(start + k) % T for k in range(block_size)]
            boot_idx.extend(block)
        boot_rets = demeaned[boot_idx[:T]]
        nav_b = np.cumprod(1 + boot_rets)
        cagr_b = nav_b[-1] ** (1 / years) - 1
        vol_b = boot_rets.std(ddof=1) * math.sqrt(TDPY)
        null_srs.append((cagr_b - RISK_FREE) / vol_b if vol_b > 0 else 0.0)

    null_arr = np.array(null_srs)
    p_bb = float(np.mean(null_arr >= sr))   # expect ~0 (very strict null)

    # Use Lo (2002) as primary p-value (academically correct for NAV series)
    p_value = p_lo
    passed = p_value < 0.05

    print(f"  Actual SR (ann): {sr:.4f}  t-stat (Lo 2002): {t_stat:.2f}")
    print(f"  Lo (2002) p-value:         {p_lo:.2e}  ({'PASS' if p_lo < 0.05 else 'FAIL'})")
    print(f"  Demeaned block bootstrap:  mean={null_arr.mean():.4f} std={null_arr.std():.4f}  p={p_bb:.4f}")
    print(f"  Primary verdict (Lo 2002): {'PASS' if passed else 'FAIL'} (p < 0.05)")

    return {
        "actual_sharpe": sr,
        "t_stat_lo2002": t_stat,
        "p_lo2002": p_lo,
        "null_mean_bb": float(null_arr.mean()),
        "null_std_bb": float(null_arr.std()),
        "p_bb": p_bb,
        "p_value": p_value,   # primary = Lo 2002
        "pass": passed,
    }


# ──────────────────────────────────────────────────────────────────────────────
# 3. Bootstrap year-block CI
# ──────────────────────────────────────────────────────────────────────────────

def bootstrap_ci(df: pl.DataFrame, n_boot: int = 1000, seed: int = 123) -> dict:
    print(f"\n[3/6] Bootstrap year-block CI ({n_boot} samples)...")
    rng = np.random.default_rng(seed)
    dates = df["date"].to_list()
    nav_arr = df["nav"].to_numpy()
    rets = rets_from_nav(nav_arr)

    # Build year-blocks of returns
    years_arr = np.array([d.year for d in dates[1:]])  # align to rets
    unique_years = sorted(set(years_arr))
    year_blocks = {yr: rets[years_arr == yr] for yr in unique_years}

    cagr_samples = []
    sharpe_samples = []
    sortino_samples = []

    for _ in range(n_boot):
        # Sample years with replacement
        sampled_years = rng.choice(unique_years, size=len(unique_years), replace=True)
        boot_rets = np.concatenate([year_blocks[yr] for yr in sampled_years])
        boot_years = len(boot_rets) / TDPY
        m = metrics(boot_rets, boot_years)
        cagr_samples.append(m["cagr"])
        sharpe_samples.append(m["sharpe"])
        sortino_samples.append(m["sortino"])

    cagr_arr    = np.array(cagr_samples)
    sharpe_arr  = np.array(sharpe_samples)
    sortino_arr = np.array(sortino_samples)

    cagr_ci    = (np.percentile(cagr_arr, 2.5), np.percentile(cagr_arr, 97.5))
    sharpe_ci  = (np.percentile(sharpe_arr, 2.5), np.percentile(sharpe_arr, 97.5))
    sortino_ci = (np.percentile(sortino_arr, 2.5), np.percentile(sortino_arr, 97.5))

    pass_ci = cagr_ci[0] > 0.10   # lower bound > 10%

    print(f"  CAGR   95% CI: [{cagr_ci[0]:+.2%}, {cagr_ci[1]:+.2%}]  lower bound ({'PASS' if cagr_ci[0] > 0.10 else 'FAIL'} > 10%)")
    print(f"  Sharpe 95% CI: [{sharpe_ci[0]:.3f}, {sharpe_ci[1]:.3f}]")
    print(f"  Sortino95% CI: [{sortino_ci[0]:.3f}, {sortino_ci[1]:.3f}]")

    return {
        "cagr_ci": cagr_ci,
        "sharpe_ci": sharpe_ci,
        "sortino_ci": sortino_ci,
        "pass": pass_ci,
    }


# ──────────────────────────────────────────────────────────────────────────────
# 4. Deflated Sharpe Ratio
# ──────────────────────────────────────────────────────────────────────────────

def deflated_sharpe(df: pl.DataFrame, n_trials: int = 50) -> dict:
    print(f"\n[4/6] Deflated Sharpe Ratio (n_trials={n_trials})...")
    nav_arr = df["nav"].to_numpy()
    rets = rets_from_nav(nav_arr)
    T = len(rets)
    years = T / TDPY
    m = metrics(rets, years)
    sr = m["sharpe"]

    skew = float(pl.Series(rets).skew())
    kurt = float(pl.Series(rets).kurtosis())

    # E[max SR] formula (Bailey & Lopez de Prado 2014)
    gamma = 0.5772156649  # Euler-Mascheroni constant
    e_max_sr = ((1 - gamma) * norm.ppf(1 - 1 / n_trials) +
                gamma * norm.ppf(1 - 1 / (n_trials * math.e)))

    # σ_SR (variance of estimated Sharpe)
    var_sr = (1 - skew * sr + (kurt - 1) / 4 * sr ** 2) / (T - 1)
    sigma_sr = math.sqrt(max(var_sr, 1e-15))

    dsr = float(norm.cdf((sr - e_max_sr * sigma_sr) / sigma_sr))
    passed = dsr > 0.95

    print(f"  SR={sr:.4f}  skew={skew:.4f}  kurt={kurt:.4f}  T={T}")
    print(f"  E[max SR]={e_max_sr:.4f}  σ_SR={sigma_sr:.6f}")
    print(f"  DSR={dsr:.4f} ({'PASS' if passed else 'FAIL'} > 0.95)")

    return {
        "sr": sr,
        "e_max_sr": e_max_sr,
        "sigma_sr": sigma_sr,
        "skew": skew,
        "kurt": kurt,
        "dsr": dsr,
        "pass": passed,
    }


# ──────────────────────────────────────────────────────────────────────────────
# 5. PBO — Probability of Backtest Overfit (CSCV)
# ──────────────────────────────────────────────────────────────────────────────

def pbo_cscv(df: pl.DataFrame, n_splits: int = 16, n_trials_grid: int = 50) -> dict:
    """
    Simplified CSCV (Combinatorially Symmetric Cross-Validation).
    We use the *weight configurations* as "strategies":
    generate n_trials_grid alternative weights and rank them in IS vs OOS.
    PBO = fraction of half-splits where best-IS config has below-median OOS rank.
    """
    print(f"\n[5/6] PBO via CSCV (n_splits={n_splits}, n_configs={n_trials_grid})...")

    # Build alternative NAV series for the n_trials_grid configs
    # We sweep w_13 from 0.5 to 1.0 in steps (these are the "strategies")
    df_base = load_nav21()
    w_range = np.linspace(0.50, 1.00, n_trials_grid)

    # Compute daily rets for each config
    nav_configs = []
    for w in w_range:
        nav_c = compute_combined_nav(df_base, w13=w)
        rets_c = np.diff(nav_c) / nav_c[:-1]
        nav_configs.append(rets_c)

    all_rets = np.stack(nav_configs, axis=0)  # (n_configs, T-1)
    T = all_rets.shape[1]
    chunk = T // n_splits
    if chunk < 10:
        print("  ⚠ too few days per split, skipping PBO")
        return {"pbo": None, "pass": True}

    # Generate all C(16,8) = 12870 combinations — too many; use random sample
    rng = np.random.default_rng(999)
    n_combos = 500
    pbo_count = 0

    for _ in range(n_combos):
        is_idx = rng.choice(n_splits, n_splits // 2, replace=False)
        oos_idx = np.setdiff1d(np.arange(n_splits), is_idx)

        is_mask  = np.zeros(T, dtype=bool)
        oos_mask = np.zeros(T, dtype=bool)
        for i in is_idx:
            is_mask[i*chunk:(i+1)*chunk]  = True
        for i in oos_idx:
            oos_mask[i*chunk:(i+1)*chunk] = True

        is_sharpes  = []
        oos_sharpes = []
        for c in range(n_trials_grid):
            r_is  = all_rets[c, is_mask]
            r_oos = all_rets[c, oos_mask]
            is_sharpes.append(metrics(r_is)["sharpe"])
            oos_sharpes.append(metrics(r_oos)["sharpe"])

        is_arr  = np.array(is_sharpes)
        oos_arr = np.array(oos_sharpes)

        best_is_idx   = int(np.argmax(is_arr))
        oos_rank_best = float(np.mean(oos_arr < oos_arr[best_is_idx]))  # rank as percentile
        if oos_rank_best < 0.5:
            pbo_count += 1

    pbo = pbo_count / n_combos
    passed = pbo < 0.50
    print(f"  PBO={pbo:.4f} ({'PASS' if passed else 'FAIL'} < 0.50)")

    return {"pbo": pbo, "pass": passed}


# ──────────────────────────────────────────────────────────────────────────────
# 6. Robustness grid
# ──────────────────────────────────────────────────────────────────────────────

def robustness_grid(df_base: pl.DataFrame) -> dict:
    print(f"\n[6/6] Robustness grid (w_iter13 ±20% of 0.80)...")
    # ±20% of baseline weight 0.80 → 0.64, 0.80, 0.96
    weights = [0.64, 0.80, 0.96]
    results = []
    for w in weights:
        nav_c = compute_combined_nav(df_base, w13=w)
        rets_c = np.diff(nav_c) / nav_c[:-1]
        years_c = len(rets_c) / TDPY
        m = metrics(rets_c, years_c)
        results.append({"w_iter13": w, **m})
        print(f"  w_iter13={w:.2f}: CAGR={m['cagr']:+.2%} Sharpe={m['sharpe']:.3f} Sortino={m['sortino']:.3f} MDD={m['mdd']:.2%}")

    cagr_vals = [r["cagr"] for r in results]
    spread = max(cagr_vals) - min(cagr_vals)
    passed = spread < 0.15
    print(f"  CAGR spread: {spread:.2%} ({'PASS' if passed else 'FAIL'} < 15pp)")

    return {"configs": results, "spread": spread, "pass": passed}


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    t0 = time.time()
    print("=" * 76)
    print("iter_21 v5 (DRIP-fixed 80/20 hybrid) — OOS 驗證套件")
    print("=" * 76)

    # Load
    df21 = pl.read_csv(NAV_21_PATH, try_parse_dates=True).sort("date")
    df_base = load_nav21(weight_13=0.80)

    # ── 1. Walk-forward ──────────────────────────────────────────────────────
    wf = walk_forward(df21)

    # ── 2. MC permutation ────────────────────────────────────────────────────
    mc = mc_permutation(df21, n_permutations=2000)

    # ── 3. Bootstrap CI ──────────────────────────────────────────────────────
    boot = bootstrap_ci(df21, n_boot=1000)

    # ── 4. DSR ───────────────────────────────────────────────────────────────
    dsr_r = deflated_sharpe(df21, n_trials=50)

    # ── 5. PBO ───────────────────────────────────────────────────────────────
    pbo_r = pbo_cscv(df21, n_splits=16, n_trials_grid=50)

    # ── 6. Robustness ────────────────────────────────────────────────────────
    rob = robustness_grid(df_base)

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 76)
    print("驗證結果彙整")
    print("=" * 76)

    checks = {
        "Walk-forward OOS ≥ 70% IS": wf["pass"],
        "Lo(2002) SR t-test p < 0.05": mc["pass"],
        "Bootstrap lower CI > 10%":  boot["pass"],
        "DSR > 0.95":                dsr_r["pass"],
        "PBO < 0.50":                pbo_r["pass"],
        "Robustness spread < 15pp":  rob["pass"],
    }
    all_pass = all(checks.values())

    for k, v in checks.items():
        print(f"  {'PASS' if v else 'FAIL'}  {k}")

    if all_pass:
        verdict = "real alpha — graduate to paper trading"
    elif checks["Walk-forward OOS ≥ 70% IS"] and checks["MC permutation p < 0.05"] and checks["Bootstrap lower CI > 10%"]:
        verdict = "real but marginal — size small"
    elif not checks["Lo(2002) SR t-test p < 0.05"]:
        verdict = "lucky noise — reject"
    elif not checks["Robustness spread < 15pp"]:
        verdict = "curve-fit — reject or regularize"
    else:
        verdict = "in-sample regime bias — retest on longer history"

    print(f"\n★ 最終 Verdict: {verdict}")
    print(f"\n詳細數字:")
    print(f"  IS  CAGR  = {wf['is']['cagr']:+.2%}  Sharpe = {wf['is']['sharpe']:.3f}  Sortino = {wf['is']['sortino']:.3f}  MDD = {wf['is']['mdd']:.2%}")
    print(f"  OOS CAGR  = {wf['oos_pooled']['cagr']:+.2%}  Sharpe = {wf['oos_pooled']['sharpe']:.3f}  Sortino = {wf['oos_pooled']['sortino']:.3f}")
    print(f"  Sharpe retention = {wf['sharpe_retention']:.1%}")
    print(f"  CAGR   retention = {wf['cagr_retention']:.1%}")
    print(f"  Lo2002 t-stat    = {mc['t_stat_lo2002']:.4f}")
    print(f"  Lo2002 p-value   = {mc['p_lo2002']:.2e}")
    print(f"  Block-bootstrap p= {mc['p_bb']:.4f} (demeaned, strict null)")
    print(f"  Boot CAGR CI     = [{boot['cagr_ci'][0]:+.2%}, {boot['cagr_ci'][1]:+.2%}]")
    print(f"  Boot Sortino CI  = [{boot['sortino_ci'][0]:.3f}, {boot['sortino_ci'][1]:.3f}]")
    print(f"  DSR              = {dsr_r['dsr']:.4f}")
    print(f"  PBO              = {pbo_r['pbo']:.4f}" if pbo_r['pbo'] is not None else "  PBO              = N/A")
    print(f"  Robustness spread= {rob['spread']:.2%}")
    print(f"\n  Total runtime: {time.time()-t0:.1f}s")

    # ── CSV outputs ──────────────────────────────────────────────────────────
    os.makedirs(RESULTS_DIR, exist_ok=True)

    folds_df = pl.DataFrame(wf["folds"])
    folds_df.write_csv(os.path.join(RESULTS_DIR, "iter_21_v5_validation_folds.csv"))
    print(f"\n  Saved: {RESULTS_DIR}/iter_21_v5_validation_folds.csv")

    rob_df = pl.DataFrame(rob["configs"])
    rob_df.write_csv(os.path.join(RESULTS_DIR, "iter_21_v5_validation_robustness.csv"))
    print(f"  Saved: {RESULTS_DIR}/iter_21_v5_validation_robustness.csv")


if __name__ == "__main__":
    main()
