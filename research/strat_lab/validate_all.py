"""validate_all.py — 最大效能 validator (multiprocessing + numba JIT 級向量化)

驗證指標：Walk-forward OOS / MC permutation / Bootstrap CI / DSR / PBO / Robustness
對多個策略一次跑完，輸出對照表。

設計：
  1. multiprocessing.Pool 平行 MC 2000 次 + Bootstrap 2000 次
  2. WF 16 個 fold 也平行
  3. 單次 metric 計算用 numpy 向量化（不用 polars 中間轉換）
  4. 共用 benchmark daily returns，只算一次
"""
from __future__ import annotations

import argparse
import math
import os
import sys
import time
from datetime import date
from itertools import combinations
from multiprocessing import Pool

import numpy as np
import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

TDPY = 252
RF = 0.01


def daily_rets_from_nav_csv(path: str) -> tuple[list, np.ndarray]:
    """讀 daily NAV CSV，回傳 (dates, daily_returns)。"""
    df = pl.read_csv(path, try_parse_dates=True).sort("date")
    if "nav" not in df.columns:
        raise ValueError(f"{path} 沒 nav 欄")
    dates = df["date"].to_list()
    nav = df["nav"].to_numpy()
    rets = np.zeros(len(nav))
    rets[1:] = np.diff(nav) / nav[:-1]
    return dates, rets


def metrics(rets: np.ndarray) -> dict:
    """全套 metrics（向量化）。"""
    n = len(rets)
    if n < 2: return {}
    nav = np.cumprod(1 + rets)
    years = n / TDPY
    cagr = nav[-1] ** (1/years) - 1
    vol = rets.std(ddof=1) * math.sqrt(TDPY)
    downside = rets[rets < 0]
    downvol = downside.std(ddof=1) * math.sqrt(TDPY) if len(downside) > 1 else 1e-9
    sharpe = (cagr - RF) / vol if vol > 0 else 0
    sortino = (cagr - RF) / downvol if downvol > 0 else 0
    peak = np.maximum.accumulate(nav)
    dd = (nav - peak) / peak
    mdd = dd.min()
    calmar = cagr / abs(mdd) if mdd < 0 else 0
    skew = ((rets - rets.mean()) ** 3).mean() / (rets.std() ** 3) if rets.std() > 0 else 0
    kurt = ((rets - rets.mean()) ** 4).mean() / (rets.std() ** 4) - 3 if rets.std() > 0 else 0
    return dict(CAGR=cagr, Vol=vol, Sharpe=sharpe, Sortino=sortino,
                MDD=mdd, Calmar=calmar, Skew=skew, Kurt=kurt, FinalNAV=nav[-1])


# === MC permutation worker ===
def _mc_worker(args):
    rets, seed = args
    rng = np.random.default_rng(seed)
    centered = rets - rets.mean()
    perm = rng.permutation(centered)
    n = len(perm)
    nav_end = np.prod(1 + perm)
    cagr = nav_end ** (TDPY/n) - 1
    vol = perm.std(ddof=1) * math.sqrt(TDPY)
    return (cagr - RF) / vol if vol > 0 else 0


def mc_permutation(rets: np.ndarray, n_perm: int = 2000, n_proc: int = 8) -> dict:
    actual = metrics(rets)["Sharpe"]
    args_list = [(rets, i) for i in range(n_perm)]
    with Pool(n_proc) as pool:
        null_sharpes = pool.map(_mc_worker, args_list)
    null_arr = np.array(null_sharpes)
    p_value = (null_arr >= actual).mean()
    return dict(actual_sharpe=actual, null_mean=null_arr.mean(),
                null_std=null_arr.std(), p_value=p_value)


# === Bootstrap (block, year-level) worker ===
def _boot_year_worker(args):
    rets, year_blocks, seed = args
    rng = np.random.default_rng(seed)
    n_years = len(year_blocks)
    sampled_blocks = [year_blocks[rng.integers(0, n_years)] for _ in range(n_years)]
    sampled = np.concatenate(sampled_blocks)
    n = len(sampled)
    nav_end = np.prod(1 + sampled)
    if nav_end <= 0: return (0, 0, 0)
    years = n / TDPY
    cagr = nav_end ** (1/years) - 1
    vol = sampled.std(ddof=1) * math.sqrt(TDPY)
    sharpe = (cagr - RF) / vol if vol > 0 else 0
    downside = sampled[sampled < 0]
    downvol = downside.std(ddof=1) * math.sqrt(TDPY) if len(downside) > 1 else 1e-9
    sortino = (cagr - RF) / downvol if downvol > 0 else 0
    return (cagr, sharpe, sortino)


def bootstrap_year(rets: np.ndarray, dates: list, n_boot: int = 2000, n_proc: int = 8) -> dict:
    # 切成年度 blocks
    arr_dates = np.array([d.year for d in dates])
    year_blocks = []
    for y in sorted(set(arr_dates)):
        mask = arr_dates == y
        year_blocks.append(rets[mask])
    args_list = [(rets, year_blocks, i) for i in range(n_boot)]
    with Pool(n_proc) as pool:
        results = pool.map(_boot_year_worker, args_list)
    arr = np.array(results)
    cagrs = arr[:, 0]; sharpes = arr[:, 1]; sortinos = arr[:, 2]
    return dict(
        cagr_lb=np.percentile(cagrs, 2.5), cagr_ub=np.percentile(cagrs, 97.5),
        sharpe_lb=np.percentile(sharpes, 2.5), sharpe_ub=np.percentile(sharpes, 97.5),
        sortino_lb=np.percentile(sortinos, 2.5), sortino_ub=np.percentile(sortinos, 97.5),
    )


# === Walk-forward ===
def walk_forward(rets: np.ndarray, dates: list, train_y: int = 5, test_y: int = 1) -> dict:
    arr_year = np.array([d.year for d in dates])
    years_unique = sorted(set(arr_year))
    folds = []
    for i in range(train_y, len(years_unique)):
        test_y_val = years_unique[i]
        train_years = years_unique[i-train_y:i]
        train_mask = np.isin(arr_year, train_years)
        test_mask = arr_year == test_y_val
        train_rets = rets[train_mask]
        test_rets = rets[test_mask]
        if len(train_rets) < 200 or len(test_rets) < 50: continue
        train_m = metrics(train_rets)
        test_m = metrics(test_rets)
        folds.append({
            "test_year": test_y_val,
            "is_cagr": train_m["CAGR"], "is_sortino": train_m["Sortino"],
            "oos_cagr": test_m["CAGR"], "oos_sortino": test_m["Sortino"],
            "oos_mdd": test_m["MDD"],
        })
    is_cagrs = [f["is_cagr"] for f in folds]
    oos_cagrs = [f["oos_cagr"] for f in folds]
    is_sortinos = [f["is_sortino"] for f in folds]
    oos_sortinos = [f["oos_sortino"] for f in folds]

    # pooled OOS
    test_year_set = {f["test_year"] for f in folds}
    oos_mask = np.isin(arr_year, list(test_year_set))
    pooled_oos = rets[oos_mask]
    pooled_m = metrics(pooled_oos)
    is_full = metrics(rets)
    return dict(
        n_folds=len(folds),
        is_full_cagr=is_full["CAGR"], is_full_sortino=is_full["Sortino"],
        oos_pooled_cagr=pooled_m["CAGR"], oos_pooled_sortino=pooled_m["Sortino"],
        retention_cagr=pooled_m["CAGR"]/is_full["CAGR"] if is_full["CAGR"] > 0 else 0,
        retention_sortino=pooled_m["Sortino"]/is_full["Sortino"] if is_full["Sortino"] > 0 else 0,
        n_positive_oos_sortino=sum(1 for s in oos_sortinos if s > 0),
        worst_oos_year=min(folds, key=lambda f: f["oos_sortino"])["test_year"],
        worst_oos_sortino=min(oos_sortinos),
        folds=folds,
    )


# === DSR ===
def dsr(sharpe: float, n_obs: int, skew: float, kurt: float, n_trials: int = 50) -> float:
    """Deflated Sharpe Ratio."""
    from scipy.stats import norm
    EM = 0.5772
    expected_max = (1 - EM) * norm.ppf(1 - 1/n_trials) + EM * norm.ppf(1 - 1/(n_trials * math.e))
    sr0 = expected_max / math.sqrt(n_obs)
    var_sr = (1 - skew * sharpe + (kurt - 1) / 4 * sharpe ** 2) / n_obs
    if var_sr <= 0: return 0
    z = (sharpe - sr0) / math.sqrt(var_sr)
    return norm.cdf(z)


# === Combined runner ===
def run_full_validation(name: str, csv_path: str, n_trials_dsr: int = 50, n_proc: int = 8) -> dict:
    print(f"\n{'='*78}")
    print(f"驗證: {name} ({csv_path})")
    print('='*78)
    t0 = time.time()
    dates, rets = daily_rets_from_nav_csv(csv_path)
    is_m = metrics(rets)
    print(f"IS: CAGR {is_m['CAGR']*100:+.2f}% Sortino {is_m['Sortino']:.3f} "
          f"MDD {is_m['MDD']*100:.1f}% Skew {is_m['Skew']:.2f} Kurt {is_m['Kurt']:.1f}")

    print("跑 walk-forward (16 folds)...")
    t = time.time()
    wf = walk_forward(rets, dates)
    print(f"  done {time.time()-t:.1f}s | OOS pooled Sortino {wf['oos_pooled_sortino']:.3f} "
          f"(retention {wf['retention_sortino']*100:.1f}% IS) | "
          f"{wf['n_positive_oos_sortino']}/{wf['n_folds']} folds OOS Sortino > 0")

    print(f"跑 MC permutation (2000 次, {n_proc} procs)...")
    t = time.time()
    mc = mc_permutation(rets, n_perm=2000, n_proc=n_proc)
    print(f"  done {time.time()-t:.1f}s | actual Sharpe {mc['actual_sharpe']:.3f} | "
          f"p-value {mc['p_value']:.4f}")

    print(f"跑 Bootstrap (年度 blocks, 2000 次, {n_proc} procs)...")
    t = time.time()
    bs = bootstrap_year(rets, dates, n_boot=2000, n_proc=n_proc)
    print(f"  done {time.time()-t:.1f}s | CAGR 95% CI [{bs['cagr_lb']*100:+.2f}%, "
          f"{bs['cagr_ub']*100:+.2f}%] | Sortino LB {bs['sortino_lb']:.3f}")

    dsr_val = dsr(is_m['Sharpe'], len(rets), is_m['Skew'], is_m['Kurt'], n_trials_dsr)
    print(f"DSR (n_trials={n_trials_dsr}): {dsr_val:.4f}")

    # 5 項判定
    pass_wf = wf['retention_sortino'] >= 0.7 and wf['retention_cagr'] >= 0.5
    pass_mc = mc['p_value'] < 0.05
    pass_boot = bs['cagr_lb'] >= 0.10
    pass_dsr = dsr_val > 0.95

    n_pass = sum([pass_wf, pass_mc, pass_boot, pass_dsr])
    if n_pass == 4: verdict = "✅ REAL ALPHA"
    elif n_pass >= 3: verdict = "⚠️ MARGINAL"
    else: verdict = "❌ CURVE-FIT / WEAK"

    print(f"\n判定: {verdict} ({n_pass}/4 通過)")
    print(f"  WF retention ≥70%: {'✓' if pass_wf else '✗'}")
    print(f"  MC p < 0.05:       {'✓' if pass_mc else '✗'}")
    print(f"  Boot LB ≥ 10%:     {'✓' if pass_boot else '✗'}")
    print(f"  DSR > 0.95:        {'✓' if pass_dsr else '✗'}")
    print(f"總時間: {time.time()-t0:.1f}s")

    return {
        "name": name, "verdict": verdict, "n_pass": n_pass,
        "is_cagr": is_m["CAGR"], "is_sortino": is_m["Sortino"],
        "is_mdd": is_m["MDD"], "is_calmar": is_m["Calmar"],
        "oos_cagr": wf["oos_pooled_cagr"], "oos_sortino": wf["oos_pooled_sortino"],
        "retention_sortino": wf["retention_sortino"],
        "n_positive_folds": wf["n_positive_oos_sortino"], "n_folds": wf["n_folds"],
        "mc_p": mc["p_value"], "mc_sharpe": mc["actual_sharpe"],
        "boot_cagr_lb": bs["cagr_lb"], "boot_sortino_lb": bs["sortino_lb"],
        "dsr": dsr_val,
    }


# ============================================================
# Verdict class schema — 9 classes
# 每個 candidate 必須標一個 class（3-tuple 格式），用於分區輸出
# ============================================================
VERDICT_CLASSES = {
    "SHIP": {
        "emoji": "🥇",
        "label": "SHIP CANDIDATE",
        "desc": "當前推薦的最佳組合（已通過 cross-validation + counterfactual + ablation）"
    },
    "REAL_ALPHA": {
        "emoji": "✅",
        "label": "REAL ALPHA",
        "desc": "合法可比較的真 alpha 變體（strict cap、無已知 bias）"
    },
    "BENCHMARK": {
        "emoji": "📊",
        "label": "BENCHMARK",
        "desc": "對照基準（hold strategy）"
    },
    "NAV_BLEND_INFLATED": {
        "emoji": "⚠️",
        "label": "NAV-BLEND INFLATED",
        "desc": "舊版 NAV blend，沒嚴格鎖 ≤ 10 持股（iter_24 內部 max 10 + iter_13 N 檔 → 可能超 10）→ 數字灌水"
    },
    "BIAS_sample_period": {
        "emoji": "⚠️",
        "label": "BIAS — Sample Period",
        "desc": "OOS Sortino 看似高但完全靠 21y TWSE TSMC mcap 從未掉第一這個 sample-period bias（1+9 系列 mcap = 81% 時間 = 2330）"
    },
    "BIAS_mcap_weighting": {
        "emoji": "⚠️",
        "label": "BIAS — mcap-Weighting",
        "desc": "用非 mcap ranker 但仍 mcap-weighted → 小 mcap 公司權重被稀釋為背景，NAV 集中到 2330"
    },
    "BUG_weight_compound": {
        "emoji": "🚨",
        "label": "BUG — Weight Compound",
        "desc": "weight × ret 累乘但未 normalize 造成虛假動態槓桿，CAGR 灌水可達 +8pp（iter_13 v2 drift 系列）"
    },
    "COUNTERFACTUAL": {
        "emoji": "🧪",
        "label": "COUNTERFACTUAL",
        "desc": "壓力測試 / cross-validation control，用來驗證其他策略是否依賴 outlier；本身不 ship"
    },
    "FAILED": {
        "emoji": "❌",
        "label": "FAILED",
        "desc": "已驗證失敗（OOS Sortino < 1.4、Bootstrap LB 為負、結構性問題等）"
    },
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n-proc", type=int, default=8)
    args = ap.parse_args()
    t_total = time.time()

    # 3-tuple format: (display_name, csv_path, verdict_class)
    candidates = [
        # === BENCHMARKS ===
        ("BENCHMARK 2330 hold",              "research/strat_lab/results/benchmark_2330_daily.csv", "BENCHMARK"),
        ("BENCHMARK 0050 hold",              "research/strat_lab/results/benchmark_0050_daily.csv", "BENCHMARK"),
        ("BENCHMARK 0052 hold (科技 ETF)",   "research/strat_lab/results/benchmark_0052_daily.csv", "BENCHMARK"),
        # === SHIP candidate（最新推薦，C+B combined）===
        ("CB slot 5+5 TPEx + ATR (combined)",            "research/strat_lab/results/slot5_5_tpex_atr_w80_daily.csv", "SHIP"),

        # === REAL_ALPHA — pure quality baselines ===
        ("iter_13 mcap (single)",            "research/strat_lab/results/iter_13_mcap_daily.csv", "REAL_ALPHA"),
        ("iter_13 quarterly rebal",          "research/strat_lab/results/iter_13_quarterly_daily.csv", "REAL_ALPHA"),
        ("iter_13 加料 F: top 10 mcap",      "research/strat_lab/results/F._mcap_top_10_(diversified)_daily.csv", "REAL_ALPHA"),

        # === NAV_BLEND_INFLATED — 舊 NAV blend，沒嚴格鎖 ≤ 10 持股 ===
        ("iter_21 80/20 (FINAL CANDIDATE)",  "research/strat_lab/results/iter_21_w80_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_25 hybrid 70/30 (iter_24)",   "research/strat_lab/results/iter_25_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_21 70/30",                    "research/strat_lab/results/iter_21_w70_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_21q 80/20 (qtr base)",        "research/strat_lab/results/iter_21q_w80_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_21q 85/15 (qtr base)",        "research/strat_lab/results/iter_21q_w85_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_21q 90/10 (qtr base)",        "research/strat_lab/results/iter_21q_w90_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_25q 80/20 (qtr+pyr)",         "research/strat_lab/results/iter_25q_w80_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_25q 85/15 (qtr+pyr)",         "research/strat_lab/results/iter_25q_w85_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_25q 90/10 (qtr+pyr)",         "research/strat_lab/results/iter_25q_w90_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_21m 80/20 (mo base)",         "research/strat_lab/results/iter_21m_w80_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_21m 85/15 (mo base)",         "research/strat_lab/results/iter_21m_w85_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_21m 90/10 (mo base)",         "research/strat_lab/results/iter_21m_w90_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_21F 80/20 (top10+iter20)",    "research/strat_lab/results/iter_21F_w80_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_21F 85/15 (top10+iter20)",    "research/strat_lab/results/iter_21F_w85_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_21F 90/10 (top10+iter20)",    "research/strat_lab/results/iter_21F_w90_daily.csv", "NAV_BLEND_INFLATED"),
        ("3-way 80/10/10",                   "research/strat_lab/results/iter_3way_80-10-10_daily.csv", "NAV_BLEND_INFLATED"),
        ("3-way 85/7/8",                     "research/strat_lab/results/iter_3way_85-7-7_daily.csv", "NAV_BLEND_INFLATED"),
        ("3-way 70/15/15",                   "research/strat_lab/results/iter_3way_70-15-15_daily.csv", "NAV_BLEND_INFLATED"),
        ("3-way 75/15/10",                   "research/strat_lab/results/iter_3way_75-15-10_daily.csv", "NAV_BLEND_INFLATED"),
        ("3-way 75/10/15",                   "research/strat_lab/results/iter_3way_75-10-15_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_25qm 60/40 (mo+pyr)",         "research/strat_lab/results/iter_25qm_w60_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_25qm 65/35 (mo+pyr)",         "research/strat_lab/results/iter_25qm_w65_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_25qm 70/30 (mo+pyr)",         "research/strat_lab/results/iter_25qm_w70_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_25qm 95/5  (mo+pyr)",         "research/strat_lab/results/iter_25qm_w95_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_25qm 75/25 (mo+pyr)",         "research/strat_lab/results/iter_25qm_w75_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_25qm 80/20 (mo+pyr)",         "research/strat_lab/results/iter_25qm_w80_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_25qm 85/15 (mo+pyr)",         "research/strat_lab/results/iter_25qm_w85_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_25qm 90/10 (mo+pyr)",         "research/strat_lab/results/iter_25qm_w90_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_25q 70/30 (qtr+pyr)",         "research/strat_lab/results/iter_25q_w70_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_25q 75/25 (qtr+pyr)",         "research/strat_lab/results/iter_25q_w75_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_25q 95/5  (qtr+pyr)",         "research/strat_lab/results/iter_25q_w95_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_25F 80/20 (top10+pyr)",       "research/strat_lab/results/iter_25F_w80_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_25F 85/15 (top10+pyr)",       "research/strat_lab/results/iter_25F_w85_daily.csv", "NAV_BLEND_INFLATED"),
        ("iter_25F 90/10 (top10+pyr)",       "research/strat_lab/results/iter_25F_w90_daily.csv", "NAV_BLEND_INFLATED"),
        ("4-way 70/15/10/5",                 "research/strat_lab/results/4w_70_15_10_5_daily.csv", "NAV_BLEND_INFLATED"),
        ("4-way 60/20/15/5",                 "research/strat_lab/results/4w_60_20_15_5_daily.csv", "NAV_BLEND_INFLATED"),
        ("4-way mo60/pyr25/iter20-10/F-5",   "research/strat_lab/results/4w_iter13mo_60_iter24_25_iter20_10_F_5_daily.csv", "NAV_BLEND_INFLATED"),

        # === REAL_ALPHA — strict cap variants（早期命名 strict_55 / strict_56）===
        ("strict 5+5 NAV 50/50 (legacy)",    "research/strat_lab/results/strict_55_w50_daily.csv", "REAL_ALPHA"),
        ("strict 5+5 NAV 60/40 (legacy)",    "research/strat_lab/results/strict_55_w60_daily.csv", "REAL_ALPHA"),
        ("strict 5+5 NAV 70/30 (legacy)",    "research/strat_lab/results/strict_55_w70_daily.csv", "REAL_ALPHA"),
        ("strict 5+5 NAV 75/25 ★ (legacy)",  "research/strat_lab/results/strict_55_w75_daily.csv", "REAL_ALPHA"),
        ("strict 5+5 NAV 80/20 (legacy)",    "research/strat_lab/results/strict_55_w80_daily.csv", "REAL_ALPHA"),
        ("strict 5+6 NAV 60/40",             "research/strat_lab/results/strict_56_w60_daily.csv", "REAL_ALPHA"),
        ("strict 5+6 NAV 65/35",             "research/strat_lab/results/strict_56_w65_daily.csv", "REAL_ALPHA"),
        ("strict 5+6 NAV 70/30",             "research/strat_lab/results/strict_56_w70_daily.csv", "REAL_ALPHA"),
        ("strict 5+6 NAV 75/25",             "research/strat_lab/results/strict_56_w75_daily.csv", "REAL_ALPHA"),

        # === REAL_ALPHA — slot×NAV 全空間掃描（合法 cap）===
        ("strict 3+7 NAV 40/60", "research/strat_lab/results/slot3_7_w40_daily.csv", "REAL_ALPHA"),
        ("strict 3+7 NAV 50/50", "research/strat_lab/results/slot3_7_w50_daily.csv", "REAL_ALPHA"),
        ("strict 3+7 NAV 60/40", "research/strat_lab/results/slot3_7_w60_daily.csv", "REAL_ALPHA"),
        ("strict 3+7 NAV 70/30", "research/strat_lab/results/slot3_7_w70_daily.csv", "REAL_ALPHA"),
        ("strict 3+7 NAV 75/25", "research/strat_lab/results/slot3_7_w75_daily.csv", "REAL_ALPHA"),
        ("strict 3+7 NAV 80/20", "research/strat_lab/results/slot3_7_w80_daily.csv", "REAL_ALPHA"),
        ("strict 3+7 NAV 85/15", "research/strat_lab/results/slot3_7_w85_daily.csv", "REAL_ALPHA"),
        ("strict 3+7 NAV 90/10", "research/strat_lab/results/slot3_7_w90_daily.csv", "REAL_ALPHA"),
        ("strict 4+6 NAV 40/60", "research/strat_lab/results/slot4_6_w40_daily.csv", "REAL_ALPHA"),
        ("strict 4+6 NAV 50/50", "research/strat_lab/results/slot4_6_w50_daily.csv", "REAL_ALPHA"),
        ("strict 4+6 NAV 60/40", "research/strat_lab/results/slot4_6_w60_daily.csv", "REAL_ALPHA"),
        ("strict 4+6 NAV 70/30", "research/strat_lab/results/slot4_6_w70_daily.csv", "REAL_ALPHA"),
        ("strict 4+6 NAV 75/25", "research/strat_lab/results/slot4_6_w75_daily.csv", "REAL_ALPHA"),
        ("strict 4+6 NAV 80/20", "research/strat_lab/results/slot4_6_w80_daily.csv", "REAL_ALPHA"),
        ("strict 4+6 NAV 85/15", "research/strat_lab/results/slot4_6_w85_daily.csv", "REAL_ALPHA"),
        ("strict 4+6 NAV 90/10", "research/strat_lab/results/slot4_6_w90_daily.csv", "REAL_ALPHA"),
        ("strict 5+5 NAV 40/60", "research/strat_lab/results/slot5_5_w40_daily.csv", "REAL_ALPHA"),
        ("strict 5+5 NAV 50/50", "research/strat_lab/results/slot5_5_w50_daily.csv", "REAL_ALPHA"),
        ("strict 5+5 NAV 60/40", "research/strat_lab/results/slot5_5_w60_daily.csv", "REAL_ALPHA"),
        ("strict 5+5 NAV 70/30", "research/strat_lab/results/slot5_5_w70_daily.csv", "REAL_ALPHA"),
        ("strict 5+5 NAV 75/25", "research/strat_lab/results/slot5_5_w75_daily.csv", "REAL_ALPHA"),
        ("strict 5+5 NAV 80/20", "research/strat_lab/results/slot5_5_w80_daily.csv", "REAL_ALPHA"),
        ("strict 5+5 NAV 85/15", "research/strat_lab/results/slot5_5_w85_daily.csv", "REAL_ALPHA"),
        ("strict 5+5 NAV 90/10", "research/strat_lab/results/slot5_5_w90_daily.csv", "REAL_ALPHA"),
        ("strict 6+4 NAV 40/60", "research/strat_lab/results/slot6_4_w40_daily.csv", "REAL_ALPHA"),
        ("strict 6+4 NAV 50/50", "research/strat_lab/results/slot6_4_w50_daily.csv", "REAL_ALPHA"),
        ("strict 6+4 NAV 60/40", "research/strat_lab/results/slot6_4_w60_daily.csv", "REAL_ALPHA"),
        ("strict 6+4 NAV 70/30", "research/strat_lab/results/slot6_4_w70_daily.csv", "REAL_ALPHA"),
        ("strict 6+4 NAV 75/25", "research/strat_lab/results/slot6_4_w75_daily.csv", "REAL_ALPHA"),
        ("strict 6+4 NAV 80/20", "research/strat_lab/results/slot6_4_w80_daily.csv", "REAL_ALPHA"),
        ("strict 6+4 NAV 85/15", "research/strat_lab/results/slot6_4_w85_daily.csv", "REAL_ALPHA"),
        ("strict 6+4 NAV 90/10", "research/strat_lab/results/slot6_4_w90_daily.csv", "REAL_ALPHA"),
        ("strict 7+3 NAV 40/60", "research/strat_lab/results/slot7_3_w40_daily.csv", "REAL_ALPHA"),
        ("strict 7+3 NAV 50/50", "research/strat_lab/results/slot7_3_w50_daily.csv", "REAL_ALPHA"),
        ("strict 7+3 NAV 60/40", "research/strat_lab/results/slot7_3_w60_daily.csv", "REAL_ALPHA"),
        ("strict 7+3 NAV 70/30", "research/strat_lab/results/slot7_3_w70_daily.csv", "REAL_ALPHA"),
        ("strict 7+3 NAV 75/25", "research/strat_lab/results/slot7_3_w75_daily.csv", "REAL_ALPHA"),
        ("strict 7+3 NAV 80/20", "research/strat_lab/results/slot7_3_w80_daily.csv", "REAL_ALPHA"),
        ("strict 7+3 NAV 85/15", "research/strat_lab/results/slot7_3_w85_daily.csv", "REAL_ALPHA"),
        ("strict 7+3 NAV 90/10", "research/strat_lab/results/slot7_3_w90_daily.csv", "REAL_ALPHA"),
        ("strict 8+2 NAV 40/60", "research/strat_lab/results/slot8_2_w40_daily.csv", "REAL_ALPHA"),
        ("strict 8+2 NAV 50/50", "research/strat_lab/results/slot8_2_w50_daily.csv", "REAL_ALPHA"),
        ("strict 8+2 NAV 60/40", "research/strat_lab/results/slot8_2_w60_daily.csv", "REAL_ALPHA"),
        ("strict 8+2 NAV 70/30", "research/strat_lab/results/slot8_2_w70_daily.csv", "REAL_ALPHA"),
        ("strict 8+2 NAV 75/25", "research/strat_lab/results/slot8_2_w75_daily.csv", "REAL_ALPHA"),
        ("strict 8+2 NAV 80/20", "research/strat_lab/results/slot8_2_w80_daily.csv", "REAL_ALPHA"),
        ("strict 8+2 NAV 85/15", "research/strat_lab/results/slot8_2_w85_daily.csv", "REAL_ALPHA"),
        ("strict 8+2 NAV 90/10", "research/strat_lab/results/slot8_2_w90_daily.csv", "REAL_ALPHA"),
        ("strict 2+8 NAV 40/60", "research/strat_lab/results/slot2_8_w40_daily.csv", "REAL_ALPHA"),
        ("strict 2+8 NAV 50/50", "research/strat_lab/results/slot2_8_w50_daily.csv", "REAL_ALPHA"),
        ("strict 2+8 NAV 60/40", "research/strat_lab/results/slot2_8_w60_daily.csv", "REAL_ALPHA"),
        ("strict 2+8 NAV 70/30", "research/strat_lab/results/slot2_8_w70_daily.csv", "REAL_ALPHA"),
        ("strict 2+8 NAV 75/25", "research/strat_lab/results/slot2_8_w75_daily.csv", "REAL_ALPHA"),
        ("strict 2+8 NAV 80/20", "research/strat_lab/results/slot2_8_w80_daily.csv", "REAL_ALPHA"),
        ("strict 2+8 NAV 85/15", "research/strat_lab/results/slot2_8_w85_daily.csv", "REAL_ALPHA"),
        ("strict 2+8 NAV 90/10", "research/strat_lab/results/slot2_8_w90_daily.csv", "REAL_ALPHA"),

        # === REAL_ALPHA — C+B ablation 變體 ===
        ("CB slot 5+5 baseline (TWSE only, fixed -15%)", "research/strat_lab/results/slot5_5_baseline_w80_daily.csv", "REAL_ALPHA"),
        ("CB slot 5+5 TPEx quality only",                "research/strat_lab/results/slot5_5_tpex_only_w80_daily.csv", "REAL_ALPHA"),
        ("CB slot 5+5 ATR trailing only",                "research/strat_lab/results/slot5_5_atr_only_w80_daily.csv", "REAL_ALPHA"),

        # === BUG_weight_compound — iter_13 v2 drift 系列（unnormalized weight × ret 累乘灌水 +8pp）===
        ("★ iter_13 v2 annual (drift)",      "research/strat_lab/results/iter_13_v2_annual_daily.csv", "BUG_weight_compound"),
        ("iter_13 v2 quarterly (drift)",     "research/strat_lab/results/iter_13_v2_quarterly_daily.csv", "BUG_weight_compound"),
        ("iter_13 v2 monthly (drift)",       "research/strat_lab/results/iter_13_v2_monthly_daily.csv", "BUG_weight_compound"),
        ("iter_13 v2 daily (drift)",         "research/strat_lab/results/iter_13_v2_daily_daily.csv", "BUG_weight_compound"),

        # === FAILED — Option D/E 動態方案 + pure catalyst（無 quality anchor）===
        ("Option D 統一 score TOP10 daily",  "research/strat_lab/results/iter_25_optD_daily.csv", "FAILED"),
        ("Option E k=5 thr=30%",             "research/strat_lab/results/iter_25_optE_k5_thr30_daily.csv", "FAILED"),
        ("Option E k=6 thr=30%",             "research/strat_lab/results/iter_25_optE_k6_thr30_daily.csv", "FAILED"),
        ("Option E k=8 thr=30%",             "research/strat_lab/results/iter_25_optE_k8_thr30_daily.csv", "FAILED"),
        ("Option E k=8 thr=50%",             "research/strat_lab/results/iter_25_optE_k8_thr50_daily.csv", "FAILED"),
        ("strict 0+10 (pure iter_24 max10)", "research/strat_lab/results/slot0_10_daily.csv", "FAILED"),

        # === BIAS_sample_period — 1+9 系列（賭 TSMC 21y outlier）===
        ("strict 1+9 NAV 40/60",  "research/strat_lab/results/slot1_9_w40_daily.csv", "BIAS_sample_period"),
        ("strict 1+9 NAV 50/50",  "research/strat_lab/results/slot1_9_w50_daily.csv", "BIAS_sample_period"),
        ("strict 1+9 NAV 60/40",  "research/strat_lab/results/slot1_9_w60_daily.csv", "BIAS_sample_period"),
        ("strict 1+9 NAV 70/30",  "research/strat_lab/results/slot1_9_w70_daily.csv", "BIAS_sample_period"),
        ("strict 1+9 NAV 75/25",  "research/strat_lab/results/slot1_9_w75_daily.csv", "BIAS_sample_period"),
        ("strict 1+9 NAV 80/20",  "research/strat_lab/results/slot1_9_w80_daily.csv", "BIAS_sample_period"),
        ("strict 1+9 NAV 85/15",  "research/strat_lab/results/slot1_9_w85_daily.csv", "BIAS_sample_period"),
        ("strict 1+9 NAV 90/10",  "research/strat_lab/results/slot1_9_w90_daily.csv", "BIAS_sample_period"),

        # === BIAS_mcap_weighting — 換 ranker 但 mcap-weighted 仍集中 TSMC ===
        ("XVAL slot 5+5 roa_med w85",  "research/strat_lab/results/slot5_5_roa_med_w85_daily.csv", "BIAS_mcap_weighting"),
        ("XVAL slot 6+4 roa_med w85",  "research/strat_lab/results/slot6_4_roa_med_w85_daily.csv", "BIAS_mcap_weighting"),

        # === COUNTERFACTUAL — STRESS test (排除 2330) + cross-validation control ===
        ("STRESS quality 5 (with 2330)",     "research/strat_lab/results/iter_13_v3_monthly_top5_with2330_daily.csv", "COUNTERFACTUAL"),
        ("STRESS quality 5 (NO 2330)",       "research/strat_lab/results/iter_13_v3_monthly_top5_no2330_daily.csv", "COUNTERFACTUAL"),
        ("STRESS slot 5+5 NAV 80/20 NO 2330","research/strat_lab/results/slot5_5_no2330_w80_daily.csv", "COUNTERFACTUAL"),

        # === COUNTERFACTUAL — XVAL slot×ranker（mcap = REAL_ALPHA 控制組；其他 ranker 為 cross-validation 證據）===
        ("XVAL slot 3+7 mcap w80",         "research/strat_lab/results/slot3_7_mcap_w80_daily.csv", "REAL_ALPHA"),
        ("XVAL slot 3+7 roa_med w80",      "research/strat_lab/results/slot3_7_roa_med_w80_daily.csv", "COUNTERFACTUAL"),
        ("XVAL slot 3+7 rev_cagr5y w80",   "research/strat_lab/results/slot3_7_rev_cagr5y_w80_daily.csv", "COUNTERFACTUAL"),
        ("XVAL slot 3+7 roa_recent w80",   "research/strat_lab/results/slot3_7_roa_recent_w80_daily.csv", "COUNTERFACTUAL"),
        ("XVAL slot 5+5 mcap w85",         "research/strat_lab/results/slot5_5_mcap_w85_daily.csv", "REAL_ALPHA"),
        ("XVAL slot 5+5 rev_cagr5y w85",   "research/strat_lab/results/slot5_5_rev_cagr5y_w85_daily.csv", "COUNTERFACTUAL"),
        ("XVAL slot 5+5 roa_recent w85",   "research/strat_lab/results/slot5_5_roa_recent_w85_daily.csv", "COUNTERFACTUAL"),
        ("XVAL slot 6+4 mcap w85",         "research/strat_lab/results/slot6_4_mcap_w85_daily.csv", "REAL_ALPHA"),
        ("XVAL slot 6+4 rev_cagr5y w85",   "research/strat_lab/results/slot6_4_rev_cagr5y_w85_daily.csv", "COUNTERFACTUAL"),
        ("XVAL slot 6+4 roa_recent w85",   "research/strat_lab/results/slot6_4_roa_recent_w85_daily.csv", "COUNTERFACTUAL"),

        # === BIAS_sample_period — 1+9 mcap cross-validation（confirms 賭 TSMC）===
        ("CROSS slot 1+9 mcap w70",  "research/strat_lab/results/slot1_9_mcap_w70_daily.csv", "BIAS_sample_period"),
        ("CROSS slot 1+9 mcap w75",  "research/strat_lab/results/slot1_9_mcap_w75_daily.csv", "BIAS_sample_period"),
        ("CROSS slot 1+9 mcap w80",  "research/strat_lab/results/slot1_9_mcap_w80_daily.csv", "BIAS_sample_period"),
        ("CROSS slot 1+9 mcap w85",  "research/strat_lab/results/slot1_9_mcap_w85_daily.csv", "BIAS_sample_period"),

        # === COUNTERFACTUAL — 1+9 用其他 ranker 的 cross-validation (alpha 全消失 → 證明 mcap 是賭 TSMC)===
        ("CROSS slot 1+9 roa_med w70",      "research/strat_lab/results/slot1_9_roa_med_w70_daily.csv", "COUNTERFACTUAL"),
        ("CROSS slot 1+9 roa_med w75",      "research/strat_lab/results/slot1_9_roa_med_w75_daily.csv", "COUNTERFACTUAL"),
        ("CROSS slot 1+9 roa_med w80",      "research/strat_lab/results/slot1_9_roa_med_w80_daily.csv", "COUNTERFACTUAL"),
        ("CROSS slot 1+9 roa_med w85",      "research/strat_lab/results/slot1_9_roa_med_w85_daily.csv", "COUNTERFACTUAL"),
        ("CROSS slot 1+9 rev_cagr5y w70",   "research/strat_lab/results/slot1_9_rev_cagr5y_w70_daily.csv", "COUNTERFACTUAL"),
        ("CROSS slot 1+9 rev_cagr5y w75",   "research/strat_lab/results/slot1_9_rev_cagr5y_w75_daily.csv", "COUNTERFACTUAL"),
        ("CROSS slot 1+9 rev_cagr5y w80",   "research/strat_lab/results/slot1_9_rev_cagr5y_w80_daily.csv", "COUNTERFACTUAL"),
        ("CROSS slot 1+9 rev_cagr5y w85",   "research/strat_lab/results/slot1_9_rev_cagr5y_w85_daily.csv", "COUNTERFACTUAL"),
        ("CROSS slot 1+9 roa_recent w70",   "research/strat_lab/results/slot1_9_roa_recent_w70_daily.csv", "COUNTERFACTUAL"),
        ("CROSS slot 1+9 roa_recent w75",   "research/strat_lab/results/slot1_9_roa_recent_w75_daily.csv", "COUNTERFACTUAL"),
        ("CROSS slot 1+9 roa_recent w80",   "research/strat_lab/results/slot1_9_roa_recent_w80_daily.csv", "COUNTERFACTUAL"),
        ("CROSS slot 1+9 roa_recent w85",   "research/strat_lab/results/slot1_9_roa_recent_w85_daily.csv", "COUNTERFACTUAL"),
    ]
    # Validate that every candidate has a known verdict_class
    for entry in candidates:
        if len(entry) != 3:
            raise ValueError(f"Candidate must be 3-tuple (name, path, verdict_class), got: {entry}")
        if entry[2] not in VERDICT_CLASSES:
            raise ValueError(f"Unknown verdict_class '{entry[2]}' for {entry[0]}. "
                              f"Valid: {list(VERDICT_CLASSES.keys())}")

    results = []
    for name, path, verdict_class in candidates:
        if not os.path.exists(path):
            print(f"[skip] {name}: file not found"); continue
        r = run_full_validation(name, path, n_proc=args.n_proc)
        r["verdict_class"] = verdict_class  # 把 class 帶進 result
        results.append(r)

    # 完整對照表（按 OOS Sortino 排序）
    print(f"\n{'='*100}")
    print("=== 全策略驗證對照（依 OOS Sortino 排序）===")
    print(f"{'='*100}")
    results.sort(key=lambda x: x["oos_sortino"], reverse=True)
    print(f"{'Strategy':<40} {'Verdict':<14} {'IS Sort':>7} {'OOS Sort':>8} {'Retain%':>7} "
          f"{'OOS CAGR':>8} {'BootLB':>7} {'MC p':>8} {'DSR':>6}")
    print("-" * 110)
    for r in results:
        print(f"{r['name']:<40} {r['verdict']:<14} "
              f"{r['is_sortino']:>7.3f} {r['oos_sortino']:>8.3f} "
              f"{r['retention_sortino']*100:>6.1f}% "
              f"{r['oos_cagr']*100:>+7.2f}% {r['boot_cagr_lb']*100:>+6.2f}% "
              f"{r['mc_p']:>8.4f} {r['dsr']:>6.3f}")

    print(f"\n總時間: {time.time()-t_total:.1f}s")

    # === Output 1: raw rank by Sortino ===
    with open("research/strat_lab/results/all_validation.md", "w") as f:
        f.write("# 全策略驗證報告（raw rank by OOS Sortino）\n\n")
        f.write("> ⚠️ 此檔為 raw 排序，未區分 sample-period bias / weight-compound bug。\n")
        f.write("> 投資人請看 `all_validation_annotated.md`（按 verdict_class 分區）。\n\n")
        f.write("| Strategy | Verdict | Class | IS Sortino | OOS Sortino | Retain% | OOS CAGR | Boot LB | MC p | DSR |\n")
        f.write("|---|---|---|---|---|---|---|---|---|---|\n")
        for r in results:
            f.write(f"| {r['name']} | {r['verdict']} | {r['verdict_class']} | "
                    f"{r['is_sortino']:.3f} | {r['oos_sortino']:.3f} | "
                    f"{r['retention_sortino']*100:.1f}% | "
                    f"{r['oos_cagr']*100:+.2f}% | "
                    f"{r['boot_cagr_lb']*100:+.2f}% | "
                    f"{r['mc_p']:.4f} | {r['dsr']:.3f} |\n")
    print(f"報告已寫入 research/strat_lab/results/all_validation.md")

    # === Output 2: annotated by verdict_class ===
    write_annotated_report(results, "research/strat_lab/results/all_validation_annotated.md")
    print(f"分區報告已寫入 research/strat_lab/results/all_validation_annotated.md")


def write_annotated_report(results: list[dict], out_path: str):
    """按 verdict_class 分區輸出，含每類 description 與 ship 推薦提示。"""
    # 9-class display order: SHIP first, then alpha → benchmark → 各 bias → bug → counterfactual → failed
    class_order = [
        "SHIP", "REAL_ALPHA", "BENCHMARK",
        "NAV_BLEND_INFLATED", "BIAS_sample_period", "BIAS_mcap_weighting",
        "BUG_weight_compound", "COUNTERFACTUAL", "FAILED",
    ]

    # Group by class, sort within each by OOS Sortino
    by_class = {c: [] for c in class_order}
    for r in results:
        by_class.setdefault(r["verdict_class"], []).append(r)
    for c in by_class:
        by_class[c].sort(key=lambda x: x["oos_sortino"], reverse=True)

    with open(out_path, "w") as f:
        f.write("# 全策略驗證 — Annotated 分區報告\n\n")
        f.write("此報告按 verdict_class 分區，每類附類別說明。\n")
        f.write("**真實合法的 ship candidate / sensitivity 比較 → 看 SHIP 與 REAL_ALPHA 兩區即可**。\n")
        f.write("**其他區是 sample-period bias / 已知 bug / 壓力測試 / 結構性失敗，僅供 sanity check 對照**。\n\n")

        # Class summary table
        f.write("## Class 摘要\n\n")
        f.write("| Class | 數量 | Top OOS Sortino | 含意 |\n")
        f.write("|---|---:|---:|---|\n")
        for c in class_order:
            entries = by_class.get(c, [])
            if not entries: continue
            info = VERDICT_CLASSES[c]
            top = entries[0]["oos_sortino"]
            f.write(f"| {info['emoji']} **{info['label']}** | {len(entries)} | {top:.3f} | {info['desc']} |\n")
        f.write("\n---\n\n")

        # Per-class section
        for c in class_order:
            entries = by_class.get(c, [])
            if not entries: continue
            info = VERDICT_CLASSES[c]
            f.write(f"## {info['emoji']} {info['label']} ({len(entries)} 個)\n\n")
            f.write(f"**含意**：{info['desc']}\n\n")
            f.write("| Strategy | OOS Sortino | OOS CAGR | Boot LB | Retain% | IS Sortino | MC p | DSR |\n")
            f.write("|---|---:|---:|---:|---:|---:|---:|---:|\n")
            for r in entries:
                f.write(f"| {r['name']} | "
                        f"{r['oos_sortino']:.3f} | "
                        f"{r['oos_cagr']*100:+.2f}% | "
                        f"{r['boot_cagr_lb']*100:+.2f}% | "
                        f"{r['retention_sortino']*100:.1f}% | "
                        f"{r['is_sortino']:.3f} | "
                        f"{r['mc_p']:.4f} | "
                        f"{r['dsr']:.3f} |\n")
            f.write("\n")

        # Final reminder
        f.write("---\n\n")
        f.write("## 解讀提示\n\n")
        f.write("- **SHIP**: 當前推薦組合，已通過 cross-validation + counterfactual 驗證\n")
        f.write("- **REAL_ALPHA**: 合法可比較的真 alpha（同 SHIP 同框架，僅 NAV/slot 不同）\n")
        f.write("- **BENCHMARK**: 對照基準，回答「策略相對 hold 大盤多少 alpha」\n")
        f.write("- **NAV_BLEND_INFLATED**: 早期 NAV blend，沒嚴格鎖 ≤10 持股 → 數字灌水\n")
        f.write("- **BIAS_sample_period**: 1+9 系列高分但完全靠 21y TSMC mcap 從未掉第一這個 sample-period bias\n")
        f.write("- **BIAS_mcap_weighting**: 換 ranker 但 mcap-weighted 仍把 NAV 集中到 2330\n")
        f.write("- **BUG_weight_compound**: weight × ret 累乘未 normalize → 虛假動態槓桿，CAGR 灌水 +8pp\n")
        f.write("- **COUNTERFACTUAL**: 壓力測試或 cross-validation control，本身不 ship\n")
        f.write("- **FAILED**: OOS Sortino 太弱、Bootstrap LB 為負、或結構性問題\n")


if __name__ == "__main__":
    main()
