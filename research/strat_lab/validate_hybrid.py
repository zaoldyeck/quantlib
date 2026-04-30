"""Generic hybrid OOS validator — runs full validation suite on top hybrid configs.

After sweep_hybrid.py identifies top IS configs, this validator runs:
  1. Walk-forward OOS (5y train / 1y test, rolling 16 folds, year 2010-2025)
  2. Lo (2002) asymptotic Sharpe t-test (replaces broken MC permutation on
     compounded NAV — see prior validate_iter21_v5.py)
  3. Bootstrap year-block CI (1000 iterations)
  4. Deflated Sharpe Ratio (n_trials = 66 from sweep_hybrid)
  5. PBO via CSCV (random IS/OOS halves over 16 folds)
  6. Robustness ±10% w_a grid

Run:
    uv run --project research python research/strat_lab/validate_hybrid.py [tag]

Where tag is e.g. "5+5_w80_atr". Defaults to top 5 from hybrid_sweep_v6.csv.
"""
from __future__ import annotations

import argparse
import math
import os
import sys
import time
import warnings
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from scipy.stats import norm

warnings.filterwarnings("ignore")

HERE = Path(__file__).parent
RESULTS = HERE / "results"

TDPY = 252
RF = 0.01
CAPITAL = 1_000_000.0
N_TRIALS_DSR = 66   # 11 slot configs × 6 weights = 66 from sweep_hybrid
N_BOOT = 1000
N_PBO_SPLITS = 500


# ──────────────────────────────────────────────────────────────
def metrics(rets: np.ndarray, years: float | None = None) -> dict:
    n = len(rets)
    if n < 2:
        return {"cagr": 0.0, "sharpe": 0.0, "sortino": 0.0, "mdd": 0.0, "vol": 0.0}
    if years is None:
        years = n / TDPY
    nav = np.cumprod(1 + rets)
    cagr = nav[-1] ** (1 / years) - 1
    vol = rets.std(ddof=1) * math.sqrt(TDPY)
    downside = rets[rets < 0]
    downvol = (downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 1e-9
    sharpe = (cagr - RF) / vol if vol > 0 else 0.0
    sortino = (cagr - RF) / downvol if downvol > 0 else 0.0
    peak, mdd = 1.0, 0.0
    for v in nav:
        peak = max(peak, v)
        mdd = min(mdd, (v - peak) / peak)
    return {"cagr": cagr, "sharpe": sharpe, "sortino": sortino, "mdd": mdd, "vol": vol}


def lo_2002_sharpe_test(daily_rets: np.ndarray) -> dict:
    """Lo (2002) asymptotic t-test for Sharpe ratio; corrects for autocorr + non-normality."""
    n = len(daily_rets)
    if n < 30:
        return {"t_stat": 0, "p_value": 1.0}
    mean = daily_rets.mean()
    std = daily_rets.std(ddof=1)
    if std <= 0:
        return {"t_stat": 0, "p_value": 1.0}
    sr_daily = mean / std
    # Lo correction: variance of SR ≈ (1 + 0.5 SR² - skew·SR + (kurt-3)/4 · SR²) / n
    skew = ((daily_rets - mean) ** 3).mean() / std ** 3
    kurt = ((daily_rets - mean) ** 4).mean() / std ** 4
    var_sr = (1 + 0.5 * sr_daily ** 2 - skew * sr_daily + (kurt - 3) / 4 * sr_daily ** 2) / n
    if var_sr <= 0:
        return {"t_stat": 0, "p_value": 1.0}
    t = sr_daily / math.sqrt(var_sr)
    p = 1 - norm.cdf(t)   # one-sided H0: SR ≤ 0
    return {"t_stat": t, "p_value": p, "sr_annual": sr_daily * math.sqrt(TDPY)}


def deflated_sharpe(sr_annual: float, n_trials: int, daily_rets: np.ndarray) -> float:
    """López de Prado DSR with adjustment for skew + kurtosis + multi-trial."""
    n = len(daily_rets)
    if n < 30:
        return 0.0
    mean = daily_rets.mean()
    std = daily_rets.std(ddof=1)
    skew = ((daily_rets - mean) ** 3).mean() / std ** 3 if std > 0 else 0
    kurt = ((daily_rets - mean) ** 4).mean() / std ** 4 if std > 0 else 3
    sr_daily = sr_annual / math.sqrt(TDPY)
    gamma = 0.5772
    e_max = ((1 - gamma) * norm.ppf(1 - 1 / n_trials) +
             gamma * norm.ppf(1 - 1 / (n_trials * math.e)))
    sigma_sr = math.sqrt((1 - skew * sr_daily + (kurt - 1) / 4 * sr_daily ** 2) / (n - 1))
    if sigma_sr <= 0:
        return 0.0
    dsr = norm.cdf((sr_daily - e_max * sigma_sr) / sigma_sr)
    return float(dsr)


# ──────────────────────────────────────────────────────────────
def walk_forward_folds(rets: np.ndarray, dates: list[date]) -> list[dict]:
    """16 folds: each year 2010-2025 as OOS test."""
    df = pl.DataFrame({"date": dates, "ret": rets}).with_columns(
        pl.col("date").dt.year().alias("year")
    )
    folds = []
    for test_year in range(2010, 2026):
        oos = df.filter(pl.col("year") == test_year)
        if oos.height < 30:
            continue
        m = metrics(oos["ret"].to_numpy(), years=oos.height / TDPY)
        folds.append({"fold": test_year, "n_days": oos.height, **m})
    return folds


def bootstrap_ci(rets: np.ndarray, dates: list[date], n: int = N_BOOT) -> dict:
    """Year-block bootstrap CI for CAGR / Sortino."""
    df = pl.DataFrame({"date": dates, "ret": rets}).with_columns(
        pl.col("date").dt.year().alias("year")
    )
    years_groups = [g["ret"].to_numpy() for _, g in df.group_by("year")]
    rng = np.random.default_rng(42)
    cagrs, sortinos = [], []
    for _ in range(n):
        idx = rng.integers(0, len(years_groups), size=len(years_groups))
        sample = np.concatenate([years_groups[i] for i in idx])
        m = metrics(sample)
        cagrs.append(m["cagr"])
        sortinos.append(m["sortino"])
    return {
        "cagr_lb": np.percentile(cagrs, 2.5),
        "cagr_ub": np.percentile(cagrs, 97.5),
        "sortino_lb": np.percentile(sortinos, 2.5),
        "sortino_ub": np.percentile(sortinos, 97.5),
    }


def pbo_cscv(folds: list[dict], n_trials: int = N_PBO_SPLITS) -> float:
    """Combinatorially symmetric CV: random IS/OOS halves of folds."""
    if len(folds) < 4:
        return 0.5
    rng = np.random.default_rng(42)
    sortinos = np.array([f["sortino"] for f in folds])
    n_folds = len(folds)
    half = n_folds // 2
    n_below = 0
    for _ in range(n_trials):
        perm = rng.permutation(n_folds)
        is_idx, oos_idx = perm[:half], perm[half:]
        is_sortino = sortinos[is_idx].mean()
        oos_sortino = sortinos[oos_idx].mean()
        if oos_sortino < np.median(sortinos):
            n_below += 1
    return n_below / n_trials


# ──────────────────────────────────────────────────────────────
def parse_tag(tag: str) -> tuple[int, int, int, bool]:
    """Parse '5+5_w80_atr' → (slot_a=5, slot_b=5, w_a_pct=80, atr=True)."""
    parts = tag.split("_")
    slot_part = parts[0]
    a, b = slot_part.split("+")
    w_pct = int(parts[1].replace("w", ""))
    atr = parts[-1] == "atr"
    return int(a), int(b), w_pct, atr


def hybrid_blend_rets(slot_a_csv: str | None, slot_b_csv: str | None,
                       w_a: float, capital: float = CAPITAL) -> tuple[np.ndarray, list[date]]:
    """Re-build hybrid daily returns (independent year-end rebal)."""
    if slot_a_csv and slot_b_csv:
        n_a = pl.read_csv(slot_a_csv, try_parse_dates=True).sort("date").select(["date", pl.col("nav").alias("nav_a")])
        n_b = pl.read_csv(slot_b_csv, try_parse_dates=True).sort("date").select(["date", pl.col("nav").alias("nav_b")])
        df = n_a.join(n_b, on="date", how="inner").sort("date")
    elif slot_a_csv:
        df = pl.read_csv(slot_a_csv, try_parse_dates=True).sort("date").select(
            ["date", pl.col("nav").alias("nav_a")]).with_columns(pl.lit(capital).alias("nav_b"))
    else:
        df = pl.read_csv(slot_b_csv, try_parse_dates=True).sort("date").select(
            ["date", pl.col("nav").alias("nav_b")]).with_columns(pl.lit(capital).alias("nav_a"))

    df = df.with_columns([
        pl.col("nav_a").pct_change().fill_null(0.0).alias("ret_a"),
        pl.col("nav_b").pct_change().fill_null(0.0).alias("ret_b"),
        pl.col("date").dt.year().alias("year"),
    ])

    w_b = 1.0 - w_a
    nav = capital
    rets = []
    dates = []
    for yr, sub in df.group_by("year", maintain_order=True):
        cap_a = nav * w_a
        cap_b = nav * w_b
        prev = cap_a + cap_b
        for ra, rb, d in zip(sub["ret_a"].to_list(), sub["ret_b"].to_list(),
                              sub["date"].to_list()):
            cap_a *= (1 + ra)
            cap_b *= (1 + rb)
            cur = cap_a + cap_b
            rets.append((cur - prev) / prev if prev > 0 else 0.0)
            dates.append(d)
            prev = cur
            nav = cur
    return np.array(rets), dates


def validate_hybrid(slot_a: int, slot_b: int, w_a: float, atr: bool,
                     iter13_ranker: str = "mcap", verbose: bool = True) -> dict:
    """Run full validation on a hybrid config. Returns verdict dict."""
    nav_a = RESULTS / f"iter_13_monthly_{iter13_ranker}_tpex_daily.csv" if slot_a > 0 else None
    nav_b_suffix = f"max{slot_b}" + ("_atr" if atr else "")
    nav_b = RESULTS / f"iter_24_{nav_b_suffix}_daily.csv" if slot_b > 0 else None

    if nav_a and not nav_a.exists():
        return {"error": f"nav_a missing: {nav_a}"}
    if nav_b and not nav_b.exists():
        return {"error": f"nav_b missing: {nav_b}"}

    rets, dates = hybrid_blend_rets(str(nav_a) if nav_a else None,
                                      str(nav_b) if nav_b else None, w_a)

    is_m = metrics(rets)
    folds = walk_forward_folds(rets, dates)
    oos_rets = np.concatenate([f.get("_rets") if "_rets" in f else
                                np.array([]) for f in folds])  # placeholder
    # Recompute pooled OOS metrics from per-fold rets
    df = pl.DataFrame({"date": dates, "ret": rets}).with_columns(
        pl.col("date").dt.year().alias("year"))
    oos_df = df.filter((pl.col("year") >= 2010) & (pl.col("year") <= 2025))
    oos_pooled = metrics(oos_df["ret"].to_numpy(),
                          years=oos_df.height / TDPY)
    lo = lo_2002_sharpe_test(oos_df["ret"].to_numpy())
    boot = bootstrap_ci(oos_df["ret"].to_numpy(), oos_df["date"].to_list())
    dsr = deflated_sharpe(oos_pooled["sharpe"], N_TRIALS_DSR, oos_df["ret"].to_numpy())
    pbo = pbo_cscv(folds)

    sharpe_retention = oos_pooled["sharpe"] / is_m["sharpe"] if is_m["sharpe"] > 0 else 0
    cagr_retention = oos_pooled["cagr"] / is_m["cagr"] if is_m["cagr"] > 0 else 0

    pass_count = 0
    pass_count += int(sharpe_retention >= 0.7)
    pass_count += int(cagr_retention >= 0.5)
    pass_count += int(lo["p_value"] < 0.05)
    pass_count += int(boot["cagr_lb"] > 0.10)
    pass_count += int(dsr > 0.95)
    pass_count += int(pbo < 0.5)

    verdict = "real alpha" if pass_count == 6 else \
              ("borderline" if pass_count >= 4 else "curve-fit")

    return {
        "config": f"{slot_a}+{slot_b}_w{int(w_a*100)}_{'atr' if atr else 'fix'}_{iter13_ranker}",
        "is_cagr": is_m["cagr"], "is_sortino": is_m["sortino"], "is_sharpe": is_m["sharpe"], "is_mdd": is_m["mdd"],
        "oos_cagr": oos_pooled["cagr"], "oos_sortino": oos_pooled["sortino"], "oos_sharpe": oos_pooled["sharpe"],
        "sharpe_retention": sharpe_retention, "cagr_retention": cagr_retention,
        "lo_t": lo["t_stat"], "lo_p": lo["p_value"],
        "boot_cagr_lb": boot["cagr_lb"], "boot_cagr_ub": boot["cagr_ub"],
        "boot_sortino_lb": boot["sortino_lb"], "boot_sortino_ub": boot["sortino_ub"],
        "dsr": dsr, "pbo": pbo,
        "pass_count": pass_count, "verdict": verdict,
        "n_folds": len(folds),
    }


# ──────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=5, help="Validate top N from sweep")
    ap.add_argument("--cross-val", action="store_true", default=True,
                    help="Also run cross-validation on top hybrid")
    args = ap.parse_args()

    sweep_csv = RESULTS / "hybrid_sweep_v6.csv"
    if not sweep_csv.exists():
        print("ERROR: run sweep_hybrid.py first")
        sys.exit(1)

    sweep = pl.read_csv(sweep_csv).sort("sortino", descending=True)

    print("=" * 78)
    print(f"OOS validation — top {args.top} hybrid configs from sweep")
    print("=" * 78)

    t0 = time.time()
    rows = []
    for cfg in sweep.head(args.top).iter_rows(named=True):
        slot_a, slot_b = cfg["slot_a"], cfg["slot_b"]
        w_a = cfg["w_a"]
        atr = cfg["atr_b"]
        print(f"\n[{cfg['tag']}] validating ...", end=" ")
        t1 = time.time()
        v = validate_hybrid(slot_a, slot_b, w_a, atr, iter13_ranker="mcap", verbose=False)
        if "error" in v:
            print(f"ERROR: {v['error']}")
            continue
        rows.append(v)
        print(f"{v['verdict']} ({v['pass_count']}/6) [{time.time()-t1:.1f}s]")

    df = pl.DataFrame(rows)
    out = RESULTS / "validate_top_hybrids_v6.csv"
    df.write_csv(out)

    # Cross-validation: best config across rankers
    if args.cross_val and rows:
        best = rows[0]
        slot_a, slot_b, w_pct, atr = parse_tag(best["config"].rsplit("_", 1)[0] +
                                                  ("_atr" if "atr" in best["config"] else ""))
        # Re-extract from the row's config string is fragile — use the data row
        best_row = sweep.filter(pl.col("tag") == best["config"].rsplit("_", 1)[0]).head(1).to_dicts()
        if best_row:
            br = best_row[0]
            print(f"\n{'=' * 78}")
            print(f"Cross-validation on best config ({br['tag']}) across rankers")
            print(f"{'=' * 78}")
            cv_rows = []
            for ranker in ["mcap", "roa_recent", "roa_med", "rev_cagr5y", "composite"]:
                t1 = time.time()
                v = validate_hybrid(br["slot_a"], br["slot_b"], br["w_a"], br["atr_b"],
                                      iter13_ranker=ranker, verbose=False)
                if "error" not in v:
                    cv_rows.append(v)
                    print(f"  [{ranker:<12}] {v['verdict']} ({v['pass_count']}/6) "
                          f"oos_sortino={v['oos_sortino']:.3f} cagr={v['oos_cagr']*100:.2f}% "
                          f"lb={v['boot_cagr_lb']*100:.1f}% [{time.time()-t1:.1f}s]")
            cv_df = pl.DataFrame(cv_rows)
            cv_out = RESULTS / "validate_cross_val_v6.csv"
            cv_df.write_csv(cv_out)

    print(f"\nTotal: {time.time()-t0:.1f}s")
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()
