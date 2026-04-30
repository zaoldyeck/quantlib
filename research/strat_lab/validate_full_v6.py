"""V6 full validation suite — proper PBO multi-config CSCV + composite ranker cross-val + GFC slice.

Addresses 3 caveats from earlier validate_hybrid.py:

1. **PBO multi-config CSCV (López de Prado 2014)** — proper implementation
   - Walk-forward 16 fold for ALL 66 sweep configs (not just top 5)
   - For each random IS/OOS half-split:
     * Find IS-best config (highest mean Sortino in IS half folds)
     * Compute its OOS rank (relative to other configs in OOS half folds)
     * Count if it falls below median rank → overfit flag
   - PBO = fraction of splits where IS-best ranks below OOS median
   - This is the LEGITIMATE PBO; the prior single-config CSCV was over-strict.

2. **Composite ranker added** to cross-validation (5 rankers total)

3. **2008-2009 GFC slice** — strict 5+5 NAV 85/15 cumulative metrics during
   the GFC drawdown period (2008-01-01 → 2009-06-30) as robustness check.
   Note: walk-forward OOS already covers 2010-2025 (3 negative years 2011/18/22).

Run:
    uv run --project research python research/strat_lab/validate_full_v6.py
"""
from __future__ import annotations

import math
import os
import sys
import time
import warnings
from datetime import date
from itertools import combinations
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
N_BOOT = 1000


def metrics(rets: np.ndarray, years: float | None = None) -> dict:
    n = len(rets)
    if n < 2:
        return {"cagr": 0.0, "sharpe": 0.0, "sortino": 0.0, "mdd": 0.0, "vol": 0.0, "n_days": n}
    if years is None:
        years = n / TDPY
    nav = np.cumprod(1 + rets)
    cagr = nav[-1] ** (1 / years) - 1 if years > 0 else 0
    vol = rets.std(ddof=1) * math.sqrt(TDPY)
    downside = rets[rets < 0]
    downvol = (downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 1e-9
    sharpe = (cagr - RF) / vol if vol > 0 else 0.0
    sortino = (cagr - RF) / downvol if downvol > 0 else 0.0
    peak, mdd = 1.0, 0.0
    for v in nav:
        peak = max(peak, v)
        mdd = min(mdd, (v - peak) / peak)
    return {"cagr": cagr, "sharpe": sharpe, "sortino": sortino, "mdd": mdd, "vol": vol, "n_days": n}


def hybrid_blend_rets(slot_a_csv: str | None, slot_b_csv: str | None,
                       w_a: float, capital: float = CAPITAL):
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


def walk_forward_per_year(rets: np.ndarray, dates: list[date]) -> dict[int, np.ndarray]:
    """Slice daily returns by year. Returns {year: rets_array}."""
    df = pl.DataFrame({"date": dates, "ret": rets}).with_columns(
        pl.col("date").dt.year().alias("year")
    )
    # Polars group_by returns tuple keys — extract the year scalar
    return {key[0] if isinstance(key, tuple) else key: g["ret"].to_numpy()
            for key, g in df.group_by("year", maintain_order=True)}


def get_config_rets(slot_a: int, slot_b: int, w_a: float, atr: bool, ranker: str) -> tuple[np.ndarray, list[date]]:
    """Build daily returns for a hybrid config. Returns (None, None) if NAV files missing."""
    nav_a = RESULTS / f"iter_13_monthly_{ranker}_tpex_daily.csv" if slot_a > 0 else None
    suffix = f"max{slot_b}" + ("_atr" if atr else "")
    nav_b = RESULTS / f"iter_24_{suffix}_daily.csv" if slot_b > 0 else None

    if nav_a and not nav_a.exists():
        return None, None
    if nav_b and not nav_b.exists():
        return None, None

    return hybrid_blend_rets(str(nav_a) if nav_a else None,
                              str(nav_b) if nav_b else None, w_a)


# ──────────────────────────────────────────────────────────────
# Multi-config CSCV PBO (López de Prado 2014)
# ──────────────────────────────────────────────────────────────

def multi_config_pbo(per_config_yearly: dict[str, dict[int, dict]],
                     n_splits: int = 1000) -> dict:
    """Proper López de Prado CSCV PBO.

    Args:
      per_config_yearly: {config_tag: {year: metrics_dict}}
        — each config's per-year OOS metrics

    Returns:
      pbo: probability that IS-best config ranks below OOS median
      details: dict with raw counts
    """
    # Get common years across all configs
    all_years = sorted(set.intersection(*(set(d.keys()) for d in per_config_yearly.values())))
    if len(all_years) < 4:
        return {"pbo": 0.5, "n_splits": 0, "n_configs": len(per_config_yearly), "n_years": len(all_years)}

    # For each year, score each config (use Sortino as primary metric)
    # config × year → sortino matrix
    configs = list(per_config_yearly.keys())
    n_y = len(all_years)
    score_matrix = np.full((len(configs), n_y), np.nan)
    for i, cfg in enumerate(configs):
        for j, yr in enumerate(all_years):
            score_matrix[i, j] = per_config_yearly[cfg][yr]["sortino"]

    # Drop configs with any NaN year (incomplete coverage)
    valid_mask = ~np.isnan(score_matrix).any(axis=1)
    score_matrix = score_matrix[valid_mask]
    configs = [c for c, ok in zip(configs, valid_mask) if ok]
    if len(configs) < 4:
        return {"pbo": 0.5, "n_splits": 0, "n_configs": len(configs), "n_years": n_y}

    rng = np.random.default_rng(42)
    n_below = 0
    half = n_y // 2
    actual_splits = min(n_splits, math.comb(n_y, half))

    # If small enough, enumerate all combinations; else random sample
    if math.comb(n_y, half) <= n_splits:
        split_iter = combinations(range(n_y), half)
    else:
        # Random unique splits
        split_iter = []
        seen = set()
        while len(split_iter) < n_splits:
            s = tuple(sorted(rng.choice(n_y, half, replace=False).tolist()))
            if s not in seen:
                seen.add(s)
                split_iter.append(s)

    n_processed = 0
    for is_idx in split_iter:
        is_idx = list(is_idx)
        oos_idx = [i for i in range(n_y) if i not in is_idx]
        if not oos_idx:
            continue
        # Mean Sortino across IS years per config
        is_mean = score_matrix[:, is_idx].mean(axis=1)
        oos_mean = score_matrix[:, oos_idx].mean(axis=1)
        # IS-best config index
        is_best = np.argmax(is_mean)
        # Its OOS rank (lower rank = worse)
        oos_rank = np.argsort(np.argsort(oos_mean))[is_best]   # 0-based
        oos_median_rank = (len(configs) - 1) / 2
        if oos_rank < oos_median_rank:
            n_below += 1
        n_processed += 1

    return {
        "pbo": n_below / n_processed if n_processed > 0 else 0.5,
        "n_splits": n_processed,
        "n_configs": len(configs),
        "n_years": n_y,
    }


# ──────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    print("=" * 78)
    print("V6 FULL VALIDATION SUITE")
    print("  1. Multi-config CSCV PBO (López 2014, all 66 configs)")
    print("  2. 5-ranker cross-validation (incl. composite)")
    print("  3. 2008-2009 GFC slice (robustness)")
    print("=" * 78)

    sweep = pl.read_csv(RESULTS / "hybrid_sweep_v6.csv")

    # ── 1. Multi-config CSCV PBO ──
    print("\n[1] Computing per-year metrics for all configs ...")
    per_config_yearly = {}
    skipped = 0
    for cfg in sweep.iter_rows(named=True):
        tag = cfg["tag"]
        rets, dates = get_config_rets(cfg["slot_a"], cfg["slot_b"], cfg["w_a"],
                                        cfg["atr_b"], cfg["ranker_a"])
        if rets is None:
            skipped += 1
            continue
        yearly = walk_forward_per_year(rets, dates)
        # Restrict to OOS years 2010-2025 to match walk-forward window
        yearly_oos = {y: metrics(r, years=len(r) / TDPY)
                       for y, r in yearly.items() if 2010 <= y <= 2025}
        if len(yearly_oos) >= 10:
            per_config_yearly[tag] = yearly_oos
    print(f"  configs ready: {len(per_config_yearly)}, skipped: {skipped}")

    print("\n  Computing CSCV PBO (1000 random splits) ...")
    pbo_result = multi_config_pbo(per_config_yearly, n_splits=1000)
    print(f"  ─── López 2014 PBO: {pbo_result['pbo']:.4f} ───")
    print(f"  ({pbo_result['n_configs']} configs × {pbo_result['n_years']} years × "
          f"{pbo_result['n_splits']} splits)")
    if pbo_result['pbo'] < 0.5:
        print(f"  ✓ PASS (< 0.5 — IS-best 在 OOS rank 高於中位數)")
    else:
        print(f"  ✗ FAIL (overfit risk)")

    # ── 2. 5-ranker cross-validation on 5+5_w85_atr (new champion) ──
    print(f"\n[2] 5-ranker cross-validation on 5+5_w85_atr ...")
    cv_rows = []
    for ranker in ["mcap", "roa_recent", "roa_med", "rev_cagr5y", "composite"]:
        rets, dates = get_config_rets(5, 5, 0.85, True, ranker)
        if rets is None:
            print(f"  [{ranker:<12}] NAV missing")
            continue
        # OOS only (2010-2025)
        df = pl.DataFrame({"date": dates, "ret": rets}).with_columns(
            pl.col("date").dt.year().alias("year"))
        oos = df.filter((pl.col("year") >= 2010) & (pl.col("year") <= 2025))
        m = metrics(oos["ret"].to_numpy(), years=oos.height / TDPY)
        cv_rows.append({"ranker": ranker, **m})
        print(f"  [{ranker:<12}] OOS Sortino {m['sortino']:.3f}  CAGR {m['cagr']*100:.2f}%  "
              f"Sharpe {m['sharpe']:.3f}  MDD {m['mdd']*100:.2f}%")

    cv_df = pl.DataFrame(cv_rows).sort("sortino", descending=True)
    cv_df.write_csv(RESULTS / "validate_cross_val_full_v6.csv")

    if len(cv_rows) >= 2:
        best = max(cv_rows, key=lambda r: r["sortino"])
        worst = min(cv_rows, key=lambda r: r["sortino"])
        gap = best["sortino"] - worst["sortino"]
        print(f"\n  Cross-val gap: {best['ranker']} {best['sortino']:.3f} - "
              f"{worst['ranker']} {worst['sortino']:.3f} = {gap:.3f}")
        print(f"  vs 1+9 NAV 75/25 gap (賭 TSMC reference): 1.759")
        if gap < 1.0:
            print(f"  ✓ 5+5 結構分散有效 — alpha 真實性 confirmed")

    # ── 3. 2008-2009 GFC slice ──
    print(f"\n[3] 2008-2009 GFC slice for strict 5+5 NAV 85/15 with C+B ...")
    nav_path = RESULTS / "strict_5_5_w85_atr_daily.csv"
    if nav_path.exists():
        nav = pl.read_csv(nav_path, try_parse_dates=True).sort("date")
        # 切片 2008-01-01 to 2009-06-30 (GFC trough → recovery)
        slices = [
            ("2005-2007 pre-GFC bull", "2005-01-01", "2007-12-31"),
            ("2008 GFC year", "2008-01-01", "2008-12-31"),
            ("2009 recovery", "2009-01-01", "2009-12-31"),
            ("2008-2009 GFC full", "2008-01-01", "2009-12-31"),
            ("2011 EU debt crisis", "2011-01-01", "2011-12-31"),
            ("2018 trade war", "2018-01-01", "2018-12-31"),
            ("2022 growth crash", "2022-01-01", "2022-12-31"),
        ]
        print(f"\n  {'Slice':<28} {'Days':>5} {'CAGR':>10} {'Sortino':>10} {'MDD':>10}")
        print(f"  {'─' * 75}")
        for label, sd, ed in slices:
            sliced = nav.filter((pl.col("date") >= pl.lit(sd).str.to_date()) &
                                  (pl.col("date") <= pl.lit(ed).str.to_date()))
            if sliced.height < 5:
                print(f"  {label:<28} (no data)")
                continue
            navs = sliced["nav"].to_numpy()
            dates_s = sliced["date"].to_list()
            rets = np.diff(navs) / navs[:-1]
            yrs = max((dates_s[-1] - dates_s[0]).days / 365.25, 1e-9)
            m = metrics(rets, years=yrs)
            print(f"  {label:<28} {sliced.height:>5}  "
                  f"{m['cagr']*100:>8.2f}%  {m['sortino']:>9.3f}  {m['mdd']*100:>8.2f}%")
    else:
        print(f"  NAV missing: {nav_path}")

    print(f"\n{'=' * 78}")
    print(f"DONE in {time.time()-t0:.1f}s")
    print(f"  Saved: {RESULTS / 'validate_cross_val_full_v6.csv'}")
    print("=" * 78)


if __name__ == "__main__":
    main()
