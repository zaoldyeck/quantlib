"""Professional validation for serenity_event_engine_v1 champion configs.

Runs, per candidate NAV series:
- Full-window + per-fold metrics (yearly folds for long windows, quarterly for
  short), Lo(2002) autocorrelation-robust Sharpe test.
- Deflated Sharpe Ratio with the campaign-wide trial count.
- PBO via combinatorially symmetric CV over the folds.
- Block bootstrap CI for CAGR/Sortino (year blocks if >=4 years else month blocks).
- Selection-permutation test: rerun the event engine with the scored top list
  replaced by random draws from the SAME day's post-filter eligible pool
  (theme + liquidity + risk gates kept). This separates theme/gate beta from
  ranking skill.
- Correlation + 50/50 blend versus the Iter95 realistic-execution champion NAV
  (indicative only: execution models differ).

Usage:
  uv run --project research python research/serenity/validate.py \
      --variant ev_full_tp60 --n-perm 200
"""

from __future__ import annotations

import argparse
import math
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[2]
RESEARCH_ROOT = REPO_ROOT / "research"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(RESEARCH_ROOT))
sys.path.insert(0, str(RESEARCH_ROOT / "serenity"))
sys.path.insert(0, str(RESEARCH_ROOT / "strat_lab"))

from constants import CAPITAL  # noqa: E402
from db import connect  # noqa: E402

from validate_hybrid import (  # noqa: E402
    TDPY,
    bootstrap_ci,
    deflated_sharpe,
    lo_2002_sharpe_test,
    metrics,
    pbo_cscv,
)

import engine as eng  # noqa: E402
from replay_2025 import (  # noqa: E402
    REGISTRY,
    active_registry_for_day,
    load_registry,
    load_point_in_time_table,
    load_price_features,
    load_revenue_features,
    load_taxonomy,
    load_universe,
    row_latest_before,
    score_candidates,
)

RESULTS = REPO_ROOT / "research" / "strat_lab" / "results"
DOCS = REPO_ROOT / "docs" / "serenity"
CHAMPION_NAV = (
    RESULTS / "iter_95_global_exit_aware_search_iter92_unconstrained_meta_switch__time50_r-1_daily.csv"
)
N_TRIALS_CAMPAIGN = 40  # unique engine configs examined in this campaign, rounded up


def read_nav(path: Path) -> pl.DataFrame:
    df = pl.read_csv(path, try_parse_dates=True).select(["date", "nav"]).sort("date")
    if df["date"].dtype != pl.Date:
        df = df.with_columns(pl.col("date").cast(pl.Date))
    return df


def rets_from_nav(df: pl.DataFrame) -> tuple[np.ndarray, list[date]]:
    nav = df["nav"].to_numpy().astype(float)
    rets = nav[1:] / nav[:-1] - 1.0
    return rets, df["date"].to_list()[1:]


def period_folds(rets: np.ndarray, dates: list[date], freq: str) -> list[dict]:
    df = pl.DataFrame({"date": dates, "ret": rets}).with_columns(
        pl.col("date").dt.year().alias("year"), pl.col("date").dt.quarter().alias("quarter")
    )
    folds = []
    if freq == "year":
        groups = df.group_by("year", maintain_order=True)
    else:
        groups = df.with_columns((pl.col("year") * 10 + pl.col("quarter")).alias("yq")).group_by(
            "yq", maintain_order=True
        )
    for key, g in groups:
        if g.height < 20:
            continue
        m = metrics(g["ret"].to_numpy(), years=g.height / TDPY)
        folds.append({"fold": key[0] if isinstance(key, tuple) else key, "n_days": g.height, **m})
    return folds


def month_block_bootstrap(rets: np.ndarray, dates: list[date], n: int = 2000) -> dict:
    df = pl.DataFrame({"date": dates, "ret": rets}).with_columns(
        (pl.col("date").dt.year() * 100 + pl.col("date").dt.month()).alias("ym")
    )
    blocks = [g["ret"].to_numpy() for _, g in df.group_by("ym", maintain_order=True)]
    rng = np.random.default_rng(42)
    cagrs, sortinos = [], []
    for _ in range(n):
        idx = rng.integers(0, len(blocks), size=len(blocks))
        sample = np.concatenate([blocks[i] for i in idx])
        m = metrics(sample, years=len(sample) / TDPY)
        cagrs.append(m["cagr"])
        sortinos.append(m["sortino"])
    return {
        "cagr_lb95": float(np.quantile(cagrs, 0.05)),
        "cagr_med": float(np.quantile(cagrs, 0.50)),
        "sortino_lb95": float(np.quantile(sortinos, 0.05)),
    }


def validate_series(name: str, path: Path) -> dict:
    df = read_nav(path)
    rets, dates = rets_from_nav(df)
    years = (dates[-1] - dates[0]).days / 365.25
    m = metrics(rets, years=years)
    lo = lo_2002_sharpe_test(rets)
    freq = "year" if years >= 4 else "quarter"
    folds = period_folds(rets, dates, freq)
    pbo = pbo_cscv(folds)
    if years >= 4:
        boot = bootstrap_ci(rets, dates)
    else:
        boot = month_block_bootstrap(rets, dates)
    dsr = deflated_sharpe(m["sharpe"], N_TRIALS_CAMPAIGN, rets)
    fold_cagrs = [f["cagr"] for f in folds]
    return {
        "name": name,
        "window": f"{dates[0]}~{dates[-1]}",
        "cagr": m["cagr"],
        "sharpe": m["sharpe"],
        "sortino": m["sortino"],
        "mdd": m["mdd"],
        "lo_t": lo.get("t_stat", float("nan")),
        "lo_p": lo.get("p_value", float("nan")),
        "dsr": dsr,
        "pbo": pbo,
        "boot_cagr_lb95": boot.get("cagr_lb95", float("nan")),
        "boot_sortino_lb95": boot.get("sortino_lb95", float("nan")),
        "folds": len(folds),
        "fold_cagr_min": min(fold_cagrs) if fold_cagrs else float("nan"),
        "fold_pos_share": float(np.mean([c > 0 for c in fold_cagrs])) if fold_cagrs else float("nan"),
    }


def selection_permutation(
    variant_name: str,
    start: date,
    mode: str,
    activation_lag_days: int,
    n_perm: int,
    actual_cagr: float,
    registry_path: Path | None = None,
) -> dict:
    """Re-run the engine with random picks from each refresh day's eligible pool."""
    con = connect(read_only=True)
    try:
        cutoff = con.sql("select max(date) from daily_quote").fetchone()[0]
        load_start = start - timedelta(days=420)
        if mode == "registry":
            registry = load_registry(registry_path or REGISTRY)
            if activation_lag_days:
                registry["active_from"] = registry["active_from"].map(
                    lambda v: v + timedelta(days=activation_lag_days)
                )
            universe = load_universe(con, registry["company_code"].tolist())
        else:
            registry = None
            universe = eng.load_universe_history(con)
        taxonomy = load_taxonomy(con, universe["company_code"].tolist())
        price_features, _ = load_price_features(con, universe, load_start, cutoff)
        revenue = load_revenue_features(con)
        per = load_point_in_time_table(con, "stock_per_pbr", ["price_to_earning_ratio", "price_book_ratio"])
        _uni_sql = ",".join(f"'{c}'" for c in sorted(set(universe["company_code"])))
        flows = (
            con.sql(
                f"SELECT date, company_code, total_difference AS inst_diff "
                f"FROM daily_trading_details WHERE company_code IN ({_uni_sql})"
            )
            .pl()
            .with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4))
            .sort(["company_code", "date"])
            .with_columns(
                pl.col("inst_diff").rolling_sum(20, min_samples=5)
                .over("company_code").alias("inst_20d")
            )
            .to_pandas()
        )

        px = price_features.copy()
        px["_date"] = pd.to_datetime(px["date"]).dt.date
        trading_days = sorted(d for d in px["_date"].unique() if start <= d <= cutoff)
        refresh_days = eng.build_refresh_days(trading_days, start, cutoff)
        window = px[px["_date"].isin(set(trading_days))]
        close_by_day = {d: g.set_index("company_code")["close"].to_dict() for d, g in window.groupby("_date")}
        ret_by_day = {
            d: g.set_index("company_code")["ret_1d"].fillna(0.0).to_dict() for d, g in window.groupby("_date")
        }

        eligible_by_refresh: dict[date, pd.DataFrame] = {}
        thesis_ok_by_refresh: dict[date, dict[str, bool]] = {}
        for day in refresh_days:
            tax_day = row_latest_before(taxonomy, day, "effective_date")
            tax_day = tax_day[(tax_day["is_financial"] == False) & (tax_day["is_special_category"] == False)]
            rev_day = row_latest_before(revenue, day, "report_date")
            if mode == "registry":
                active = active_registry_for_day(registry, day)
            else:
                active = eng.mechanical_registry_for_day(day, tax_day, rev_day)
            if active.empty:
                continue
            joined = (
                active.merge(tax_day, on="company_code", how="inner")
                .merge(px[px["_date"] == day], on="company_code", how="inner")
                .merge(rev_day, on="company_code", how="left", suffixes=("", "_rev"))
                .merge(row_latest_before(per, day, "date"), on="company_code", how="left")
                .merge(
                    row_latest_before(flows[["date", "company_code", "inst_20d"]], day, "date"),
                    on="company_code",
                    how="left",
                )
            )
            scored = score_candidates(joined)  # keeps the gates; ranking is what we permute
            if scored.empty:
                continue
            eligible_by_refresh[day] = scored.reset_index(drop=True)
            thesis_ok_by_refresh[day] = {
                str(r.company_code): not (pd.notna(r.yoy_3m) and float(r.yoy_3m) < 0.0)
                for r in rev_day.itertuples(index=False)
            }

        variant = next(v for v in eng.VARIANTS if v.name == variant_name)
        rng = np.random.default_rng(7)
        perm_cagrs: list[float] = []
        for _ in range(n_perm):
            shuffled = {
                day: pool.sample(frac=1.0, random_state=int(rng.integers(0, 2**31 - 1))).head(40)
                for day, pool in eligible_by_refresh.items()
            }
            daily, _trades, _to = eng.simulate_event_variant(
                variant, trading_days, close_by_day, ret_by_day, shuffled, thesis_ok_by_refresh, {}
            )
            nav = daily["nav"].to_numpy()
            yrs = (trading_days[-1] - trading_days[0]).days / 365.25
            perm_cagrs.append((nav[-1] / CAPITAL) ** (1 / yrs) - 1.0)
        arr = np.array(perm_cagrs)
        return {
            "n_perm": n_perm,
            "perm_cagr_med": float(np.median(arr)),
            "perm_cagr_p95": float(np.quantile(arr, 0.95)),
            "p_value": float((arr >= actual_cagr).mean()),
        }
    finally:
        con.close()


def blend_with_champion(nav_path: Path) -> dict:
    if not CHAMPION_NAV.exists():
        return {}
    a = read_nav(nav_path).rename({"nav": "nav_a"})
    b = read_nav(CHAMPION_NAV).rename({"nav": "nav_b"})
    j = a.join(b, on="date", how="inner").sort("date")
    if j.height < 60:
        return {}
    ra = j["nav_a"].to_numpy()
    rb = j["nav_b"].to_numpy()
    ra = ra[1:] / ra[:-1] - 1.0
    rb = rb[1:] / rb[:-1] - 1.0
    corr = float(np.corrcoef(ra, rb)[0, 1])
    blend = 0.5 * ra + 0.5 * rb
    yrs = (j["date"][-1] - j["date"][0]).days / 365.25
    mb = metrics(blend, years=yrs)
    ma = metrics(ra, years=yrs)
    mc = metrics(rb, years=yrs)
    return {
        "overlap": f"{j['date'][0]}~{j['date'][-1]}",
        "corr_daily": corr,
        "engine_cagr": ma["cagr"],
        "champion_cagr": mc["cagr"],
        "blend_cagr": mb["cagr"],
        "engine_mdd": ma["mdd"],
        "champion_mdd": mc["mdd"],
        "blend_mdd": mb["mdd"],
        "blend_sortino": mb["sortino"],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--variant", default="ev_full_tp60")
    parser.add_argument("--n-perm", type=int, default=200)
    parser.add_argument("--skip-perm", action="store_true")
    parser.add_argument(
        "--registry", default=None,
        help="alternative registry CSV — permutation eligible pool matches the NAV series' pool",
    )
    args = parser.parse_args()

    series = [
        (f"registry_lag0/{args.variant}", RESULTS / f"serenity_event_engine_v1_{args.variant}_daily.csv"),
        (f"registry_lag90/{args.variant}", RESULTS / f"serenity_event_engine_v1_lag90_{args.variant}_daily.csv"),
        (
            f"registry_lag180/{args.variant}",
            RESULTS / f"serenity_event_engine_v1_lag180_{args.variant}_daily.csv",
        ),
        (f"mech_2018/{args.variant}", RESULTS / f"serenity_event_engine_v1_mech_{args.variant}_daily.csv"),
    ]
    rows = []
    for name, path in series:
        if path.exists():
            rows.append(validate_series(name, path))
        else:
            print(f"skip missing {path.name}")
    table = pd.DataFrame(rows)
    pd.set_option("display.width", 220)
    print(table.to_string(index=False, float_format=lambda x: f"{x:.4f}"))

    perm_results = {}
    if not args.skip_perm:
        base = next((r for r in rows if r["name"].startswith("registry_lag0")), None)
        if base:
            perm_results["registry_lag0"] = selection_permutation(
                args.variant, date(2025, 1, 1), "registry", 0, args.n_perm, base["cagr"],
                registry_path=Path(args.registry) if args.registry else None,
            )
            print("perm registry_lag0:", perm_results["registry_lag0"])
        # Mechanical mode showed no risk-adjusted alpha vs 2330/0050, so a
        # selection-permutation test there is uninformative; registry-mode
        # permutation (does ranking add value inside the curated pool?) is the
        # meaningful one.

    blend = blend_with_champion(RESULTS / f"serenity_event_engine_v1_{args.variant}_daily.csv")
    if blend:
        print("blend vs iter95 champion (execution models differ — indicative):")
        for k, v in blend.items():
            print(f"  {k}: {v}")

    out = DOCS / f"serenity_event_engine_v1_validation_{args.variant}.md"
    lines = [
        f"# serenity_event_engine_v1 validation — {args.variant}",
        "",
        f"- campaign trial count for DSR: {N_TRIALS_CAMPAIGN}",
        "",
        table.to_markdown(index=False, floatfmt=".4f"),
        "",
        f"- selection permutation: {perm_results}",
        f"- champion blend: {blend}",
    ]
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"report -> {out}")


if __name__ == "__main__":
    main()
