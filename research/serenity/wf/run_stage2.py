"""戰役十八 Stage 2:出場軸格點(Stage 1 top-3 計分 × 出場配置 × 2 train 折).

出場配置:48 格點(tp×trail×abs×time,--grid-exit 一次載入)+ regime 自適應
(rgx)+ 主題失效(tdx)+ 兩者組合(rgx_tdx)。彙整全部 cells 的兩折 boot P5
幾何均排名。cells 總數 3×2×51 = 306(全計 DSR)。

Run: uv run --project research python -m research.serenity.wf.run_stage2 [--workers 2]
"""

from __future__ import annotations

import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))
ENGINE = REPO_ROOT / "research" / "serenity" / "engine.py"
REGISTRY = Path(__file__).parent / "registry_wf.csv"
RESULTS = REPO_ROOT / "research" / "strat_lab" / "results"
VARIANT = "ev_v2_thesis_inst"

from research.serenity.backfill.pool_quality_duel import boot_cagr_lb  # noqa: E402

SCORES = {
    "role20": ["--role-bonus", "20"],
    "nofilt": ["--ablate", "filters"],
    "noval": ["--ablate", "pe_pen,pb_pen"],
}
GRID = "tp=none,0.4,0.6,0.8;trail=0.2,0.3,none;abs=0.15,none;time=50,none"
EXITS = {
    "grid": ["--grid-exit", GRID],
    "rgx": ["--regime-exit"],
    "tdx": ["--theme-dead-exit"],
    "rgxtdx": ["--regime-exit", "--theme-dead-exit"],
}
FOLDS = {"F1": "2024-12-31", "F2": "2025-12-31"}


def run_one(score: str, exit_name: str, fold: str) -> str | None:
    label = f"b18s2_{score}_{exit_name}_{fold}"
    if list(RESULTS.glob(f"{label}_summary.csv")):
        return label
    cmd = [sys.executable, str(ENGINE), "--start", "2022-08-01", "--end", FOLDS[fold],
           "--registry", str(REGISTRY), "--variants", VARIANT, "--label", label,
           *SCORES[score], *EXITS[exit_name]]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=5400)
    if proc.returncode != 0:
        print(f"FAIL {label}: {proc.stderr[-300:]}")
        return None
    print(f"done {label}")
    return label


def consolidate() -> None:
    rng = np.random.default_rng(20260716)
    rows = []
    for score in SCORES:
        for exit_name in EXITS:
            for fold in FOLDS:
                label = f"b18s2_{score}_{exit_name}_{fold}"
                sp = RESULTS / f"{label}_summary.csv"
                if not sp.exists():
                    continue
                s = pd.read_csv(sp)
                cand = s[s.name.str.startswith(("g_", VARIANT))]
                for _, r in cand.iterrows():
                    dp = RESULTS / f"{label}_{r['name']}_daily.csv"
                    if not dp.exists():
                        continue
                    daily = pd.read_csv(dp, parse_dates=["date"])
                    nav = daily.set_index("date")["nav"]
                    mrets = nav.groupby(nav.index.astype(str).str.slice(0, 7)).last() \
                               .pct_change().dropna()
                    p5, _, _ = boot_cagr_lb(mrets, rng)
                    rows.append({"score": score, "exit": exit_name, "cell": r["name"],
                                 "fold": fold, "cagr": float(r["cagr"]),
                                 "sortino": float(r["sortino"]), "mdd": float(r["mdd"]),
                                 "boot_p5": round(p5, 4)})
    df = pd.DataFrame(rows)
    df["key"] = df.score + "|" + df.exit + "|" + df.cell
    wide = df.pivot(index="key", columns="fold", values="boot_p5").dropna()
    wide["p5_geo"] = np.sqrt((1 + wide["F1"]) * (1 + wide["F2"])) - 1
    rank = wide.sort_values("p5_geo", ascending=False)
    df.to_csv(Path(__file__).parent / "stage2_results.csv", index=False)
    rank.to_csv(Path(__file__).parent / "stage2_ranking.csv")
    print(f"\n=== Stage 2 排名 top-15(共 {len(rank)} cells)===")
    print(rank.head(15).round(3).to_string())


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=2)
    ns = ap.parse_args()
    jobs = [(s, e, f) for s in SCORES for e in EXITS for f in FOLDS]
    with ThreadPoolExecutor(max_workers=ns.workers) as pool:
        list(pool.map(lambda j: run_one(*j), jobs))
    consolidate()


if __name__ == "__main__":
    main()
