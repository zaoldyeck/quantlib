"""戰役十八 Stage 1:計分軸粗掃(固定現役出場+10 席).

14 cells × 2 個 train 折(F1: ~2024-12、F2: ~2025-12),每 cell 收 train 內
CAGR/Sortino/MDD 與月報酬 block bootstrap P5;排名 = 兩折 P5 幾何均。
嚴禁看 OOS 選型(OOS 只在 Stage 4 裁決時跑)。

Run: uv run --project research python -m research.serenity.wf.run_stage1
"""

from __future__ import annotations

import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
from research import paths

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))
ENGINE = REPO_ROOT / "research" / "serenity" / "engine.py"
REGISTRY = Path(__file__).parent / "registry_wf.csv"
RESULTS = paths.OUT_STRAT_LAB
VARIANT = "ev_v2_thesis_inst"

from research.serenity.backfill.pool_quality_duel import boot_cagr_lb  # noqa: E402

CELLS: list[tuple[str, list[str]]] = [
    ("S00_baseline", []),
    ("S01_momentum_only", ["--ablate", "conviction,revenue,adv,inst,pe_pen,pb_pen"]),
    ("S02_revenue_only", ["--ablate", "conviction,momentum,adv,inst,pe_pen,pb_pen"]),
    ("S03_conviction_only", ["--ablate", "revenue,momentum,adv,inst,pe_pen,pb_pen"]),
    ("S04_no_valuation", ["--ablate", "pe_pen,pb_pen"]),
    ("S05_no_inst", ["--ablate", "inst"]),
    ("S06_mom_conv", ["--ablate", "revenue,adv,inst,pe_pen,pb_pen"]),
    ("S07_no_filters", ["--ablate", "filters"]),
    ("S08_role20", ["--role-bonus", "20"]),
    ("S09_role40", ["--role-bonus", "40"]),
    ("S10_fresh3_15", ["--fresh-bonus", "15", "--fresh-months", "3"]),
    ("S11_fresh6_15", ["--fresh-bonus", "15", "--fresh-months", "6"]),
    ("S12_fresh12_10", ["--fresh-bonus", "10", "--fresh-months", "12"]),
    ("S13_role20_fresh6", ["--role-bonus", "20", "--fresh-bonus", "15", "--fresh-months", "6"]),
]
FOLDS = {"F1": "2024-12-31", "F2": "2025-12-31"}
START = "2022-08-01"
# 主線修訂(2026-07-16):--window t3 = EV43 完全同窗(3 年 train 極限優化)
T3_FOLDS, T3_START = {"T3": "2026-07-09"}, "2023-07-11"
# EV36 同框架(定案):train 2022-07-11~2025-07-10;OOS 只給最終 top-1
WF_FOLDS, WF_START = {"WF": "2025-07-10"}, "2022-07-11"


def run_cell(cell: str, extra: list[str], fold: str, end: str) -> dict | None:
    label = f"b18s1_{cell}_{fold}"
    cmd = [sys.executable, str(ENGINE), "--start", START, "--end", end,
           "--registry", str(REGISTRY), "--variants", VARIANT, "--label", label, *extra]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=3600)
    if proc.returncode != 0:
        print(f"FAIL {label}: {proc.stderr[-300:]}")
        return None
    s = pd.read_csv(RESULTS / f"{label}_summary.csv")
    row = s[s.name == VARIANT].iloc[0]
    daily = pd.read_csv(RESULTS / f"{label}_{VARIANT}_daily.csv", parse_dates=["date"])
    nav = daily.set_index("date")["nav"]
    mrets = nav.groupby(nav.index.astype(str).str.slice(0, 7)).last().pct_change().dropna()
    rng = np.random.default_rng(20260716)
    p5, p50, _ = boot_cagr_lb(mrets, rng)
    print(f"done {label}: cagr={row['cagr']:.3f} p5={p5:.3f}")
    return {"cell": cell, "fold": fold, "cagr": float(row["cagr"]),
            "sortino": float(row["sortino"]), "mdd": float(row["mdd"]),
            "boot_p5": round(p5, 4), "boot_p50": round(p50, 4),
            "n_trades": int(row.get("n_trades", 0))}


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--cells", nargs="*", default=None, help="只跑指定 cells(補跑用)")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--window", choices=("folds", "t3", "ev36"), default="folds")
    ns = ap.parse_args()
    global FOLDS, START
    if ns.window == "t3":
        FOLDS, START = T3_FOLDS, T3_START
    elif ns.window == "ev36":
        FOLDS, START = WF_FOLDS, WF_START
    cells = [(c, e) for c, e in CELLS if not ns.cells or c in ns.cells]
    jobs = [(c, e, f, end) for c, e in cells for f, end in FOLDS.items()
            if not (RESULTS / f"b18s1_{c}_{f}_summary.csv").exists()]
    print(f"to run: {len(jobs)} jobs")
    rows = []
    with ThreadPoolExecutor(max_workers=ns.workers) as pool:
        for r in pool.map(lambda j: run_cell(*j), jobs):
            if r:
                rows.append(r)
    # 彙整一律全量重掃磁碟(補跑模式下 rows 只含增量)
    rows = []
    rng = np.random.default_rng(20260716)
    for cell, _ in CELLS:
        for fold in FOLDS:
            label = f"b18s1_{cell}_{fold}"
            sp = RESULTS / f"{label}_summary.csv"
            if not sp.exists():
                continue
            s = pd.read_csv(sp)
            row_ = s[s.name == VARIANT].iloc[0]
            daily = pd.read_csv(RESULTS / f"{label}_{VARIANT}_daily.csv", parse_dates=["date"])
            nav = daily.set_index("date")["nav"]
            mrets = nav.groupby(nav.index.astype(str).str.slice(0, 7)).last().pct_change().dropna()
            p5, p50, _ = boot_cagr_lb(mrets, rng)
            rows.append({"cell": cell, "fold": fold, "cagr": float(row_["cagr"]),
                         "sortino": float(row_["sortino"]), "mdd": float(row_["mdd"]),
                         "boot_p5": round(p5, 4), "boot_p50": round(p50, 4),
                         "n_trades": int(row_.get("n_trades", 0))})
    df = pd.DataFrame(rows)
    wide = df.pivot(index="cell", columns="fold", values="boot_p5")
    fold_cols = [c for c in wide.columns]
    wide["p5_geo"] = np.prod([1 + wide[c] for c in fold_cols], axis=0) ** (1 / len(fold_cols)) - 1
    rank = wide.sort_values("p5_geo", ascending=False)
    tag = {"2023-07-11": "t3", "2022-07-11": "ev36"}.get(START, "folds")
    df.to_csv(Path(__file__).parent / f"stage1_results_{tag}.csv", index=False)
    rank.to_csv(Path(__file__).parent / f"stage1_ranking_{tag}.csv")
    print("\n=== Stage 1 排名(兩 train 折 boot P5 幾何均)===")
    print(rank.round(3).to_string())


if __name__ == "__main__":
    main()
