"""戰役十八 Stage 3:席位/權重軸(Stage 2 top-3 組合 × 席位{5,10,15} × 權重{equal,score}).

18 cells,EV36 train 窗;排名 = train 月報酬 boot P5。top-1 = 凍結參數,進 OOS 一跑。

Run: uv run --project research python -m research.serenity.wf.run_stage3
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
START, END = "2022-07-11", "2025-07-10"  # EV36 train

from research.serenity.backfill.pool_quality_duel import boot_cagr_lb  # noqa: E402

# Stage 2 top-3(nofilt 計分 × 出場組合;P5 0.123/0.110/0.100)
COMBOS = {
    "c1_tp40_noabs": (["--ablate", "filters"], "tp=0.4;trail=0.2;abs=none;time=50"),
    "c2_tp40_abs15": (["--ablate", "filters"], "tp=0.4;trail=0.2;abs=0.15;time=50"),
    "c3_tpnone_abs15": (["--ablate", "filters"], "tp=none;trail=0.2;abs=0.15;time=50"),
}
SLOTS = (5, 10, 15)
WEIGHTS = ("equal", "score")


def run_one(combo: str, slots: int, weight: str) -> None:
    label = f"b18s3_{combo}_n{slots}_{weight}"
    if list(RESULTS.glob(f"{label}_summary.csv")):
        return
    score_args, grid = COMBOS[combo]
    cmd = [sys.executable, str(ENGINE), "--start", START, "--end", END,
           "--registry", str(REGISTRY), "--variants", VARIANT, "--label", label,
           "--max-positions", str(slots), "--weight-mode", weight,
           "--grid-exit", grid, *score_args]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=3600)
    print(f"{'done' if proc.returncode == 0 else 'FAIL'} {label}"
          + ("" if proc.returncode == 0 else f": {proc.stderr[-200:]}"))


def main() -> None:
    jobs = [(c, s, w) for c in COMBOS for s in SLOTS for w in WEIGHTS]
    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(lambda j: run_one(*j), jobs))

    rng = np.random.default_rng(20260716)
    rows = []
    for combo in COMBOS:
        for slots in SLOTS:
            for weight in WEIGHTS:
                label = f"b18s3_{combo}_n{slots}_{weight}"
                sps = list(RESULTS.glob(f"{label}_summary.csv"))
                if not sps:
                    continue
                s = pd.read_csv(sps[0])
                r = s[s.name.str.startswith("g_")].iloc[0]
                daily = pd.read_csv(RESULTS / f"{label}_{r['name']}_daily.csv", parse_dates=["date"])
                nav = daily.set_index("date")["nav"]
                mrets = nav.groupby(nav.index.astype(str).str.slice(0, 7)).last().pct_change().dropna()
                p5, _, _ = boot_cagr_lb(mrets, rng)
                rows.append({"combo": combo, "slots": slots, "weight": weight,
                             "cell_name": r["name"], "cagr": round(float(r["cagr"]), 3),
                             "sortino": round(float(r["sortino"]), 2),
                             "mdd": round(float(r["mdd"]), 3), "boot_p5": round(p5, 4)})
    df = pd.DataFrame(rows).sort_values("boot_p5", ascending=False)
    df.to_csv(Path(__file__).parent / "stage3_ranking_ev36.csv", index=False)
    print("\n=== Stage 3 排名(train P5)===")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
