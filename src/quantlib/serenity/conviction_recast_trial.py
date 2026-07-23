"""戰役十六:conviction 語義重錨定(純瓶頸強度)trial.

用途:44 檔新池(戰役十五除名後)按 SOP §1.5 新語義重打 conviction
(TIER5 = 獨佔/近獨佔本尊、owner = 4、enabler = 3),跑三窗對照戰役十五 V1
(同池 × grandfather conviction)。預註冊與判準見 trials ledger 戰役十六。

Run(依賴 cache 最新;3 次引擎重放):
    uv run --project . python -m quantlib.serenity.conviction_recast_trial
"""

from __future__ import annotations

import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
from quantlib import paths

REPO_ROOT = Path(__file__).resolve().parents[2]
ENGINE = Path(__file__).parent / "engine.py"
REGISTRY = Path(__file__).parent / "registry" / "thesis_registry_2025.csv"
ROLES = Path(__file__).parent / "registry" / "member_roles.csv"
RESULTS = paths.OUT_STRAT_LAB
VARIANT = "ev_v2_thesis_inst"
LAGS = (0, 90, 180)

# SOP §1.5 新語義:5 = 獨佔/近獨佔本尊;4 = 寡占本尊(其餘 owner);3 = enabler
TIER5 = {"2059", "3653", "3529", "6446", "2408"}


def recast_conviction(code: str, role: str) -> int:
    if role == "chokepoint_owner":
        return 5 if code in TIER5 else 4
    return 3  # enabler(beneficiary 已除名,不在池)


def materialize() -> Path:
    registry = pd.read_csv(REGISTRY, dtype=str).fillna("")
    roles = pd.read_csv(ROLES, dtype={"company_code": str})
    role_of = dict(zip(roles["company_code"], roles["role"]))
    keep = registry["company_code"].map(role_of) != "beneficiary"
    pool = registry[keep].copy()
    pool["conviction"] = [
        str(recast_conviction(c, role_of[c])) for c in pool["company_code"]
    ]
    path = RESULTS / "conviction_recast_registry.csv"
    RESULTS.mkdir(parents=True, exist_ok=True)
    pool.to_csv(path, index=False)
    n = pool["company_code"].nunique()
    print(f"recast pool: {n} codes -> {path.name}")
    return path


def run_engine(registry_path: Path, lag: int) -> str:
    label = f"conviction_recast_lag{lag}"
    cmd = [
        sys.executable, str(ENGINE),
        "--start", "2025-01-01",
        # 同輪對比:戰役十五 V1 baseline 的 data cutoff 是 2026-07-13,
        # cache 已續抓 7-14 之後,--end 凍窗保證兩臂同窗
        "--end", "2026-07-13",
        "--registry", str(registry_path),
        "--activation-lag-days", str(lag),
        "--variants", VARIANT,
        "--label", label,
    ]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=3600)
    if proc.returncode != 0:
        print(proc.stdout[-2000:], proc.stderr[-2000:])
        raise SystemExit(f"engine run failed: {label}")
    print(f"done: {label}")
    return label


def main() -> None:
    path = materialize()
    with ThreadPoolExecutor(max_workers=3) as pool:
        labels = list(pool.map(lambda lag: run_engine(path, lag), LAGS))

    rows = []
    for lag, label in zip(LAGS, labels):
        for arm, summary_name in (
            ("V1_grandfather", f"role_trial_no_beneficiary_lag{lag}"),
            ("C1_recast", label),
        ):
            s = pd.read_csv(RESULTS / f"{summary_name}_summary.csv")
            r = s[s["name"] == VARIANT].iloc[0]
            rows.append(
                {
                    "arm": arm, "lag": lag,
                    "cagr": round(float(r["cagr"]) * 100, 1),
                    "sortino": round(float(r["sortino"]), 2),
                    "mdd": round(float(r["mdd"]) * 100, 1),
                    "n_trades": int(r.get("n_trades", 0)),
                }
            )
    table = pd.DataFrame(rows).sort_values(["lag", "arm"]).reset_index(drop=True)
    print("\n=== 戰役十六 三窗對照(baseline = 戰役十五 V1)===")
    print(table.to_string(index=False))
    table.to_csv(RESULTS / "conviction_recast_comparison.csv", index=False)


if __name__ == "__main__":
    main()
