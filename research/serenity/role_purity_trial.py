"""戰役十五:成員層瓶頸角色 gate — role purity trial.

用途:驗證「把 beneficiary(瓶頸下游受益轉導:模組庫存財/組裝 ODM)逐出策展池」
對 champion `ev_v2_thesis_inst` 三窗(lag0/90/180)績效的影響,並對 baseline 的
成交做 role 歸因(各角色席位的實現貢獻)。預註冊與判準見
`docs/serenity/serenity_engine_trials_ledger.md` 戰役十五。

角色標注:`research/serenity/registry/member_roles.csv`(58 檔,時不變結構屬性)。
變體池:V1 no_beneficiary(58→44)、V2 owner_only(25)。

Run(依賴 cache 最新;約 9 次引擎重放,10-30 分鐘):
    uv run --project research python -m research.serenity.role_purity_trial
"""

from __future__ import annotations

import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
from research import paths

REPO_ROOT = Path(__file__).resolve().parents[2]
ENGINE = Path(__file__).parent / "engine.py"
REGISTRY = Path(__file__).parent / "registry" / "thesis_registry_2025.csv"
ROLES = Path(__file__).parent / "registry" / "member_roles.csv"
RESULTS = paths.OUT_STRAT_LAB
VARIANT = "ev_v2_thesis_inst"
LAGS = (0, 90, 180)


def materialize_pools() -> dict[str, Path]:
    registry = pd.read_csv(REGISTRY, dtype={"company_code": str})
    roles = pd.read_csv(ROLES, dtype={"company_code": str})
    reg_codes = set(registry["company_code"])
    role_codes = set(roles["company_code"])
    if reg_codes != role_codes:
        raise SystemExit(
            f"role annotation out of sync with registry: "
            f"missing={sorted(reg_codes - role_codes)} extra={sorted(role_codes - reg_codes)}"
        )
    role_of = dict(zip(roles["company_code"], roles["role"]))
    pools = {
        "base": registry,
        "no_beneficiary": registry[registry["company_code"].map(role_of) != "beneficiary"],
        "owner_only": registry[registry["company_code"].map(role_of) == "chokepoint_owner"],
    }
    paths: dict[str, Path] = {}
    RESULTS.mkdir(parents=True, exist_ok=True)
    for arm, frame in pools.items():
        path = RESULTS / f"role_trial_registry_{arm}.csv"
        frame.to_csv(path, index=False)
        paths[arm] = path
        print(f"pool {arm}: {frame['company_code'].nunique()} codes -> {path.name}")
    return paths


def run_engine(arm: str, registry_path: Path, lag: int) -> tuple[str, int, str]:
    label = f"role_trial_{arm}_lag{lag}"
    cmd = [
        sys.executable, str(ENGINE),
        "--start", "2025-01-01",
        "--registry", str(registry_path),
        "--activation-lag-days", str(lag),
        "--variants", VARIANT,
        "--label", label,
    ]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=3600)
    if proc.returncode != 0:
        print(proc.stdout[-2000:], proc.stderr[-2000:])
        raise SystemExit(f"engine run failed: {label}")
    return arm, lag, label


def collect(labels: dict[tuple[str, int], str]) -> pd.DataFrame:
    rows = []
    for (arm, lag), label in labels.items():
        summary = pd.read_csv(RESULTS / f"{label}_summary.csv")
        row = summary[summary["name"] == VARIANT].iloc[0]
        rows.append(
            {
                "arm": arm, "lag": lag,
                "cagr": float(row["cagr"]), "sortino": float(row["sortino"]),
                "sharpe": float(row["sharpe"]), "mdd": float(row["mdd"]),
                "n_trades": int(row.get("n_trades", 0)),
                "avg_active": float(row.get("avg_active", float("nan"))),
            }
        )
    return pd.DataFrame(rows).sort_values(["lag", "arm"]).reset_index(drop=True)


def attribution(labels: dict[tuple[str, int], str]) -> pd.DataFrame:
    roles = pd.read_csv(ROLES, dtype={"company_code": str})
    role_of = dict(zip(roles["company_code"], roles["role"]))
    rows = []
    for lag in LAGS:
        trades = pd.read_csv(
            RESULTS / f"{labels[('base', lag)]}_{VARIANT}_trades.csv", dtype={"code": str}
        )
        trades["code"] = trades["code"].str.zfill(4)
        trades["role"] = trades["code"].map(role_of).fillna("(bb-channel)")
        for role, g in trades.groupby("role"):
            rows.append(
                {
                    "lag": lag, "role": role, "n_trades": len(g),
                    "win_rate": float((g["ret"] > 0).mean()),
                    "avg_ret": float(g["ret"].mean()),
                    "sum_ret": float(g["ret"].sum()),
                    "avg_days": float(g["days_held"].mean()),
                }
            )
    return pd.DataFrame(rows)


def main() -> None:
    paths = materialize_pools()
    jobs = [(arm, paths[arm], lag) for lag in LAGS for arm in paths]
    labels: dict[tuple[str, int], str] = {}
    with ThreadPoolExecutor(max_workers=3) as pool:
        for arm, lag, label in pool.map(lambda j: run_engine(*j), jobs):
            labels[(arm, lag)] = label
            print(f"done: {label}")

    table = collect(labels)
    print("\n=== 戰役十五 三窗對照(champion ev_v2_thesis_inst)===")
    print(
        table.assign(
            cagr=lambda d: (d.cagr * 100).round(1), mdd=lambda d: (d.mdd * 100).round(1),
            sortino=lambda d: d.sortino.round(2), sharpe=lambda d: d.sharpe.round(2),
            avg_active=lambda d: d.avg_active.round(1),
        ).to_string(index=False)
    )

    attr = attribution(labels)
    print("\n=== baseline 成交 × role 歸因(等權席位近似)===")
    print(
        attr.assign(
            win_rate=lambda d: (d.win_rate * 100).round(0),
            avg_ret=lambda d: (d.avg_ret * 100).round(1),
            sum_ret=lambda d: (d.sum_ret * 100).round(0),
            avg_days=lambda d: d.avg_days.round(0),
        ).to_string(index=False)
    )

    table.to_csv(RESULTS / "role_trial_comparison.csv", index=False)
    attr.to_csv(RESULTS / "role_trial_attribution.csv", index=False)
    print(f"\nsaved -> {RESULTS / 'role_trial_comparison.csv'}, {RESULTS / 'role_trial_attribution.csv'}")


if __name__ == "__main__":
    main()
