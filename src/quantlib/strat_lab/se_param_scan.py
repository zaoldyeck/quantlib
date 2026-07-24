"""Serenity ev_v3_wf 非出場參數高原掃描(goal:檔數/權重/新鮮度等全維度測完)。

出場網格(T-02/03)與八成分 ablation 已定案;本檔補剩餘維度(one-at-a-time vs 現役):
- slots(--max-positions):5 / 8 / 10(現役)/ 12
- 權重(--weight-mode):equal(現役)/ score / inv_atr
- 主題新鮮度:fresh-bonus {0, 10(現役), 20}、fresh-months {6, 12(現役), 24}
機械 registry 全窗(battle18 語義,2022-07-11~2026-07-09)。subprocess 編排 engine CLI,
每變體獨立 label 輸出,末尾彙總。判準(D2):同時 ≥ 現役(87.1%/4.36/-25.7%)才算候選,
候選須配對檢定 + ledger 才可提案。

Run: uv run --project . python -m quantlib.strat_lab.se_param_scan
依賴 cache:是。
"""
from __future__ import annotations

import subprocess
import sys

import polars as pl

from quantlib import paths

BASE = ["uv", "run", "--project", ".", "python", "-m", "quantlib.serenity.engine",
        "--variants", "ev_v3_wf", "--ablate", "filters",
        "--start", "2022-07-11", "--end", "2026-07-09",
        "--registry", "src/quantlib/serenity/wf/registry_wf.csv"]
CUR = {"fresh": ["--fresh-bonus", "10", "--fresh-months", "12"]}

SCANS: dict[str, list[str]] = {
    "slots5":   ["--max-positions", "5",  *CUR["fresh"]],
    "slots8":   ["--max-positions", "8",  *CUR["fresh"]],
    "slots12":  ["--max-positions", "12", *CUR["fresh"]],
    "w_score":  ["--weight-mode", "score",   *CUR["fresh"]],
    "w_invatr": ["--weight-mode", "inv_atr", *CUR["fresh"]],
    "fb0":      ["--fresh-bonus", "0",  "--fresh-months", "12"],
    "fb20":     ["--fresh-bonus", "20", "--fresh-months", "12"],
    "fm6":      ["--fresh-bonus", "10", "--fresh-months", "6"],
    "fm24":     ["--fresh-bonus", "10", "--fresh-months", "24"],
}


def main() -> None:
    for label, extra in SCANS.items():
        cmd = BASE + extra + ["--label", f"scan_{label}"]
        r = subprocess.run(cmd, cwd=paths.REPO, capture_output=True, text=True, timeout=1800)
        if r.returncode != 0:
            print(f"  {label}: FAILED {r.stderr[-120:]}", flush=True)
            continue
        df = pl.read_csv(paths.OUT_STRAT_LAB / f"scan_{label}_summary.csv")
        k = df.filter(pl.col("name") == "ev_v3_wf").select(["cagr", "sortino", "mdd", "calmar"]).to_dicts()[0]
        print(f"  {label:10}: CAGR {k['cagr']:+.1%}  Sortino {k['sortino']:.2f}  "
              f"MDD {k['mdd']:+.1%}  Calmar {k['calmar']:.2f}", flush=True)
    print("\n  現役基準  : CAGR +87.1%  Sortino 4.36  MDD -25.7%  Calmar 3.39")
    print("  判準:同時 ≥ 現役才算候選;候選須配對檢定 + ledger 預註冊。")


if __name__ == "__main__":
    sys.exit(main())
