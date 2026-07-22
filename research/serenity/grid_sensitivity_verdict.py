"""戰役十七判準計算:出場參數跨 regime 敏感度裁決.

讀 `--grid-exit` 兩窗輸出(backcast_grid / newpool_grid 的 summary CSV),
按預註冊判準(trials ledger 戰役十七)輸出 穩健/敏感/弱敏感 裁決,
gate 回溯標記投資決策。

Run: uv run --project research python -m research.serenity.grid_sensitivity_verdict
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from research import paths

RESULTS = paths.OUT_STRAT_LAB
CURRENT = "g_tp0.6_tr0.2_ab0.15_td50"


def load(label: str) -> pd.DataFrame:
    s = pd.read_csv(RESULTS / f"{label}_summary.csv")
    return s[s.name.str.startswith("g_")].reset_index(drop=True)


def pct_rank(g: pd.DataFrame, col: str) -> float:
    cur = g.loc[g.name == CURRENT, col].iloc[0]
    return float((g[col] > cur).mean())  # 0.0 = 最優


def main() -> None:
    back, new = load("backcast_grid"), load("newpool_grid")
    rows = []
    for label, g in (("backcast_2020-23", back), ("newpool_2025-26", new)):
        cur = g.loc[g.name == CURRENT].iloc[0]
        best = g.loc[g.cagr.idxmax()]
        rows.append(
            {
                "window": label,
                "cur_cagr": f"{cur.cagr:.1%}", "cur_sortino": round(cur.sortino, 2),
                "cagr_pctile": f"前 {pct_rank(g, 'cagr'):.0%}",
                "sortino_pctile": f"前 {pct_rank(g, 'sortino'):.0%}",
                "best_cell": best["name"], "best_cagr": f"{best.cagr:.1%}",
                "gap_pp": round((best.cagr - cur.cagr) * 100, 1),
            }
        )
    print(pd.DataFrame(rows).to_string(index=False))

    top10_back = set(back.nlargest(10, "cagr")["name"])
    top10_new = set(new.nlargest(10, "cagr")["name"])
    overlap = top10_back & top10_new
    print(f"\nbackcast top-10 ∩ newpool top-10 = {len(overlap)} cells: {sorted(overlap)}")
    print(f"backcast top-10: {sorted(top10_back)}")

    b_cagr, b_sort = pct_rank(back, "cagr"), pct_rank(back, "sortino")
    gap = back.cagr.max() - back.loc[back.name == CURRENT, "cagr"].iloc[0]
    robust = (b_cagr <= 0.25 and b_sort <= 0.25) or gap < 0.05
    sensitive = (b_cagr > 0.50 or b_sort > 0.50) and not overlap
    verdict = "穩健(gate 關)" if robust else "敏感(gate 開)" if sensitive else "弱敏感(gate 半開)"
    print(f"\n戰役十七裁決:{verdict} — backcast CAGR 前 {b_cagr:.0%} / Sortino 前 {b_sort:.0%} / 距最優 {gap*100:.1f}pp / top-10 交集 {len(overlap)}")


if __name__ == "__main__":
    main()
