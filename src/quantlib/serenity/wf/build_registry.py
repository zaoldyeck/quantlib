"""戰役十八:walk-forward registry 材料化.

backcast_2022_2024(含 role/conviction 演化)+ live registry(2025+,role 由
member_roles join)縫合;全部 active_from +1 個月(標記月內證據 → 次月可交易的
PIT 慣例,同池品質對決);active_until 保留原樣(live 段 2026 回填 caveat 已於
戰役十八預註冊聲明)。輸出 src/quantlib/serenity/wf/registry_wf.csv。

Run: uv run --project . python -m quantlib.serenity.wf.build_registry
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

REG_DIR = Path(__file__).parents[1] / "registry"
OUT = Path(__file__).parent / "registry_wf.csv"


def shift_month(iso: str) -> str:
    y, m, d = iso.split("-")
    y, m = int(y), int(m)
    y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return f"{y}-{m:02d}-01"


def main() -> None:
    back = pd.read_csv(REG_DIR / "backcast_2022_2024.csv", dtype=str).fillna("")
    live = pd.read_csv(REG_DIR / "thesis_registry_2025.csv", dtype=str).fillna("")
    roles = pd.read_csv(REG_DIR / "member_roles.csv", dtype={"company_code": str})
    live = live.merge(roles[["company_code", "role"]], on="company_code", how="left")
    live["role"] = live["role"].fillna("")

    cols = ["theme_id", "theme_name", "bottleneck_layer", "active_from", "active_until",
            "company_code", "conviction", "source_note", "evidence_date", "evidence_url",
            "invalidation_criteria", "review_by", "role"]
    for f in (back, live):
        for c in cols:
            if c not in f:
                f[c] = ""
    reg = pd.concat([back[cols], live[cols]], ignore_index=True)
    reg["active_from"] = reg["active_from"].map(shift_month)
    reg.to_csv(OUT, index=False)
    n_role = (reg.role != "").sum()
    print(f"registry_wf: {len(reg)} rows, {reg.company_code.nunique()} codes, "
          f"role 覆蓋 {n_role}/{len(reg)} -> {OUT}")


if __name__ == "__main__":
    main()
