"""全量回溯標記總合併:五批 backcast CSV → backcast_2022_2024.csv.

縫合規則:同 (theme_id, company_code) 跨批出現時,active_from 取最早批(入冊時點
以首次 admit 為準),conviction/role/source_note 取最後批(演化終值);失效日
(active_until)來自各批 agent 的當時判定(硬編碼於 INVALIDATIONS,附來源月)。
2020-2022 粗 backcast(另一時代 schema、無 role)保持獨立檔,不併入本表。

Run: uv run --project research python -m research.serenity.backfill.merge_backcast
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

REG_DIR = Path(__file__).parents[1] / "registry"
BATCHES = ["backcast_2022H2.csv", "backcast_2023H1.csv", "backcast_2023H2.csv",
           "backcast_2024H1.csv", "backcast_2024H2.csv"]

# 各批標記 agent 以「當時證據」判定的主題失效(來源:該批 label_runs/摘要)
INVALIDATIONS = {
    "abf_substrate": "2022-10-31",      # 2022-10 判定:PC 崩壞、缺口崩解
    "foundry_shortage": "2022-10-31",   # 2022-10 判定:砍單、去化延長
    "ev_supply": "2023-12-31",          # 2023-12 判定:產能過剩+價格戰
}

# 跨主題污染剔除(已逐筆查證:2023H2 carry 快照把多主題摘要混進 cowos cluster,
# 這兩檔的正冊分別在 grid_heavy_electrical / epaper_esl,source_note 僅「carry 持平」
# 無入冊檢核理由)
EXCLUDE = {("advanced_packaging_cowos", "1519"), ("advanced_packaging_cowos", "8069")}


def main() -> None:
    frames = []
    for i, name in enumerate(BATCHES):
        df = pd.read_csv(REG_DIR / name, dtype=str).fillna("")
        df["_batch"] = i
        frames.append(df)
    allrows = pd.concat(frames, ignore_index=True)

    merged = []
    for (theme, code), g in allrows.groupby(["theme_id", "company_code"], sort=False):
        if (theme, code) in EXCLUDE:
            continue
        first, last = g.loc[g["_batch"].idxmin()], g.loc[g["_batch"].idxmax()]
        row = last.drop(labels="_batch").to_dict()
        row["active_from"] = min(g["active_from"])
        row["active_until"] = INVALIDATIONS.get(theme, "")
        row["source_note"] = (
            f"[跨批縫合:首見 {first['active_from']}(批 {BATCHES[int(first['_batch'])][9:-4]}),"
            f"conviction 終值 {last['conviction']}] " + str(last["source_note"])
        )[:400]
        merged.append(row)

    out = pd.DataFrame(merged).sort_values(["theme_id", "active_from", "company_code"])
    path = REG_DIR / "backcast_2022_2024.csv"
    out.to_csv(path, index=False)
    print(out[["theme_id", "company_code", "role", "conviction", "active_from", "active_until"]]
          .to_string(index=False))
    print(f"\n-> {path} ({len(out)} rows, {out.theme_id.nunique()} themes)")


if __name__ == "__main__":
    main()
