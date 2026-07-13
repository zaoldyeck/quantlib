"""EV26 Phase C — 池工程:v1∪v2 聯合池 / 交集共識池(抗抽樣噪音)。

live 對應:每月標記跑 2 次取 union/intersection(成本 ×2,已知)。
Run: uv run --project research python -m research.evergreen.ev26_phase_c
"""
from __future__ import annotations

import polars as pl

from research.evergreen.ev26_engine import EngineSpec, Lab, fmt

COLS = ["month", "code", "conviction"]


def main() -> None:
    lab = Lab()
    v1 = lab.regs["v1"].select(COLS).with_columns(
        pl.col("month").str.slice(0, 7).alias("ym"))
    v2 = lab.regs["v2"].select(COLS).with_columns(
        pl.col("month").str.slice(0, 7).alias("ym"))
    # union:同 (ym, code) 取 conviction max;month 鍵統一用 v2 的原 month
    uni = (pl.concat([v1, v2]).group_by(["ym", "code"])
           .agg(pl.col("conviction").max(), pl.col("month").first())
           .select(["month", "code", "conviction"]))
    inter_keys = (v1.select(["ym", "code"]).unique()
                  .join(v2.select(["ym", "code"]).unique(),
                        on=["ym", "code"], how="inner"))
    inter = (pl.concat([v1, v2]).join(inter_keys, on=["ym", "code"], how="semi")
             .group_by(["ym", "code"])
             .agg(pl.col("conviction").max(), pl.col("month").first())
             .select(["month", "code", "conviction"]))
    lab.regs["union"] = uni
    lab.regs["inter"] = inter
    print(f"union {uni.height} 筆;intersection {inter.height} 筆\n")
    for rn in ["union", "inter"]:
        print(fmt(f"{rn}(凍結參數)", lab.run(rn, EngineSpec())))
    # union 上的席位敏感度(池變大,容量可能不同)
    for spec, name in [(EngineSpec(n_slots=7), "union 7席"),
                       (EngineSpec(n_slots=7, max_new=3), "union 7席mn3"),
                       (EngineSpec(n_slots=10, max_new=3), "union 10席mn3")]:
        print(fmt(name, lab.run("union", spec)))


if __name__ == "__main__":
    main()
