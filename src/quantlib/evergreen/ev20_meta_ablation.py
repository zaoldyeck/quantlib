"""EV20 — 元資訊消融:conviction 可靠性 × 池大小(零 token)。

(a) EV17 vs v1 各 conviction 組 fwd63(單調性=Agent 自信可靠度);
(b) 引擎 weight 公式(conv/組均/5, clip 0.10-0.30)加權 fwd63(引擎實吃);
(c) EV17 池大小門檻(全部 / conv>=4 / top8)濾後 fwd63。
判準見 LEDGER EV20。

Run: uv run --project . python -m quantlib.evergreen.ev20_meta_ablation
"""
from __future__ import annotations

from datetime import date as Date

import numpy as np
import polars as pl

from quantlib.apex import data

C = "company_code"
ASOF = ["2023-02-01", "2023-08-01", "2024-03-01", "2025-04-01"]


def main() -> None:
    ev17 = (pl.read_parquet("src/quantlib/evergreen/data/ev17_chips_labels.parquet")
            .select(["month", pl.col("code").alias(C), "conviction"]))
    v1 = (pl.read_parquet("src/quantlib/evergreen/data/registry_v1.parquet")
          .filter(pl.col("month").is_in(ASOF))
          .select(["month", pl.col("code").alias(C), "conviction"]))

    con = data.connect()
    panel = data.common_stocks(
        data.load_panel(con, "2022-06-01", "2026-07-09", warmup_days=10))
    px = (panel.sort([C, "date"])
          .with_columns([
              (pl.col("close").shift(-63) / pl.col("close") - 1)
              .over(C).alias("fwd63"),
              (pl.col("close") / pl.col("close").rolling_max(120))
              .over(C).alias("h120"),
          ]).select(["date", C, "fwd63", "h120"]))
    dates_all = panel.select("date").unique().sort("date")["date"].to_list()

    def next_td(d: Date) -> Date:
        return min(x for x in dates_all if x > d)

    def joined(labels: pl.DataFrame) -> pl.DataFrame:
        outs = []
        for m in ASOF:
            d0 = next_td(Date.fromisoformat(m))
            outs.append(px.filter(pl.col("date") == d0)
                        .join(labels.filter(pl.col("month") == m),
                              on=C, how="inner"))
        return pl.concat(outs).drop_nulls(subset=["fwd63"])

    j17, jv1 = joined(ev17), joined(v1)

    print("=== (a) conviction 單調性(各組 fwd63,裸池)===")
    for name, df in [("EV17 手寫", j17), ("v1 切片 ", jv1)]:
        g = (df.group_by("conviction")
             .agg(pl.col("fwd63").mean().alias("mean"), pl.len())
             .sort("conviction"))
        line = f"{name}:"
        for r in g.to_dicts():
            line += f"  c{r['conviction']} {r['mean']:+.1%}(n={r['len']})"
        print(line)

    print("\n=== (b) 引擎實吃:conviction 加權 fwd63(組內 weight = conv/組均/5 clip 0.10-0.30)===")
    for name, df in [("EV17 手寫", j17), ("v1 切片 ", jv1)]:
        d = df.with_columns(
            ((pl.col("conviction") / pl.col("conviction").mean().over("month")) / 5)
            .clip(0.10, 0.30).alias("w"))
        cw = float((d["fwd63"] * d["w"]).sum() / d["w"].sum())
        flt = d.filter(pl.col("h120") > 0.7)
        cwf = float((flt["fwd63"] * flt["w"]).sum() / flt["w"].sum())
        print(f"{name}:裸加權 {cw:+.1%}  濾後加權 {cwf:+.1%}(等權濾後對照:"
              f"{flt['fwd63'].mean():+.1%})")

    print("\n=== (c) EV17 池大小門檻(濾後,等權/加權)===")
    base = j17.filter(pl.col("h120") > 0.7)
    variants = {
        "全部(12-14/月)": base,
        "conv>=4 精兵   ": base.filter(pl.col("conviction") >= 4),
        "每月 top8(conv 排序)": (j17.sort(["month", "conviction"],
                                          descending=[False, True])
                                  .group_by("month", maintain_order=True).head(8)
                                  .filter(pl.col("h120") > 0.7)),
    }
    for name, df in variants.items():
        d = df.with_columns(
            ((pl.col("conviction") / pl.col("conviction").mean().over("month")) / 5)
            .clip(0.10, 0.30).alias("w"))
        cw = float((d["fwd63"] * d["w"]).sum() / d["w"].sum()) if d.height else float("nan")
        print(f"{name}:n={d.height}  等權 {d['fwd63'].mean():+.1%}  加權 {cw:+.1%}")


if __name__ == "__main__":
    main()
