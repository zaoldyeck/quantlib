"""EV36c — train 窗補掃「寬席位」軸(EV36 網格僅到 6 席的補遺)。

池等權 beta 在 OOS 354%/−14.3 暗示寬持有形態被低估;本掃描僅在 train 窗
(2022-07-11~2025-07-10)選擇,不觸碰 OOS(OOS 第二次窺視需使用者核准並記帳)。

Run: uv run --project research python -m research.evergreen.ev36c_wide
依賴 cache: 是
"""
from __future__ import annotations

import itertools

import polars as pl

from research.evergreen.ev36_walkforward import Lab, run_cfg, seg_kpi, TRAIN0, TRAIN1


def train_pool_beta(lab: Lab, pm: int) -> dict:
    memb, _ = lab.memb(pm)
    ret = (lab.panel.sort(["company_code", "date"])
           .with_columns((pl.col("close") / pl.col("close").shift(1) - 1)
                         .over("company_code").alias("r"))
           .select(["date", "company_code", "r"]))
    daily = (memb.join(ret, on=["date", "company_code"], how="left")
             .group_by("date").agg(pl.col("r").mean()).sort("date")
             .filter((pl.col("date") >= TRAIN0) & (pl.col("date") <= TRAIN1))
             .with_columns((1 + pl.col("r").fill_null(0)).cum_prod().alias("nav")))
    return seg_kpi(daily.select(["date", "nav"]))


def main() -> None:
    lab = Lab()
    print("=== train 窗池等權 beta(全池持有,零選股)===")
    for pm in (2, 3, 4):
        b = train_pool_beta(lab, pm)
        print(f"pm{pm}: CAGR {b['cagr']:7.1%}  MDD {b['mdd']:6.1%}  "
              f"Martin {b['martin']:5.1f}")

    print("\n=== 寬席位網格(train 選擇,量尺同 EV36:Martin/tie CAGR)===")
    grid = list(itertools.product(
        (2, 3, 4), (0.0, 0.6), (8, 10, 12), (1, 2, 3, 6),
        (0.30, 0.40, None), (30, 45, None)))
    rows = []
    for pm, h1, ns, mn, tr, lt in grid:
        if tr is None and lt is None:
            pass  # 純池籍出場也合法(池籍輪換仍在)
        k = run_cfg(lab, pool_months=pm, h120=h1, trail=tr, lts=lt,
                    n_slots=ns, max_new=mn)["train"]
        rows.append({"pm": pm, "h120": h1, "slots": ns, "mn": mn,
                     "trail": tr, "lts": lt,
                     **{f"tr_{x}": v for x, v in k.items()}})
    df = (pl.DataFrame(rows, schema_overrides={"trail": pl.Float64, "lts": pl.Int64},
                       infer_schema_length=None)
          .sort(["tr_martin", "tr_cagr"], descending=True))
    df.write_parquet("research/evergreen/data/ev36c_wide.parquet")
    print(df.head(8))
    print("\nEV36 原榜首(6 席窄形態)train:CAGR 210.7% MDD −34.6 Martin 30.3")


if __name__ == "__main__":
    main()
