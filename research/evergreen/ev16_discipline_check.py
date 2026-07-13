"""EV16 — 新紀律 vs 舊紀律池品質(registry_v2 go/no-go 第一步)。

單一變因 = 提示詞紀律:EV13-B 臂(新紀律+現行表)vs registry_v1 同月
(舊紀律+現行表)。同起點(月末次交易日)fwd63 池品質。預註冊見 LEDGER
EV16。零 agent 成本(兩份標記皆現成)。

需要 cache 最新。Run: uv run --project research python -m research.evergreen.ev16_discipline_check
"""
from __future__ import annotations

from datetime import date as Date

import numpy as np
import polars as pl

from research.apex import data

C = "company_code"
MONTHS = ["2023-02", "2023-08", "2024-03", "2025-04"]


def main() -> None:
    b13 = (pl.read_parquet("research/evergreen/data/ev13_duel_labels.parquet")
           .filter(pl.col("arm") == "B")
           .select(["month", pl.col("code").alias(C), "conviction"]))
    v1 = (pl.read_parquet("research/evergreen/data/registry_v1.parquet")
          .filter(pl.col("month").is_in([m + "-01" for m in MONTHS]))
          .select([pl.col("month").str.slice(0, 7).alias("month"),
                   pl.col("code").alias(C), "conviction"]))

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
    month_last = {}
    for d in dates_all:
        month_last[d.strftime("%Y-%m")] = d

    def next_td(d: Date) -> Date:
        return min(x for x in dates_all if x > d)

    def pool_quality(labels: pl.DataFrame, m: str) -> dict:
        d0 = next_td(month_last[m])
        day = px.filter(pl.col("date") == d0)
        sub = labels.filter(pl.col("month") == m)
        hit = day.join(sub, on=C, how="semi")
        flt = hit.filter(pl.col("h120") > 0.7)
        return {"n": sub.height,
                "raw": hit["fwd63"].drop_nulls().to_list(),
                "flt": flt["fwd63"].drop_nulls().to_list()}

    def fm(xs: list) -> str:
        return f"{np.mean(xs):+.1%}" if xs else "  na "

    agg = {"新紀律(EV13-B)": {"raw": [], "flt": []},
           "舊紀律(v1 切片)": {"raw": [], "flt": []}}
    src = {"新紀律(EV13-B)": b13, "舊紀律(v1 切片)": v1}
    print("=== 逐月池品質(fwd63 自月末次交易日;濾後 = h120>0.7)===")
    for m in MONTHS:
        line = f"{m}:"
        for name, labels in src.items():
            q = pool_quality(labels, m)
            agg[name]["raw"] += q["raw"]
            agg[name]["flt"] += q["flt"]
            line += (f"  {name} n={q['n']:2d} 裸 {fm(q['raw'])}"
                     f" 濾後 {fm(q['flt'])}({len(q['flt'])})")
        print(line)
    print()
    means = {}
    for name, a in agg.items():
        raw, flt = np.array(a["raw"]), np.array(a["flt"])
        means[name] = flt.mean()
        print(f"{name} 合計:裸 {raw.mean():+.1%}(n={len(raw)},"
              f"失敗率 {(raw < 0).mean():.0%})  濾後 {flt.mean():+.1%}"
              f"(n={len(flt)},失敗率 {(flt < 0).mean():.0%})")

    # 重疊/獨有分解(合併 4 個月的 (month, code) 鍵)
    kb = set(zip(b13["month"], b13[C]))
    kv = set(zip(v1["month"], v1[C]))
    print(f"\n重疊 {len(kb & kv)};新紀律獨有 {len(kb - kv)};"
          f"v1 獨有 {len(kv - kb)}")

    diff = means["新紀律(EV13-B)"] - means["舊紀律(v1 切片)"]
    verdict = ("新紀律晉級 v2 基底" if diff >= 0.03
               else "v1 紀律留任" if diff <= -0.03
               else "平手帶 → 考慮 v1 主體+空手權混合")
    print(f"\n濾後合計差(新−舊)= {diff:+.1%} → {verdict}")


if __name__ == "__main__":
    main()
