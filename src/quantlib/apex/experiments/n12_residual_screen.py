"""N12 — 殘差條件篩選:因子帶來的是**現有六因子沒有的資訊**,還是同一軸的重複?

**為什麼要有這一支(N08/N10 的失敗診斷)**:N08 在 S 池內用火箭機率篩出
hvn_dist(2.6x)、range_pos_60(2.3x)、donchian_60(專砍崩跌),N10 把它們加進計分後
**七臂在後窗全負**。事後看機制很清楚:這三個全是**價格位置**軸,而現役六因子裡
`high_52w` 與 `close_pos_20` 已經佔了那條軸兩席。加進去不是加資訊,是把價格位置
從 2 席變 4 席,把權重從營收軸搶走。

**N08 的方法缺陷**:它量的是因子**單獨**的鑑別力,沒有控制「現有分數已經知道了多少」。
一個純粹與現有分數相關的因子,在那種篩選下一樣會顯示漂亮的火箭提升——**那是重複計數,
不是增量**。這是本專案第二次踩到同型的錯(第一次是 F01「IC 顯著 ≠ 可交易」),差別在
上一次靠端到端回測才發現、這次可以在篩選層就擋掉。

**正確作法(本檔)**:先按當日 canonical 分數分五層,**在每一層之內**再看因子的火箭提升。
與現有分數同軸的因子,層內提升會塌回 ~1.0;帶新資訊的因子,層內仍有提升。
判準:**層內加權平均提升 IS/OOS 兩窗皆 ≥ 1.3,且至少 3 層同號**——才准進端到端。

依賴 cache: 是。
run: uv run --project . python -m quantlib.apex.experiments.n12_residual_screen
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib import paths
from quantlib.apex import data
from quantlib.apex.experiments.n08_conditional_factor_screen import (
    CANDIDATES, CRASH, ROCKET, forward_outcomes,
)
from quantlib.apex.experiments.n11_accel_zscore import accel_z
from quantlib.apex.strategy_s import prep_cached, score_pool

C = "company_code"
START = "2015-01-01"
NQ = 5              # 分數分層數
OUT = paths.OUT / "apex" / "n12_residual"


def residual_lift(pool: pl.DataFrame, col: str) -> dict:
    """分數分層內的火箭提升:層內高五分位 ÷ 層內低五分位,再依層樣本數加權平均。"""
    d = (pool.drop_nulls([col, "maxgain60"]).filter(pl.col(col).is_finite())
         .filter(pl.len().over("date") >= 25))          # 要能同時切分數層與因子層
    if d.height < 5000:
        return {"factor": col, "n": d.height, "備註": "樣本不足"}
    d = d.with_columns(
        ((pl.col("score").rank("ordinal") / pl.len()).over("date") * NQ)
        .ceil().clip(1, NQ).cast(pl.Int32).alias("sq"))
    d = d.with_columns(
        ((pl.col(col).rank("ordinal") / pl.len()).over(["date", "sq"]) * 5)
        .ceil().clip(1, 5).cast(pl.Int32).alias("fq"))
    lifts, ws, signs = [], [], []
    for s in range(1, NQ + 1):
        g = d.filter(pl.col("sq") == s)
        hi = g.filter(pl.col("fq") == 5)["maxgain60"]
        lo = g.filter(pl.col("fq") == 1)["maxgain60"]
        if hi.len() < 300 or lo.len() < 300:
            continue
        p_hi, p_lo = float((hi >= ROCKET).mean()), float((lo >= ROCKET).mean())
        if p_lo <= 0:
            continue
        lifts.append(p_hi / p_lo)
        ws.append(hi.len() + lo.len())
        signs.append(1 if p_hi > p_lo else -1)
    if not lifts:
        return {"factor": col, "n": d.height, "備註": "層內樣本不足"}
    w = np.array(ws, dtype=float)
    hi_all = d.filter(pl.col("fq") == 5)
    lo_all = d.filter(pl.col("fq") == 1)
    c_hi = float((hi_all["maxdraw60"] <= CRASH).mean())
    c_lo = float((lo_all["maxdraw60"] <= CRASH).mean())
    return {"factor": col, "n": d.height,
            "層內火箭提升": round(float(np.average(lifts, weights=w)), 2),
            "同號層數": f"{sum(1 for s in signs if s > 0)}/{len(signs)}",
            "層內崩跌提升": round(c_hi / c_lo, 2) if c_lo > 0 else None,
            "各層": [round(x, 2) for x in lifts]}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    fo = forward_outcomes(panel)
    days = fo.select("date").unique().sort("date")["date"].to_list()
    pool = (score_pool(feat, elig)
            .filter(pl.col("date") >= pl.lit(START).str.to_date())
            .filter(pl.col("date") <= days[-61])
            .join(fo, on=["date", C], how="inner")
            .join(accel_z(feat).rename({"accel_z": "accel_z_f"}),
                  on=["date", C], how="left"))
    cols = CANDIDATES + ["accel_z_f"]
    cut = pool.select("date").unique().sort("date")["date"].to_list()
    mid = cut[int(len(cut) * 0.75)]
    print(f"池 {pool.height:,} 列 | IS ≤ {mid} | OOS > {mid}")
    print("\n對照組先看:S 現役六因子自己的層內提升(它們本來就在分數裡,"
          "層內提升應接近 1 —— 這是本方法的正確性自檢)")

    from quantlib.apex.strategy_s import WREL
    res = {}
    for tag, sub in (("IS", pool.filter(pl.col("date") <= mid)),
                     ("OOS", pool.filter(pl.col("date") > mid))):
        rows = [residual_lift(sub, c) for c in list(WREL) + cols]
        t = pl.DataFrame([r for r in rows if "層內火箭提升" in r])
        t = t.with_columns(pl.col("factor").is_in(list(WREL)).alias("現役"))
        print("\n" + "=" * 82)
        print(f"【{tag}】層內火箭提升(控制現有分數之後,還剩多少鑑別力)")
        print("=" * 82)
        with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=190, fmt_str_lengths=60):
            print(t.sort("層內火箭提升", descending=True))
        res[tag] = t
        t.write_parquet(OUT / f"residual_{tag}.parquet")

    m = (res["IS"].select(["factor", "現役", "層內火箭提升", "同號層數", "層內崩跌提升"])
         .rename({"層內火箭提升": "IS提升", "同號層數": "IS同號", "層內崩跌提升": "IS崩跌"})
         .join(res["OOS"].select(["factor", "層內火箭提升", "同號層數", "層內崩跌提升"])
               .rename({"層內火箭提升": "OOS提升", "同號層數": "OOS同號",
                        "層內崩跌提升": "OOS崩跌"}), on="factor", how="inner"))
    ok = m.filter((~pl.col("現役")) & (pl.col("IS提升") >= 1.3) & (pl.col("OOS提升") >= 1.3))
    print("\n" + "=" * 82)
    print("【過關名單】非現役 + 層內提升兩窗皆 ≥ 1.3")
    print("=" * 82)
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=190):
        print(ok if ok.height else "(無因子過關 —— 現有六因子已吃下這些軸的資訊)")
        print("\n全部對照:")
        print(m.sort("OOS提升", descending=True))
    m.write_parquet(OUT / "residual_merged.parquet")
    print(f"\n產物 → {OUT}")


if __name__ == "__main__":
    main()
