"""L01 — 匿名化資料包生成:S 於 W3 的每筆交易 → LLM agent 判斷用 PIT 包。

匿名化協定(ledger/batches.md L-LINE):代碼/名稱/產業/絕對日期全剝除;
價格歸一化(決策日=100)、營收僅 YoY/QoQ %、流動性給 cohort 分位、
大盤僅動能/波動數字。真實身分映射另存 l01_truth.parquet(僅評分用)。

Run: uv run --project research python -m research.apex.experiments.l01_build_packs
"""
from __future__ import annotations

import json

import numpy as np
import polars as pl
from datetime import date as Date

from research.apex import data
from research.apex.assemble import entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.apex.experiments.g01_ml_ranker import C, S_WTS, W3_START, prep

TRAIN_CUT = Date(2025, 7, 1)


def main() -> None:
    con, panel, feat = prep()
    elig = (data.eligibility(panel, min_adv=5_000_000.0)
            .filter(pl.col("eligible")).select(["date", C]))
    pool = (feat.filter(pl.col("rev_fresh_days") <= 7)
            .join(elig, on=["date", C], how="semi")
            .drop_nulls(subset=list(S_WTS))
            .filter(pl.col("cfo_ni_ratio_ttm")
                    >= pl.col("cfo_ni_ratio_ttm").median().over("date")))
    geo = None
    for c_, wt in S_WTS.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        geo = term if geo is None else geo * term
    pool = pool.with_columns([
        geo.alias("geo_score"), pl.len().over("date").alias("cohort_size"),
        pl.col("geo_score").rank(descending=True).over("date").alias("geo_rank"),
    ] if False else [
        geo.alias("geo_score"), pl.len().over("date").alias("cohort_size"),
    ])
    pool = pool.with_columns(
        pl.col("geo_score").rank(descending=True).over("date").alias("geo_rank"))
    sc = (pool.select(["date", C, "geo_score"])
          .rename({"geo_score": "score"})
          .filter(pl.col("date") >= pl.lit(W3_START).str.to_date()))
    e, _ = entries_and_flags(sc, 5, 10**9)
    f = (feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C])
         .filter(pl.col("date") >= pl.lit(W3_START).str.to_date()))
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30,
                                      loser_time_stop=15),
                   start=Date.fromisoformat(W3_START))
    trades = res.trades.sort("entry_date")
    print(f"trades:{trades.height} 筆(W3)")

    # 決策日 = entry(fill)日的前一交易日
    dates_all = panel.select("date").unique().sort("date")["date"].to_list()
    didx = {d: i for i, d in enumerate(dates_all)}
    # 大盤(發行量加權)動能/波動
    mkt = (con.sql("SELECT date, close FROM market_index WHERE market='twse' "
                   "AND name='發行量加權股價指數' ORDER BY date").pl()
           .with_columns([
               (pl.col("close") / pl.col("close").shift(20) - 1).alias("mkt_mom_20"),
               (pl.col("close") / pl.col("close").shift(60) - 1).alias("mkt_mom_60"),
               (pl.col("close") / pl.col("close").shift(1) - 1)
               .rolling_std(20).alias("mkt_vol_20"),
           ]))
    px = panel.select(["date", C, "close", "volume"]).sort([C, "date"])
    rev_raw = (data.load_monthly_revenue(con, "2026-07-09")
               .sort([C, "year", "month"]))

    packs, truth = [], []
    rng = np.random.default_rng(42)
    order = rng.permutation(trades.height)  # 打亂編號,避免時間順序洩漏
    for i, row in enumerate(trades.to_dicts()):
        tid = f"T{order[i]+1:03d}"
        dd = dates_all[didx[row["entry_date"]] - 1]  # 決策日
        fx = pool.filter((pl.col("date") == dd) & (pl.col(C) == row[C]))
        if fx.height == 0:
            continue
        fx = fx.to_dicts()[0]
        hist = (px.filter((pl.col(C) == row[C]) & (pl.col("date") <= dd))
                .tail(121))
        closes = hist["close"].to_numpy()
        if len(closes) < 60:
            continue
        norm = closes / closes[-1] * 100.0
        weekly = [round(float(v), 1) for v in norm[::-1][::5][::-1]]
        vols = hist["volume"].cast(pl.Float64).to_numpy()
        vol_surge = float(vols[-5:].mean() / (vols[-60:].mean() + 1))
        rv = (rev_raw.filter(pl.col(C) == row[C])
              .filter((pl.col("year") * 100 + pl.col("month"))
                      <= (dd.year * 100 + dd.month - 2 if dd.month >= 3
                          else (dd.year - 1) * 100 + dd.month + 10))
              .tail(12))
        yoy = [round(float(v), 1) for v in
               (rv["monthly_revenue_yoy"].to_numpy())] if rv.height else []
        mk = mkt.filter(pl.col("date") <= dd).tail(1).to_dicts()[0]
        adv_pctl = None  # cohort 內 ADV 分位(資料包簡化:用 geo_rank/cohort 提供強度)
        packs.append({
            "id": tid,
            "segment": "train" if row["entry_date"] < TRAIN_CUT else "test",
            "cohort": {"size": int(fx["cohort_size"]),
                       "geo_rank": int(fx["geo_rank"])},
            "axes": {k: (round(float(fx[k]), 4) if fx[k] is not None else None)
                     for k in S_WTS},
            "rev_yoy_last12m_pct": yoy,
            "rev_seq_3m_pct": (round(float(fx["rev_seq"]) * 100, 1)
                               if fx["rev_seq"] is not None else None),
            "price_120d_weekly_norm100": weekly,
            "volume_surge_5v60": round(vol_surge, 2),
            "market": {"mom_20": round(float(mk["mkt_mom_20"]), 4),
                       "mom_60": round(float(mk["mkt_mom_60"]), 4),
                       "vol_20": round(float(mk["mkt_vol_20"]), 4)},
        })
        truth.append({"id": tid, C: row[C], "entry_date": row["entry_date"],
                      "exit_date": row["exit_date"], "ret_net": row["ret_net"],
                      "days_held": row["days_held"],
                      "exit_reason": row["exit_reason"],
                      "segment": packs[-1]["segment"]})
    with open("research/apex/ledger/l01_packs.json", "w") as fh:
        json.dump(packs, fh, ensure_ascii=False, indent=1)
    pl.DataFrame(truth).write_parquet("research/apex/ledger/l01_truth.parquet")
    tr = [p for p in packs if p["segment"] == "train"]
    te = [p for p in packs if p["segment"] == "test"]
    t_df = pl.DataFrame(truth)
    print(f"packs:{len(packs)}(train {len(tr)} / test {len(te)})")
    print(f"全樣本 ret_net:mean {t_df['ret_net'].mean():+.2%}  "
          f"win {(t_df['ret_net'] > 0).mean():.0%}")


if __name__ == "__main__":
    main()
