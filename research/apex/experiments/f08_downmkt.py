"""F08 — down-market 相對強度軸 × S 引擎(預註冊見 ledger/batches.md F08 段)。

dm_rs60 / dm_win60 定義同 evergreen EV46。變體:S+dm 第七軸(^0.5/^1.0)、
dm_pos 進場 gate、dm 替換 mom_126_5。dev 窗(2019-01-02~2025-06-30)判準:
P5 > 74.4(S 基準)且 MDD 劣化 ≤5pp → 現代 era 確認;否則負結果入檔。

Run: uv run --project research python -m research.apex.experiments.f08_downmkt
依賴 cache: 是(需最新)
"""
from __future__ import annotations

from datetime import date as Date

import duckdb
import polars as pl

from research.apex import data
from research.apex.assemble import build_features, entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev36_walkforward import kpis_full

C = "company_code"
WREL = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0,
        "mom_126_5": 0.5, "rev_seq": 0.5, "accel_rel": 0.5}
DEV0, DEV1 = "2019-01-02", "2025-06-30"


def prep():
    con = data.connect()
    panel, feat, elig = build_features(con, "2017-06-01", DEV1, warmup_days=420)
    # rev_seq / accel_rel(照 chart_s 組裝)
    rev = (data.load_monthly_revenue(con, DEV1)
           .sort([C, "year", "month"])
           .with_columns([
               pl.date(pl.col("year") + pl.col("month") // 12,
                       pl.col("month") % 12 + 1, 10).alias("avail"),
               (pl.col("monthly_revenue").rolling_sum(3)
                / pl.col("monthly_revenue").rolling_sum(3).shift(3) - 1)
               .over(C).alias("rev_seq"),
           ]).select([C, "avail", "rev_seq"]).drop_nulls().sort("avail"))
    feat = (feat.sort("date")
            .join_asof(rev, left_on="date", right_on="avail", by=C,
                       strategy="backward", tolerance="70d").sort([C, "date"]))
    raw = duckdb.connect("var/cache/cache.duckdb", read_only=True)
    tax = raw.sql("SELECT company_code, effective_date, industry FROM "
                  "industry_taxonomy_pit WHERE industry IS NOT NULL "
                  "ORDER BY effective_date").pl()
    fx = (feat.sort("effective_date" if False else "date")
          .join_asof(tax.sort("effective_date"), left_on="date",
                     right_on="effective_date", by=C, strategy="backward")
          .drop_nulls(subset=["industry"]))
    ind_med = fx.group_by(["date", "industry"]).agg(
        pl.col("rev_yoy_accel").median().alias("m"))
    rel = (fx.join(ind_med, on=["date", "industry"], how="left")
           .with_columns((pl.col("rev_yoy_accel") - pl.col("m")).alias("accel_rel"))
           .select(["date", C, "accel_rel"]))
    feat = feat.join(rel, on=["date", C], how="left")
    # down-market 因子
    mkt = (raw.execute("SELECT date, close FROM market_index "
                       "WHERE name = '發行量加權股價指數' ORDER BY date").pl()
           .with_columns((pl.col("close") / pl.col("close").shift(1) - 1)
                         .alias("mkt_ret")).select(["date", "mkt_ret"]))
    dm = (panel.sort([C, "date"])
          .with_columns((pl.col("close") / pl.col("close").shift(1) - 1)
                        .over(C).alias("ret"))
          .select(["date", C, "ret"]).join(mkt, on="date", how="left")
          .with_columns([
              pl.when(pl.col("mkt_ret") < 0)
              .then(pl.col("ret") - pl.col("mkt_ret")).otherwise(None).alias("_ex"),
              pl.when(pl.col("mkt_ret") < 0)
              .then((pl.col("ret") > 0).cast(pl.Float64)).otherwise(None).alias("_w"),
          ])
          .with_columns([
              pl.col("_ex").rolling_mean(60, min_samples=10).over(C).alias("dm_rs60"),
              pl.col("_w").rolling_mean(60, min_samples=10).over(C).alias("dm_win60"),
          ]).select(["date", C, "dm_rs60", "dm_win60"]))
    feat = feat.join(dm, on=["date", C], how="left")
    elig = data.eligibility(panel, min_adv=5_000_000.0)
    return panel, feat, elig


def run_variant(panel, feat, elig, *, wts: dict, dm_gate=False) -> dict:
    pool = feat.filter(pl.col("rev_fresh_days") <= 7)
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi")
          .drop_nulls(subset=[c for c in wts if c in pool.columns])
          .filter(pl.col("cfo_ni_ratio_ttm")
                  >= pl.col("cfo_ni_ratio_ttm").median().over("date")))
    if dm_gate:
        df = df.filter(pl.col("dm_rs60").fill_null(-1) > 0)
    expr = None
    for c_, wt in wts.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = (df.with_columns(expr.alias("score"))
          .select(["date", C, "score"])
          .filter(pl.col("date") >= pl.lit(DEV0).str.to_date()))
    entries, _ = entries_and_flags(sc, 5, 10**9)
    stale = (feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C])
             .filter(pl.col("date") >= pl.lit(DEV0).str.to_date()))
    res = simulate(panel, entries, exit_flags=stale, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30,
                                      loser_time_stop=15),
                   start=Date.fromisoformat(DEV0))
    nav = (res.nav.sort("date")
           .filter(pl.col("date") >= pl.lit(DEV0).str.to_date())
           .select(["date", "nav"]))
    return kpis_full(nav)


def main() -> None:
    panel, feat, elig = prep()
    variants = [
        ("S 基準(重現驗證)", dict(wts=WREL)),
        ("S + dm_rs^0.5", dict(wts={**WREL, "dm_rs60": 0.5})),
        ("S + dm_rs^1.0", dict(wts={**WREL, "dm_rs60": 1.0})),
        ("S + dm_win^0.5", dict(wts={**WREL, "dm_win60": 0.5})),
        ("S × dm_pos gate", dict(wts=WREL, dm_gate=True)),
        ("dm_rs 替換 mom", dict(wts={**{k: v for k, v in WREL.items()
                                        if k != "mom_126_5"}, "dm_rs60": 0.5})),
    ]
    print(f"dev 窗 {DEV0}~{DEV1};S 官方基準 CAGR 120.9%/P5 74.4/MDD −32.6")
    for name, kw in variants:
        k = run_variant(panel, feat, elig, **kw)
        flag = "★" if (k["p5"] > 0.744 and k["mdd"] > -0.376) else " "
        print(f"{flag} {name:18s} CAGR {k['cagr']:7.1%}  P5 {k['p5']:6.1%}  "
              f"MDD {k['mdd']:6.1%}  Martin {k['martin']:5.1f}")


if __name__ == "__main__":
    main()
