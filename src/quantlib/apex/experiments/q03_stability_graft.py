"""Q03 — Q01 晉級因子(穩定性族)× S 引擎嫁接(預註冊見 ledger Q-LINE)。

變體:S+gm_vol8^0.5 / S+ni_vol8^0.5 / S+cfo_ta^0.5(第七軸 AOI)、cfo 閘替換為
gm_vol8 閘、雙閘(cfo ∩ gm_vol8)。harness 同 F08(dev 窗 2019-01-02~2025-06-30,
判準:P5 > 74.4 且 MDD 劣化 ≤5pp)。季報 PIT = 法定期限(同 F02/Q01)。

Run: uv run --project . python -m quantlib.apex.experiments.q03_stability_graft
依賴 cache:是。
"""
from __future__ import annotations

from datetime import date as Date

import duckdb
import polars as pl

from quantlib.apex import data
from quantlib.apex.assemble import build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.evergreen.ev36_walkforward import kpis_full

C = "company_code"
WREL = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0,
        "mom_126_5": 0.5, "rev_seq": 0.5, "accel_rel": 0.5}
DEV0, DEV1 = "2019-01-02", "2025-06-30"


def prep():
    con = data.connect()
    panel, feat, _ = build_features(con, "2017-06-01", DEV1, warmup_days=420)
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
    fx = (feat.sort("date")
          .join_asof(tax.sort("effective_date"), left_on="date",
                     right_on="effective_date", by=C, strategy="backward")
          .drop_nulls(subset=["industry"]))
    ind_med = fx.group_by(["date", "industry"]).agg(
        pl.col("rev_yoy_accel").median().alias("m"))
    rel = (fx.join(ind_med, on=["date", "industry"], how="left")
           .with_columns((pl.col("rev_yoy_accel") - pl.col("m")).alias("accel_rel"))
           .select(["date", C, "accel_rel"]))
    feat = feat.join(rel, on=["date", C], how="left")
    # 穩定性族(Q01 晉級;PIT = 法定期限,as-of 展開)
    pos = lambda c: pl.when(pl.col(c) > 0).then(pl.col(c))
    rq = (pl.read_parquet(data.RAW_QUARTERLY_PARQUET)
          .sort([C, "year", "quarter"])
          .with_columns([
              (-(pl.col("ni_q").rolling_std(8).over(C)
                 / pos("total_assets"))).alias("ni_vol8_neg"),
              (-pl.col("gross_margin_q").rolling_std(8).over(C)).alias("gm_vol8_neg"),
              (pl.col("cfo_ttm") / pos("total_assets")).alias("cfo_ta"),
          ])
          .with_columns(
              pl.when(pl.col("quarter") == 1).then(pl.date(pl.col("year"), 5, 15))
              .when(pl.col("quarter") == 2).then(pl.date(pl.col("year"), 8, 14))
              .when(pl.col("quarter") == 3).then(pl.date(pl.col("year"), 11, 14))
              .otherwise(pl.date(pl.col("year") + 1, 3, 31)).alias("q_avail"))
          .select([C, "q_avail", "gm_vol8_neg", "ni_vol8_neg", "cfo_ta"])
          .sort("q_avail"))
    feat = (feat.sort("date")
            .join_asof(rq, left_on="date", right_on="q_avail", by=C,
                       strategy="backward", tolerance="150d").sort([C, "date"]))
    elig = data.eligibility(panel, min_adv=5_000_000.0)
    return panel, feat, elig


def run_variant(panel, feat, elig, *, wts: dict, gate: str = "cfo") -> dict:
    pool = feat.filter(pl.col("rev_fresh_days") <= 7)
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi")
          .drop_nulls(subset=[c for c in wts if c in pool.columns]))
    if gate in ("cfo", "both"):
        df = df.filter(pl.col("cfo_ni_ratio_ttm")
                       >= pl.col("cfo_ni_ratio_ttm").median().over("date"))
    if gate in ("gmvol", "both"):
        df = df.filter(pl.col("gm_vol8_neg")
                       >= pl.col("gm_vol8_neg").median().over("date"))
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
        ("S + gm_vol8^0.5", dict(wts={**WREL, "gm_vol8_neg": 0.5})),
        ("S + ni_vol8^0.5", dict(wts={**WREL, "ni_vol8_neg": 0.5})),
        ("S + cfo_ta^0.5", dict(wts={**WREL, "cfo_ta": 0.5})),
        ("閘替換 cfo→gm_vol8", dict(wts=WREL, gate="gmvol")),
        ("雙閘 cfo∩gm_vol8", dict(wts=WREL, gate="both")),
    ]
    print(f"dev 窗 {DEV0}~{DEV1};S 官方基準 CAGR 120.9%/P5 74.4/MDD −32.6")
    for name, kw in variants:
        k = run_variant(panel, feat, elig, **kw)
        flag = "★" if (k["p5"] > 0.744 and k["mdd"] > -0.376) else " "
        print(f"{flag} {name:20s} CAGR {k['cagr']:7.1%}  P5 {k['p5']:6.1%}  "
              f"MDD {k['mdd']:6.1%}  Martin {k['martin']:5.1f}")


if __name__ == "__main__":
    main()
