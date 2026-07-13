"""EV21 — 主題(產業)動能層:第四排位軸 / gate / 反轉出場(路徑二+五)。

產業 = industry_taxonomy_pit asof(PIT 鐵律);產業動能 ind_mom =
該產業全體成分 mom(126-5)之日截面均值。三變體 × v1 全量端到端:
  T1 axis:score = rank(conv)×rank(h52)×rank(mom)×rank(ind_mom)
  T2 gate:產業動能截面前 50% 才可持有(進出場即時)
  T3 exit:持倉產業動能落後 30% → exit flag(其餘同基準)
基準:115.4% / P5 48.8 / MDD −29.9 / OOS +428.9。判準見 LEDGER EV21。

Run: uv run --project research python -m research.evergreen.ev21_theme_momentum
"""
from __future__ import annotations

from datetime import date as Date

import duckdb
import polars as pl

from research.apex import data
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.apex.experiments.g01_ml_ranker import kpi
from research.evergreen.harvest import build_feats, monthly_membership

C = "company_code"


def industry_momentum(panel: pl.DataFrame) -> pl.DataFrame:
    """(date, company_code, industry, ind_mom):PIT 產業 × 產業動能截面。"""
    raw = duckdb.connect("research/cache.duckdb", read_only=True)
    tax = (raw.sql("SELECT company_code, effective_date, industry "
                   "FROM industry_taxonomy_pit WHERE industry IS NOT NULL "
                   "ORDER BY effective_date").pl())
    mom = (panel.sort([C, "date"])
           .with_columns((pl.col("close").shift(5) / pl.col("close").shift(126) - 1)
                         .over(C).alias("mom"))
           .select(["date", C, "mom"]))
    withind = (mom.sort("date")
               .join_asof(tax.sort("effective_date"), left_on="date",
                          right_on="effective_date", by=C, strategy="backward")
               .drop_nulls(subset=["industry"]))
    ind = (withind.group_by(["date", "industry"])
           .agg(pl.col("mom").mean().alias("ind_mom")))
    return (withind.select(["date", C, "industry"])
            .join(ind, on=["date", "industry"], how="left"))


def run(panel, feats, memb, indmom, mode: str):
    def rank(c):
        return (pl.col(c).rank() / pl.len()).over("date")

    base = (memb.join(feats, on=["date", C], how="left")
            .join(indmom.select(["date", C, "ind_mom", "ind_pct"]),
                  on=["date", C], how="left")
            .filter(pl.col("h120").fill_null(0) > 0.7))
    if mode == "T1":
        sc = base.with_columns(
            (rank("conv") * rank("h52") * rank("mom") * rank("ind_mom"))
            .alias("score"))
    else:
        sc = base.with_columns(
            (rank("conv") * rank("h52") * rank("mom")).alias("score"))
    if mode == "T2":
        sc = sc.filter(pl.col("ind_pct").fill_null(0.5) >= 0.5)
    sc = (sc.with_columns(((pl.col("conv") / pl.col("conv").mean().over("date")) / 5)
                          .clip(0.10, 0.30).alias("weight"))
          .select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))

    dates_all = panel.select("date").unique().sort("date")["date"].to_list()
    days = [d for d in dates_all if d >= Date(2022, 7, 1)]
    all_codes = memb[C].unique().to_list()
    flag = (pl.DataFrame({"date": days})
            .join(pl.DataFrame({C: all_codes}), how="cross")
            .join(memb.select(["date", C]), on=["date", C], how="anti"))
    if mode == "T3":
        weak = (indmom.filter(pl.col("ind_pct") < 0.3)
                .select(["date", C]).filter(pl.col("date").is_in(days)))
        flag = pl.concat([flag, weak]).unique(subset=["date", C])
    flag = flag.sort(["date", C])
    return simulate(panel, sc, exit_flags=flag, exec_spec=ExecSpec(),
                    port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                    exit_spec=ExitSpec(trailing_stop=0.35, loser_time_stop=30),
                    start=Date(2022, 7, 1))


def main() -> None:
    reg = pl.read_parquet("research/evergreen/data/registry_v1.parquet")
    con = data.connect()
    panel = data.common_stocks(
        data.load_panel(con, "2022-01-01", "2026-07-09", warmup_days=300))
    dates_all = panel.select("date").unique().sort("date")["date"].to_list()
    feats = build_feats(panel)
    memb = monthly_membership(reg, dates_all, Date(2022, 7, 1))
    indmom = industry_momentum(panel).with_columns(
        (pl.col("ind_mom").rank() / pl.len()).over("date").alias("_r"))
    # 產業層截面分位(每產業一值 → 用產業去重後 rank 再 broadcast)
    ind_rank = (indmom.select(["date", "industry", "ind_mom"]).unique()
                .with_columns((pl.col("ind_mom").rank() / pl.len())
                              .over("date").alias("ind_pct")))
    indmom = indmom.join(ind_rank.select(["date", "industry", "ind_pct"]),
                         on=["date", "industry"], how="left")

    for mode in ["T1", "T2", "T3"]:
        res = run(panel, feats, memb, indmom, mode)
        nav = res.nav.sort("date")
        tr = kpi(nav.filter(pl.col("date") <= Date(2025, 6, 30)))
        oos = nav.filter(pl.col("date") > Date(2025, 6, 30))
        print(f"{mode}:train CAGR {tr['cagr']:7.1%}  P5 {tr['p5']:.1%}  "
              f"MDD {tr['mdd']:.1%}  OOS {oos['nav'][-1]/oos['nav'][0]-1:+.1%}  "
              f"交易 {res.trades.height}")


if __name__ == "__main__":
    main()
