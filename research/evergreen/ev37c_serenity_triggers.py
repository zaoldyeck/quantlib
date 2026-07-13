"""EV37c — Serenity 觸發器全家移植:法人買超 / 營收落地 / 突破(train 選擇)。

EV37 只測了價格行為 gate(don60/h52_95,train 否定);本輪補齊 Serenity
觸發三件套的另外兩件,並測「任一確認即可進」的聯集形態:

  gate=none        基準(EV36 榜首形態)
  gate=inst5       近 5 日法人(外資+投信)合計淨買超 > 0
  gate=trust5      近 5 日投信淨買超 > 0(Serenity 最重的訊號)
  gate=rev_pos     最新已公布月營收 YoY > 0(10 日 avail 規則)
  gate=rev_accel   最新已公布月營收 YoY 較上月改善
  gate=any_confirm don60 ∪ inst5 ∪ rev_accel(任一行為確認)

Run: uv run --project research python -m research.evergreen.ev37c_serenity_triggers
依賴 cache: 是。train 選擇量尺同 EV36(Martin/tie CAGR);勝過 30.3 才動 OOS。
"""
from __future__ import annotations

import itertools

import polars as pl

from research.apex import data
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev36_walkforward import (C, OOS0, OOS1, TRAIN0, TRAIN1,
                                                 Lab, seg_kpi)

OUT = "research/evergreen/data/ev37c_results.parquet"


class LabT(Lab):
    def __init__(self):
        super().__init__()
        con = data.connect()
        codes = self.reg["code"].unique().to_list()
        fl = (data.load_flows(con, "2021-06-01", "2026-07-09")
              .filter(pl.col(C).is_in(codes)).sort([C, "date"])
              .with_columns([
                  ((pl.col("foreign_diff") + pl.col("trust_diff"))
                   .rolling_sum(5).over(C) > 0).alias("inst5"),
                  (pl.col("trust_diff").rolling_sum(5).over(C) > 0).alias("trust5"),
              ]).select(["date", C, "inst5", "trust5"]))
        rev = (data.load_monthly_revenue(con, "2026-07-09")
               .filter(pl.col(C).is_in(codes)).sort([C, "year", "month"])
               .with_columns([
                   pl.date(pl.col("year") + pl.col("month") // 12,
                           pl.col("month") % 12 + 1, 10).alias("avail"),
                   (pl.col("monthly_revenue_yoy") > 0).alias("rev_pos"),
                   (pl.col("monthly_revenue_yoy")
                    > pl.col("monthly_revenue_yoy").shift(1).over(C))
                   .alias("rev_accel"),
               ]).select([C, "avail", "rev_pos", "rev_accel"])
               .drop_nulls(subset=["avail"]).sort("avail"))
        don = (self.panel.sort([C, "date"])
               .with_columns((pl.col("close")
                              > pl.col("close").shift(1).rolling_max(60))
                             .over(C).alias("don60"))
               .select(["date", C, "don60"]))
        self.trig = (self.feats.select(["date", C])
                     .join(fl, on=["date", C], how="left")
                     .join(don, on=["date", C], how="left")
                     .sort("date")
                     .join_asof(rev, left_on="date", right_on="avail", by=C,
                                strategy="backward", tolerance="70d")
                     .with_columns([
                         pl.col("inst5").fill_null(False),
                         pl.col("trust5").fill_null(False),
                         pl.col("don60").fill_null(False),
                         pl.col("rev_pos").fill_null(False),
                         pl.col("rev_accel").fill_null(False),
                     ])
                     .with_columns((pl.col("don60") | pl.col("inst5")
                                    | pl.col("rev_accel")).alias("any_confirm")))
        self._empty = pl.DataFrame(schema={"date": pl.Date, C: pl.Utf8})


def run(lab: LabT, *, gate, pool_months, h120, trail, lts, n_slots, max_new,
        want_nav=False):
    memb, pool_flag = lab.memb(pool_months)

    def rank(c):
        return (pl.col(c).rank() / pl.len()).over("date")

    sc = (memb.join(lab.feats, on=["date", C], how="left")
          .join(lab.trig.select(["date", C, "inst5", "trust5", "don60",
                                 "rev_pos", "rev_accel", "any_confirm"]),
                on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > h120))
    if gate != "none":
        sc = sc.filter(pl.col(gate).fill_null(False))
    sc = (sc.with_columns((rank("h52") * rank("h120")).alias("score"))
          .with_columns(pl.lit(1.0 / n_slots).alias("weight"))
          .select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))

    def one(start, end):
        res = simulate(lab.panel.filter(pl.col("date") <= end), sc,
                       exit_flags=pool_flag, exec_spec=ExecSpec(),
                       port_spec=PortSpec(n_slots=n_slots, max_new_per_day=max_new),
                       exit_spec=ExitSpec(trailing_stop=trail,
                                          loser_time_stop=lts),
                       start=start)
        return res.nav.sort("date").filter(
            (pl.col("date") >= start) & (pl.col("date") <= end))

    out = {"train": seg_kpi(one(TRAIN0, TRAIN1))}
    if want_nav:
        out["oos"] = seg_kpi(one(OOS0, OOS1))
    return out


def main() -> None:
    lab = LabT()
    grid = list(itertools.product(
        ("none", "inst5", "trust5", "rev_pos", "rev_accel", "any_confirm"),
        (2, 3), (0.0, 0.5, 0.6), (0.30, 0.40), (30, 45), (5, 6), (1, 2)))
    rows = []
    for i, (g, pm, h1, tr, lt, ns, mn) in enumerate(grid):
        cfg = dict(gate=g, pool_months=pm, h120=h1, trail=tr, lts=lt,
                   n_slots=ns, max_new=mn)
        k = run(lab, **cfg)["train"]
        rows.append({**cfg, **{f"tr_{x}": v for x, v in k.items()}})
        if (i + 1) % 200 == 0:
            print(f"  {i + 1}/{len(grid)}")
    df = (pl.DataFrame(rows).sort(["tr_martin", "tr_cagr"], descending=True))
    df.write_parquet(OUT)

    print("\n=== 各 gate 最佳(train)===")
    best = (df.group_by("gate").agg(pl.all().sort_by("tr_martin").last())
            .sort("tr_martin", descending=True))
    with pl.Config(tbl_cols=-1, tbl_width_chars=180):
        print(best.select(["gate", "pool_months", "h120", "trail", "lts",
                           "n_slots", "max_new", "tr_cagr", "tr_mdd", "tr_martin"]))

    top = df.head(1).to_dicts()[0]
    if top["tr_martin"] > 30.3 and top["gate"] != "none":
        print("\n★ 觸發 gate 勝出 → 動用 OOS 窺視 #2(LEDGER 記帳)")
        cfg = {k: top[k] for k in ("gate", "pool_months", "h120", "trail",
                                   "lts", "n_slots", "max_new")}
        out = run(lab, **cfg, want_nav=True)
        print(f"{cfg} | OOS CAGR {out['oos']['cagr']:7.1%} "
              f"MDD {out['oos']['mdd']:6.1%}(Serenity 572.6%)")
    else:
        print(f"\n觸發 gate 未勝過基準(榜首 gate={top['gate']},"
              f"Martin {top['tr_martin']:.1f} vs 30.3)——OOS 不動用")


if __name__ == "__main__":
    main()
