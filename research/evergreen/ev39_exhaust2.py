"""EV39 — 手段窮盡第二波(設計見 LEDGER.md EV39 預註冊段)。

Run: uv run --project research python -m research.evergreen.ev39_exhaust2
依賴 cache: 是。輸出:data/ev39_results.parquet + stdout 兩折對決表。
"""
from __future__ import annotations

import itertools
from datetime import date as Date

import polars as pl

from research.apex import data
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev36_walkforward import C, Lab, seg_kpi
from research.evergreen.ev38_exhaust import FOLDS, bench

OUT = "research/evergreen/data/ev39_results.parquet"

SCORES = ("h52xh120", "inst_amt", "h52xinst", "anti_h52", "vol_surge",
          "h52xh120xinst")
GATES = ("none", "f5", "inst5", "any_confirm", "pullback", "f5_or_pullback")
ROTS = (None, 8, 12)
SIZINGS = ("equal", "volt")


class LabY(Lab):
    def __init__(self):
        super().__init__()
        con = data.connect()
        codes = self.reg["code"].unique().to_list()
        inst = pl.col("foreign_diff") + pl.col("trust_diff")
        fl = (data.load_flows(con, "2021-06-01", "2026-07-09")
              .filter(pl.col(C).is_in(codes)).sort([C, "date"])
              .with_columns([
                  (inst.rolling_sum(5).over(C) > 0).alias("inst5"),
                  (pl.col("foreign_diff").rolling_sum(5).over(C) > 0).alias("f5"),
                  inst.rolling_sum(5).over(C).alias("inst_amt5"),
              ]).select(["date", C, "inst5", "f5", "inst_amt5"]))
        px = (self.panel.sort([C, "date"])
              .with_columns([
                  (pl.col("close") / pl.col("close").shift(1) - 1).over(C).alias("ret"),
                  (pl.col("close")
                   > pl.col("close").shift(1).rolling_max(60)).over(C).alias("don60"),
                  pl.col("close").rolling_mean(20).over(C).alias("ma20"),
                  pl.col("close").rolling_mean(60).over(C).alias("ma60"),
                  (pl.col("volume").cast(pl.Float64).rolling_mean(5)
                   / pl.col("volume").cast(pl.Float64).rolling_mean(60))
                  .over(C).alias("vsurge"),
              ])
              .with_columns([
                  pl.col("ret").rolling_std(20).over(C).alias("sigma20"),
                  ((pl.col("close") < pl.col("ma20"))
                   & (pl.col("close") > pl.col("ma60"))).alias("pullback"),
              ])
              .select(["date", C, "don60", "pullback", "vsurge", "sigma20"]))
        rev = (data.load_monthly_revenue(con, "2026-07-09")
               .filter(pl.col(C).is_in(codes)).sort([C, "year", "month"])
               .with_columns([
                   pl.date(pl.col("year") + pl.col("month") // 12,
                           pl.col("month") % 12 + 1, 10).alias("avail"),
                   (pl.col("monthly_revenue_yoy")
                    > pl.col("monthly_revenue_yoy").shift(1).over(C))
                   .alias("rev_accel"),
               ]).select([C, "avail", "rev_accel"])
               .drop_nulls(subset=["avail"]).sort("avail"))
        self.trig = (self.feats.select(["date", C])
                     .join(fl, on=["date", C], how="left")
                     .join(px, on=["date", C], how="left")
                     .sort("date")
                     .join_asof(rev, left_on="date", right_on="avail", by=C,
                                strategy="backward", tolerance="70d")
                     .with_columns([
                         pl.col("inst5").fill_null(False),
                         pl.col("f5").fill_null(False),
                         pl.col("don60").fill_null(False),
                         pl.col("pullback").fill_null(False),
                         pl.col("rev_accel").fill_null(False),
                         pl.col("inst_amt5").fill_null(0.0),
                         pl.col("vsurge").fill_null(0.0),
                     ])
                     .with_columns([
                         (pl.col("don60") | pl.col("inst5")
                          | pl.col("rev_accel")).alias("any_confirm"),
                         (pl.col("f5") | pl.col("pullback")).alias("f5_or_pullback"),
                     ]))


def run(lab: LabY, fold: dict, *, score_mode, gate, rot, sizing,
        pool_months, h120, trail, lts, n_slots, max_new, want_oos=False):
    memb, pool_flag = lab.memb(pool_months)

    def rank(c):
        return (pl.col(c).rank() / pl.len()).over("date")

    sc = (memb.join(lab.feats, on=["date", C], how="left")
          .join(lab.trig, on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > h120))
    if gate != "none":
        sc = sc.filter(pl.col(gate).fill_null(False))
    score_expr = {
        "h52xh120": rank("h52") * rank("h120"),
        "inst_amt": rank("inst_amt5"),
        "h52xinst": rank("h52") * rank("inst_amt5"),
        "anti_h52": 1.0 - rank("h52"),
        "vol_surge": rank("vsurge"),
        "h52xh120xinst": rank("h52") * rank("h120") * rank("inst_amt5"),
    }[score_mode]
    sc = sc.with_columns(score_expr.alias("score"))
    if sizing == "equal":
        sc = sc.with_columns(pl.lit(1.0 / n_slots).alias("weight"))
    else:
        sc = sc.with_columns(
            (0.015 / pl.col("sigma20").clip(1e-4, None))
            .clip(0.08, 0.30).alias("weight"))
    sc = (sc.select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))

    flag = pool_flag
    if rot is not None:
        in_rank = sc.with_columns(
            pl.col("score").rank(descending=True).over("date").alias("rk"))
        out1 = in_rank.filter(pl.col("rk") > rot).select(["date", C])
        # gate/濾網把持有股擠出 sc 者亦視同掉出排位
        out2 = (memb.select(["date", C])
                .join(sc.select(["date", C]), on=["date", C], how="anti"))
        flag = (pl.concat([pool_flag, out1, out2])
                .unique(subset=["date", C]).sort(["date", C]))

    def one(start, end):
        res = simulate(lab.panel.filter(pl.col("date") <= end), sc,
                       exit_flags=flag, exec_spec=ExecSpec(),
                       port_spec=PortSpec(n_slots=n_slots, max_new_per_day=max_new),
                       exit_spec=ExitSpec(trailing_stop=trail,
                                          loser_time_stop=lts),
                       start=start)
        return res.nav.sort("date").filter(
            (pl.col("date") >= start) & (pl.col("date") <= end))

    out = {"train": seg_kpi(one(fold["t0"], fold["t1"]))}
    if want_oos:
        out["oos"] = seg_kpi(one(fold["o0"], fold["o1"]))
    return out


def main() -> None:
    lab = LabY()
    core = list(itertools.product((0.0, 0.6), (5, 6), (1, 2)))
    all_rows = []
    for fold in FOLDS:
        rows = []
        for sm, g, rot, sz in itertools.product(SCORES, GATES, ROTS, SIZINGS):
            for h1, ns, mn in core:
                cfg = dict(score_mode=sm, gate=g, rot=rot, sizing=sz,
                           pool_months=3, h120=h1, trail=0.30, lts=45,
                           n_slots=ns, max_new=mn)
                k = run(lab, fold, **cfg)["train"]
                rows.append({"fold": fold["name"], **cfg,
                             **{f"tr_{x}": v for x, v in k.items()}})
        df = (pl.DataFrame(rows, schema_overrides={"rot": pl.Int64},
                           infer_schema_length=None)
              .sort(["tr_martin", "tr_cagr"], descending=True))
        all_rows.append(df)
        top = df.head(1).to_dicts()[0]
        cfg = {k: top[k] for k in ("score_mode", "gate", "rot", "sizing",
                                   "pool_months", "h120", "trail", "lts",
                                   "n_slots", "max_new")}
        out = run(lab, fold, **cfg, want_oos=True)
        b = bench(fold)
        print(f"\n=== {fold['name']} train top-1:{cfg}")
        print(f"train Martin {out['train']['martin']:5.1f}(EV38 折內榜首對照:"
              f"{'61.5' if fold['name'] == '折1' else '33.4'})")
        print(f"OOS:CAGR {out['oos']['cagr']:7.1%} MDD {out['oos']['mdd']:6.1%} "
              f"Martin {out['oos']['martin']:5.1f}")
        for nm, k in b.items():
            if k:
                print(f"  對手 {nm}: CAGR {k['cagr']:7.1%} MDD {k['mdd']:6.1%}")
        h20 = df.head(20)
        for col in ("score_mode", "gate", "rot", "sizing"):
            print(f"  榜首20 {col}: {dict(h20.group_by(col).len().iter_rows())}")
    pl.concat(all_rows).write_parquet(OUT)


if __name__ == "__main__":
    main()
