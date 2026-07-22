"""EV38 — 進出場手段窮盡 × 兩折 walk-forward(設計見 LEDGER.md EV38 段)。

Run: uv run --project research python -m research.evergreen.ev38_exhaust
依賴 cache: 是。輸出:data/ev38_results.parquet + stdout 兩折對決表。
"""
from __future__ import annotations

import itertools
from datetime import date as Date

import polars as pl

from research.apex import data
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev36_walkforward import C, Lab, seg_kpi
from research import paths

FOLDS = [
    {"name": "折1", "t0": Date(2022, 7, 11), "t1": Date(2024, 7, 10),
     "o0": Date(2024, 7, 11), "o1": Date(2025, 7, 10)},
    {"name": "折2", "t0": Date(2022, 7, 11), "t1": Date(2025, 7, 10),
     "o0": Date(2025, 7, 11), "o1": Date(2026, 7, 3)},
]
OUT = "research/evergreen/data/ev38_results.parquet"

GATES = ("none", "inst5", "inst10", "inst20", "f5", "t10", "don60",
         "any_confirm", "inst5h", "rev_accel")
EXITS = ("base", "isell10", "isell20", "ma60", "isell10_ma60")


class LabX(Lab):
    def __init__(self):
        super().__init__()
        con = data.connect()
        codes = self.reg["code"].unique().to_list()
        fl = (data.load_flows(con, "2021-06-01", "2026-07-09")
              .filter(pl.col(C).is_in(codes)).sort([C, "date"]))
        inst = pl.col("foreign_diff") + pl.col("trust_diff")
        fl = fl.with_columns([
            (inst.rolling_sum(5).over(C) > 0).alias("inst5"),
            (inst.rolling_sum(10).over(C) > 0).alias("inst10"),
            (inst.rolling_sum(20).over(C) > 0).alias("inst20"),
            (pl.col("foreign_diff").rolling_sum(5).over(C) > 0).alias("f5"),
            (pl.col("trust_diff").rolling_sum(10).over(C) > 0).alias("t10"),
            (inst.rolling_sum(10).over(C) < 0).alias("isell10"),
            (inst.rolling_sum(20).over(C) < 0).alias("isell20"),
        ]).select(["date", C, "inst5", "inst10", "inst20", "f5", "t10",
                   "isell10", "isell20"])
        px = (self.panel.sort([C, "date"])
              .with_columns([
                  (pl.col("close")
                   > pl.col("close").shift(1).rolling_max(60)).over(C).alias("don60"),
                  (pl.col("close")
                   < pl.col("close").rolling_mean(60)).over(C).alias("ma60x"),
              ]).select(["date", C, "don60", "ma60x"]))
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
        self.trig = (self.feats.select(["date", C, "h52"])
                     .join(fl, on=["date", C], how="left")
                     .join(px, on=["date", C], how="left")
                     .sort("date")
                     .join_asof(rev, left_on="date", right_on="avail", by=C,
                                strategy="backward", tolerance="70d")
                     .with_columns([pl.col(c).fill_null(False) for c in
                                    ("inst5", "inst10", "inst20", "f5", "t10",
                                     "isell10", "isell20", "don60", "ma60x",
                                     "rev_accel")])
                     .with_columns([
                         (pl.col("don60") | pl.col("inst5")
                          | pl.col("rev_accel")).alias("any_confirm"),
                         (pl.col("inst5") & (pl.col("h52") > 0.8)).alias("inst5h"),
                     ]).drop("h52"))
        # 出場增補 flag 快取(全窗;date>=最早 fold train 起點)
        d0 = FOLDS[0]["t0"]
        tw = self.trig.filter(pl.col("date") >= d0)
        self.exit_flags_extra = {
            "base": None,
            "isell10": tw.filter(pl.col("isell10")).select(["date", C]),
            "isell20": tw.filter(pl.col("isell20")).select(["date", C]),
            "ma60": tw.filter(pl.col("ma60x")).select(["date", C]),
            "isell10_ma60": tw.filter(pl.col("isell10") | pl.col("ma60x"))
                              .select(["date", C]),
        }


def run(lab: LabX, fold: dict, *, gate, exitf, pool_months, h120, trail, lts,
        n_slots, max_new, want_oos=False):
    memb, pool_flag = lab.memb(pool_months)

    def rank(c):
        return (pl.col(c).rank() / pl.len()).over("date")

    sc = (memb.join(lab.feats, on=["date", C], how="left")
          .join(lab.trig, on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > h120))
    if gate != "none":
        sc = sc.filter(pl.col(gate).fill_null(False))
    sc = (sc.with_columns((rank("h52") * rank("h120")).alias("score"))
          .with_columns(pl.lit(1.0 / n_slots).alias("weight"))
          .select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))
    flag = pool_flag
    extra = lab.exit_flags_extra[exitf]
    if extra is not None:
        flag = (pl.concat([pool_flag, extra]).unique(subset=["date", C])
                .sort(["date", C]))

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


def bench(fold: dict) -> dict:
    s = pl.read_parquet("research/apex/ledger/curves/T0334.parquet")
    ser = pl.read_csv(
        f"{paths.OUT_STRAT_LAB}/abl_adv_l0_ev_v2_thesis_inst_daily.csv",
        schema_overrides={"date": pl.Date})
    out = {}
    for name, df in (("S", s), ("Serenity", ser)):
        w = df.filter((pl.col("date") >= fold["o0"]) & (pl.col("date") <= fold["o1"]))
        out[name] = seg_kpi(w.select(["date", "nav"])) if w.height > 60 else None
    return out


def main() -> None:
    lab = LabX()
    core = list(itertools.product((2, 3), (0.0, 0.6), (0.30, 0.40),
                                  (30, 45), (5, 6), (1, 2)))
    all_rows = []
    for fold in FOLDS:
        rows = []
        for gate, exitf in itertools.product(GATES, EXITS):
            for pm, h1, tr, lt, ns, mn in core:
                cfg = dict(gate=gate, exitf=exitf, pool_months=pm, h120=h1,
                           trail=tr, lts=lt, n_slots=ns, max_new=mn)
                k = run(lab, fold, **cfg)["train"]
                rows.append({"fold": fold["name"], **cfg,
                             **{f"tr_{x}": v for x, v in k.items()}})
        df = pl.DataFrame(rows).sort(["tr_martin", "tr_cagr"], descending=True)
        all_rows.append(df)
        top = df.head(1).to_dicts()[0]
        cfg = {k: top[k] for k in ("gate", "exitf", "pool_months", "h120",
                                   "trail", "lts", "n_slots", "max_new")}
        out = run(lab, fold, **cfg, want_oos=True)
        b = bench(fold)
        print(f"\n=== {fold['name']} train top-1:{cfg}")
        print(f"train CAGR {out['train']['cagr']:7.1%} MDD {out['train']['mdd']:6.1%} "
              f"Martin {out['train']['martin']:5.1f}")
        print(f"OOS({fold['o0']}~{fold['o1']}):CAGR {out['oos']['cagr']:7.1%} "
              f"MDD {out['oos']['mdd']:6.1%} Martin {out['oos']['martin']:5.1f}")
        for nm, k in b.items():
            print(f"  對手 {nm}: " + (f"CAGR {k['cagr']:7.1%} MDD {k['mdd']:6.1%}"
                                     if k else "曲線未覆蓋(部分段另行標注)"))
        top["oos_cagr"] = out["oos"]["cagr"]
        top["oos_mdd"] = out["oos"]["mdd"]
        # gate / exit 家族分佈(train 榜首 20 的組成)
        h20 = df.head(20)
        print("  榜首20 gate 分佈:", dict(h20.group_by("gate").len().iter_rows()))
        print("  榜首20 exit 分佈:", dict(h20.group_by("exitf").len().iter_rows()))
    pl.concat(all_rows).write_parquet(OUT)


if __name__ == "__main__":
    main()
