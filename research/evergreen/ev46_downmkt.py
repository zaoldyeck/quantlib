"""EV46 — 「相對抗跌」因子 × Evergreen 引擎(設計見 LEDGER.md EV46 預註冊段)。

dm_rs60:近 60 交易日中大盤(發行量加權指數)下跌日的個股平均超額報酬。
dm_win60:大盤跌日個股收紅比率。

Run: uv run --project research python -m research.evergreen.ev46_downmkt
依賴 cache: 是。輸出:data/ev46_results.parquet + stdout 兩折榜。
"""
from __future__ import annotations

import itertools

import duckdb
import polars as pl

from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev36_walkforward import C, kpis_full, seg_kpi
from research.evergreen.ev38_exhaust import FOLDS, LabX, bench

OUT = "research/evergreen/data/ev46_results.parquet"
SCORES = ("base", "x_dm_rs", "x_dm_win")
GATES = ("none", "f5", "inst5", "dm_pos")


def taiex_returns() -> pl.DataFrame:
    con = duckdb.connect("research/cache.duckdb", read_only=True)
    idx = con.execute(
        "SELECT date, close FROM market_index WHERE name = '發行量加權股價指數' "
        "ORDER BY date").pl()
    return (idx.sort("date")
            .with_columns((pl.col("close") / pl.col("close").shift(1) - 1)
                          .alias("mkt_ret"))
            .select(["date", "mkt_ret"]).drop_nulls())


class LabD(LabX):
    def __init__(self):
        super().__init__()
        mkt = taiex_returns()
        px = (self.panel.sort([C, "date"])
              .with_columns((pl.col("close") / pl.col("close").shift(1) - 1)
                            .over(C).alias("ret"))
              .select(["date", C, "ret"])
              .join(mkt, on="date", how="left"))
        dm = (px.with_columns([
                  pl.when(pl.col("mkt_ret") < 0)
                  .then(pl.col("ret") - pl.col("mkt_ret"))
                  .otherwise(None).alias("_ex"),
                  pl.when(pl.col("mkt_ret") < 0)
                  .then((pl.col("ret") > 0).cast(pl.Float64))
                  .otherwise(None).alias("_win"),
              ])
              .with_columns([
                  pl.col("_ex").rolling_mean(60, min_samples=10).over(C)
                  .alias("dm_rs60"),
                  pl.col("_win").rolling_mean(60, min_samples=10).over(C)
                  .alias("dm_win60"),
              ]).select(["date", C, "dm_rs60", "dm_win60"]))
        self.trig = (self.trig.join(dm, on=["date", C], how="left")
                     .with_columns([
                         pl.col("dm_rs60").fill_null(0.0),
                         pl.col("dm_win60").fill_null(0.0),
                         (pl.col("dm_rs60") > 0).alias("dm_pos"),
                     ]))


def run(lab: LabD, fold: dict, *, score, gate, pool_months, h120, trail, lts,
        n_slots, max_new, want_oos=False):
    memb, pool_flag = lab.memb(pool_months)

    def rank(c):
        return (pl.col(c).rank() / pl.len()).over("date")

    sc = (memb.join(lab.feats, on=["date", C], how="left")
          .join(lab.trig, on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > h120))
    if gate != "none":
        sc = sc.filter(pl.col(gate).fill_null(False))
    base = rank("h52") * rank("h120")
    expr = {"base": base,
            "x_dm_rs": base * rank("dm_rs60"),
            "x_dm_win": base * rank("dm_win60")}[score]
    sc = (sc.with_columns(expr.alias("score"))
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

    out = {"train": kpis_full(one(fold["t0"], fold["t1"]))}
    if want_oos:
        out["oos"] = seg_kpi(one(fold["o0"], fold["o1"]))
    return out


def main() -> None:
    lab = LabD()
    core = list(itertools.product((2, 3), (0.0, 0.6), (0.30, 0.40),
                                  (30, 45), (5, 6), (1, 2)))
    for fold in FOLDS:
        rows = []
        for sm, g in itertools.product(SCORES, GATES):
            for pm, h1, tr, lt, ns, mn in core:
                cfg = dict(score=sm, gate=g, pool_months=pm, h120=h1,
                           trail=tr, lts=lt, n_slots=ns, max_new=mn)
                k = run(lab, fold, **cfg)["train"]
                rows.append({"fold": fold["name"], **cfg,
                             **{f"tr_{x}": v for x, v in k.items()}})
        df = pl.DataFrame(rows).sort(["tr_p5", "tr_cagr"], descending=True)
        df.write_parquet(OUT.replace(".parquet", f"_{fold['name']}.parquet"))
        base_best = (df.filter(pl.col("score") == "base")
                     .filter(pl.col("gate").is_in(["none", "f5", "inst5"]))
                     .head(1).to_dicts()[0])
        top = df.head(1).to_dicts()[0]
        print(f"\n=== {fold['name']}(選擇尺 P5)===")
        print(f"對照(EV38 存活軸 P5 最優):{base_best['score']}/{base_best['gate']} "
              f"train P5 {base_best['tr_p5']:.1%}")
        print(f"全榜 top-1:{top['score']}/{top['gate']}/pm{top['pool_months']}"
              f"/h{top['h120']}/t{top['trail']}/l{top['lts']}/s{top['n_slots']}"
              f"/m{top['max_new']} train P5 {top['tr_p5']:.1%}")
        h20 = df.head(20)
        print("  榜首20 score:", dict(h20.group_by("score").len().iter_rows()))
        print("  榜首20 gate:", dict(h20.group_by("gate").len().iter_rows()))
        if top["tr_p5"] > base_best["tr_p5"] and (
                top["score"] != "base" or top["gate"] == "dm_pos"):
            cfg = {k: top[k] for k in ("score", "gate", "pool_months", "h120",
                                       "trail", "lts", "n_slots", "max_new")}
            out = run(lab, fold, **cfg, want_oos=True)
            b = bench(fold)
            print(f"★ dm 軸勝出 → OOS CAGR {out['oos']['cagr']:7.1%} "
                  f"MDD {out['oos']['mdd']:6.1%}")
            ob = run(lab, fold, **{k: base_best[k] for k in
                                   ("score", "gate", "pool_months", "h120",
                                    "trail", "lts", "n_slots", "max_new")},
                     want_oos=True)
            print(f"  對照 OOS      CAGR {ob['oos']['cagr']:7.1%} "
                  f"MDD {ob['oos']['mdd']:6.1%}")
            for nm, k in b.items():
                if k:
                    print(f"  對手 {nm}: CAGR {k['cagr']:7.1%}")
        else:
            print("dm 軸未勝過存活對照——OOS 不動用")


if __name__ == "__main__":
    main()
