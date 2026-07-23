"""EV47 — ML 排名分數作 Evergreen 池內排位軸(預註冊見 LEDGER.md EV47 段)。

Run: uv run --project . python -m quantlib.evergreen.ev47_ml_axis
依賴 cache: 是 + src/quantlib/apex/ledger/g04_scores_fwd10.parquet。
"""
from __future__ import annotations

import itertools

import polars as pl

from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.evergreen.ev36_walkforward import C, kpis_full, seg_kpi
from quantlib.evergreen.ev38_exhaust import FOLDS, LabX, bench

OUT = "src/quantlib/evergreen/data/ev47_results.parquet"
SCORES = ("base", "x_ml", "ml_only")
GATES = ("none", "f5", "inst5")


class LabM(LabX):
    def __init__(self):
        super().__init__()
        ml = (pl.read_parquet("src/quantlib/apex/ledger/g04_scores_fwd10.parquet")
              .select(["date", C, pl.col("pred").alias("ml")]))
        self.trig = self.trig.join(ml, on=["date", C], how="left")


def run(lab: LabM, fold: dict, *, score, gate, pool_months, h120, trail, lts,
        n_slots, max_new, want_oos=False):
    memb, pool_flag = lab.memb(pool_months)

    def rank(c):
        return (pl.col(c).rank() / pl.len()).over("date")

    sc = (memb.join(lab.feats, on=["date", C], how="left")
          .join(lab.trig, on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > h120)
          .with_columns(rank("ml").fill_null(0.5).alias("ml_r")))
    if gate != "none":
        sc = sc.filter(pl.col(gate).fill_null(False))
    base = rank("h52") * rank("h120")
    expr = {"base": base, "x_ml": base * pl.col("ml_r"),
            "ml_only": pl.col("ml_r")}[score]
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
    lab = LabM()
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
        base_best = df.filter(pl.col("score") == "base").head(1).to_dicts()[0]
        top = df.head(1).to_dicts()[0]
        print(f"\n=== {fold['name']}(P5 量尺)===")
        print(f"對照 base 最優:{base_best['gate']} train P5 {base_best['tr_p5']:.1%}")
        print(f"全榜 top-1:{top['score']}/{top['gate']}/pm{top['pool_months']}"
              f"/h{top['h120']}/t{top['trail']}/l{top['lts']}/s{top['n_slots']}"
              f"/m{top['max_new']} train P5 {top['tr_p5']:.1%}")
        h20 = df.head(20)
        print("  榜首20 score:", dict(h20.group_by("score").len().iter_rows()))
        if top["score"] != "base" and top["tr_p5"] > base_best["tr_p5"]:
            cfg = {k: top[k] for k in ("score", "gate", "pool_months", "h120",
                                       "trail", "lts", "n_slots", "max_new")}
            out = run(lab, fold, **cfg, want_oos=True)
            ob = run(lab, fold, **{k: base_best[k] for k in cfg}, want_oos=True)
            b = bench(fold)
            print(f"★ ml 軸勝出 → OOS CAGR {out['oos']['cagr']:7.1%} "
                  f"MDD {out['oos']['mdd']:6.1%} | 對照 OOS "
                  f"{ob['oos']['cagr']:7.1%} / {ob['oos']['mdd']:6.1%}")
            for nm, k in b.items():
                if k:
                    print(f"  對手 {nm}: {k['cagr']:+7.1%}")
        else:
            print("ml 軸未勝過 base——OOS 不動用")


if __name__ == "__main__":
    main()
