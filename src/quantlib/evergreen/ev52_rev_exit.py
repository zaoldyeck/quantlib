"""EV52 — 營收轉衰出場 × Evergreen(預註冊見 LEDGER.md EV52 段)。

Run: uv run --project . python -m quantlib.evergreen.ev52_rev_exit
依賴 cache: 是
"""
from __future__ import annotations

import itertools

import polars as pl

from quantlib.apex import data
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.evergreen.ev36_walkforward import C, kpis_full, seg_kpi
from quantlib.evergreen.ev38_exhaust import FOLDS, LabX, bench

REVX = ("off", "neg1", "neg2", "dual")


class LabRv(LabX):
    def __init__(self):
        super().__init__()
        con = data.connect()
        codes = self.reg["code"].unique().to_list()
        rev = (data.load_monthly_revenue(con, "2026-07-19")
               .filter(pl.col(C).is_in(codes)).sort([C, "year", "month"])
               .with_columns([
                   pl.date(pl.col("year") + pl.col("month") // 12,
                           pl.col("month") % 12 + 1, 10).alias("avail"),
                   (pl.col("monthly_revenue_yoy") < 0).alias("neg"),
                   ((pl.col("monthly_revenue_yoy") < 0)
                    & (pl.col("monthly_revenue_yoy").shift(1).over(C) < 0))
                   .alias("neg2"),
                   ((pl.col("monthly_revenue_yoy") < 0)
                    & ((pl.col("monthly_revenue").rolling_sum(3)
                        / pl.col("monthly_revenue").rolling_sum(3).shift(3) - 1)
                       .over(C) < 0)).alias("dual"),
               ]).select([C, "avail", "neg", "neg2", "dual"])
               .drop_nulls(subset=["avail"]).sort("avail"))
        # 日頻展開:每日以最新已公布月營收判定
        days = pl.DataFrame({"date": [d for d in self.dates_all
                                      if d >= FOLDS[0]["t0"]]})
        grid = (days.join(pl.DataFrame({C: codes}), how="cross").sort("date")
                .join_asof(rev, left_on="date", right_on="avail", by=C,
                           strategy="backward", tolerance="70d"))
        self.rev_flags = {
            "neg1": grid.filter(pl.col("neg").fill_null(False)).select(["date", C]),
            "neg2": grid.filter(pl.col("neg2").fill_null(False)).select(["date", C]),
            "dual": grid.filter(pl.col("dual").fill_null(False)).select(["date", C]),
        }


def run(lab: LabRv, fold: dict, *, revx, gate, h120, trail, n_slots, max_new,
        want_oos=False):
    memb, pool_flag = lab.memb(3)

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
    if revx != "off":
        flag = (pl.concat([pool_flag, lab.rev_flags[revx]])
                .unique(subset=["date", C]).sort(["date", C]))

    def one(start, end):
        res = simulate(lab.panel.filter(pl.col("date") <= end), sc,
                       exit_flags=flag, exec_spec=ExecSpec(),
                       port_spec=PortSpec(n_slots=n_slots, max_new_per_day=max_new),
                       exit_spec=ExitSpec(trailing_stop=trail,
                                          loser_time_stop=45),
                       start=start)
        return res.nav.sort("date").filter(
            (pl.col("date") >= start) & (pl.col("date") <= end))

    out = {"train": kpis_full(one(fold["t0"], fold["t1"]))}
    if want_oos:
        out["oos"] = seg_kpi(one(fold["o0"], fold["o1"]))
    return out


def main() -> None:
    lab = LabRv()
    core = list(itertools.product(("none", "inst5"), (0.0, 0.6),
                                  (0.30, 0.40), (5, 6), (1, 2)))
    for fold in FOLDS:
        rows = []
        for rx in REVX:
            for g, h1, tr, ns, mn in core:
                cfg = dict(revx=rx, gate=g, h120=h1, trail=tr,
                           n_slots=ns, max_new=mn)
                k = run(lab, fold, **cfg)["train"]
                rows.append({**cfg, **{f"tr_{x}": v for x, v in k.items()}})
        df = pl.DataFrame(rows).sort(["tr_p5", "tr_cagr"], descending=True)
        base_best = df.filter(pl.col("revx") == "off").head(1).to_dicts()[0]
        top = df.head(1).to_dicts()[0]
        print(f"\n=== {fold['name']}(P5 量尺)===")
        print(f"對照(無營收出場):P5 {base_best['tr_p5']:.1%}")
        print(f"top-1:revx={top['revx']} {top['gate']}/h{top['h120']}"
              f"/t{top['trail']}/s{top['n_slots']}/m{top['max_new']} "
              f"P5 {top['tr_p5']:.1%}")
        h20 = df.head(20)
        print("  榜首20 revx:", dict(h20.group_by("revx").len().iter_rows()))
        if top["revx"] != "off" and top["tr_p5"] > base_best["tr_p5"]:
            cfg = {k: top[k] for k in ("revx", "gate", "h120", "trail",
                                       "n_slots", "max_new")}
            out = run(lab, fold, **cfg, want_oos=True)
            ob = run(lab, fold, **{**cfg, "revx": "off"}, want_oos=True)
            b = bench(fold)
            print(f"★ 營收出場勝出 → OOS {out['oos']['cagr']:7.1%}/"
                  f"{out['oos']['mdd']:6.1%} | 對照 {ob['oos']['cagr']:7.1%}"
                  f"/{ob['oos']['mdd']:6.1%}"
                  + "".join(f" | {nm} {v['cagr']:+.1%}"
                            for nm, v in b.items() if v))
        else:
            print("營收出場未勝過對照——OOS 不動用")


if __name__ == "__main__":
    main()
