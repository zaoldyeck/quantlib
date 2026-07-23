"""EV50 — 動態加減碼 × Evergreen(預註冊見 LEDGER.md EV50 段)。

Run: uv run --project . python -m quantlib.evergreen.ev50_pyramid
依賴 cache: 是
"""
from __future__ import annotations

import itertools

import polars as pl

from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.evergreen.ev36_walkforward import C, kpis_full, seg_kpi
from quantlib.evergreen.ev38_exhaust import FOLDS, LabX, bench

OUT = "src/quantlib/evergreen/data/ev50_results"
PYRS = (None, (0.15, 1, 0.5), (0.15, 1, 1.0), (0.30, 1, 0.5),
        (0.30, 1, 1.0), (0.15, 2, 0.5))
RCS = (None, (0.6, 0.4))


def run(lab: LabX, fold: dict, *, gate, h120, trail, n_slots, max_new,
        pyr, rc, want_oos=False):
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
    ps = (PortSpec(n_slots=n_slots, max_new_per_day=max_new) if pyr is None
          else PortSpec(n_slots=n_slots, max_new_per_day=max_new,
                        pyramid_trigger=pyr[0], pyramid_max=pyr[1],
                        pyramid_frac=pyr[2]))

    def one(start, end):
        res = simulate(lab.panel.filter(pl.col("date") <= end), sc,
                       exit_flags=pool_flag, exec_spec=ExecSpec(),
                       port_spec=ps,
                       exit_spec=ExitSpec(trailing_stop=trail,
                                          loser_time_stop=45,
                                          profit_recycle=rc),
                       start=start)
        return res.nav.sort("date").filter(
            (pl.col("date") >= start) & (pl.col("date") <= end))

    out = {"train": kpis_full(one(fold["t0"], fold["t1"]))}
    if want_oos:
        out["oos"] = seg_kpi(one(fold["o0"], fold["o1"]))
    return out


def main() -> None:
    lab = LabX()
    core = list(itertools.product(("none", "inst5"), (0.0, 0.6),
                                  (0.30, 0.40), (5, 6), (1, 2)))
    for fold in FOLDS:
        rows = []
        for pyr, rc in itertools.product(PYRS, RCS):
            for g, h1, tr, ns, mn in core:
                cfg = dict(gate=g, h120=h1, trail=tr, n_slots=ns, max_new=mn,
                           pyr=pyr, rc=rc)
                k = run(lab, fold, **cfg)["train"]
                rows.append({"gate": g, "h120": h1, "trail": tr, "n_slots": ns,
                             "max_new": mn,
                             "pyr": "off" if pyr is None else f"{pyr[0]}/{pyr[1]}/{pyr[2]}",
                             "rc": "off" if rc is None else f"{rc[0]}/{rc[1]}",
                             **{f"tr_{x}": v for x, v in k.items()}})
        df = pl.DataFrame(rows).sort(["tr_p5", "tr_cagr"], descending=True)
        df.write_parquet(f"{OUT}_{fold['name']}.parquet")
        base_best = (df.filter((pl.col("pyr") == "off") & (pl.col("rc") == "off"))
                     .head(1).to_dicts()[0])
        top = df.head(1).to_dicts()[0]
        print(f"\n=== {fold['name']}(P5 量尺)===")
        print(f"對照(加減碼全關):P5 {base_best['tr_p5']:.1%}")
        print(f"top-1:pyr={top['pyr']} rc={top['rc']} "
              f"{top['gate']}/h{top['h120']}/t{top['trail']}/s{top['n_slots']}"
              f"/m{top['max_new']} P5 {top['tr_p5']:.1%}")
        h20 = df.head(20)
        print("  榜首20 pyr:", dict(h20.group_by("pyr").len().iter_rows()))
        print("  榜首20 rc:", dict(h20.group_by("rc").len().iter_rows()))
        if (top["pyr"] != "off" or top["rc"] != "off") and \
                top["tr_p5"] > base_best["tr_p5"]:
            def parse(row):
                pyr = (None if row["pyr"] == "off"
                       else tuple(float(x) if i != 1 else int(float(x))
                                  for i, x in enumerate(row["pyr"].split("/"))))
                rc = (None if row["rc"] == "off"
                      else tuple(float(x) for x in row["rc"].split("/")))
                return dict(gate=row["gate"], h120=row["h120"],
                            trail=row["trail"], n_slots=row["n_slots"],
                            max_new=row["max_new"], pyr=pyr, rc=rc)
            out = run(lab, fold, **parse(top), want_oos=True)
            ob = run(lab, fold, **parse(base_best), want_oos=True)
            b = bench(fold)
            print(f"★ 加減碼勝出 → OOS {out['oos']['cagr']:7.1%}/"
                  f"{out['oos']['mdd']:6.1%} | 對照 {ob['oos']['cagr']:7.1%}"
                  f"/{ob['oos']['mdd']:6.1%}"
                  + "".join(f" | {nm} {v['cagr']:+.1%}"
                            for nm, v in b.items() if v))
        else:
            print("加減碼未勝過對照——OOS 不動用")


if __name__ == "__main__":
    main()
