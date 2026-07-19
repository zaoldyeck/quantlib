"""EV48 — 庫內籌碼資料軸 × Evergreen(預註冊見 LEDGER.md EV48 段)。

Run: uv run --project research python -m research.evergreen.ev48_chip_axes
依賴 cache: 是
"""
from __future__ import annotations

import itertools

import duckdb
import polars as pl

from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev36_walkforward import C, kpis_full, seg_kpi
from research.evergreen.ev38_exhaust import FOLDS, LabX, bench

OUT = "research/evergreen/data/ev48_results"
SCORES = ("base", "x_margin_inv", "x_sbl_inv", "x_fhold")
GATES = ("none", "f5", "fut_pos")


class LabC(LabX):
    def __init__(self):
        super().__init__()
        raw = duckdb.connect("research/cache.duckdb", read_only=True)
        codes = self.reg["code"].unique().to_list()
        ph = ",".join(f"'{c}'" for c in codes)
        mg = (raw.execute(
            f"SELECT date, company_code, margin_balance FROM margin_transactions "
            f"WHERE company_code IN ({ph})").pl()
            .sort([C, "date"])
            .with_columns((pl.col("margin_balance")
                           / pl.col("margin_balance").shift(5) - 1)
                          .over(C).alias("margin_chg5"))
            .select(["date", C, "margin_chg5"]))
        sb = (raw.execute(
            f"SELECT date, company_code, daily_balance FROM sbl_borrowing "
            f"WHERE company_code IN ({ph})").pl()
            .sort([C, "date"])
            .with_columns((pl.col("daily_balance")
                           / pl.col("daily_balance").shift(5).clip(1, None) - 1)
                          .over(C).alias("sbl_chg5"))
            .select(["date", C, "sbl_chg5"]))
        fh = (raw.execute(
            f"SELECT date, company_code, foreign_held_ratio FROM foreign_holding_ratio "
            f"WHERE company_code IN ({ph})").pl()
            .sort([C, "date"])
            .with_columns((pl.col("foreign_held_ratio")
                           - pl.col("foreign_held_ratio").shift(5))
                          .over(C).alias("fhold_chg5"))
            .select(["date", C, "fhold_chg5"]))
        fut = (raw.execute(
            "SELECT date, foreign_tx_net_oi FROM taifex_futures_daily_factors "
            "ORDER BY date").pl()
            .with_columns((pl.col("foreign_tx_net_oi") > 0).alias("fut_pos"))
            .select(["date", "fut_pos"]))
        self.trig = (self.trig
                     .join(mg, on=["date", C], how="left")
                     .join(sb, on=["date", C], how="left")
                     .join(fh, on=["date", C], how="left")
                     .join(fut, on="date", how="left")
                     .with_columns(pl.col("fut_pos").fill_null(False)))


def run(lab: LabC, fold: dict, *, score, gate, pool_months, h120, trail, lts,
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
    expr = {
        "base": base,
        "x_margin_inv": base * (1.0 - rank("margin_chg5").fill_null(0.5)),
        "x_sbl_inv": base * (1.0 - rank("sbl_chg5").fill_null(0.5)),
        "x_fhold": base * rank("fhold_chg5").fill_null(0.5),
    }[score]
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
    lab = LabC()
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
        df.write_parquet(f"{OUT}_{fold['name']}.parquet")
        base_best = (df.filter((pl.col("score") == "base")
                               & pl.col("gate").is_in(["none", "f5"]))
                     .head(1).to_dicts()[0])
        top = df.head(1).to_dicts()[0]
        print(f"\n=== {fold['name']}(P5 量尺)===")
        print(f"對照(存活軸):{base_best['score']}/{base_best['gate']} "
              f"P5 {base_best['tr_p5']:.1%}")
        print(f"top-1:{top['score']}/{top['gate']}/pm{top['pool_months']}"
              f"/h{top['h120']}/t{top['trail']}/l{top['lts']}/s{top['n_slots']}"
              f"/m{top['max_new']} P5 {top['tr_p5']:.1%}")
        h20 = df.head(20)
        print("  榜首20 score:", dict(h20.group_by("score").len().iter_rows()))
        print("  榜首20 gate:", dict(h20.group_by("gate").len().iter_rows()))
        newwin = (top["score"] != "base" or top["gate"] == "fut_pos")
        if newwin and top["tr_p5"] > base_best["tr_p5"]:
            cfg = {k: top[k] for k in ("score", "gate", "pool_months", "h120",
                                       "trail", "lts", "n_slots", "max_new")}
            out = run(lab, fold, **cfg, want_oos=True)
            ob = run(lab, fold, **{k: base_best[k] for k in cfg}, want_oos=True)
            b = bench(fold)
            print(f"★ 籌碼軸勝出 → OOS {out['oos']['cagr']:7.1%}/"
                  f"{out['oos']['mdd']:6.1%} | 對照 {ob['oos']['cagr']:7.1%}"
                  f"/{ob['oos']['mdd']:6.1%}")
            for nm, k in b.items():
                if k:
                    print(f"  對手 {nm}: {k['cagr']:+7.1%}")
        else:
            print("籌碼軸未勝過存活對照——OOS 不動用")


if __name__ == "__main__":
    main()
