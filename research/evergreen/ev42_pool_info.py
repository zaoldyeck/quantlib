"""EV42 — 池側免費資訊窮盡(設計見 LEDGER.md EV42 預註冊段)。

Run: uv run --project research python -m research.evergreen.ev42_pool_info
依賴 cache: 是。輸出:data/ev42_results.parquet + stdout 兩折榜。
"""
from __future__ import annotations

import itertools
from datetime import date as Date

import polars as pl

from research.apex import data
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev36_walkforward import C, Lab, seg_kpi
from research.evergreen.ev38_exhaust import FOLDS, bench

OUT = "research/evergreen/data/ev42_results.parquet"
SCORES = ("base", "xfresh", "xrestamp", "xtheme", "xrevacc", "xadv_inv")
POOLF = ("conv_all", "conv5_only")
FRESHW = (None, 10, 20)
GATES = ("none", "f5")


def load_reg_full() -> pl.DataFrame:
    reg = pl.read_parquet("research/evergreen/data/registry_v3.parquet")
    pilot = (pl.read_parquet("research/evergreen/data/ev28_pilot_labels.parquet")
             .filter(~pl.col("month").is_in(reg["month"].unique().implode())))
    cols = ["month", "code", "conviction", "theme"]
    return (pl.concat([reg.select(cols), pilot.select(cols)])
            .filter(pl.col("month") <= "2026-06").sort(["month", "code"]))


class LabP(Lab):
    """membership 擴充:池齡 / 重標次數 / 題材動能;+ 法人 f5、營收加速、ADV。"""

    def __init__(self):
        super().__init__()
        self.reg_full = load_reg_full()
        con = data.connect()
        codes = self.reg["code"].unique().to_list()
        fl = (data.load_flows(con, "2021-06-01", "2026-07-09")
              .filter(pl.col(C).is_in(codes)).sort([C, "date"])
              .with_columns((pl.col("foreign_diff").rolling_sum(5).over(C) > 0)
                            .alias("f5")).select(["date", C, "f5"]))
        rev = (data.load_monthly_revenue(con, "2026-07-09")
               .filter(pl.col(C).is_in(codes)).sort([C, "year", "month"])
               .with_columns([
                   pl.date(pl.col("year") + pl.col("month") // 12,
                           pl.col("month") % 12 + 1, 10).alias("avail"),
                   (pl.col("monthly_revenue_yoy").rolling_mean(3)
                    - pl.col("monthly_revenue_yoy").rolling_mean(12))
                   .over(C).alias("revacc"),
               ]).select([C, "avail", "revacc"])
               .drop_nulls().sort("avail"))
        adv = (self.panel.sort([C, "date"])
               .with_columns(pl.col("trade_value").cast(pl.Float64)
                             .rolling_median(20).over(C).alias("adv20"))
               .select(["date", C, "adv20"]))
        self.extra = (self.feats.select(["date", C])
                      .join(fl, on=["date", C], how="left")
                      .join(adv, on=["date", C], how="left")
                      .sort("date")
                      .join_asof(rev, left_on="date", right_on="avail", by=C,
                                 strategy="backward", tolerance="70d")
                      .with_columns([pl.col("f5").fill_null(False),
                                     pl.col("revacc").fill_null(0.0),
                                     pl.col("adv20").fill_null(1e12)]))
        self._memb_rich: dict[tuple, tuple] = {}

    def memb_rich(self, pool_months: int, conv5: bool):
        key = (pool_months, conv5)
        if key in self._memb_rich:
            return self._memb_rich[key]
        reg = self.reg_full
        if conv5:
            reg = reg.filter(pl.col("conviction") >= 5)
        yms = sorted(reg["month"].unique().to_list())
        stance = {}
        for ym in yms:
            y, m = int(ym[:4]), int(ym[5:7])
            stance[ym] = min(d for d in self.dates_all
                             if d.year == y and d.month == m and d.day > 10)
        ordered = [stance[ym] for ym in yms]
        rows = []
        for i, ym in enumerate(yms):
            start = ordered[i]
            end = (ordered[i + pool_months]
                   if i + pool_months < len(yms) else self.dates_all[-1])
            window = yms[max(0, i - pool_months + 1): i + 1]
            w = reg.filter(pl.col("month").is_in(window))
            theme_n = w.group_by("theme").len().rename({"len": "theme_n"})
            cur = (w.join(theme_n, on="theme")
                   .group_by("code").agg([
                       pl.col("conviction").max(),
                       pl.col("month").max().alias("last_stamp"),
                       pl.col("month").n_unique().alias("restamp"),
                       pl.col("theme_n").max(),
                   ]))
            for r in cur.to_dicts():
                age = window.index(r["last_stamp"]) if r["last_stamp"] in window else 0
                rows.append({"m_start": start, "m_end": end, C: r["code"],
                             "conv": r["conviction"], "restamp": r["restamp"],
                             "theme_n": r["theme_n"],
                             "fresh": float(len(window) - age)})
        memb = pl.DataFrame(rows)
        days = [d for d in self.dates_all if d >= ordered[0]]
        memb_d = (pl.DataFrame({"date": days}).join(memb, how="cross")
                  .filter((pl.col("date") >= pl.col("m_start"))
                          & (pl.col("date") < pl.col("m_end")))
                  .select(["date", C, "conv", "restamp", "theme_n", "fresh",
                           "m_start"])
                  .unique(subset=["date", C]).sort(["date", C]))
        flag = (pl.DataFrame({"date": days})
                .join(pl.DataFrame({C: memb_d[C].unique().to_list()}), how="cross")
                .join(memb_d.select(["date", C]), on=["date", C], how="anti")
                .sort(["date", C]))
        self._memb_rich[key] = (memb_d, flag)
        return self._memb_rich[key]


def run(lab: LabP, fold: dict, *, score, poolf, freshw, gate,
        h120, n_slots, max_new, want_oos=False):
    memb, flag = lab.memb_rich(3, poolf == "conv5_only")

    def rank(c):
        return (pl.col(c).rank() / pl.len()).over("date")

    sc = (memb.join(lab.feats, on=["date", C], how="left")
          .join(lab.extra.select(["date", C, "f5", "revacc", "adv20"]),
                on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > h120))
    if gate == "f5":
        sc = sc.filter(pl.col("f5").fill_null(False))
    if freshw is not None:
        # 站位後 N 交易日內才可新進:score 僅在新鮮窗內給值,窗外不進場
        # (以 m_start 距今交易日數近似:date - m_start 日曆日 ≤ freshw*1.5)
        sc = sc.filter((pl.col("date") - pl.col("m_start")).dt.total_days()
                       <= freshw * 1.5)
    base = rank("h52") * rank("h120")
    expr = {
        "base": base,
        "xfresh": base * rank("fresh"),
        "xrestamp": base * rank("restamp"),
        "xtheme": base * rank("theme_n"),
        "xrevacc": base * rank("revacc"),
        "xadv_inv": base * (1.0 - rank("adv20")),
    }[score]
    sc = (sc.with_columns(expr.alias("score"))
          .with_columns(pl.lit(1.0 / n_slots).alias("weight"))
          .select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))

    def one(start, end):
        res = simulate(lab.panel.filter(pl.col("date") <= end), sc,
                       exit_flags=flag, exec_spec=ExecSpec(),
                       port_spec=PortSpec(n_slots=n_slots, max_new_per_day=max_new),
                       exit_spec=ExitSpec(trailing_stop=0.30, loser_time_stop=45),
                       start=start)
        return res.nav.sort("date").filter(
            (pl.col("date") >= start) & (pl.col("date") <= end))

    out = {"train": seg_kpi(one(fold["t0"], fold["t1"]))}
    if want_oos:
        out["oos"] = seg_kpi(one(fold["o0"], fold["o1"]))
    return out


def main() -> None:
    lab = LabP()
    core = list(itertools.product((0.0, 0.6), (5, 6), (1, 2)))
    ref = {"折1": 61.5, "折2": 33.4}
    all_rows = []
    for fold in FOLDS:
        rows = []
        for sm, pf, fw, g in itertools.product(SCORES, POOLF, FRESHW, GATES):
            for h1, ns, mn in core:
                cfg = dict(score=sm, poolf=pf, freshw=fw, gate=g,
                           h120=h1, n_slots=ns, max_new=mn)
                k = run(lab, fold, **cfg)["train"]
                rows.append({"fold": fold["name"], **cfg,
                             **{f"tr_{x}": v for x, v in k.items()}})
        df = (pl.DataFrame(rows, schema_overrides={"freshw": pl.Int64},
                           infer_schema_length=None)
              .sort(["tr_martin", "tr_cagr"], descending=True))
        all_rows.append(df)
        top = df.head(1).to_dicts()[0]
        print(f"\n=== {fold['name']} top-1(EV38 折內榜首 {ref[fold['name']]}):"
              f"Martin {top['tr_martin']:.1f}")
        print({k: top[k] for k in ("score", "poolf", "freshw", "gate",
                                   "h120", "n_slots", "max_new")})
        if top["tr_martin"] > ref[fold["name"]]:
            cfg = {k: top[k] for k in ("score", "poolf", "freshw", "gate",
                                       "h120", "n_slots", "max_new")}
            out = run(lab, fold, **cfg, want_oos=True)
            b = bench(fold)
            print(f"★ 勝出 → OOS:CAGR {out['oos']['cagr']:7.1%} "
                  f"MDD {out['oos']['mdd']:6.1%}")
            for nm, k in b.items():
                if k:
                    print(f"  對手 {nm}: CAGR {k['cagr']:7.1%}")
        h20 = df.head(20)
        for col in ("score", "poolf", "freshw", "gate"):
            print(f"  榜首20 {col}: {dict(h20.group_by(col).len().iter_rows())}")
    pl.concat(all_rows).write_parquet(OUT)


if __name__ == "__main__":
    main()
