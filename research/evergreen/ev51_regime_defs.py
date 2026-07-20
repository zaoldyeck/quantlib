"""EV51 — Regime 定義家族掃描(預註冊見 LEDGER.md EV51 段)。

Run: uv run --project research python -m research.evergreen.ev51_regime_defs
依賴 cache: 是
"""
from __future__ import annotations

import itertools

import duckdb
import numpy as np
import polars as pl

from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev36_walkforward import C, kpis_full, seg_kpi
from research.evergreen.ev38_exhaust import FOLDS, LabX, bench

CORE = [dict(gate=g, pool_months=pm, h120=h1, trail=tr, lts=lt,
             n_slots=5, max_new=2)
        for g, pm, h1, tr, lt in itertools.product(
            ("none", "inst5"), (2, 3), (0.0, 0.6), (0.30, 0.40), (30, 45))]


def regime_bool(kind: str, lab: LabX) -> pl.DataFrame:
    raw = duckdb.connect("research/cache.duckdb", read_only=True)
    idx = (raw.execute("SELECT date, close FROM market_index "
                       "WHERE name = '發行量加權股價指數' ORDER BY date").pl()
           .sort("date"))
    if kind.startswith("ma"):
        w = int(kind[2:])
        s = (idx.with_columns(pl.col("close").rolling_mean(w).alias("m"))
             .with_columns((pl.col("close") > pl.col("m")).alias("att")))
    elif kind == "cross20_120":
        s = (idx.with_columns([
                pl.col("close").rolling_mean(20).alias("f"),
                pl.col("close").rolling_mean(120).alias("s_")])
             .with_columns((pl.col("f") > pl.col("s_")).alias("att")))
    elif kind == "pool_breadth50":
        pb = (lab.panel.sort([C, "date"])
              .with_columns((pl.col("close")
                             > pl.col("close").rolling_mean(60))
                            .over(C).alias("ab"))
              .group_by("date").agg(pl.col("ab").mean().alias("b"))
              .with_columns((pl.col("b") > 0.5).alias("att"))
              .select(["date", "att"]).sort("date"))
        return pb.drop_nulls()
    elif kind == "fut_oi":
        s = (raw.execute("SELECT date, foreign_tx_net_oi FROM "
                         "taifex_futures_daily_factors ORDER BY date").pl()
             .with_columns((pl.col("foreign_tx_net_oi") > 0).alias("att")))
        return s.select(["date", "att"]).drop_nulls()
    return s.select(["date", "att"]).drop_nulls()


def confirmed(att: pl.DataFrame, confirm: int) -> pl.DataFrame:
    if confirm <= 1:
        return att.rename({"att": "regime"})
    rows = att.sort("date").to_dicts()
    out, cur, streak = [], None, 0
    for r in rows:
        s = r["att"]
        if cur is None:
            cur = s
        elif s != cur:
            streak += 1
            if streak >= confirm:
                cur, streak = s, 0
        else:
            streak = 0
        out.append({"date": r["date"], "regime": cur})
    return pl.DataFrame(out)


def full_navs(lab: LabX, fold: dict) -> list[dict]:
    """32 config × (train NAV, OOS 日報酬) 一次算好快取。"""
    out = []
    for cfg in CORE:
        memb, pool_flag = lab.memb(cfg["pool_months"])

        def rank(c):
            return (pl.col(c).rank() / pl.len()).over("date")

        sc = (memb.join(lab.feats, on=["date", C], how="left")
              .join(lab.trig, on=["date", C], how="left")
              .filter(pl.col("h120").fill_null(0) > cfg["h120"]))
        if cfg["gate"] != "none":
            sc = sc.filter(pl.col(cfg["gate"]).fill_null(False))
        sc = (sc.with_columns((rank("h52") * rank("h120")).alias("score"))
              .with_columns(pl.lit(0.2).alias("weight"))
              .select(["date", C, "score", "weight"]).drop_nulls()
              .sort(["date", "score", C], descending=[False, True, False]))

        def one(start, end):
            res = simulate(lab.panel.filter(pl.col("date") <= end), sc,
                           exit_flags=pool_flag, exec_spec=ExecSpec(),
                           port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                           exit_spec=ExitSpec(trailing_stop=cfg["trail"],
                                              loser_time_stop=cfg["lts"]),
                           start=start)
            return res.nav.sort("date").filter(
                (pl.col("date") >= start) & (pl.col("date") <= end))

        tr = one(fold["t0"], fold["t1"])
        v = tr["nav"].to_numpy()
        tr_r = pl.DataFrame({"date": tr["date"],
                             "r": np.concatenate([[0.0], v[1:] / v[:-1] - 1])})
        out.append({"cfg": cfg, "tr_r": tr_r, "sc": sc, "flagm": pool_flag})
    return out


def seg_p5(tr_r: pl.DataFrame, reg: pl.DataFrame, want: bool) -> float:
    j = tr_r.join(reg, on="date", how="left").filter(pl.col("regime") == want)
    if j.height < 60:
        return float("-inf")
    nav = pl.DataFrame({"date": j["date"],
                        "nav": np.cumprod(1 + j["r"].to_numpy())})
    return kpis_full(nav)["p5"]


def switch_train_p5(a_r, d_r, reg) -> float:
    j = (a_r.rename({"r": "ra"}).join(d_r.rename({"r": "rd"}), on="date")
         .join(reg, on="date", how="left")
         .with_columns(pl.when(pl.col("regime")).then(pl.col("ra"))
                       .otherwise(pl.col("rd")).alias("r")))
    nav = pl.DataFrame({"date": j["date"],
                        "nav": np.cumprod(1 + j["r"].to_numpy())})
    return kpis_full(nav)["p5"]


def real_switch_oos(lab, fold, cfg_a, cfg_d, reg) -> dict:
    from research.evergreen.ev49b_regime_real import run_seg
    rows = (reg.filter((pl.col("date") >= fold["o0"])
                       & (pl.col("date") <= fold["o1"])).sort("date").to_dicts())
    segs, cur, start = [], None, None
    for r in rows:
        if cur is None:
            cur, start = r["regime"], r["date"]
        elif r["regime"] != cur:
            segs.append((start, r["date"], cur))
            cur, start = r["regime"], r["date"]
    segs.append((start, rows[-1]["date"], cur))
    cap, parts = 1_000_000.0, []
    for (s0, s1, att) in segs:
        nav, end_nav, inv = run_seg(lab, cfg_a if att else cfg_d, s0, s1, cap)
        if nav.height:
            parts.append(nav)
        cap = end_nav * (1 - 0.004 * inv)
    full = pl.concat(parts).sort("date").unique(subset="date", keep="last")
    k = seg_kpi(full)
    k["n_switch"] = len(segs) - 1
    return k


def main() -> None:
    lab = LabX()
    DEFS = ([(f"ma{w}", c) for w in (60, 120, 200) for c in (1, 3, 5)]
            + [("cross20_120", 1)]
            + [("pool_breadth50", c) for c in (1, 3)]
            + [("fut_oi", c) for c in (1, 3)])
    for fold in FOLDS:
        navs = full_navs(lab, fold)
        picks = []
        for kind, conf in DEFS:
            att = regime_bool(kind, lab)
            reg = confirmed(att, conf)
            best_a = max(navs, key=lambda x: seg_p5(x["tr_r"], reg, True))
            best_d = max(navs, key=lambda x: seg_p5(x["tr_r"], reg, False))
            p5 = switch_train_p5(best_a["tr_r"], best_d["tr_r"], reg)
            picks.append({"kind": kind, "conf": conf, "p5": p5,
                          "cfg_a": best_a["cfg"], "cfg_d": best_d["cfg"]})
        mono = max(navs, key=lambda x: kpis_full(pl.DataFrame({
            "date": x["tr_r"]["date"],
            "nav": np.cumprod(1 + x["tr_r"]["r"].to_numpy())}))["p5"])
        mono_p5 = kpis_full(pl.DataFrame({
            "date": mono["tr_r"]["date"],
            "nav": np.cumprod(1 + mono["tr_r"]["r"].to_numpy())}))["p5"]
        top = max(picks, key=lambda x: x["p5"])
        print(f"\n=== {fold['name']} ===")
        print(f"單引擎 train P5 對照:{mono_p5:.1%}")
        for p in sorted(picks, key=lambda x: -x["p5"])[:5]:
            print(f"  {p['kind']:15s} conf={p['conf']}: train 切換 P5 {p['p5']:.1%}")
        print(f"train 自選:{top['kind']} conf={top['conf']}"
              f"(P5 {top['p5']:.1%} vs 單引擎 {mono_p5:.1%})")
        if top["p5"] > mono_p5:
            att = regime_bool(top["kind"], lab)
            reg = confirmed(att, top["conf"])
            k = real_switch_oos(lab, fold, top["cfg_a"], top["cfg_d"], reg)
            b = bench(fold)
            print(f"★ OOS 真切換:CAGR {k['cagr']:7.1%} MDD {k['mdd']:6.1%}"
                  f"(切換 {k['n_switch']} 次)"
                  + "".join(f" | {nm} {v['cagr']:+.1%}"
                            for nm, v in b.items() if v))
            print("  對照 單引擎 OOS:折1 94.5/−33.7、折2 467.3/−21.0")
        else:
            print("train 切換未勝單引擎——OOS 不動用")


if __name__ == "__main__":
    main()
