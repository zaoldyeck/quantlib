"""EV36 — 49 月全窗 walk-forward 單折(設計見 LEDGER.md EV36 預註冊段)。

train(2022-07-11~2025-07-10)兩階段極限優化 → top-1 凍結 → OOS
(2025-07-11~2026-07-03)一跑,對決 S(T0334)與 Serenity(abl_adv_l0)同窗。

Run: uv run --project research python -m research.evergreen.ev36_walkforward
依賴 cache: 是(需最新)。輸出:stdout 榜單 + data/ev36_results.parquet(全 config
train/OOS 指標)+ data/ev36_top1_nav.parquet(冠軍兩段 NAV)。
"""
from __future__ import annotations

import itertools
from datetime import date as Date

import numpy as np
import polars as pl

from research.apex import data
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev30_baseline import midmonth_membership
from research.evergreen.harvest import C

TRAIN0, TRAIN1 = Date(2022, 7, 11), Date(2025, 7, 10)
OOS0, OOS1 = Date(2025, 7, 11), Date(2026, 7, 3)
OUT_RES = "research/evergreen/data/ev36_results.parquet"
OUT_NAV = "research/evergreen/data/ev36_top1_nav.parquet"


def load_registry() -> pl.DataFrame:
    reg = pl.read_parquet("research/evergreen/data/registry_v3.parquet")
    pilot = (pl.read_parquet("research/evergreen/data/ev28_pilot_labels.parquet")
             .filter(~pl.col("month").is_in(reg["month"].unique().implode())))
    cols = ["month", "code", "conviction"]
    return (pl.concat([reg.select(cols), pilot.select(cols)])
            # 2026-07 標記站位日(07-13)在 OOS 終點(07-03)之後,窗外不載
            .filter(pl.col("month") <= "2026-06")
            .sort(["month", "code"]))


class Lab:
    """panel 縮至標記股票(池外股票與引擎無關),全網格提速 ~10x。"""

    def __init__(self):
        self.reg = load_registry()
        con = data.connect()
        panel_full = data.common_stocks(
            data.load_panel(con, "2021-06-01", "2026-07-09", warmup_days=300))
        self.dates_all = (panel_full.select("date").unique()
                          .sort("date")["date"].to_list())
        codes = self.reg["code"].unique().to_list()
        self.panel = panel_full.filter(pl.col(C).is_in(codes)).sort([C, "date"])
        self.feats = (self.panel
                      .with_columns([
                          (pl.col("close") / pl.col("close").rolling_max(120))
                          .over(C).alias("h120"),
                          (pl.col("close") / pl.col("close").rolling_max(252))
                          .over(C).alias("h52"),
                          (pl.col("close").shift(5) / pl.col("close").shift(126) - 1)
                          .over(C).alias("mom"),
                      ]).select(["date", C, "h120", "h52", "mom"]))
        self._memb_cache: dict[int, tuple[pl.DataFrame, pl.DataFrame]] = {}

    def memb(self, pool_months: int) -> tuple[pl.DataFrame, pl.DataFrame]:
        if pool_months not in self._memb_cache:
            m = midmonth_membership(self.reg, self.dates_all, pool_months)
            days = [d for d in self.dates_all if d >= TRAIN0]
            flag = (pl.DataFrame({"date": days})
                    .join(pl.DataFrame({C: m[C].unique().to_list()}), how="cross")
                    .join(m.select(["date", C]), on=["date", C], how="anti")
                    .sort(["date", C]))
            self._memb_cache[pool_months] = (m, flag)
        return self._memb_cache[pool_months]


def kpis_full(nav: pl.DataFrame) -> dict:
    """seg_kpi + sortino + bootstrap P5(選擇量尺研究/refit 用)。"""
    from research.apex.validate import block_bootstrap_cagr

    k = seg_kpi(nav)
    r = nav["nav"].to_numpy()
    ret = r[1:] / r[:-1] - 1
    dn = ret[ret < 0]
    dstd = dn.std(ddof=1) if len(dn) > 2 else 1e-9
    k["sortino"] = float(ret.mean() / max(dstd, 1e-9) * np.sqrt(252))
    k["p5"] = float(block_bootstrap_cagr(nav, n_boot=500)["ci_lo"])
    return k


def seg_kpi(nav: pl.DataFrame) -> dict:
    yrs = (nav["date"][-1] - nav["date"][0]).days / 365.25
    cagr = (nav["nav"][-1] / nav["nav"][0]) ** (1 / yrs) - 1
    dd = nav["nav"] / nav["nav"].cum_max() - 1
    ulcer = max(float(np.sqrt((dd.to_numpy() ** 2).mean())), 1e-9)
    return {"cagr": float(cagr), "mdd": float(dd.min()),
            "martin": float(cagr / ulcer)}


def run_cfg(lab: Lab, *, pool_months, h120, trail, lts, n_slots, max_new,
            h52_gate=None, time_stop=None, rank_mode="h52",
            want_nav=False):
    memb, flag = lab.memb(pool_months)

    def rank(c):
        return (pl.col(c).rank() / pl.len()).over("date")

    sc = (memb.join(lab.feats, on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > h120))
    if h52_gate is not None:
        sc = sc.filter(pl.col("h52") > h52_gate)
    if rank_mode == "h52":
        sc = sc.with_columns(rank("h52").alias("score"))
    elif rank_mode == "conv_h52":
        sc = sc.with_columns((rank("conv") * rank("h52")).alias("score"))
    else:  # h52_h120
        sc = sc.with_columns((rank("h52") * rank("h120")).alias("score"))
    sc = (sc.with_columns(pl.lit(1.0 / n_slots).alias("weight"))
          .select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))

    def one(start: Date, end: Date):
        res = simulate(
            lab.panel.filter(pl.col("date") <= end), sc, exit_flags=flag,
            exec_spec=ExecSpec(),
            port_spec=PortSpec(n_slots=n_slots, max_new_per_day=max_new),
            exit_spec=ExitSpec(trailing_stop=trail, loser_time_stop=lts,
                               time_stop=time_stop),
            start=start)
        nav = res.nav.sort("date").filter(
            (pl.col("date") >= start) & (pl.col("date") <= end))
        return nav

    tr_nav = one(TRAIN0, TRAIN1)
    out = {"train": seg_kpi(tr_nav)}
    if want_nav:
        oos_nav = one(OOS0, OOS1)
        out["oos"] = seg_kpi(oos_nav)
        out["navs"] = (tr_nav.with_columns(pl.lit("train").alias("seg")),
                       oos_nav.with_columns(pl.lit("oos").alias("seg")))
    return out


def bench_same_window() -> dict:
    s = pl.read_parquet("research/apex/ledger/curves/T0334.parquet")
    ser = (pl.read_csv("research/strat_lab/results/abl_adv_l0_ev_v2_thesis_inst_daily.csv",
                       schema_overrides={"date": pl.Date}))
    out = {}
    for name, df in (("S", s), ("Serenity", ser)):
        w = df.filter((pl.col("date") >= OOS0) & (pl.col("date") <= OOS1))
        if w.height < 100:
            out[name] = None
            continue
        out[name] = seg_kpi(w.select(["date", "nav"]))
    return out


def main() -> None:
    lab = Lab()
    print(f"registry ∪ pilot:{lab.reg.height} 筆 / {lab.reg['month'].n_unique()} 月;"
          f"panel {lab.panel[C].n_unique()} 檔標記股")

    # Stage A(預註冊網格)
    A_GRID = list(itertools.product(
        (2, 3, 4, 5), (0.0, 0.5, 0.6, 0.7),
        (0.30, 0.35, 0.40, 0.45), (30, 45, 60, None),
        (4, 5, 6), (1, 2, 3)))
    rows = []
    for i, (pm, h1, tr, lt, ns, mn) in enumerate(A_GRID):
        cfg = dict(pool_months=pm, h120=h1, trail=tr, lts=lt,
                   n_slots=ns, max_new=mn)
        k = run_cfg(lab, **cfg)["train"]
        rows.append({**cfg, "h52_gate": None, "time_stop": None,
                     "rank_mode": "h52", "stage": "A", **{f"tr_{x}": v for x, v in k.items()}})
        if (i + 1) % 200 == 0:
            print(f"  Stage A {i + 1}/{len(A_GRID)}")
    SCHEMA_OV = {"h52_gate": pl.Float64, "time_stop": pl.Int64,
                 "lts": pl.Int64, "h120": pl.Float64, "trail": pl.Float64}
    dfA = pl.DataFrame(rows, schema_overrides=SCHEMA_OV, infer_schema_length=None)

    # Stage B:A 榜 top-10 × 附加軸
    top10 = dfA.sort("tr_martin", descending=True).head(10)
    rowsB = []
    for base in top10.to_dicts():
        for h52g, ts, rk in itertools.product((None, 0.85, 0.95),
                                              (None, 120),
                                              ("h52", "conv_h52", "h52_h120")):
            if h52g is None and ts is None and rk == "h52":
                continue  # = base 本身
            cfg = dict(pool_months=base["pool_months"], h120=base["h120"],
                       trail=base["trail"], lts=base["lts"],
                       n_slots=base["n_slots"], max_new=base["max_new"],
                       h52_gate=h52g, time_stop=ts, rank_mode=rk)
            k = run_cfg(lab, **cfg)["train"]
            rowsB.append({**{c: base[c] for c in ("pool_months", "h120", "trail",
                                                  "lts", "n_slots", "max_new")},
                          "h52_gate": h52g, "time_stop": ts, "rank_mode": rk,
                          "stage": "B", **{f"tr_{x}": v for x, v in k.items()}})
    dfB = pl.DataFrame(rowsB, schema_overrides=SCHEMA_OV, infer_schema_length=None)
    allr = pl.concat([dfA, dfB], how="diagonal").sort(
        ["tr_martin", "tr_cagr"], descending=True)
    allr.write_parquet(OUT_RES)

    # top-3 OOS(判定僅 top-1)
    print("\n=== train 榜首 3(選擇尺 Martin,tie CAGR)===")
    navs_written = False
    oos_rows = []
    for j, r in enumerate(allr.head(3).to_dicts()):
        cfg = dict(pool_months=r["pool_months"], h120=r["h120"], trail=r["trail"],
                   lts=r["lts"], n_slots=r["n_slots"], max_new=r["max_new"],
                   h52_gate=r["h52_gate"], time_stop=r["time_stop"],
                   rank_mode=r["rank_mode"])
        out = run_cfg(lab, **cfg, want_nav=True)
        tag = "★top1" if j == 0 else f"top{j + 1}"
        print(f"{tag} {cfg}")
        print(f"   train CAGR {out['train']['cagr']:7.1%} MDD {out['train']['mdd']:6.1%} "
              f"Martin {out['train']['martin']:5.1f} | OOS CAGR {out['oos']['cagr']:7.1%} "
              f"MDD {out['oos']['mdd']:6.1%} Martin {out['oos']['martin']:5.1f}")
        oos_rows.append({"rank": j + 1, **cfg, **{f"oos_{k}": v for k, v in out["oos"].items()}})
        if not navs_written:
            pl.concat(out["navs"]).write_parquet(OUT_NAV)
            navs_written = True

    print("\n=== 同 OOS 窗對手 ===")
    for name, k in bench_same_window().items():
        if k is None:
            print(f"{name}: 曲線未覆蓋 OOS 窗——需另行重跑該策略同窗")
        else:
            print(f"{name}: CAGR {k['cagr']:7.1%}  MDD {k['mdd']:6.1%}  "
                  f"Martin {k['martin']:5.1f}")
    t1 = oos_rows[0]
    print(f"\n判準:top-1 OOS 年化 {t1['oos_cagr']:.1%} 是否同時 > S 與 Serenity 同窗")


if __name__ == "__main__":
    main()
