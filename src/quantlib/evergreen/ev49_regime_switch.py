"""EV49 — Regime 切換上界(預註冊見 LEDGER.md EV49 段)。

regime:TAIEX > MA120 = 攻擊;≤ MA120 = 防禦。各折 train 按 regime 分段
各選 top-1(EV43 核心網格)→ OOS 以 regime 日拼接兩引擎日報酬 = 無縫切換
上界(忽略切換成本)。上界不勝單引擎 → regime 線關閉。

Run: uv run --project . python -m quantlib.evergreen.ev49_regime_switch
依賴 cache: 是
"""
from __future__ import annotations

import itertools

import duckdb
import numpy as np
import polars as pl

from quantlib.evergreen.ev36_walkforward import seg_kpi
from quantlib.evergreen.ev38_exhaust import FOLDS, LabX, bench
from quantlib.evergreen.ev43_live_refit import run_live  # noqa: F401(文件參照)
from quantlib.evergreen.ev47_ml_axis import run as run_cfg_base
from quantlib import paths


def regime_series() -> pl.DataFrame:
    raw = duckdb.connect(f"{paths.CACHE_DB}", read_only=True)
    idx = (raw.execute("SELECT date, close FROM market_index "
                       "WHERE name = '發行量加權股價指數' ORDER BY date").pl()
           .with_columns(pl.col("close").rolling_mean(120).alias("ma120"))
           .with_columns((pl.col("close") > pl.col("ma120")).alias("attack"))
           .select(["date", "attack"]).drop_nulls())
    return idx


class LabR(LabX):
    pass


def nav_daily_returns(nav: pl.DataFrame) -> pl.DataFrame:
    v = nav["nav"].to_numpy()
    r = np.concatenate([[0.0], v[1:] / v[:-1] - 1.0])
    return pl.DataFrame({"date": nav["date"], "r": r})


def run_full(lab, fold, cfg, seg_filter=None, regime=None):
    """跑單 config;train KPI 可限 regime 分段(seg_filter)。回傳 train_kpi 或 OOS 日報酬。"""
    from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
    from quantlib.evergreen.ev36_walkforward import C, kpis_full

    memb, pool_flag = lab.memb(cfg["pool_months"])

    def rank(c):
        return (pl.col(c).rank() / pl.len()).over("date")

    sc = (memb.join(lab.feats, on=["date", C], how="left")
          .join(lab.trig, on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > cfg["h120"]))
    if cfg["gate"] != "none":
        sc = sc.filter(pl.col(cfg["gate"]).fill_null(False))
    sc = (sc.with_columns((rank("h52") * rank("h120")).alias("score"))
          .with_columns(pl.lit(1.0 / cfg["n_slots"]).alias("weight"))
          .select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))

    def one(start, end):
        res = simulate(lab.panel.filter(pl.col("date") <= end), sc,
                       exit_flags=pool_flag, exec_spec=ExecSpec(),
                       port_spec=PortSpec(n_slots=cfg["n_slots"],
                                          max_new_per_day=cfg["max_new"]),
                       exit_spec=ExitSpec(trailing_stop=cfg["trail"],
                                          loser_time_stop=cfg["lts"]),
                       start=start)
        return res.nav.sort("date").filter(
            (pl.col("date") >= start) & (pl.col("date") <= end))

    tr_nav = one(fold["t0"], fold["t1"])
    if seg_filter is not None:
        rr = nav_daily_returns(tr_nav).join(regime, on="date", how="left")
        rr = rr.filter(pl.col("attack") == seg_filter)
        if rr.height < 60:
            return None, None
        nav_seg = pl.DataFrame({
            "date": rr["date"],
            "nav": np.cumprod(1 + rr["r"].to_numpy())})
        return kpis_full(nav_seg), None
    oos_nav = one(fold["o0"], fold["o1"])
    return kpis_full(tr_nav), nav_daily_returns(oos_nav)


def main() -> None:
    lab = LabR()
    reg = regime_series()
    core = [dict(score="x", gate=g, pool_months=pm, h120=h1, trail=tr,
                 lts=lt, n_slots=ns, max_new=mn)
            for g, pm, h1, tr, lt, ns, mn in itertools.product(
                ("none", "f5", "inst5"), (2, 3), (0.0, 0.6),
                (0.30, 0.40), (30, 45), (5, 6), (1, 2))]
    for c in core:
        c.pop("score")

    for fold in FOLDS:
        best = {}
        for seg, name in ((True, "攻擊段"), (False, "防禦段")):
            rows = []
            for cfg in core:
                k, _ = run_full(lab, fold, cfg, seg_filter=seg, regime=reg)
                if k:
                    rows.append({**cfg, **{f"tr_{x}": v for x, v in k.items()}})
            df = pl.DataFrame(rows).sort(["tr_p5", "tr_cagr"], descending=True)
            best[seg] = df.head(1).to_dicts()[0]
            print(f"{fold['name']} {name} top-1:"
                  f"{ {k: best[seg][k] for k in ('gate','pool_months','h120','trail','lts','n_slots','max_new')} }"
                  f" 段內 P5 {best[seg]['tr_p5']:.1%}")
        # 單引擎對照 = 全窗 train P5 最優
        rows = []
        for cfg in core:
            k, _ = run_full(lab, fold, cfg)
            rows.append({**cfg, **{f"tr_{x}": v for x, v in k.items()}})
        mono = (pl.DataFrame(rows).sort(["tr_p5", "tr_cagr"], descending=True)
                .head(1).to_dicts()[0])
        mono_cfg = {k: mono[k] for k in ("gate", "pool_months", "h120", "trail",
                                         "lts", "n_slots", "max_new")}
        _, mono_oos = run_full(lab, fold, mono_cfg)
        # 上界拼接:OOS 各日按 regime 取對應引擎日報酬
        _, ra = run_full(lab, fold, {k: best[True][k] for k in mono_cfg})
        _, rd = run_full(lab, fold, {k: best[False][k] for k in mono_cfg})
        j = (ra.rename({"r": "r_a"})
             .join(rd.rename({"r": "r_d"}), on="date", how="inner")
             .join(reg, on="date", how="left")
             .with_columns(pl.when(pl.col("attack")).then(pl.col("r_a"))
                           .otherwise(pl.col("r_d")).alias("r")))
        sw_nav = pl.DataFrame({"date": j["date"],
                               "nav": np.cumprod(1 + j["r"].to_numpy())})
        mono_nav = pl.DataFrame({"date": mono_oos["date"],
                                 "nav": np.cumprod(1 + mono_oos["r"].to_numpy())})
        ks, km = seg_kpi(sw_nav), seg_kpi(mono_nav)
        b = bench(fold)
        print(f"{fold['name']} OOS:切換上界 CAGR {ks['cagr']:7.1%} MDD {ks['mdd']:6.1%}"
              f" | 單引擎 {km['cagr']:7.1%} / {km['mdd']:6.1%}"
              + "".join(f" | {nm} {k['cagr']:+.1%}" for nm, k in b.items() if k))


if __name__ == "__main__":
    main()
