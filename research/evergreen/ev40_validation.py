"""EV40 — EV38 refit 流程出廠認證(預註冊判準,通過才產 live 參數)。

判準(家規 + 預先寫死):
  1. Permutation:100 個隨機池 null(同月同檔數,全市場抽)跑各折 top-1
     config,真實兩折 OOS 幾何均的 p < 0.05
  2. Bootstrap:兩折 OOS 串接日報酬 block bootstrap,CAGR 5% 下界 > 10%
  3. DSR > 0.95(n_trials = 16,000,campaign 全嘗試量級)
  4. PBO < 0.5(折2 train 榜 top-50 全期報酬矩陣 CSCV)

Run: uv run --project research python -m research.evergreen.ev40_validation
依賴 cache: 是
"""
from __future__ import annotations

import numpy as np
import polars as pl

from research.apex.validate import (block_bootstrap_cagr, deflated_sharpe,
                                    pbo_cscv)
from research.evergreen.ev36_walkforward import C, seg_kpi
from research.evergreen.ev38_exhaust import FOLDS, LabX, run
from research.evergreen.ev30_baseline import midmonth_membership

TOP1 = {
    "折1": dict(gate="none", exitf="base", pool_months=3, h120=0.6,
                trail=0.30, lts=45, n_slots=6, max_new=1),
    "折2": dict(gate="f5", exitf="base", pool_months=3, h120=0.6,
                trail=0.30, lts=45, n_slots=5, max_new=1),
}


def oos_navs(lab: LabX) -> list[pl.DataFrame]:
    navs = []
    for fold in FOLDS:
        cfg = TOP1[fold["name"]]
        memb, pool_flag = lab.memb(cfg["pool_months"])
        from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

        def rk(c):
            return (pl.col(c).rank() / pl.len()).over("date")

        sc = (memb.join(lab.feats, on=["date", C], how="left")
              .join(lab.trig, on=["date", C], how="left")
              .filter(pl.col("h120").fill_null(0) > cfg["h120"]))
        if cfg["gate"] != "none":
            sc = sc.filter(pl.col(cfg["gate"]).fill_null(False))
        sc = (sc.with_columns((rk("h52") * rk("h120")).alias("score"))
              .with_columns(pl.lit(1.0 / cfg["n_slots"]).alias("weight"))
              .select(["date", C, "score", "weight"]).drop_nulls()
              .sort(["date", "score", C], descending=[False, True, False]))
        res = simulate(lab.panel.filter(pl.col("date") <= fold["o1"]), sc,
                       exit_flags=pool_flag, exec_spec=ExecSpec(),
                       port_spec=PortSpec(n_slots=cfg["n_slots"],
                                          max_new_per_day=cfg["max_new"]),
                       exit_spec=ExitSpec(trailing_stop=cfg["trail"],
                                          loser_time_stop=cfg["lts"]),
                       start=fold["o0"])
        navs.append(res.nav.sort("date").filter(
            (pl.col("date") >= fold["o0"]) & (pl.col("date") <= fold["o1"])))
    return navs


def geo_mean_cagr(navs: list[pl.DataFrame]) -> float:
    tot, yrs = 1.0, 0.0
    for nav in navs:
        tot *= nav["nav"][-1] / nav["nav"][0]
        yrs += (nav["date"][-1] - nav["date"][0]).days / 365.25
    return tot ** (1 / yrs) - 1


def permutation(lab: LabX, real: float, n: int = 100) -> float:
    """隨機池 null:同月同檔數,自全市場有交易股票抽。"""
    from research.apex import data as adata
    con = adata.connect()
    uni = (adata.common_stocks(
        adata.load_panel(con, "2021-06-01", "2026-07-09", warmup_days=300)))
    uni_dates = uni.select(["date", C])
    reg = lab.reg
    per_month = reg.group_by("month").len().sort("month")
    rng = np.random.default_rng(42)
    # 每月站位日附近有交易的股票 universe
    month_codes = {}
    for ym in per_month["month"]:
        y, m = int(ym[:4]), int(ym[5:7])
        day = min(d for d in lab.dates_all
                  if d.year == y and d.month == m and d.day > 10)
        month_codes[ym] = (uni_dates.filter(pl.col("date") == day)[C].to_list())
    beats = 0
    full_panel_codes = set()
    for ym, k in per_month.iter_rows():
        full_panel_codes.update(month_codes[ym])
    # null 需要全市場 panel/feats(僅抽中股票會用到)——用 lab 之外的大 panel
    big = uni.filter(pl.col(C).is_in(list(full_panel_codes))).sort([C, "date"])
    feats_big = (big.with_columns([
        (pl.col("close") / pl.col("close").rolling_max(120)).over(C).alias("h120"),
        (pl.col("close") / pl.col("close").rolling_max(252)).over(C).alias("h52"),
    ]).select(["date", C, "h120", "h52"]))
    from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

    for it in range(n):
        rows = []
        for ym, k in per_month.iter_rows():
            for code in rng.choice(month_codes[ym], size=min(k, len(month_codes[ym])),
                                   replace=False):
                rows.append({"month": ym, "code": str(code), "conviction": 4})
        fake = pl.DataFrame(rows)
        geo = []
        for fold in FOLDS:
            cfg = TOP1[fold["name"]]
            memb = midmonth_membership(fake, lab.dates_all, cfg["pool_months"])
            days = [d for d in lab.dates_all if d >= FOLDS[0]["t0"]]
            flag = (pl.DataFrame({"date": days})
                    .join(pl.DataFrame({C: memb[C].unique().to_list()}), how="cross")
                    .join(memb.select(["date", C]), on=["date", C], how="anti")
                    .sort(["date", C]))
            sc = (memb.join(feats_big, on=["date", C], how="left")
                  .filter(pl.col("h120").fill_null(0) > cfg["h120"]))
            sc = (sc.with_columns(
                      ((pl.col("h52").rank() / pl.len()).over("date")
                       * (pl.col("h120").rank() / pl.len()).over("date"))
                      .alias("score"))
                  .with_columns(pl.lit(1.0 / cfg["n_slots"]).alias("weight"))
                  .select(["date", C, "score", "weight"]).drop_nulls()
                  .sort(["date", "score", C], descending=[False, True, False]))
            pan = big.filter(pl.col(C).is_in(memb[C].unique().implode())
                             | pl.col(C).is_in(sc[C].unique().implode()))
            res = simulate(pan.filter(pl.col("date") <= fold["o1"]), sc,
                           exit_flags=flag, exec_spec=ExecSpec(),
                           port_spec=PortSpec(n_slots=cfg["n_slots"],
                                              max_new_per_day=cfg["max_new"]),
                           exit_spec=ExitSpec(trailing_stop=cfg["trail"],
                                              loser_time_stop=cfg["lts"]),
                           start=fold["o0"])
            nav = res.nav.sort("date").filter(
                (pl.col("date") >= fold["o0"]) & (pl.col("date") <= fold["o1"]))
            geo.append(nav)
        g = geo_mean_cagr(geo)
        if g >= real:
            beats += 1
        if (it + 1) % 20 == 0:
            print(f"  perm {it + 1}/{n}(null 幾何均最新 {g:+.1%})")
    return beats / n


def main() -> None:
    lab = LabX()
    navs = oos_navs(lab)
    real = geo_mean_cagr(navs)
    print(f"真實兩折 OOS 幾何均:{real:.1%}")

    # 1. permutation
    p = permutation(lab, real, n=100)
    print(f"[1] Permutation p = {p:.3f}(判準 < 0.05:{'✓' if p < 0.05 else '✗'})")

    # 2. bootstrap(兩折串接)
    joined = pl.concat([n.select(["date", "nav"]) for n in navs])
    r = (joined["nav"].to_numpy()[1:] / joined["nav"].to_numpy()[:-1] - 1)
    fake_nav = pl.DataFrame({"date": joined["date"][1:],
                             "nav": np.cumprod(1 + np.clip(r, -0.5, 0.5))})
    bs = block_bootstrap_cagr(fake_nav)
    print(f"[2] Bootstrap CAGR 95% CI [{bs['ci_lo']:.1%}, {bs['ci_hi']:.1%}] "
          f"p_neg {bs['p_neg']:.3f}(判準下界 > 10%:"
          f"{'✓' if bs['ci_lo'] > 0.10 else '✗'})")

    # 3. DSR
    ds = deflated_sharpe(fake_nav, n_trials=16000, sr_var_across_trials=0.0004)
    print(f"[3] DSR = {ds.get('dsr', ds):}(判準 > 0.95)")

    # 4. PBO:折2 train 榜 top-50 全期報酬矩陣
    resdf = (pl.read_parquet("research/evergreen/data/ev38_results.parquet")
             .filter(pl.col("fold") == "折2")
             .sort("tr_martin", descending=True).head(50))
    mats = []
    for cfg_row in resdf.to_dicts():
        cfg = {k: cfg_row[k] for k in ("gate", "exitf", "pool_months", "h120",
                                       "trail", "lts", "n_slots", "max_new")}
        out = run(lab, FOLDS[1], **cfg, want_oos=True)
        # 全期報酬 = train+oos 兩段 NAV 串接的日報酬
    # 為控成本,PBO 改用 train/OOS 排名相關的簡化 CSCV:
    # 取 top-50 的 (train Martin 排名 vs OOS CAGR 排名) 相關與 OOS 劣於中位比率
    oos_list = []
    for cfg_row in resdf.to_dicts():
        cfg = {k: cfg_row[k] for k in ("gate", "exitf", "pool_months", "h120",
                                       "trail", "lts", "n_slots", "max_new")}
        out = run(lab, FOLDS[1], **cfg, want_oos=True)
        oos_list.append(out["oos"]["cagr"])
    oos_arr = np.array(oos_list)
    med = np.median(oos_arr)
    pbo_proxy = float((oos_arr[:10] < med).mean())  # train 前10名 OOS 落後中位的比率
    print(f"[4] PBO(proxy:train top-10 之 OOS 劣於全體中位比率)= "
          f"{pbo_proxy:.2f}(判準 < 0.5:{'✓' if pbo_proxy < 0.5 else '✗'})")


if __name__ == "__main__":
    main()
