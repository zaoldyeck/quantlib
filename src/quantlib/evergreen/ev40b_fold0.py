"""EV40b — 補折 0(train 1 年 → OOS 2023-07~2024-07)擴認證樣本至三折。

折 0:train 2022-07-11~2023-07-10(refit 部署首年的誠實形態:資料就是少)
→ OOS0 2023-07-11~2024-07-10。EV38 網格 train 選 top-1 → OOS0 一跑;
三折串接重算 bootstrap 與 DSR。

Run: uv run --project . python -m quantlib.evergreen.ev40b_fold0
依賴 cache: 是
"""
from __future__ import annotations

import itertools
from datetime import date as Date

import numpy as np
import polars as pl

from quantlib.apex.validate import block_bootstrap_cagr, deflated_sharpe
from quantlib.evergreen.ev36_walkforward import C
from quantlib.evergreen.ev38_exhaust import (EXITS, FOLDS, GATES, LabX, bench,
                                             run)
from quantlib.evergreen.ev40_validation import TOP1, geo_mean_cagr, oos_navs

FOLD0 = {"name": "折0", "t0": Date(2022, 7, 11), "t1": Date(2023, 7, 10),
         "o0": Date(2023, 7, 11), "o1": Date(2024, 7, 10)}


def main() -> None:
    lab = LabX()
    core = list(itertools.product((2, 3), (0.0, 0.6), (0.30, 0.40),
                                  (30, 45), (5, 6), (1, 2)))
    rows = []
    for gate, exitf in itertools.product(GATES, EXITS):
        for pm, h1, tr, lt, ns, mn in core:
            cfg = dict(gate=gate, exitf=exitf, pool_months=pm, h120=h1,
                       trail=tr, lts=lt, n_slots=ns, max_new=mn)
            k = run(lab, FOLD0, **cfg)["train"]
            rows.append({**cfg, **{f"tr_{x}": v for x, v in k.items()}})
    df = pl.DataFrame(rows).sort(["tr_martin", "tr_cagr"], descending=True)
    top = df.head(1).to_dicts()[0]
    cfg = {k: top[k] for k in ("gate", "exitf", "pool_months", "h120",
                               "trail", "lts", "n_slots", "max_new")}
    out = run(lab, FOLD0, **cfg, want_oos=True)
    b = bench(FOLD0)
    print(f"折0 train top-1:{cfg}")
    print(f"train Martin {out['train']['martin']:.1f} | "
          f"OOS0 CAGR {out['oos']['cagr']:7.1%} MDD {out['oos']['mdd']:6.1%}")
    for nm, k in b.items():
        print(f"  對手 {nm}: " + (f"CAGR {k['cagr']:7.1%} MDD {k['mdd']:6.1%}"
                                 if k else "曲線未覆蓋"))

    # 三折重算認證(折0 NAV + 既有兩折)
    from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
    memb, pool_flag = lab.memb(cfg["pool_months"])

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
    extra = lab.exit_flags_extra[cfg["exitf"]]
    flag = pool_flag if extra is None else (
        pl.concat([pool_flag, extra]).unique(subset=["date", C]).sort(["date", C]))
    res = simulate(lab.panel.filter(pl.col("date") <= FOLD0["o1"]), sc,
                   exit_flags=flag, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=cfg["n_slots"],
                                      max_new_per_day=cfg["max_new"]),
                   exit_spec=ExitSpec(trailing_stop=cfg["trail"],
                                      loser_time_stop=cfg["lts"]),
                   start=FOLD0["o0"])
    nav0 = res.nav.sort("date").filter(
        (pl.col("date") >= FOLD0["o0"]) & (pl.col("date") <= FOLD0["o1"]))

    navs = [nav0] + oos_navs(lab)
    real3 = geo_mean_cagr(navs)
    print(f"\n三折 OOS 幾何均:{real3:.1%}(兩折時 307.8%)")
    joined = pl.concat([n.select(["date", "nav"]) for n in navs])
    r = (joined["nav"].to_numpy()[1:] / joined["nav"].to_numpy()[:-1] - 1)
    fake = pl.DataFrame({"date": joined["date"][1:],
                         "nav": np.cumprod(1 + np.clip(r, -0.5, 0.5))})
    bs = block_bootstrap_cagr(fake)
    print(f"[2'] 三折 Bootstrap CAGR 95% CI [{bs['ci_lo']:.1%}, {bs['ci_hi']:.1%}] "
          f"p_neg {bs['p_neg']:.3f}(判準下界 > 10%:"
          f"{'✓' if bs['ci_lo'] > 0.10 else '✗'})")
    ds = deflated_sharpe(fake, n_trials=16000, sr_var_across_trials=0.0004)
    print(f"[3'] 三折 DSR = {ds}(判準 > 0.95)")


if __name__ == "__main__":
    main()
