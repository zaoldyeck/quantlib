"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-10T00:41:56.172Z(工具 Bash)
涵蓋 trials(47):ev5_n12-t35-l30, ev5_n12-t35-l50, ev5_n12-t35-lx, ev5_n12-t50-l30, ev5_n12-t50-l50, ev5_n12-t50-lx, ev5_n12-tx-l30, ev5_n12-tx-l50, ev5_n12-tx-l50-cw, ev5_n12-tx-lx, ev5_n5-t35-l30, ev5_n5-t35-l50, ev5_n5-t35-lx, ev5_n5-t50-l30, ev5_n5-t50-l50, ev5_n5-t50-lx, ev5_n5-tx-l30, ev5_n5-tx-l50, ev5_n5-tx-lx, ev5_n8-t35-l30 …
"""
"""EV5 第三波:hold 上限 + 進場濾網。"""
import itertools
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.apex.experiments.g01_ml_ranker import C, kpi
from quantlib.apex.experiments.n01_momentum_single import month_firsts

reg = pl.read_parquet("src/quantlib/evergreen/data/registry_v1.parquet")
con = data.connect()
panel = data.common_stocks(data.load_panel(con, "2022-01-01", "2026-07-09", warmup_days=60))
dates_all = panel.select("date").unique().sort("date")["date"].to_list()
months = month_firsts([d for d in dates_all if d >= Date(2022, 7, 1)])
TRAIN_END = Date(2025, 7, 1)
feats = (panel.sort([C, "date"])
         .with_columns([
             (pl.col("close") / pl.col("close").rolling_max(120)).over(C).alias("h120"),
             (pl.col("close") / pl.col("close").rolling_max(252)).over(C).alias("h52"),
         ]).select(["date", C, "h120", "h52"]))

def build(hold_m, power, cap, filt):
    ent_rows, flag_rows = [], []
    all_codes = set(reg["code"].to_list())
    for i, md in enumerate(months):
        window = [m.isoformat() for m in months[max(0, i - hold_m + 1): i + 1]]
        cur = (reg.filter(pl.col("month").is_in(window))
               .group_by("code").agg(pl.col("conviction").max()).rename({"code": C}))
        if filt:
            fm = feats.filter(pl.col("date") == md)
            cur = cur.join(fm, on=C, how="left")
            col, thr = filt
            cur = cur.filter(pl.col(col).fill_null(0) > thr)
        if cur.height == 0:
            continue
        w = cur["conviction"].cast(pl.Float64).to_numpy() ** power
        w = w / w.sum()
        if cap:
            for _ in range(10):
                over = w > cap
                if not over.any():
                    break
                ex = (w[over] - cap).sum(); w[over] = cap
                un = ~over
                if w[un].sum() > 0:
                    w[un] += ex * w[un] / w[un].sum()
        for r, wi in zip(cur.to_dicts(), w):
            ent_rows.append({"date": md, C: r[C], "score": float(r["conviction"]),
                             "weight": float(wi)})
        keep = set(cur[C].to_list())
        for c_ in all_codes - keep:
            flag_rows.append({"date": md, C: c_})
    e = pl.DataFrame(ent_rows).with_columns(pl.col("date").cast(pl.Date))
    f = pl.DataFrame(flag_rows).with_columns(pl.col("date").cast(pl.Date))
    return e, f

rows = []
CELLS = []
for hold in [4, 5, 6]:
    CELLS.append(dict(hold=hold, filt=None))
for filt in [("h120", 0.7), ("h52", 0.6)]:
    for hold in [4, 5]:
        CELLS.append(dict(hold=hold, filt=filt))
for g in CELLS:
    e, f = build(g["hold"], 2.0, 0.12, g["filt"])
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=45),
                   exit_spec=ExitSpec(loser_time_stop=30),
                   start=Date(2022, 7, 1))
    nav = res.nav.select(["date", "nav"]).sort("date")
    tr = nav.filter(pl.col("date") < TRAIN_END)
    oo = nav.filter(pl.col("date") >= TRAIN_END)
    k = kpi(tr)
    v = oo["nav"].to_numpy()
    oo_r = (v[-1] / v[0] - 1) if len(v) > 10 else float("nan")
    fn = f"{g['filt'][0]}>{g['filt'][1]}" if g["filt"] else "x"
    name = f"h{g['hold']}f{fn}"
    ledger.log_trial(family="evergreen", name=f"ev5c_{name}", hypothesis="hold 上限+濾網",
                     config={"hold": g["hold"], "filt": fn}, window="2022-07..2026-07",
                     metrics={**{kk: float(vv) for kk, vv in k.items()}, "oos": float(oo_r)},
                     batch="EV5", curve=nav)
    rows.append({"cell": name, "tr_cagr": round(k["cagr"], 3), "tr_p5": round(k["p5"], 3),
                 "tr_mdd": round(k["mdd"], 3), "oos": round(oo_r, 3)})
print(pl.DataFrame(rows).sort("tr_p5", descending=True))

# MDD 解剖(champion h4p2c12l30)
e, f = build(4, 2.0, 0.12, None)
res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
               port_spec=PortSpec(n_slots=45), exit_spec=ExitSpec(loser_time_stop=30),
               start=Date(2022, 7, 1))
nav = res.nav.select(["date", "nav"]).sort("date").filter(pl.col("date") < TRAIN_END)
import numpy as np
v = nav["nav"].to_numpy(); dd = v / np.maximum.accumulate(v) - 1
d = nav["date"].to_list()
worst = np.argsort(dd)[:1]
print("\ntrain MDD 谷底日:", [(d[i], f"{dd[i]:.1%}") for i in worst])
