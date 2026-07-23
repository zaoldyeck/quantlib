"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-10T00:43:04.693Z(工具 Bash)
涵蓋 trials(58):ev5_n12-t35-l30, ev5_n12-t35-l50, ev5_n12-t35-lx, ev5_n12-t50-l30, ev5_n12-t50-l50, ev5_n12-t50-lx, ev5_n12-tx-l30, ev5_n12-tx-l50, ev5_n12-tx-l50-cw, ev5_n12-tx-lx, ev5_n5-t35-l30, ev5_n5-t35-l50, ev5_n5-t35-lx, ev5_n5-t50-l30, ev5_n5-t50-l50, ev5_n5-t50-lx, ev5_n5-tx-l30, ev5_n5-tx-l50, ev5_n5-tx-lx, ev5_n8-t35-l30 …
"""
"""EV5 第四波:濾網閾值×權重交互。"""
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
         .with_columns((pl.col("close") / pl.col("close").rolling_max(120)).over(C).alias("h120"))
         .select(["date", C, "h120"]))

def build(hold_m, power, cap, thr):
    ent_rows, flag_rows = [], []
    all_codes = set(reg["code"].to_list())
    for i, md in enumerate(months):
        window = [m.isoformat() for m in months[max(0, i - hold_m + 1): i + 1]]
        cur = (reg.filter(pl.col("month").is_in(window))
               .group_by("code").agg(pl.col("conviction").max()).rename({"code": C}))
        fm = feats.filter(pl.col("date") == md)
        cur = cur.join(fm, on=C, how="left").filter(pl.col("h120").fill_null(0) > thr)
        if cur.height == 0:
            continue
        w = cur["conviction"].cast(pl.Float64).to_numpy() ** power
        w = w / w.sum()
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
for thr, power, cap, lts in itertools.product([0.6, 0.7, 0.8], [1.5, 2.0, 3.0],
                                              [0.10, 0.12], [30]):
    e, f = build(4, power, cap, thr)
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=45),
                   exit_spec=ExitSpec(loser_time_stop=lts), start=Date(2022, 7, 1))
    nav = res.nav.select(["date", "nav"]).sort("date")
    tr = nav.filter(pl.col("date") < TRAIN_END)
    oo = nav.filter(pl.col("date") >= TRAIN_END)
    k = kpi(tr)
    v = oo["nav"].to_numpy()
    oo_r = (v[-1] / v[0] - 1) if len(v) > 10 else float("nan")
    name = f"t{int(thr*100)}p{power}c{int(cap*100)}"
    ledger.log_trial(family="evergreen", name=f"ev5d_{name}", hypothesis="濾網×權重交互",
                     config={"thr": thr, "power": power, "cap": cap}, window="2022-07..2026-07",
                     metrics={**{kk: float(vv) for kk, vv in k.items()}, "oos": float(oo_r)},
                     batch="EV5", curve=nav)
    rows.append({"cell": name, "tr_cagr": round(k["cagr"], 3), "tr_p5": round(k["p5"], 3),
                 "tr_mdd": round(k["mdd"], 3), "tr_martin": round(k["martin"], 1),
                 "oos": round(oo_r, 3)})
print(pl.DataFrame(rows).sort("tr_p5", descending=True))
