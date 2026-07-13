"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-10T00:32:54.723Z(工具 Bash)
涵蓋 trials(24):ev4_h1c1A, ev4_h1c1AB, ev4_h1c3A, ev4_h1c3AB, ev4_h1c4A, ev4_h1c4AB, ev4_h2c1A, ev4_h2c1AB, ev4_h2c3A, ev4_h2c3AB, ev4_h2c4A, ev4_h2c4AB, ev4_h3c1A, ev4_h3c1AB, ev4_h3c3A, ev4_h3c3AB, ev4_h3c4A, ev4_h3c4AB, oos_r03d_n8, oos_r03h_n10_momw75 …
"""
"""EV4 — registry × 量化引擎網格(train 窗)+ OOS 段披露。"""
import itertools
import polars as pl
from datetime import date as Date
from research.apex import data, ledger
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.apex.experiments.g01_ml_ranker import C, kpi
from research.apex.experiments.n01_momentum_single import month_firsts

reg = pl.read_parquet("research/evergreen/data/registry_v1.parquet")
con = data.connect()
panel = data.common_stocks(data.load_panel(con, "2022-01-01", "2026-07-09", warmup_days=60))
dates_all = panel.select("date").unique().sort("date")["date"].to_list()
months = month_firsts([d for d in dates_all if d >= Date(2022, 7, 1)])
m_index = {m: i for i, m in enumerate(months)}

def build_book(reg_f, hold_m):
    """每月 target = 近 hold_m 個月標記聯集;entries + 輪換 flags。"""
    ent_rows, flag_rows = [], []
    for i, md in enumerate(months):
        window = months[max(0, i - hold_m + 1): i + 1]
        wset = [m.isoformat() for m in window]
        cur = reg_f.filter(pl.col("month").is_in(wset))
        codes = cur.group_by("code").agg(pl.col("conviction").max()).sort("conviction", descending=True)
        for r in codes.to_dicts():
            ent_rows.append({"date": md, C: r["code"], "score": float(r["conviction"])})
        keep = set(codes["code"].to_list())
        all_lbl = set(reg_f["code"].to_list())
        for c_ in all_lbl - keep:
            flag_rows.append({"date": md, C: c_})
    e = pl.DataFrame(ent_rows).with_columns(pl.col("date").cast(pl.Date))
    f = pl.DataFrame(flag_rows).with_columns(pl.col("date").cast(pl.Date))
    return e, f

TRAIN_END = Date(2025, 7, 1)
rows = []
navs = {}
for hold, cmin, arch in itertools.product([1, 2, 3], [1, 3, 4], ["A", "AB"]):
    rf = reg.filter(pl.col("conviction") >= cmin)
    if arch == "A":
        rf = rf.filter(pl.col("archetype") == "A")
    if rf.height < 50:
        continue
    e, f = build_book(rf, hold)
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=25),
                   exit_spec=ExitSpec(loser_time_stop=50),
                   start=Date(2022, 7, 1))
    nav = res.nav.select(["date", "nav"]).sort("date")
    tr = nav.filter(pl.col("date") < TRAIN_END)
    oo = nav.filter(pl.col("date") >= TRAIN_END)
    k_tr = kpi(tr)
    v = oo["nav"].to_numpy()
    oo_ret = v[-1] / v[0] - 1 if len(v) > 10 else float("nan")
    name = f"h{hold}c{cmin}{arch}"
    navs[name] = nav
    ledger.log_trial(family="evergreen", name=f"ev4_{name}", hypothesis="registry×engine",
                     config={"hold": hold, "cmin": cmin, "arch": arch},
                     window="2022-07..2026-07",
                     metrics={**{kk: float(vv) for kk, vv in k_tr.items()},
                              "oos_ret": float(oo_ret)}, batch="EV4", curve=nav)
    rows.append({"cell": name, "tr_cagr": round(k_tr["cagr"], 3),
                 "tr_p5": round(k_tr["p5"], 3), "tr_mdd": round(k_tr["mdd"], 3),
                 "oos_1y_ret": round(oo_ret, 3)})
out = pl.DataFrame(rows).sort("tr_p5", descending=True)
with pl.Config(tbl_rows=20):
    print(out)
import pickle
pickle.dump({k: v.to_dicts() for k, v in navs.items()},
            open("research/evergreen/data/ev4_navs.pkl", "wb"))
