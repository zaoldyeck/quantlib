"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-10T00:44:00.022Z(工具 Bash)
涵蓋 trials(49):ev5_n12-t35-l30, ev5_n12-t35-l50, ev5_n12-t35-lx, ev5_n12-t50-l30, ev5_n12-t50-l50, ev5_n12-t50-lx, ev5_n12-tx-l30, ev5_n12-tx-l50, ev5_n12-tx-l50-cw, ev5_n12-tx-lx, ev5_n5-t35-l30, ev5_n5-t35-l50, ev5_n5-t35-lx, ev5_n5-t50-l30, ev5_n5-t50-l50, ev5_n5-t50-lx, ev5_n5-tx-l30, ev5_n5-tx-l50, ev5_n5-tx-lx, ev5_n8-t35-l30 …
"""
"""EV5 第五波:signal_type 分型消融 + lts 精掃。"""
import polars as pl
import numpy as np
from datetime import date as Date
from research.apex import data, ledger
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.apex.experiments.g01_ml_ranker import C, kpi
from research.apex.experiments.n01_momentum_single import month_firsts

reg = pl.read_parquet("research/evergreen/data/registry_v1.parquet")
# signal_type 歸一(agent 用詞不一,做關鍵詞分類)
def norm_type(s):
    if "拐點" in s or "營收" in s: return "rev_inflect"
    if "瓶頸" in s or "寡占" in s or "利基" in s: return "bottleneck"
    if "題材" in s or "再評價" in s: return "theme"
    if "質變" in s or "催化" in s or "轉型" in s: return "transform"
    if "循環" in s or "谷底" in s: return "cyclical"
    if "錯殺" in s or "反彈" in s or "恐慌" in s: return "panic"
    if "資產" in s or "NAV" in s: return "nav"
    return "other"
reg = reg.with_columns(pl.col("signal_type").map_elements(norm_type, return_dtype=pl.String).alias("st"))
print(reg.group_by("st").len().sort("len", descending=True))

con = data.connect()
panel = data.common_stocks(data.load_panel(con, "2022-01-01", "2026-07-09", warmup_days=60))
dates_all = panel.select("date").unique().sort("date")["date"].to_list()
months = month_firsts([d for d in dates_all if d >= Date(2022, 7, 1)])
TRAIN_END = Date(2025, 7, 1)
feats = (panel.sort([C, "date"])
         .with_columns((pl.col("close") / pl.col("close").rolling_max(120)).over(C).alias("h120"))
         .select(["date", C, "h120"]))

def run_reg(reg_f, lts=30, trail=None):
    ent_rows, flag_rows = [], []
    all_codes = set(reg_f["code"].to_list())
    for i, md in enumerate(months):
        window = [m.isoformat() for m in months[max(0, i - 3): i + 1]]
        cur = (reg_f.filter(pl.col("month").is_in(window))
               .group_by("code").agg(pl.col("conviction").max()).rename({"code": C}))
        cur = (cur.join(feats.filter(pl.col("date") == md), on=C, how="left")
               .filter(pl.col("h120").fill_null(0) > 0.7))
        if cur.height == 0:
            continue
        w = cur["conviction"].cast(pl.Float64).to_numpy() ** 2.0
        w = w / w.sum()
        for _ in range(10):
            over = w > 0.10
            if not over.any(): break
            ex = (w[over] - 0.10).sum(); w[over] = 0.10
            un = ~over
            if w[un].sum() > 0: w[un] += ex * w[un] / w[un].sum()
        for r, wi in zip(cur.to_dicts(), w):
            ent_rows.append({"date": md, C: r[C], "score": float(r["conviction"]), "weight": float(wi)})
        for c_ in all_codes - set(cur[C].to_list()):
            flag_rows.append({"date": md, C: c_})
    e = pl.DataFrame(ent_rows).with_columns(pl.col("date").cast(pl.Date))
    f = pl.DataFrame(flag_rows).with_columns(pl.col("date").cast(pl.Date))
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=45),
                   exit_spec=ExitSpec(loser_time_stop=lts, trailing_stop=trail),
                   start=Date(2022, 7, 1))
    nav = res.nav.select(["date", "nav"]).sort("date")
    tr = nav.filter(pl.col("date") < TRAIN_END)
    oo = nav.filter(pl.col("date") >= TRAIN_END)
    k = kpi(tr)
    v = oo["nav"].to_numpy()
    return k, (v[-1]/v[0]-1) if len(v) > 10 else float("nan"), nav

rows = []
k, oo, nav = run_reg(reg)
rows.append({"cell": "champion(全類)", "tr_cagr": round(k["cagr"],3), "tr_p5": round(k["p5"],3),
             "tr_mdd": round(k["mdd"],3), "oos": round(oo,3)})
for st in ["rev_inflect", "bottleneck", "theme", "transform", "cyclical", "panic", "nav", "other"]:
    sub = reg.filter(pl.col("st") != st)
    if reg.height - sub.height < 15: continue
    k, oo, _ = run_reg(sub)
    rows.append({"cell": f"LOO-{st}", "tr_cagr": round(k["cagr"],3), "tr_p5": round(k["p5"],3),
                 "tr_mdd": round(k["mdd"],3), "oos": round(oo,3)})
for lts in [20, 40]:
    k, oo, _ = run_reg(reg, lts=lts)
    rows.append({"cell": f"lts{lts}", "tr_cagr": round(k["cagr"],3), "tr_p5": round(k["p5"],3),
                 "tr_mdd": round(k["mdd"],3), "oos": round(oo,3)})
k, oo, _ = run_reg(reg, trail=0.35)
rows.append({"cell": "＋trail35", "tr_cagr": round(k["cagr"],3), "tr_p5": round(k["p5"],3),
             "tr_mdd": round(k["mdd"],3), "oos": round(oo,3)})
out = pl.DataFrame(rows)
with pl.Config(tbl_rows=14):
    print(out)
for r in rows:
    ledger.log_trial(family="evergreen", name=f"ev5e_{r['cell']}", hypothesis="分型消融+出場精掃",
                     config={}, window="2022-07..2026-07",
                     metrics={kk: float(vv) for kk, vv in r.items() if kk != "cell"}, batch="EV5")
