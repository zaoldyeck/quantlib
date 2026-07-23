"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-10T00:51:50.996Z(工具 Bash)
涵蓋 trials(7):ev6_champ月頻, ev6_keep兩命脈, ev6_vol加權, ev6_週頻, ev6_週頻+vol, ev6_週頻+命脈, ev6_雙週頻
"""
"""EV6 — refresh 頻率 + keep-only 命脈 + 突破觸發 + 波動權重。"""
import numpy as np
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.apex.experiments.g01_ml_ranker import C, kpi
from quantlib.apex.experiments.n01_momentum_single import month_firsts

reg = pl.read_parquet("src/quantlib/evergreen/data/registry_v1.parquet")
reg = reg.filter(~(pl.col("signal_type").str.contains("質變")
                   | pl.col("signal_type").str.contains("催化")
                   | pl.col("signal_type").str.contains("轉型")))
con = data.connect()
panel = data.common_stocks(data.load_panel(con, "2022-01-01", "2026-07-09", warmup_days=60))
dates_all = panel.select("date").unique().sort("date")["date"].to_list()
months = month_firsts([d for d in dates_all if d >= Date(2022, 7, 1)])
TRAIN_END = Date(2025, 7, 1)
feats = (panel.sort([C, "date"])
         .with_columns([
             (pl.col("close") / pl.col("close").rolling_max(120)).over(C).alias("h120"),
             (pl.col("close") / pl.col("close").shift(1) - 1).over(C)
             .rolling_std(60).over(C).alias("vol60"),
         ]).select(["date", C, "h120", "vol60"]))

def refresh_days(freq):
    if freq == "M":
        return months
    idx = {d: i for i, d in enumerate(dates_all)}
    if freq == "W":
        return [d for d in dates_all if d >= Date(2022, 7, 1) and d.weekday() == 0] 
    if freq == "2W":
        base = [d for d in dates_all if d >= Date(2022, 7, 1) and d.weekday() == 0]
        return base[::2]

def month_of(d):
    return [m for m in months if m <= d][-1] if [m for m in months if m <= d] else None

def run(freq="M", keep_only=False, vol_w=False, thr=0.7):
    rg = reg
    if keep_only:
        rg = reg.filter(pl.col("signal_type").str.contains("題材")
                        | pl.col("signal_type").str.contains("再評價")
                        | pl.col("signal_type").str.contains("拐點")
                        | pl.col("signal_type").str.contains("營收"))
    rdays = refresh_days(freq)
    ent_rows, flag_rows = [], []
    all_codes = set(rg["code"].to_list())
    for rd in rdays:
        mo = month_of(rd)
        if mo is None: continue
        i = months.index(mo)
        window = [m.isoformat() for m in months[max(0, i - 3): i + 1]]
        cur = (rg.filter(pl.col("month").is_in(window))
               .group_by("code").agg(pl.col("conviction").max()).rename({"code": C}))
        fm = feats.filter(pl.col("date") == rd)
        cur = cur.join(fm, on=C, how="left").filter(pl.col("h120").fill_null(0) > thr)
        if cur.height == 0: continue
        if vol_w:
            iv = 1.0 / (cur["vol60"].fill_null(0.03).to_numpy() + 0.005)
            cv = cur["conviction"].cast(pl.Float64).to_numpy()
            w = (cv ** 2) * iv
        else:
            w = cur["conviction"].cast(pl.Float64).to_numpy() ** 2
        w = w / w.sum()
        for _ in range(10):
            over = w > 0.10
            if not over.any(): break
            ex = (w[over] - 0.10).sum(); w[over] = 0.10
            un = ~over
            if w[un].sum() > 0: w[un] += ex * w[un] / w[un].sum()
        for r, wi in zip(cur.to_dicts(), w):
            ent_rows.append({"date": rd, C: r[C], "score": float(r["conviction"]), "weight": float(wi)})
        for c_ in all_codes - set(cur[C].to_list()):
            flag_rows.append({"date": rd, C: c_})
    e = pl.DataFrame(ent_rows).with_columns(pl.col("date").cast(pl.Date))
    f = pl.DataFrame(flag_rows).with_columns(pl.col("date").cast(pl.Date))
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=45),
                   exit_spec=ExitSpec(loser_time_stop=30), start=Date(2022, 7, 1))
    nav = res.nav.select(["date", "nav"]).sort("date")
    tr = nav.filter(pl.col("date") < TRAIN_END)
    k = kpi(tr)
    v = nav.filter(pl.col("date") >= TRAIN_END)["nav"].to_numpy()
    return k, v[-1]/v[0]-1, res.trades.height

rows = []
for name, kw in [("champ月頻", {}), ("週頻", {"freq": "W"}), ("雙週頻", {"freq": "2W"}),
                 ("keep兩命脈", {"keep_only": True}), ("週頻+命脈", {"freq": "W", "keep_only": True}),
                 ("vol加權", {"vol_w": True}), ("週頻+vol", {"freq": "W", "vol_w": True})]:
    k, oo, nt = run(**kw)
    ledger.log_trial(family="evergreen", name=f"ev6_{name}", hypothesis="頻率/命脈/波動權重",
                     config=kw, window="2022-07..2026-07",
                     metrics={**{kk: float(vv) for kk, vv in k.items()}, "oos": float(oo)},
                     batch="EV6")
    rows.append({"cell": name, "tr_cagr": round(k["cagr"],3), "tr_p5": round(k["p5"],3),
                 "tr_mdd": round(k["mdd"],3), "tr_martin": round(k["martin"],1),
                 "trades": nt, "oos": round(oo,3)})
print(pl.DataFrame(rows).sort("tr_p5", descending=True))
