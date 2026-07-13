"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-10T00:52:51.834Z(工具 Bash)
涵蓋 trials(12):ev6_champ月頻, ev6_keep兩命脈, ev6_vol加權, ev6_週頻, ev6_週頻+vol, ev6_週頻+命脈, ev6_雙週頻, ev6b_champ, ev6b_突破+遲滯0.55, ev6b_突破觸發, ev6b_遲滯0.55, ev6b_遲滯0.6
"""
"""EV6b — 最後兩刀:突破觸發進場 + h120 出場遲滯。"""
import numpy as np
import polars as pl
from datetime import date as Date
from research.apex import data, ledger
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.apex.experiments.g01_ml_ranker import C, kpi
from research.apex.experiments.n01_momentum_single import month_firsts

reg = pl.read_parquet("research/evergreen/data/registry_v1.parquet")
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
             (pl.col("close") / pl.col("close").rolling_max(20).shift(1) - 1).over(C).alias("brk20"),
         ]).select(["date", C, "h120", "brk20"]))

def run(entry_mode="M", hyst=None):
    """entry_mode M=月初;BRK=月內首個突破20日高日進場。hyst=出場遲滯閾值。"""
    ent_rows, flag_rows = [], []
    all_codes = set(reg["code"].to_list())
    for i, md in enumerate(months):
        nxt = months[i + 1] if i + 1 < len(months) else None
        window = [m.isoformat() for m in months[max(0, i - 3): i + 1]]
        cur = (reg.filter(pl.col("month").is_in(window))
               .group_by("code").agg(pl.col("conviction").max()).rename({"code": C}))
        fm = feats.filter(pl.col("date") == md)
        cur_in = cur.join(fm, on=C, how="left").filter(pl.col("h120").fill_null(0) > 0.7)
        if cur_in.height == 0: continue
        w = cur_in["conviction"].cast(pl.Float64).to_numpy() ** 2
        w = w / w.sum()
        for _ in range(10):
            over = w > 0.10
            if not over.any(): break
            ex = (w[over] - 0.10).sum(); w[over] = 0.10
            un = ~over
            if w[un].sum() > 0: w[un] += ex * w[un] / w[un].sum()
        if entry_mode == "M":
            for r, wi in zip(cur_in.to_dicts(), w):
                ent_rows.append({"date": md, C: r[C], "score": float(r["conviction"]),
                                 "weight": float(wi)})
        else:  # BRK:月內首個 brk20>0 日
            span = feats.filter((pl.col("date") >= md)
                                & (pl.col("date") < (nxt or Date(2026, 7, 10)))
                                & pl.col(C).is_in(cur_in[C].to_list())
                                & (pl.col("brk20") > 0))
            first = span.group_by(C).agg(pl.col("date").min())
            wmap = dict(zip(cur_in[C].to_list(), w))
            for r in first.to_dicts():
                ent_rows.append({"date": r["date"], C: r[C], "score": 1.0,
                                 "weight": float(wmap[r[C]])})
        # 出場 flags:月初,持有豁免至 hyst(若設)否則同 0.7 由 cur_in 定義
        if hyst:
            keep_set = set(cur.join(fm, on=C, how="left")
                           .filter(pl.col("h120").fill_null(0) > hyst)[C].to_list())
        else:
            keep_set = set(cur_in[C].to_list())
        for c_ in all_codes - keep_set:
            flag_rows.append({"date": md, C: c_})
    e = pl.DataFrame(ent_rows).with_columns(pl.col("date").cast(pl.Date))
    f = pl.DataFrame(flag_rows).with_columns(pl.col("date").cast(pl.Date))
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=45),
                   exit_spec=ExitSpec(loser_time_stop=30), start=Date(2022, 7, 1))
    nav = res.nav.select(["date", "nav"]).sort("date")
    tr = nav.filter(pl.col("date") < TRAIN_END)
    k = kpi(tr)
    v = nav.filter(pl.col("date") >= TRAIN_END)["nav"].to_numpy()
    return k, v[-1]/v[0]-1

rows = []
for name, kw in [("champ", {}), ("突破觸發", {"entry_mode": "BRK"}),
                 ("遲滯0.55", {"hyst": 0.55}), ("遲滯0.6", {"hyst": 0.6}),
                 ("突破+遲滯0.55", {"entry_mode": "BRK", "hyst": 0.55})]:
    k, oo = run(**kw)
    ledger.log_trial(family="evergreen", name=f"ev6b_{name}", hypothesis="突破觸發/遲滯",
                     config=kw, window="2022-07..2026-07",
                     metrics={**{kk: float(vv) for kk, vv in k.items()}, "oos": float(oo)},
                     batch="EV6")
    rows.append({"cell": name, "tr_cagr": round(k["cagr"],3), "tr_p5": round(k["p5"],3),
                 "tr_mdd": round(k["mdd"],3), "oos": round(oo,3)})
print(pl.DataFrame(rows).sort("tr_p5", descending=True))
