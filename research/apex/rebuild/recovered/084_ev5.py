"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-10T00:39:01.276Z(工具 Bash)
涵蓋 trials(40):ev5_n12-t35-l30, ev5_n12-t35-l50, ev5_n12-t35-lx, ev5_n12-t50-l30, ev5_n12-t50-l50, ev5_n12-t50-lx, ev5_n12-tx-l30, ev5_n12-tx-l50, ev5_n12-tx-l50-cw, ev5_n12-tx-lx, ev5_n5-t35-l30, ev5_n5-t35-l50, ev5_n5-t35-lx, ev5_n5-t50-l30, ev5_n5-t50-l50, ev5_n5-t50-lx, ev5_n5-tx-l30, ev5_n5-tx-l50, ev5_n5-tx-lx, ev5_n8-t35-l30 …
"""
"""EV5 第一波:集中度 × 出場 × 節流 × 權重 主網格(train P5 排序)。"""
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
TRAIN_END = Date(2025, 7, 1)

# 月表動能複合(進場排名用):用 panel 現算 mom60 × h52 rank
feats = (panel.sort([C, "date"])
         .with_columns([
             (pl.col("close") / pl.col("close").rolling_max(252)).over(C).alias("h52"),
             (pl.col("close") / pl.col("close").shift(60) - 1).over(C).alias("m60"),
         ]).select(["date", C, "h52", "m60"]))

def build(reg_f, hold_m, topn=None, conv_w=False):
    ent_rows, flag_rows = [], []
    all_codes = set(reg_f["code"].to_list())
    for i, md in enumerate(months):
        window = [m.isoformat() for m in months[max(0, i - hold_m + 1): i + 1]]
        cur = (reg_f.filter(pl.col("month").is_in(window))
               .group_by("code").agg(pl.col("conviction").max()))
        if cur.height == 0:
            continue
        f_md = feats.filter((pl.col("date") == md) & pl.col(C).is_in(cur["code"].to_list()))
        cur = (cur.rename({"code": C}).join(f_md.select([C, "h52", "m60"]), on=C, how="left")
               .with_columns(((pl.col("conviction").rank() / pl.len())
                              * (pl.col("h52").rank() / pl.len()).fill_null(0.5)
                              * (pl.col("m60").rank() / pl.len()).fill_null(0.5)).alias("cs"))
               .sort("cs", descending=True))
        pick = cur.head(topn) if topn else cur
        tot_c = pick["conviction"].sum()
        for r in pick.to_dicts():
            row = {"date": md, C: r[C], "score": float(r["cs"])}
            if conv_w:
                row["weight"] = float(r["conviction"]) / tot_c
            ent_rows.append(row)
        keep = set(pick[C].to_list())
        for c_ in all_codes - keep:
            flag_rows.append({"date": md, C: c_})
    e = pl.DataFrame(ent_rows).with_columns(pl.col("date").cast(pl.Date))
    f = pl.DataFrame(flag_rows).with_columns(pl.col("date").cast(pl.Date))
    return e, f

def run(e, f, n_slots, trail, lts, mn):
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=n_slots, max_new_per_day=mn),
                   exit_spec=ExitSpec(trailing_stop=trail, loser_time_stop=lts),
                   start=Date(2022, 7, 1))
    nav = res.nav.select(["date", "nav"]).sort("date")
    tr = nav.filter(pl.col("date") < TRAIN_END)
    oo = nav.filter(pl.col("date") >= TRAIN_END)
    k = kpi(tr)
    v = oo["nav"].to_numpy()
    return k, (v[-1] / v[0] - 1) if len(v) > 10 else float("nan"), nav

rows = []
GRID = []
for topn, trail, lts in itertools.product([None, 12, 8, 5], [None, 0.35, 0.5], [50, 30, None]):
    GRID.append(dict(topn=topn, trail=trail, lts=lts, mn=None, cw=False))
GRID.append(dict(topn=None, trail=None, lts=50, mn=None, cw=True))   # conviction 加權
GRID.append(dict(topn=12, trail=None, lts=50, mn=None, cw=True))
GRID.append(dict(topn=None, trail=None, lts=50, mn=5, cw=False))     # 節流
GRID.append(dict(topn=None, trail=None, lts=50, mn=2, cw=False))
for g in GRID:
    e, f = build(reg, 3, topn=g["topn"], conv_w=g["cw"])
    n_slots = (g["topn"] or 25)
    k, oo, nav = run(e, f, n_slots, g["trail"], g["lts"], g["mn"])
    name = (f"n{g['topn'] or 'All'}-t{int(g['trail']*100) if g['trail'] else 'x'}"
            f"-l{g['lts'] or 'x'}{'-cw' if g['cw'] else ''}{'-mn'+str(g['mn']) if g['mn'] else ''}")
    ledger.log_trial(family="evergreen", name=f"ev5_{name}", hypothesis="引擎進化第一波",
                     config=g, window="2022-07..2026-07",
                     metrics={**{kk: float(vv) for kk, vv in k.items()}, "oos": float(oo)},
                     batch="EV5", curve=nav)
    rows.append({"cell": name, "tr_cagr": round(k["cagr"], 3), "tr_p5": round(k["p5"], 3),
                 "tr_mdd": round(k["mdd"], 3), "oos": round(oo, 3)})
out = pl.DataFrame(rows).sort("tr_p5", descending=True)
with pl.Config(tbl_rows=42):
    print(out)
print("\nEV4 champion 基準:train P5 19.6 / CAGR 42.1 / OOS 105-115%")
