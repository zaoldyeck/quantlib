"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-10T01:05:14.635Z(工具 Bash)
涵蓋 trials(27):ev8_conv*h52*mom(v2)|t60, ev8_conv*h52*mom(v2)|t70, ev8_conv*h52*mom(v2)|t80, ev8_conv*h52*mom*acc|t60, ev8_conv*h52*mom*acc|t70, ev8_conv*h52*mom*acc|t80, ev8_conv*h52*mom*vs|t60, ev8_conv*h52*mom*vs|t70, ev8_conv*h52*mom*vs|t80, ev8_conv*h52|t60, ev8_conv*h52|t70, ev8_conv*h52|t80, ev8_conv*mom|t60, ev8_conv*mom|t70, ev8_conv*mom|t80, ev8_conv2*h52*mom|t60, ev8_conv2*h52*mom|t70, ev8_conv2*h52*mom|t80, ev8b_conv加權席位, ev8b_min_hold3 …
"""
"""EV8b — 出場側:uw_trail/min_hold/profit/加權席位/池內弱勢出場。"""
import numpy as np
import polars as pl
from datetime import date as Date
from research.apex import data, ledger
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.apex.experiments.g01_ml_ranker import C, kpi
from research.apex.experiments.n01_momentum_single import month_firsts

reg = pl.read_parquet("research/evergreen/data/registry_v1.parquet")
con = data.connect()
panel = data.common_stocks(data.load_panel(con, "2022-01-01", "2026-07-09", warmup_days=300))
dates_all = panel.select("date").unique().sort("date")["date"].to_list()
months = month_firsts([d for d in dates_all if d >= Date(2022, 7, 1)])
TRAIN_END = Date(2025, 7, 1)
feats = (panel.sort([C, "date"])
         .with_columns([
             (pl.col("close") / pl.col("close").rolling_max(120)).over(C).alias("h120"),
             (pl.col("close") / pl.col("close").rolling_max(252)).over(C).alias("h52"),
             (pl.col("close").shift(5) / pl.col("close").shift(126) - 1).over(C).alias("mom"),
         ]).select(["date", C, "h120", "h52", "mom"]))
day_df = pl.DataFrame({"date": [d for d in dates_all if d >= Date(2022, 7, 1)]})
memb_rows = []
for i, md in enumerate(months):
    nxt = months[i + 1] if i + 1 < len(months) else Date(2026, 7, 10)
    window = [m.isoformat() for m in months[max(0, i - 3): i + 1]]
    cur = (reg.filter(pl.col("month").is_in(window))
           .group_by("code").agg(pl.col("conviction").max()))
    for r in cur.to_dicts():
        memb_rows.append({"m_start": md, "m_end": nxt, C: r["code"], "conv": r["conviction"]})
memb = pl.DataFrame(memb_rows)
membership = (day_df.join(memb, how="cross")
              .filter((pl.col("date") >= pl.col("m_start")) & (pl.col("date") < pl.col("m_end")))
              .select(["date", C, "conv"]).unique(subset=["date", C]))
base = membership.join(feats, on=["date", C], how="left").filter(pl.col("h120").fill_null(0) > 0.7)
def rank(c): return (pl.col(c).rank() / pl.len()).over("date")
scored = base.with_columns((rank("conv") * rank("h52") * rank("mom")).alias("score"))
sc = scored.select(["date", C, "score"]).drop_nulls()
all_codes = reg["code"].unique().to_list()
flag_pool = (day_df.join(pl.DataFrame({C: all_codes}), how="cross")
             .join(membership.select(["date", C]), on=["date", C], how="anti"))
# 池內弱勢 flag:score 排名後 30%(持有中跌到隊尾也出)
weak = (scored.with_columns((pl.col("score").rank() / pl.len()).over("date").alias("sp"))
        .filter(pl.col("sp") < 0.3).select(["date", C]))
flag_weak = pl.concat([flag_pool, weak]).unique(subset=["date", C])

def run(flag, *, uw=None, mh=1, pt=None, cw=False, lts=30):
    s = sc
    if cw:
        s = (scored.with_columns((pl.col("conv").cast(pl.Float64)
                                  / pl.col("conv").cast(pl.Float64).sum().over("date") * 5 / 5)
                                 .alias("weight"))
             .select(["date", C, "score", "weight"]).drop_nulls())
        # weight 正規化到 slot 語意(1/5 基準 × conv 相對):conv/mean(conv)/5
        s = s.with_columns((pl.col("weight") * 0 + 1).alias("_"))  # 保底不用—改下方
        s = (scored.with_columns(((pl.col("conv") / pl.col("conv").mean().over("date")) / 5)
                                 .clip(0.1, 0.3).alias("weight"))
             .select(["date", C, "score", "weight"]).drop_nulls())
    res = simulate(panel, s, exit_flags=flag, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2, min_hold_days=mh),
                   exit_spec=ExitSpec(trailing_stop=0.35, loser_time_stop=lts,
                                      underwater_trail=uw, profit_take=pt),
                   start=Date(2022, 7, 1))
    nav = res.nav.select(["date", "nav"]).sort("date")
    tr = nav.filter(pl.col("date") < TRAIN_END)
    k = kpi(tr)
    v = nav.filter(pl.col("date") >= TRAIN_END)["nav"].to_numpy()
    return k, v[-1]/v[0]-1

rows = []
for name, kw, fl in [
    ("v2基準", {}, flag_pool),
    ("uw_trail20", {"uw": 0.20}, flag_pool),
    ("uw_trail25", {"uw": 0.25}, flag_pool),
    ("min_hold3", {"mh": 3}, flag_pool),
    ("min_hold5", {"mh": 5}, flag_pool),
    ("profit100", {"pt": 1.0}, flag_pool),
    ("conv加權席位", {"cw": True}, flag_pool),
    ("池內弱勢出場", {}, flag_weak),
    ("弱勢+uw20", {"uw": 0.20}, flag_weak),
]:
    k, oo = run(fl, **kw)
    ledger.log_trial(family="evergreen", name=f"ev8b_{name}", hypothesis="席位制出場側",
                     config=kw, window="2022-07..2026-07",
                     metrics={**{kk: float(vv) for kk, vv in k.items()}, "oos": float(oo)},
                     batch="EV8")
    rows.append({"cell": name, "tr_cagr": round(k["cagr"],3), "tr_p5": round(k["p5"],3),
                 "tr_mdd": round(k["mdd"],3), "oos": round(oo,3)})
print(pl.DataFrame(rows).sort("tr_p5", descending=True))
