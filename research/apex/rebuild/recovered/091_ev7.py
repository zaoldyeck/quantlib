"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-10T00:59:32.183Z(工具 Bash)
涵蓋 trials(48):ev7_n10m2t25ts60l15, ev7_n10m2t25ts60l30, ev7_n10m2t25tsxl15, ev7_n10m2t25tsxl30, ev7_n10m2t35ts60l15, ev7_n10m2t35ts60l30, ev7_n10m2t35tsxl15, ev7_n10m2t35tsxl30, ev7_n10m3t25ts60l15, ev7_n10m3t25ts60l30, ev7_n10m3t25tsxl15, ev7_n10m3t25tsxl30, ev7_n10m3t35ts60l15, ev7_n10m3t35ts60l30, ev7_n10m3t35tsxl15, ev7_n10m3t35tsxl30, ev7_n5m2t25ts60l15, ev7_n5m2t25ts60l30, ev7_n5m2t25tsxl15, ev7_n5m2t25tsxl30 …
"""
"""EV7 — Serenity 拓撲移植:registry 池 × 席位制日頻事件引擎。"""
import itertools
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

# 池成員資格(日頻):date → 近 4 月標記聯集 + conviction
memb_rows = []
for i, md in enumerate(months):
    nxt = months[i + 1] if i + 1 < len(months) else Date(2026, 7, 10)
    window = [m.isoformat() for m in months[max(0, i - 3): i + 1]]
    cur = (reg.filter(pl.col("month").is_in(window))
           .group_by("code").agg(pl.col("conviction").max()))
    for r in cur.to_dicts():
        memb_rows.append({"m_start": md, "m_end": nxt, C: r["code"], "conv": r["conviction"]})
memb = pl.DataFrame(memb_rows)
day_df = pl.DataFrame({"date": [d for d in dates_all if d >= Date(2022, 7, 1)]})
membership = (day_df.join(memb, how="cross")
              .filter((pl.col("date") >= pl.col("m_start")) & (pl.col("date") < pl.col("m_end")))
              .select(["date", C, "conv"]).unique(subset=["date", C]))

# 每日 entries:池內 ∩ h120>0.7,score = conv 幾何 × 價格結構
sc = (membership.join(feats, on=["date", C], how="left")
      .filter(pl.col("h120").fill_null(0) > 0.7)
      .with_columns(((pl.col("conv").rank() / pl.len()).over("date")
                     * (pl.col("h52").rank() / pl.len()).over("date")
                     * (pl.col("mom").rank() / pl.len()).over("date")).alias("score"))
      .select(["date", C, "score"]))
# 池外出場 flag:不在當日 membership
all_codes = reg["code"].unique().to_list()
flag = (day_df.join(pl.DataFrame({C: all_codes}), how="cross")
        .join(membership.select(["date", C]), on=["date", C], how="anti"))

rows = []
for n, mn, trail, ts, lts in itertools.product([5, 8, 10], [2, 3], [0.25, 0.35],
                                               [60, None], [15, 30]):
    res = simulate(panel, sc, exit_flags=flag, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=n, max_new_per_day=mn),
                   exit_spec=ExitSpec(trailing_stop=trail, time_stop=ts, loser_time_stop=lts),
                   start=Date(2022, 7, 1))
    nav = res.nav.select(["date", "nav"]).sort("date")
    tr = nav.filter(pl.col("date") < TRAIN_END)
    k = kpi(tr)
    v = nav.filter(pl.col("date") >= TRAIN_END)["nav"].to_numpy()
    oo = v[-1]/v[0]-1 if len(v) > 10 else float("nan")
    name = f"n{n}m{mn}t{int(trail*100)}ts{ts or 'x'}l{lts}"
    ledger.log_trial(family="evergreen", name=f"ev7_{name}", hypothesis="Serenity 拓撲移植",
                     config={"n": n, "mn": mn, "trail": trail, "ts": ts, "lts": lts},
                     window="2022-07..2026-07",
                     metrics={**{kk: float(vv) for kk, vv in k.items()}, "oos": float(oo)},
                     batch="EV7")
    rows.append({"cell": name, "tr_cagr": round(k["cagr"],3), "tr_p5": round(k["p5"],3),
                 "tr_mdd": round(k["mdd"],3), "oos": round(oo,3)})
out = pl.DataFrame(rows).sort("tr_p5", descending=True)
with pl.Config(tbl_rows=24):
    print(out.head(24))
print("\n對照:月調拓撲 champion P5 41.1/CAGR 86.4;S 45.9/96.0;Serenity 253%")
