"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-10T01:04:23.093Z(工具 Bash)
涵蓋 trials(18):ev8_conv*h52*mom(v2)|t60, ev8_conv*h52*mom(v2)|t70, ev8_conv*h52*mom(v2)|t80, ev8_conv*h52*mom*acc|t60, ev8_conv*h52*mom*acc|t70, ev8_conv*h52*mom*acc|t80, ev8_conv*h52*mom*vs|t60, ev8_conv*h52*mom*vs|t70, ev8_conv*h52*mom*vs|t80, ev8_conv*h52|t60, ev8_conv*h52|t70, ev8_conv*h52|t80, ev8_conv*mom|t60, ev8_conv*mom|t70, ev8_conv*mom|t80, ev8_conv2*h52*mom|t60, ev8_conv2*h52*mom|t70, ev8_conv2*h52*mom|t80
"""
"""EV8 — 席位制下濾網重掃 × 計分軸網格。"""
import itertools
import numpy as np
import polars as pl
from datetime import date as Date
from research.apex import data, ledger
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.apex.experiments.g01_ml_ranker import C, kpi
from research.apex.experiments.n01_momentum_single import month_firsts
import duckdb

reg = pl.read_parquet("research/evergreen/data/registry_v1.parquet")
con = data.connect()
panel = data.common_stocks(data.load_panel(con, "2022-01-01", "2026-07-09", warmup_days=300))
dates_all = panel.select("date").unique().sort("date")["date"].to_list()
months = month_firsts([d for d in dates_all if d >= Date(2022, 7, 1)])
TRAIN_END = Date(2025, 7, 1)
raw = duckdb.connect('research/cache.duckdb', read_only=True)
rev = raw.sql("""SELECT company_code, year, month, monthly_revenue_yoy AS yoy
                 FROM (SELECT *, row_number() OVER (PARTITION BY company_code, year, month
                       ORDER BY monthly_revenue DESC) rn FROM operating_revenue) WHERE rn=1""").pl()
rev = (rev.sort([C, "year", "month"])
       .with_columns([
           pl.date(pl.col("year") + pl.col("month") // 12, pl.col("month") % 12 + 1, 10).alias("avail"),
           (pl.col("yoy").rolling_mean(3) - pl.col("yoy").rolling_mean(12)).over(C).alias("acc"),
       ]).select([C, "avail", "acc"]).drop_nulls().sort("avail"))
feats = (panel.sort([C, "date"])
         .with_columns([
             (pl.col("close") / pl.col("close").rolling_max(120)).over(C).alias("h120"),
             (pl.col("close") / pl.col("close").rolling_max(252)).over(C).alias("h52"),
             (pl.col("close").shift(5) / pl.col("close").shift(126) - 1).over(C).alias("mom"),
             (pl.col("volume").cast(pl.Float64).rolling_mean(5)
              / (pl.col("volume").cast(pl.Float64).rolling_mean(60) + 1)).over(C).alias("vs"),
         ]).select(["date", C, "h120", "h52", "mom", "vs"]))
feats = (feats.sort("date")
         .join_asof(rev, left_on="date", right_on="avail", by=C, strategy="backward",
                    tolerance="70d").sort([C, "date"]))
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
base = membership.join(feats, on=["date", C], how="left")
all_codes = reg["code"].unique().to_list()
flag = (day_df.join(pl.DataFrame({C: all_codes}), how="cross")
        .join(membership.select(["date", C]), on=["date", C], how="anti"))

def rank(c):
    return (pl.col(c).rank() / pl.len()).over("date")

AXES = {
    "conv*h52*mom(v2)": rank("conv") * rank("h52") * rank("mom"),
    "conv*h52*mom*vs": rank("conv") * rank("h52") * rank("mom") * rank("vs") ** 0.5,
    "conv*h52*mom*acc": rank("conv") * rank("h52") * rank("mom") * rank("acc") ** 0.5,
    "conv*mom": rank("conv") * rank("mom"),
    "conv*h52": rank("conv") * rank("h52"),
    "conv2*h52*mom": rank("conv") ** 2 * rank("h52") * rank("mom"),
}
rows = []
for (aname, expr), thr in itertools.product(AXES.items(), [0.6, 0.7, 0.8]):
    j = base.filter(pl.col("h120").fill_null(0) > thr)
    sc = j.with_columns(expr.alias("score")).select(["date", C, "score"]).drop_nulls()
    res = simulate(panel, sc, exit_flags=flag, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.35, loser_time_stop=30),
                   start=Date(2022, 7, 1))
    nav = res.nav.select(["date", "nav"]).sort("date")
    tr = nav.filter(pl.col("date") < TRAIN_END)
    k = kpi(tr)
    v = nav.filter(pl.col("date") >= TRAIN_END)["nav"].to_numpy()
    oo = v[-1]/v[0]-1
    name = f"{aname}|t{int(thr*100)}"
    ledger.log_trial(family="evergreen", name=f"ev8_{name}", hypothesis="席位制軸/濾網重掃",
                     config={"axes": aname, "thr": thr}, window="2022-07..2026-07",
                     metrics={**{kk: float(vv) for kk, vv in k.items()}, "oos": float(oo)},
                     batch="EV8")
    rows.append({"cell": name, "tr_cagr": round(k["cagr"],3), "tr_p5": round(k["p5"],3),
                 "tr_mdd": round(k["mdd"],3), "oos": round(oo,3)})
out = pl.DataFrame(rows).sort("tr_p5", descending=True)
with pl.Config(tbl_rows=18, tbl_width_chars=110):
    print(out)
print("\nv2 基準:P5 68.9 / CAGR 129.3")
