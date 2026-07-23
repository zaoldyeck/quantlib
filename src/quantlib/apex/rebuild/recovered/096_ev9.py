"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-10T01:10:58.594Z(工具 Bash)
涵蓋 trials(10):ev9_eligibility補丁, ev9_guard_0050MA120, ev9_guard_0050MA60, ev9_guard_池DD15, ev9_v3基準, ev9b_DD10, ev9b_DD15, ev9b_DD20, ev9b_v3基準, ev9b_法人淨賣出場
"""
"""EV9b — 池 DD guard 鄰域 + cfo 閘 + 法人出場 flag。"""
import numpy as np
import polars as pl
from datetime import date as Date
import duckdb
from quantlib.apex import data, ledger
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.apex.experiments.g01_ml_ranker import C, kpi
from quantlib.apex.experiments.n01_momentum_single import month_firsts

reg = pl.read_parquet("src/quantlib/evergreen/data/registry_v1.parquet")
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
def rank(c): return (pl.col(c).rank() / pl.len()).over("date")
scored = (membership.join(feats, on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > 0.7)
          .with_columns((rank("conv") * rank("h52") * rank("mom")).alias("score")))
all_codes = reg["code"].unique().to_list()
flag_pool = (day_df.join(pl.DataFrame({C: all_codes}), how="cross")
             .join(membership.select(["date", C]), on=["date", C], how="anti"))
pool_ret = (panel.sort([C, "date"])
            .with_columns((pl.col("close") / pl.col("close").shift(1) - 1).over(C).alias("r"))
            .join(membership.select(["date", C]), on=["date", C], how="semi")
            .group_by("date").agg(pl.col("r").mean()).sort("date")
            .with_columns((1 + pl.col("r").fill_null(0)).cum_prod().alias("idx")))
pool_ret = pool_ret.with_columns((pl.col("idx") / pl.col("idx").cum_max() - 1).alias("dd"))

# 法人 20 日淨買(出場用):inst_neg = 外資+投信 20 日累積 < 0
inst = duckdb.connect('var/cache/cache.duckdb', read_only=True).sql("""
    SELECT date, company_code,
           COALESCE(foreign_buy_sell,0) + COALESCE(trust_buy_sell,0) AS net
    FROM daily_trading_details
""").pl()
if "net" in inst.columns:
    inst = (inst.sort([C, "date"])
            .with_columns(pl.col("net").rolling_sum(20).over(C).alias("net20"))
            .filter(pl.col("net20") < 0).select(["date", C]))

def run(sc_in, flag, name, guard_thr=None):
    s = (sc_in.with_columns(((pl.col("conv") / pl.col("conv").mean().over("date")) / 5)
                            .clip(0.10, 0.30).alias("weight"))
         .select(["date", C, "score", "weight"]).drop_nulls())
    if guard_thr is not None:
        g = set(pool_ret.filter(pl.col("dd") < -guard_thr)["date"].to_list())
        s = s.filter(~pl.col("date").is_in(list(g)))
    res = simulate(panel, s, exit_flags=flag, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.35, loser_time_stop=30),
                   start=Date(2022, 7, 1))
    nav = res.nav.select(["date", "nav"]).sort("date")
    tr = nav.filter(pl.col("date") < TRAIN_END)
    k = kpi(tr)
    v = nav.filter(pl.col("date") >= TRAIN_END)["nav"].to_numpy()
    oo = v[-1]/v[0]-1
    ledger.log_trial(family="evergreen", name=f"ev9b_{name}", hypothesis="DD鄰域/cfo/法人",
                     config={}, window="2022-07..2026-07",
                     metrics={**{kk: float(vv) for kk, vv in k.items()}, "oos": float(oo)},
                     batch="EV9")
    return {"cell": name, "tr_cagr": round(k["cagr"],3), "tr_p5": round(k["p5"],3),
            "tr_mdd": round(k["mdd"],3), "oos": round(oo,3)}

rows = []
for thr, nm in [(None, "v3基準"), (0.10, "DD10"), (0.15, "DD15"), (0.20, "DD20")]:
    rows.append(run(scored, flag_pool, nm, thr))
try:
    flag_inst = pl.concat([flag_pool, inst.filter(pl.col("date") >= Date(2022, 7, 1))]).unique(subset=["date", C])
    rows.append(run(scored, flag_inst, "法人淨賣出場"))
except Exception as ex:
    print("inst flag 失敗:", ex)
print(pl.DataFrame(rows).sort("tr_p5", descending=True))
