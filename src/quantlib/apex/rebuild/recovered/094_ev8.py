"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-10T01:06:07.452Z(工具 Bash)
涵蓋 trials(24):ev8_conv*h52*mom(v2)|t60, ev8_conv*h52*mom(v2)|t70, ev8_conv*h52*mom(v2)|t80, ev8_conv*h52*mom*acc|t60, ev8_conv*h52*mom*acc|t70, ev8_conv*h52*mom*acc|t80, ev8_conv*h52*mom*vs|t60, ev8_conv*h52*mom*vs|t70, ev8_conv*h52*mom*vs|t80, ev8_conv*h52|t60, ev8_conv*h52|t70, ev8_conv*h52|t80, ev8_conv*mom|t60, ev8_conv*mom|t70, ev8_conv*mom|t80, ev8_conv2*h52*mom|t60, ev8_conv2*h52*mom|t70, ev8_conv2*h52*mom|t80, ev8c_p1.0w10-25, ev8c_p1.0w10-30 …
"""
"""EV8c — 加權席位精修 + 防禦。"""
import numpy as np
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.apex.experiments.g01_ml_ranker import C, kpi
from quantlib.apex.experiments.n01_momentum_single import month_firsts

reg = pl.read_parquet("src/quantlib/evergreen/data/registry_v1.parquet")
con = data.connect()
panel = data.common_stocks(data.load_panel(con, "2022-01-01", "2026-07-09", warmup_days=300))
E5 = (data.eligibility(panel, min_adv=5_000_000.0)
      .filter(pl.col("eligible")).select(["date", C]))
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

def build(reg_in):
    memb_rows = []
    for i, md in enumerate(months):
        nxt = months[i + 1] if i + 1 < len(months) else Date(2026, 7, 10)
        window = [m.isoformat() for m in months[max(0, i - 3): i + 1]]
        cur = (reg_in.filter(pl.col("month").is_in(window))
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
    all_codes = reg_in["code"].unique().to_list()
    flag = (day_df.join(pl.DataFrame({C: all_codes}), how="cross")
            .join(membership.select(["date", C]), on=["date", C], how="anti"))
    return scored, flag

def run(scored, flag, power=1.0, lo=0.10, hi=0.30):
    s = (scored.with_columns((((pl.col("conv") ** power)
                               / (pl.col("conv") ** power).mean().over("date")) / 5)
                             .clip(lo, hi).alias("weight"))
         .select(["date", C, "score", "weight"]).drop_nulls())
    res = simulate(panel, s, exit_flags=flag, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.35, loser_time_stop=30),
                   start=Date(2022, 7, 1))
    nav = res.nav.select(["date", "nav"]).sort("date")
    tr = nav.filter(pl.col("date") < TRAIN_END)
    k = kpi(tr)
    v = nav.filter(pl.col("date") >= TRAIN_END)["nav"].to_numpy()
    return k, v[-1]/v[0]-1, nav

scored, flag = build(reg)
rows = []
for power, lo, hi in [(1.0, 0.10, 0.30), (1.5, 0.10, 0.30), (2.0, 0.10, 0.30),
                      (1.0, 0.10, 0.25), (1.0, 0.12, 0.35), (1.5, 0.10, 0.35)]:
    k, oo, nav = run(scored, flag, power, lo, hi)
    name = f"p{power}w{int(lo*100)}-{int(hi*100)}"
    ledger.log_trial(family="evergreen", name=f"ev8c_{name}", hypothesis="加權席位精修",
                     config={"power": power, "lo": lo, "hi": hi}, window="2022-07..2026-07",
                     metrics={**{kk: float(vv) for kk, vv in k.items()}, "oos": float(oo)},
                     batch="EV8")
    rows.append({"cell": name, "tr_cagr": round(k["cagr"],3), "tr_p5": round(k["p5"],3),
                 "tr_mdd": round(k["mdd"],3), "oos": round(oo,3)})
print(pl.DataFrame(rows).sort("tr_p5", descending=True))

# 防禦 on 新 champion(p1 w10-30)
k0, oo0, nav0 = run(scored, flag)
nav0.write_parquet("src/quantlib/evergreen/data/evergreen_v3_nav.parquet")
mmap3 = {m.isoformat(): months[min(i+3, len(months)-1)].isoformat() for i, m in enumerate(months)}
reg_l3 = reg.with_columns(pl.col("month").replace_strict(mmap3, default=None)).drop_nulls(subset=["month"])
s3, f3 = build(reg_l3)
k3, o3, _ = run(s3, f3)
print(f"\nlag3m:train {k3['cagr']:+.1%}/P5 {k3['p5']:+.1%}  OOS {o3:+.1%}")
rng = np.random.default_rng(42)
pool_by_m = {m: E5.filter(pl.col("date") == m)[C].to_list() for m in months}
pt = []
for it in range(5):
    perm_rows = []
    for m in months:
        sub = reg.filter(pl.col("month_d") == m)
        if sub.height and pool_by_m[m]:
            cs = rng.choice(pool_by_m[m], size=min(sub.height, len(pool_by_m[m])), replace=False)
            for c_, cv in zip(cs, sub["conviction"].to_list()):
                perm_rows.append({"month": m.isoformat(), "month_d": m, "code": str(c_),
                                  "conviction": cv})
    rp = pl.DataFrame(perm_rows)
    sp, fp = build(rp)
    kp, _, _ = run(sp, fp)
    pt.append(kp["cagr"])
print(f"置換 train 中位 {np.median(pt):+.1%}(max {max(pt):+.1%})vs champion {k0['cagr']:+.1%}")
