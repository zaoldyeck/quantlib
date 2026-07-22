# transcript 逐字復原(零改動)。
#
# 來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T01:02:35.473Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/research/apex/experiments/b11_new_families.py)
# 涵蓋 trials(8):b11a_breakout, b11b_pattern_tail, b11c_seasonal, b11d_flow_contrarian, b11ef_rev_monthly_n10, b11ef_rev_monthly_n20, b11g_flow_momentum, b11h_price_tail
"""B11 — 新家族探索 I(8 trials × 5 家族;預註冊見 ledger/batches.md)。

Run: uv run --project research python -m research.apex.experiments.b11_new_families
"""
from __future__ import annotations

import time
from datetime import date as Date

import polars as pl

from research.apex import data
from research.apex.assemble import blend_score, build_features, entries_and_flags, run_trial
from research.apex.engine import ExitSpec, PortSpec

C = "company_code"
DEV_START, DEV_END = "2012-01-02", "2023-12-29"
BATCH = "B11"
WINDOW = f"{DEV_START}..{DEV_END}"
START = Date.fromisoformat(DEV_START)
TRI = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0}
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]

t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DEV_START, DEV_END)
bench = data.benchmark_nav(con, DEV_START, DEV_END)
trading_days = panel.select(pl.col("date").unique().sort()).get_column("date")
td = pl.DataFrame({"date": trading_days}).with_columns(
    [pl.col("date").dt.day().alias("dom"), pl.col("date").dt.month().alias("m"),
     pl.col("date").dt.year().alias("y")]
)
first_ge = lambda lo, hi: (td.filter(pl.col("dom").is_between(lo, hi))
                           .group_by(["y", "m"]).agg(pl.col("date").min()).get_column("date"))
DAY11 = first_ge(11, 24)   # 每月首個 ≥11 日交易日
DAY25 = first_ge(25, 31)   # 每月首個 ≥25 日交易日


def W(df):
    return df.filter(pl.col("date") >= pl.lit(DEV_START).str.to_date())


def go(name, hypothesis, family, entries, flags, *, topn=20, trail=0.25, tstop=None, max_new=3):
    return run_trial(
        name=name, hypothesis=hypothesis, family=family, batch=BATCH,
        panel=panel, entries=entries, exit_flags=flags, bench=bench,
        window=WINDOW, start=START,
        config={"topn": topn, "trail": trail, "time_stop": tstop},
        port_spec=PortSpec(n_slots=topn, max_new_per_day=max_new),
        exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop),
        verbose=False,
    )


runs = []

# 1. breakout-event
brk = W(feat.filter((pl.col("donchian_60") > 1.0) & (pl.col("close_pos_20") >= 0.6))
        .join(elig.filter(pl.col("eligible")).select(["date", C]), on=["date", C], how="semi")
        .select(["date", C, pl.col("donchian_60").alias("score")]))
runs.append(go("b11a_breakout", "donchian 突破事件 × 吸收確認", "breakout_event",
               brk, None, trail=0.35, tstop=60))

# 2. pattern-tail
sc2 = W(blend_score(feat, elig, {"fvg_20": 1.0, "updays_20": 1.0, "hvn_dist": 1.0}))
e2, f2 = entries_and_flags(sc2, 20, 80)
runs.append(go("b11b_pattern_tail", "SMC/型態 tail 組合", "pattern_tail", e2, f2,
               trail=0.35))

# 3. seasonal(25 日進、11 日全出)
sc3 = W(blend_score(feat, elig, TRI, require=GATE))
e3 = sc3.filter(pl.col("date").is_in(DAY25.implode())).with_columns(
    pl.col("score").rank("ordinal", descending=True).over("date").alias("rk")
).filter(pl.col("rk") <= 20).select(["date", C, "score"])
f3 = panel.select(["date", C]).filter(pl.col("date").is_in(DAY11.implode()))
runs.append(go("b11c_seasonal", "月底→揭露日 seasonal 窗", "seasonal", e3, f3, max_new=20))

# 4. flow-contrarian(融資 20d 減幅 z<-1)
mg = (data.load_margin(con, DEV_START, DEV_END)
      .sort([C, "date"])
      .with_columns(((pl.col("margin_balance") - pl.col("margin_balance").shift(20))
                     / (pl.col("margin_balance").shift(20) + 1)).over(C).alias("mchg"))
      .with_columns(((pl.col("mchg") - pl.col("mchg").mean().over("date"))
                     / (pl.col("mchg").std().over("date") + 1e-12)).alias("z"))
      .filter(pl.col("z") < -1.0).select(["date", C]))
sc4 = W(blend_score(feat, elig, TRI, require=GATE)).join(mg, on=["date", C], how="semi")
e4, f4 = entries_and_flags(sc4, 20, 10**9)
runs.append(go("b11d_flow_contrarian", "融資大減 + 品質 + TRI", "flow_contrarian",
               e4, None, trail=0.25, tstop=40))

# 5/6. rev-monthly-simple(11 日決策、簽名死亡=不在新 top-N)
sc5 = W(blend_score(feat, elig, TRI, require=GATE)).filter(pl.col("date").is_in(DAY11.implode()))
for topn in (20, 10):
    r5 = sc5.with_columns(pl.col("score").rank("ordinal", descending=True).over("date").alias("rk"))
    e5 = r5.filter(pl.col("rk") <= topn).select(["date", C, "score"])
    all5 = panel.select(["date", C]).filter(pl.col("date").is_in(DAY11.implode()))
    f5 = all5.join(e5.select(["date", C]), on=["date", C], how="anti")
    runs.append(go(f"b11ef_rev_monthly_n{topn}", "月頻 TRI 最簡版", "rev_monthly",
                   e5, f5, topn=topn, max_new=topn))

# 7. flow-momentum(frn_60 月頻)
sc7 = W(blend_score(feat, elig, {"frn_60": 1.0})).filter(pl.col("date").is_in(DAY11.implode()))
r7 = sc7.with_columns(pl.col("score").rank("ordinal", descending=True).over("date").alias("rk"))
e7 = r7.filter(pl.col("rk") <= 20).select(["date", C, "score"])
f7 = (panel.select(["date", C]).filter(pl.col("date").is_in(DAY11.implode()))
      .join(e7.select(["date", C]), on=["date", C], how="anti"))
runs.append(go("b11g_flow_momentum", "外資 60d 累積 月頻", "flow_momentum", e7, f7, max_new=20))

# 8. price-tail(無營收)
sc8 = W(blend_score(feat, elig, {"hvn_dist": 1.0, "close_pos_20": 1.0}))
e8, f8 = entries_and_flags(sc8, 20, 80)
runs.append(go("b11h_price_tail", "純價格 tail 書", "price_tail", e8, f8, trail=0.35))

cmp = pl.DataFrame(
    [{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd", "calmar", "n_trades"]} for r in runs]
).sort("cagr", descending=True)
with pl.Config(tbl_rows=10, tbl_width_chars=120):
    print(cmp)
print("\n晉級門檻:frontier over v1s26(30.8/1.70/−30.9)或 CAGR≥27 ∧ Sharpe≥1.60 ∧ MDD≥−30")
print(f"total {time.time()-t0:.1f}s")

