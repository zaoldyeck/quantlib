# transcript 逐字復原(零改動)。
#
# 來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T09:47:12.191Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/src/quantlib/apex/experiments/r09_finlab_harvest.py)
# 涵蓋 trials(10):r09a_size50, r09b_size25, r09c_buyback, r09d_breadth_halt, r09e_rev_stability, r09f_yoy_cap150, r09g_frn20, r09h_low_margin_usage, r09i_lowvol5, r09j_size_bb
"""R09 — FinLab 全站收割批(10 trials;預註冊見 ledger/batches.md)。

Run: uv run --project . python -m quantlib.apex.experiments.r09_finlab_harvest
"""
from __future__ import annotations

import time
from datetime import date as Date

import numpy as np
import polars as pl

from quantlib.apex import data, metrics
from quantlib.apex.assemble import build_features, entries_and_flags, run_trial
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec

C = "company_code"
DS, DE = "2019-01-02", "2025-06-30"
BATCH = "R09"
WINDOW = f"{DS}..{DE}"
START = Date.fromisoformat(DS)
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W4 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5}

t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DS, DE)
bench = data.benchmark_nav(con, DS, DE)

# ── 收割資料層 ──────────────────────────────────────────────────────────
# size:每日市值(外資表流通股數 × raw_close)
fh = con.sql(f"""
  SELECT date, company_code, outstanding_shares FROM foreign_holding_ratio
  WHERE date BETWEEN DATE '{DS}' - INTERVAL '30 days' AND DATE '{DE}'
""").pl()
mc = (panel.select(["date", C, "raw_close"])
      .join(fh, on=["date", C], how="left")
      .sort([C, "date"])
      .with_columns(pl.col("outstanding_shares").forward_fill().over(C))
      .with_columns((-(pl.col("outstanding_shares") * pl.col("raw_close")).log()).alias("smallness"))
      .select(["date", C, "smallness"]))
feat = feat.join(mc, on=["date", C], how="left")

# 庫藏股:公告後 60 日窗 flag
bb = con.sql(f"""
  SELECT announce_date, company_code FROM treasury_stock_buyback
  WHERE announce_date BETWEEN DATE '{DS}' - INTERVAL '70 days' AND DATE '{DE}'
""").pl()
td_all = panel.select(pl.col("date").unique().sort()).get_column("date")
bb_win = (bb.join(pl.DataFrame({"date": td_all}), how="cross")
          .filter((pl.col("date") >= pl.col("announce_date"))
                  & (pl.col("date") <= pl.col("announce_date") + pl.duration(days=60)))
          .select(["date", C]).unique())
feat = feat.join(bb_win.with_columns(pl.lit(1.0).alias("bb_flag")), on=["date", C], how="left")
feat = feat.with_columns(pl.col("bb_flag").fill_null(0.0))

# 寬度 regime:eligible 池收盤 > SMA120 比例(遲滯 0.45/0.55)
brd = (panel.sort([C, "date"])
       .with_columns(pl.col("close").rolling_mean(120).over(C).alias("sma120"))
       .join(elig.filter(pl.col("eligible")).select(["date", C]), on=["date", C], how="semi")
       .group_by("date").agg((pl.col("close") > pl.col("sma120")).mean().alias("breadth"))
       .sort("date"))
bvals = brd["breadth"].to_numpy()
state = np.zeros(len(bvals), dtype=bool)   # True = halt
on = False
for i, x in enumerate(bvals):
    if not np.isnan(x):
        if not on and x < 0.45:
            on = True
        elif on and x > 0.55:
            on = False
    state[i] = on
halt_days = brd.select("date").with_columns(pl.Series("h", state)).filter(pl.col("h")).select("date")

# 營收穩定度:12 月 YoY std(月頻 → PIT snap)
rev = (data.load_monthly_revenue(con, DE)
       .sort([C, "year", "month"])
       .with_columns([
           pl.date(pl.col("year") + pl.col("month") // 12, pl.col("month") % 12 + 1, 10).alias("avail"),
           pl.col("monthly_revenue_yoy").rolling_std(12).over(C).alias("yoy_std12"),
       ])
       .select([C, "avail", "yoy_std12"]).drop_nulls().sort("avail"))
feat = (feat.sort("date")
        .join_asof(rev, left_on="date", right_on="avail", by=C,
                   strategy="backward", tolerance="70d")
        .sort([C, "date"]))

# 法人淨買強度 frn_20 已在 FEATURE... 無(有 frn_60);建 frn_20
fl20 = (data.load_flows(con, DS, DE)
        .join(panel.select(["date", C, "volume"]), on=["date", C], how="inner")
        .sort([C, "date"])
        .with_columns((pl.col("foreign_diff").cast(pl.Float64).rolling_sum(20)
                       / pl.col("volume").cast(pl.Float64).rolling_sum(20)).over(C).alias("frn_20"))
        .select(["date", C, "frn_20"]))
feat = feat.join(fl20, on=["date", C], how="left")

# 融資使用率
mg = (data.load_margin(con, DS, DE)
      .with_columns((pl.col("margin_balance") / (pl.col("margin_quota") + 1)).alias("mgn_usage"))
      .select(["date", C, "mgn_usage"]))
feat = feat.join(mg.unique(subset=["date", C], keep="first"), on=["date", C], how="left")

# 5 日波動
v5 = (panel.sort([C, "date"])
      .with_columns((pl.col("close") / pl.col("close").shift(1) - 1).over(C).alias("ret"))
      .with_columns(pl.col("ret").rolling_std(5).over(C).alias("vol5"))
      .select(["date", C, "vol5"]))
feat = feat.join(v5, on=["date", C], how="left")

print(f"features+harvest ready {time.time()-t0:.1f}s | halt days {halt_days.height}")


def W_(df):
    return df.filter(pl.col("date") >= pl.lit(DS).str.to_date())


def geo(pool, exps, require):
    cols = list(exps)
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=cols))
    for cond in require:
        df = df.filter(cond)
    expr = None
    for c_, wt in exps.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    return df.with_columns(expr.alias("score"))


def go(name, *, exps=None, require=None, boost_bb=False, halt=None, extra_filter=None):
    pool = feat.filter(pl.col("rev_fresh_days") <= 5)
    if extra_filter is not None:
        pool = pool.filter(extra_filter)
    sc = geo(pool, exps or W4, GATE + (require or []))
    if boost_bb:
        sc = sc.with_columns(pl.col("score") * (1 + 0.15 * pl.col("bb_flag")))
    sc = W_(sc.select(["date", C, "score"]))
    e, _ = entries_and_flags(sc, 8, 10**9)
    if halt is not None:
        e = e.join(halt, on="date", how="anti")
    f = W_(feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]))
    return run_trial(
        name=name, hypothesis="FinLab harvest", family="r09", batch=BATCH,
        panel=panel, entries=e, exit_flags=f, bench=bench, window=WINDOW, start=START,
        config={"name": name},
        port_spec=PortSpec(n_slots=8, max_new_per_day=5),
        exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30), verbose=False)


runs = [
    go("r09a_size50", exps=dict(W4) | {"smallness": 0.5}),
    go("r09b_size25", exps=dict(W4) | {"smallness": 0.25}),
    go("r09c_buyback", boost_bb=True),
    go("r09d_breadth_halt", halt=halt_days),
    go("r09e_rev_stability", require=[pl.col("yoy_std12") <= pl.col("yoy_std12").median().over("date")]),
    go("r09f_yoy_cap150", require=[pl.col("rev_yoy") <= 150]),
    go("r09g_frn20", exps=dict(W4) | {"frn_20": 0.25}),
    go("r09h_low_margin_usage", require=[pl.col("mgn_usage") <= pl.col("mgn_usage").median().over("date")]),
    go("r09i_lowvol5", require=[pl.col("vol5") <= pl.col("vol5").quantile(0.8).over("date")]),
    go("r09j_size_bb", exps=dict(W4) | {"smallness": 0.5}, boost_bb=True),
]
cmp = pl.DataFrame(
    [{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd", "calmar"]} for r in runs]
).sort("cagr", descending=True)
with pl.Config(tbl_rows=12, tbl_width_chars=110):
    print(cmp)
print("\n旗艦對照:60.9/1.72/−38.7 | 晉級:CAGR≥62.9 或(Sharpe≥1.82∧CAGR≥60.9),MDD≥−45")
print(f"total {time.time()-t0:.1f}s")

