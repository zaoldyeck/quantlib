"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T12:39:10.728Z(工具 Bash)
涵蓋 trials(8):r22a_rel_replace, r22b_rel_add, r22c_minrank, r22d_squeeze, r22e_trust, r22f_spread, r22g_rel_minrank, r22h_squeeze_n4
"""
"""R22 — 新資訊角度掃蕩(8 trials,R3-n5 scaffold;n4 cell 供對照)。"""
import time
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger, metrics
from quantlib.apex.assemble import build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W5 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5, "rev_seq": 0.5}
t0 = time.time()
con = data.connect()
DS, DE = "2019-01-02", "2025-06-30"
panel, feat, _ = build_features(con, DS, DE)
rev = (data.load_monthly_revenue(con, DE)
       .sort([C, "year", "month"])
       .with_columns([
           pl.date(pl.col("year") + pl.col("month") // 12, pl.col("month") % 12 + 1, 10).alias("avail"),
           (pl.col("monthly_revenue").rolling_sum(3)
            / pl.col("monthly_revenue").rolling_sum(3).shift(3) - 1).over(C).alias("rev_seq"),
       ])
       .select([C, "avail", "rev_seq"]).drop_nulls().sort("avail"))
feat = (feat.sort("date")
        .join_asof(rev, left_on="date", right_on="avail", by=C, strategy="backward", tolerance="70d")
        .sort([C, "date"]))
E5 = data.eligibility(panel, min_adv=5_000_000.0)
bench = data.benchmark_nav(con, DS, DE)

# (1) 同業相對加速:PIT 產業 → accel 產業中位(全市場 accel 有值者)
tax = con.sql("SELECT company_code, effective_date, industry FROM industry_taxonomy_pit "
              "WHERE industry IS NOT NULL ORDER BY effective_date").pl()
fx = (feat.select(["date", C, "rev_yoy_accel"]).drop_nulls()
      .sort("date")
      .join_asof(tax.sort("effective_date"), left_on="date", right_on="effective_date",
                 by=C, strategy="backward")
      .drop_nulls(subset=["industry"]))
ind_med = fx.group_by(["date", "industry"]).agg(pl.col("rev_yoy_accel").median().alias("ind_acc"))
rel = (fx.join(ind_med, on=["date", "industry"], how="left")
       .with_columns((pl.col("rev_yoy_accel") - pl.col("ind_acc")).alias("accel_rel"))
       .select(["date", C, "accel_rel"]))
feat = feat.join(rel, on=["date", C], how="left")

# (3) 融券軋空 (4) 投信 (5) 價差
mg = (data.load_margin(con, DS, DE)
      .join(panel.select(["date", C, "volume"]), on=["date", C], how="inner")
      .sort([C, "date"])
      .with_columns((((pl.col("short_balance") - pl.col("short_balance").shift(20)) * 1000.0)
                     / pl.col("volume").cast(pl.Float64).rolling_sum(20)).over(C).alias("squeeze"))
      .select(["date", C, "squeeze"]).unique(subset=["date", C], keep="first"))
fl = (data.load_flows(con, DS, DE)
      .join(panel.select(["date", C, "volume"]), on=["date", C], how="inner")
      .sort([C, "date"])
      .with_columns((pl.col("trust_diff").cast(pl.Float64).rolling_sum(10)
                     / pl.col("volume").cast(pl.Float64).rolling_sum(10)).over(C).alias("trust10"))
      .select(["date", C, "trust10"]).unique(subset=["date", C], keep="first"))
sp = con.sql(f"""
  SELECT date, company_code,
         CASE WHEN last_best_ask_price > 0 AND last_best_bid_price > 0
              THEN -(last_best_ask_price - last_best_bid_price) / closing_price
              ELSE NULL END AS neg_spread
  FROM daily_quote
  WHERE date BETWEEN DATE '{DS}' AND DATE '{DE}' AND closing_price > 0
""").pl().unique(subset=["date", C], keep="first")
feat = (feat.join(mg, on=["date", C], how="left")
        .join(fl, on=["date", C], how="left")
        .join(sp, on=["date", C], how="left"))

def go(name, *, topn=5, w=None, minrank=False):
    w = w or W5
    pool = feat.filter(pl.col("rev_fresh_days") <= 6)
    df = (pool.join(E5.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=list(w)))
    for cond in GATE:
        df = df.filter(cond)
    if minrank:
        expr = None
        for c_ in w:
            term = (pl.col(c_).rank() / pl.len()).over("date")
            expr = term if expr is None else pl.min_horizontal(expr, term)
    else:
        expr = None
        for c_, wt in w.items():
            term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
            expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(DS).str.to_date())
    e, _ = entries_and_flags(sc, topn, 10**9)
    f = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(DS).str.to_date())
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=topn, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30),
                   start=Date.fromisoformat(DS))
    s = metrics.summarize(res.nav, res.trades, bench)
    ledger.log_trial(family="mod_line", name=name, hypothesis="R22 新資訊角度",
                     config={"name": name}, window=f"{DS}..{DE}", metrics=s,
                     batch="R22", curve=res.nav)
    return {"name": name, **{k: s[k] for k in ("cagr", "sharpe", "mdd")}}

W_rel_repl = {k: v for k, v in W5.items() if k != "rev_yoy_accel"} | {"accel_rel": 1.0}
runs = [
    go("r22a_rel_replace", w=W_rel_repl),
    go("r22b_rel_add", w=dict(W5) | {"accel_rel": 0.5}),
    go("r22c_minrank", minrank=True),
    go("r22d_squeeze", w=dict(W5) | {"squeeze": 0.25}),
    go("r22e_trust", w=dict(W5) | {"trust10": 0.25}),
    go("r22f_spread", w=dict(W5) | {"neg_spread": 0.25}),
    go("r22g_rel_minrank", w=W_rel_repl, minrank=True),
    go("r22h_squeeze_n4", topn=4, w=dict(W5) | {"squeeze": 0.25, "rev_seq": 0.4}),
]
cmp = pl.DataFrame(runs).sort("cagr", descending=True)
with pl.Config(tbl_rows=10, tbl_width_chars=100):
    print(cmp)
print("\nR3 基準:113.7/2.44 | n4 cell:126.1/2.40 | 晉級 ≥128 或(2.55∧120)")
print(f"total {time.time()-t0:.1f}s")
