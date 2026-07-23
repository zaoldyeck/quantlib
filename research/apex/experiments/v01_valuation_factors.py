"""V01 — 估值模型 IC 廣篩(goal 1d;預註冊見 ledger Q/V/T-LINE 段)。

因子:ep/bp(stock_per_pbr 日頻,對照錨=tilt 預期)、cfo_yield、ev_ebit_inv
(EV=市值+總負債,無現金扣除聲明)、peg_inv(E/P×營收成長,g≤0 域外)、
dcf_proxy(兩階段:CFO_ttm 基準、g1=3y 營收 CAGR cap 25%、terminal 2%、r 10%
——誠實=基本面變換+常數假設,無前瞻)、fiveline_z_neg(五線譜:close 對 400 日
對數迴歸殘差 z,負向=低於趨勢;閉式 rolling OLS)。

市值 = raw_close × capital_stock(千元;截面 rank 下常數尺度不影響 IC;
EV 相加處單位已對齊:mcap_k = raw_close×capital_stock/10)。判準同 F01。

Run: uv run --project research python -m research.apex.experiments.v01_valuation_factors
依賴 cache:是。
"""
from __future__ import annotations

import time

import polars as pl

from research.apex import data, factors

DEV_START, DEV_END = "2012-01-02", "2023-12-29"
BATCH = "V01"
C = "company_code"
W5 = 400  # 五線譜迴歸窗(交易日)


def win(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("date").is_between(
        pl.lit(DEV_START).str.to_date(), pl.lit(DEV_END).str.to_date()))


t0 = time.time()
con = data.connect()
panel = data.common_stocks(data.load_panel(con, DEV_START, DEV_END, warmup_days=700))
elig = win(data.eligibility(panel))
fwd = factors.forward_returns(panel)
grid = win(panel.select(["date", C]))
trading_days = panel.select(pl.col("date").unique().sort()).get_column("date")
td = pl.DataFrame({"td": trading_days}).sort("td")

# ── (1) 日頻估值:ep/bp(stock_per_pbr)─────────────────────────────────
pp = con.sql(
    "SELECT date, company_code, price_to_earning_ratio AS pe, "
    "price_book_ratio AS pb FROM stock_per_pbr").pl()
epbp = (grid.join(pp, on=["date", C], how="left")
        .with_columns([
            pl.when(pl.col("pe") > 0).then(1.0 / pl.col("pe")).alias("ep"),
            pl.when(pl.col("pb") > 0).then(1.0 / pl.col("pb")).alias("bp"),
        ]))

# ── (2) 季報估值(PIT 法定期限 → as-of → join raw_close)──────────────────
pos = lambda c: pl.when(pl.col(c) > 0).then(pl.col(c))
rq = (pl.read_parquet(data.RAW_QUARTERLY_PARQUET)
      .sort([C, "year", "quarter"])
      .with_columns([
          (pl.col("current_liabilities") + pl.col("non_current_liab")).alias("tl"),
          pl.col("op_income_q").rolling_sum(4).over(C).alias("ebit_ttm"),
          ((pl.col("rev_ttm") / pos("rev_ttm").shift(12).over(C)) ** (1 / 3) - 1)
          .alias("g3y"),
          ((pl.col("rev_ttm") / pos("rev_ttm").shift(4).over(C)) - 1).alias("g_yoy"),
      ])
      .with_columns(
          pl.when(pl.col("quarter") == 1).then(pl.date(pl.col("year"), 5, 15))
          .when(pl.col("quarter") == 2).then(pl.date(pl.col("year"), 8, 14))
          .when(pl.col("quarter") == 3).then(pl.date(pl.col("year"), 11, 14))
          .otherwise(pl.date(pl.col("year") + 1, 3, 31)).alias("deadline"))
      .sort("deadline")
      .join_asof(td, left_on="deadline", right_on="td", strategy="forward")
      .rename({"td": "avail"}).drop_nulls(subset=["avail"])
      .select([C, "avail", "cfo_ttm", "ebit_ttm", "ni_ttm", "tl",
               "capital_stock", "g3y", "g_yoy"])
      .sort("avail"))
qd = (grid.sort("date")
      .join_asof(rq, left_on="date", right_on="avail", by=C,
                 strategy="backward", tolerance="150d")
      .join(panel.select(["date", C, "raw_close"]), on=["date", C], how="left")
      .with_columns((pl.col("raw_close") * pos("capital_stock") / 10).alias("mcap_k")))

G1 = pl.min_horizontal(pl.max_horizontal(pl.col("g3y"), pl.lit(0.0)), pl.lit(0.25))
_dcf_pv = sum((1 + G1) ** t_ / 1.10 ** t_ for t_ in range(1, 6)) \
    + (1 + G1) ** 5 * 1.02 / (0.10 - 0.02) / 1.10 ** 5
# 命名口徑誠實標註(2026-07-23 稽核 D-valuation;這批皆 IC-dead 研究因子、非生產):
# - ev_ebit_inv:EV = 市值 + **總負債 tl**(非教科書淨負債 = 有息負債 − 現金),屬資料受限
#   代理;此檔無有息負債/現金明細欄,總負債為粗代理,截面 rank 用途可接受但非嚴格 EV。
# - peg_inv:= 盈餘殖利率 × **營收 YoY 成長 g_yoy**(非 PEG 的盈餘成長),故非教科書 PEG;
#   營收成長 ≠ EPS 成長,此為啟發式代理。兩者稽核實測 IC 近零(peg mean_ic −0.017)已判死、
#   不進任何生產策略,保留僅為 IC 廣篩存證;若日後要真 PEG/EV 需接 EPS 成長與淨負債欄位。
qd = qd.with_columns([
    (pos("cfo_ttm") / pos("mcap_k")).alias("cfo_yield"),
    (pl.col("ebit_ttm") / (pos("mcap_k") + pos("tl"))).alias("ev_ebit_inv"),
    pl.when(pl.col("g_yoy") > 0)
      .then((pl.col("ni_ttm") / pos("mcap_k")) * pl.col("g_yoy")).alias("peg_inv"),
    (pos("cfo_ttm") * _dcf_pv / pos("mcap_k")).alias("dcf_proxy"),
])

# ── (3) 五線譜:400 日對數迴歸殘差 z(閉式 rolling OLS,等距 t)────────────
var_t = (W5 ** 2 - 1) / 12.0
fl = (panel.sort([C, "date"])
      .with_columns([
          pl.col("close").log().alias("y"),
          pl.col("date").cum_count().over(C).cast(pl.Float64).alias("t"),
      ])
      .with_columns([
          pl.col("y").rolling_mean(W5).over(C).alias("my"),
          pl.col("y").rolling_var(W5).over(C).alias("vy"),
          ((pl.col("t") * pl.col("y")).rolling_mean(W5).over(C)
           - pl.col("t").rolling_mean(W5).over(C)
           * pl.col("y").rolling_mean(W5).over(C)).alias("cov_ty"),
      ])
      .with_columns((pl.col("cov_ty") / var_t).alias("slope"))
      .with_columns(
          ((pl.col("y") - pl.col("my") - pl.col("slope") * (W5 - 1) / 2)
           / (pl.col("vy") - pl.col("slope") ** 2 * var_t).clip(1e-12).sqrt())
          .alias("z"))
      .select(["date", C, (-pl.col("z")).alias("fiveline_z_neg")]))

print(f"data ready in {time.time()-t0:.1f}s\n")

SPECS = [
    (epbp, ["ep", "bp"]),
    (qd, ["cfo_yield", "ev_ebit_inv", "peg_inv", "dcf_proxy"]),
    (win(fl), ["fiveline_z_neg"]),
]
for frame, names in SPECS:
    for name in names:
        fac = (frame.select(["date", C, pl.col(name).alias("value")])
               .drop_nulls(subset=["value"])
               .filter(pl.col("value").is_finite()))
        r = factors.evaluate_factor(name, fac, fwd, elig, family="valuation", batch=BATCH)
        print(factors.fmt_factor(r))

print(f"\ntotal {time.time()-t0:.1f}s")
