"""T01 — 傳統技術指標 + VWAP 系 IC 廣篩(goal 1b;預註冊見 ledger Q/V/T-LINE 段)。

因子:rsi14(SMA 版,聲明非 Wilder EMA)、stoch_k14、macd_hist(12/26/9,÷close)、
boll_pctb(20,2)、boll_bw_neg(squeeze,lowvol 近親預期)、mfi14、obv_slope20、
vwap20_dev(20 日 rolling VWAP 偏離;raw 量價 vs raw_close 同基準)、
avwap_dev(錨定 VWAP,錨=該檔月營收法定生效日——事件錨定,本批唯一未測家族)。
TPO/Market Profile 無日內資料不可測(POC proxy=hvn_dist 已測 tail 家族)——聲明。

判準同 F01。Run: uv run --project research python -m research.apex.experiments.t01_technical_factors
依賴 cache:是。
"""
from __future__ import annotations

import time

import polars as pl

from research.apex import data, factors

DEV_START, DEV_END = "2012-01-02", "2023-12-29"
BATCH = "T01"
C = "company_code"


def win(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("date").is_between(
        pl.lit(DEV_START).str.to_date(), pl.lit(DEV_END).str.to_date()))


t0 = time.time()
con = data.connect()
panel = data.common_stocks(data.load_panel(con, DEV_START, DEV_END, warmup_days=420))
elig = win(data.eligibility(panel))
fwd = factors.forward_returns(panel)

p = (panel.sort([C, "date"])
     .with_columns([
         (pl.col("close") / pl.col("close").shift(1) - 1).over(C).alias("ret"),
         ((pl.col("high") + pl.col("low") + pl.col("close")) / 3).alias("tp"),
     ])
     .with_columns([
         pl.when(pl.col("ret") > 0).then(pl.col("ret")).otherwise(0.0).alias("gain"),
         pl.when(pl.col("ret") < 0).then(-pl.col("ret")).otherwise(0.0).alias("loss"),
         (pl.col("tp") * pl.col("volume")).alias("mf"),
         (pl.col("tp") > pl.col("tp").shift(1).over(C)).alias("tp_up"),
         pl.when(pl.col("ret") > 0).then(pl.col("volume"))
         .when(pl.col("ret") < 0).then(-pl.col("volume"))
         .otherwise(0).cast(pl.Float64).alias("sv"),
         pl.col("close").ewm_mean(span=12).over(C).alias("e12"),
         pl.col("close").ewm_mean(span=26).over(C).alias("e26"),
         pl.col("close").rolling_mean(20).over(C).alias("ma20"),
         pl.col("close").rolling_std(20).over(C).alias("sd20"),
     ])
     .with_columns((pl.col("e12") - pl.col("e26")).alias("macd"))
     .with_columns([
         # rsi14(g/(g+l) 與 RSI 單調等價)
         (pl.col("gain").rolling_mean(14).over(C)
          / (pl.col("gain").rolling_mean(14).over(C)
             + pl.col("loss").rolling_mean(14).over(C)).clip(1e-12)).alias("rsi14"),
         ((pl.col("close") - pl.col("low").rolling_min(14).over(C))
          / (pl.col("high").rolling_max(14).over(C)
             - pl.col("low").rolling_min(14).over(C)).clip(1e-12)).alias("stoch_k14"),
         ((pl.col("macd") - pl.col("macd").ewm_mean(span=9).over(C))
          / pl.col("close")).alias("macd_hist"),
         ((pl.col("close") - pl.col("ma20")) / (2 * pl.col("sd20")).clip(1e-12)).alias("boll_pctb"),
         (-(pl.col("sd20") / pl.col("ma20"))).alias("boll_bw_neg"),
         (pl.when(pl.col("tp_up")).then(pl.col("mf")).otherwise(0.0)
          .rolling_sum(14).over(C)
          / pl.col("mf").rolling_sum(14).over(C).clip(1e-12)).alias("mfi14"),
         ((pl.col("sv").cum_sum().over(C) - pl.col("sv").cum_sum().over(C).shift(20).over(C))
          / pl.col("volume").rolling_sum(20).over(C).clip(1.0)).alias("obv_slope20"),
         (pl.col("raw_close")
          / (pl.col("trade_value").rolling_sum(20).over(C)
             / pl.col("volume").rolling_sum(20).over(C).clip(1.0)) - 1).alias("vwap20_dev"),
     ]))

# ── 錨定 VWAP(錨 = 該檔月營收法定生效日;事件錨定家族)────────────────────
trading_days = panel.select(pl.col("date").unique().sort()).get_column("date")
td = pl.DataFrame({"td": trading_days}).sort("td")
anchors = (data.load_monthly_revenue(con, DEV_END)
           .with_columns(pl.date(pl.col("year") + pl.col("month") // 12,
                                 pl.col("month") % 12 + 1, 10).alias("deadline"))
           .sort("deadline")
           .join_asof(td, left_on="deadline", right_on="td", strategy="forward")
           .rename({"td": "anchor"}).drop_nulls(subset=["anchor"])
           .select([C, "anchor"]).unique().sort("anchor"))
av = (p.select(["date", C, "raw_close", "trade_value", "volume"]).sort("date")
      .join_asof(anchors, left_on="date", right_on="anchor", by=C,
                 strategy="backward", tolerance="70d")
      .drop_nulls(subset=["anchor"])
      .sort([C, "date"])
      .with_columns([
          pl.col("trade_value").cum_sum().over([C, "anchor"]).alias("cv"),
          pl.col("volume").cum_sum().over([C, "anchor"]).alias("cq"),
      ])
      .with_columns((pl.col("raw_close") / (pl.col("cv") / pl.col("cq").clip(1.0)) - 1)
                    .alias("avwap_dev"))
      .select(["date", C, "avwap_dev"]))

print(f"data ready in {time.time()-t0:.1f}s\n")

NAMES = ["rsi14", "stoch_k14", "macd_hist", "boll_pctb", "boll_bw_neg",
         "mfi14", "obv_slope20", "vwap20_dev"]
for name in NAMES:
    fac = (win(p).select(["date", C, pl.col(name).alias("value")])
           .drop_nulls(subset=["value"]).filter(pl.col("value").is_finite()))
    r = factors.evaluate_factor(name, fac, fwd, elig, family="technical", batch=BATCH)
    print(factors.fmt_factor(r))
fac = (win(av).select(["date", C, pl.col("avwap_dev").alias("value")])
       .drop_nulls(subset=["value"]).filter(pl.col("value").is_finite()))
r = factors.evaluate_factor("avwap_dev", fac, fwd, elig, family="technical", batch=BATCH)
print(factors.fmt_factor(r))

print(f"\ntotal {time.time()-t0:.1f}s")
