"""Q01 — 財務分析法 IC 廣篩(Z''/O-score/杜邦/accruals/GP:A 等 18 因子;預註冊見
ledger/batches.md Q-LINE)。PIT 骨架同 F02(法定期限生效);判準同 F01。

資料限制(誠實):Z 用 Altman 1993 Z''(四項無市值);X2 用真保留盈餘(保留盈餘（或
累積虧損）,含負值累虧,非「權益−股本」代理);X3 EBIT 以稅前淨利(EBT)代理——
concise IS 自 2013 IFRS 起不逐項列利息費用(僅「營業外收入及支出」彙總),稅前淨利
已含營業外損益、優於營業利益;O-score 9 項全實作(INTWO/CHIN 用年度 TTM 口徑,唯
GNP deflator 省略,ln(TA) 千元尺度只平移不影響 rank);Beneish M 不可算(無應收/折舊明細)。

Run: uv run --project . python -m quantlib.apex.experiments.q01_financial_scores
依賴 cache:是。
"""
from __future__ import annotations

import time

import polars as pl

from quantlib.apex import data, factors

DEV_START, DEV_END = "2012-01-02", "2023-12-29"
BATCH = "Q01"
C = "company_code"


def win(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("date").is_between(
        pl.lit(DEV_START).str.to_date(), pl.lit(DEV_END).str.to_date()))


t0 = time.time()
con = data.connect()
panel = data.common_stocks(data.load_panel(con, DEV_START, DEV_END, warmup_days=420))
elig = win(data.eligibility(panel))
fwd = factors.forward_returns(panel)
grid = win(panel.select(["date", C]))
trading_days = panel.select(pl.col("date").unique().sort()).get_column("date")


def _snap(df: pl.DataFrame, date_col: str) -> pl.DataFrame:
    td = pl.DataFrame({"td": trading_days}).sort("td")
    return (df.sort(date_col)
            .join_asof(td, left_on=date_col, right_on="td", strategy="forward")
            .rename({"td": "avail"})
            .drop_nulls(subset=["avail"]))


def to_daily(event: pl.DataFrame, value_col: str, tolerance: str) -> pl.DataFrame:
    ev = (event.drop_nulls(subset=[value_col])
          .filter(pl.col(value_col).is_finite())
          .sort("avail"))
    return (grid.sort("date")
            .join_asof(ev.select([C, "avail", value_col]),
                       left_on="date", right_on="avail", by=C,
                       strategy="backward", tolerance=tolerance)
            .select(["date", C, pl.col(value_col).alias("value")])
            .drop_nulls(subset=["value"]))


# ── 季報衍生因子(raw_quarterly + 法定期限 PIT,慣例同 F02)────────────────
rq = pl.read_parquet(data.RAW_QUARTERLY_PARQUET)
pos = lambda c: pl.when(pl.col(c) > 0).then(pl.col(c))  # 分母守門:非正 → null
rq = (
    rq.sort([C, "year", "quarter"])
    .with_columns([
        (pl.col("current_liabilities") + pl.col("non_current_liab")).alias("tl"),
        # Altman Z'' X3 EBIT ≈ 稅前淨利(EBT)TTM。concise IS 自 2013 起不逐項列利息費用
        # (僅「營業外收入及支出」彙總、已內含利息),故以稅前淨利代理:已含營業外損益,
        # 修正舊版用營業利益(排除全部業外)之偏差(2023Q4 實測 86 家因業外致 EBIT 正負
        # 號翻轉)。嚴格 EBIT 的利息加回因 concise 表 2013+ 無利息明細而不可得(見 docstring)。
        pl.col("pretax_q").rolling_sum(4).over(C).alias("ebit_ttm"),
        pl.col("gross_pf_q").rolling_sum(4).over(C).alias("gp_ttm"),
    ])
    .with_columns([
        ((pl.col("current_assets") - pl.col("current_liabilities"))
         / pos("total_assets")).alias("wc_ta"),
        # Altman Z'' X2 = 保留盈餘/總資產,用真保留盈餘(可為負=累積虧損)。舊版以
        # 「權益−股本」代理灌入資本公積/NCI,抹掉累虧訊號(2023Q4 實測 114 家真 RE<0
        # 被判為正,6854 X2 由 +1.25 誤成 −8.18)。分子不做正值守門(須保留負值),僅
        # 分母 total_assets 守門。
        (pl.col("retained_earnings") / pos("total_assets")).alias("re_ta"),
        # 杜邦三項在 q01 各自當「獨立因子」逐一評估 IC(NAMES 列表,從不相乘),故不
        # 要求相乘還原 ROE 的恆等式:dupont_turnover 用「年初資產」(Piotroski 口徑,見
        # raw_quarterly asset_turnover_ttm = rev_ttm/年初資產),而 leverage/roe 用「期末
        # 資產/期末權益」——口徑刻意不同、各自皆合法截面指標,不構成自洽 DuPont 分解。
        # 權益/淨利採「總權益/總淨利」口徑(含非控制權益 NCI):分子分母同含 NCI 內部
        # 自洽(= 總權益報酬率),非嚴格「歸屬母公司」ROE(歸母欄位可得,此處刻意採總
        # 口徑故不轉;若需嚴格歸母另取「歸屬於母公司業主之權益」/「淨利歸屬母公司」)。
        (pl.col("ni_ttm") / pos("rev_ttm")).alias("dupont_margin"),
        (pl.col("total_assets") / pos("total_equity")).alias("dupont_leverage"),
        (pl.col("ni_ttm") / pos("total_equity")).alias("roe_ttm"),
        (-(pl.col("ni_ttm") - pl.col("cfo_ttm")) / pos("total_assets")).alias("accruals_neg"),
        (pl.col("gp_ttm") / pos("total_assets")).alias("gpoa"),
        (pl.col("cfo_ttm") / pos("total_assets")).alias("cfo_ta"),
        (-pl.col("d_capital_stock_yoy")).alias("net_iss_neg"),
        (-(pl.col("ni_q").rolling_std(8).over(C) / pos("total_assets"))).alias("ni_vol8_neg"),
        (-pl.col("gross_margin_q").rolling_std(8).over(C)).alias("gm_vol8_neg"),
        pl.col("f_score_raw").cast(pl.Float64),
    ])
    .with_columns([
        (-(pl.col("total_assets") / pos("total_assets").shift(4).over(C) - 1)).alias("asset_g_neg"),
        (-(pl.col("tl") / pos("tl").shift(4).over(C) - 1)).alias("liab_g_neg"),
        (pl.col("dupont_margin") - pl.col("dupont_margin").shift(4).over(C)).alias("d_margin_yoy"),
        (pl.col("dupont_leverage") - pl.col("dupont_leverage").shift(4).over(C)).alias("d_leverage_yoy"),
        # Altman Z''(1993 四項):6.56·WC/TA + 3.26·RE/TA + 6.72·EBIT/TA + 1.05·BE/TL
        (6.56 * pl.col("wc_ta") + 3.26 * pl.col("re_ta")
         + 6.72 * (pl.col("ebit_ttm") / pos("total_assets"))
         + 1.05 * (pl.col("total_equity") / pos("tl"))).alias("z_pp"),
        # Ohlson O(9 項全實作,唯 GNP deflator 省略〔rank 不變〕;高 = 危險 → 取負讓高 = 好)
        (-(-1.32
           - 0.407 * pos("total_assets").log()
           + 6.03 * (pl.col("tl") / pos("total_assets"))
           - 1.43 * pl.col("wc_ta")
           + 0.0757 * (pl.col("current_liabilities") / pos("current_assets"))
           - 2.37 * (pl.col("ni_ttm") / pos("total_assets"))
           - 1.83 * (pl.col("cfo_ttm") / pos("tl"))
           # INTWO = 連續兩「年度」淨利為負(Ohlson 1980 第 8 項,係數 +0.285),故用本年
           # TTM 與去年同季 TTM(shift(4))皆 < 0,非舊版「連兩季」(ni_q shift(1))。
           + 0.285 * ((pl.col("ni_ttm") < 0) & (pl.col("ni_ttm").shift(4).over(C) < 0)).cast(pl.Float64)
           - 1.72 * (pl.col("tl") > pl.col("total_assets")).cast(pl.Float64)
           # CHIN = 年對年淨利變動(Ohlson 1980 第 9 項,係數 −0.521),lag 用去年同季
           # TTM(shift(4)),分子分母同為年對年 TTM 口徑,非舊版季位移 shift(1)。
           - 0.521 * ((pl.col("ni_ttm") - pl.col("ni_ttm").shift(4).over(C))
                      / (pl.col("ni_ttm").abs() + pl.col("ni_ttm").shift(4).over(C).abs()))
           )).alias("o_score_neg"),
    ])
    .with_columns(
        pl.when(pl.col("quarter") == 1).then(pl.date(pl.col("year"), 5, 15))
        .when(pl.col("quarter") == 2).then(pl.date(pl.col("year"), 8, 14))
        .when(pl.col("quarter") == 3).then(pl.date(pl.col("year"), 11, 14))
        .otherwise(pl.date(pl.col("year") + 1, 3, 31))
        .alias("deadline")
    )
)
rq = _snap(rq, "deadline")
print(f"data ready in {time.time()-t0:.1f}s\n")

NAMES = [
    "z_pp", "o_score_neg",
    "dupont_margin", "dupont_turnover", "dupont_leverage", "roe_ttm",
    "d_margin_yoy", "d_leverage_yoy",
    "accruals_neg", "gpoa", "cfo_ta",
    "asset_g_neg", "net_iss_neg", "liab_g_neg",
    "ni_vol8_neg", "gm_vol8_neg",
    "f_score_raw",  # 對照錨(F02 已測 t'2.9)
]
rq = rq.rename({"asset_turnover_ttm": "dupont_turnover"})

for name in NAMES:
    fac = to_daily(rq, name, tolerance="150d")
    r = factors.evaluate_factor(name, fac, fwd, elig, family="fin_scores", batch=BATCH)
    print(factors.fmt_factor(r))

print(f"\ntotal {time.time()-t0:.1f}s")
