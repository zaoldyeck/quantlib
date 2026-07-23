"""Q02 — 純財務組合(不看股價選股;goal 方向 2 成長 + 方向 3 品質;預註冊見 ledger)。

**選股零價格輸入**:排名只用財務資料(月營收/季報),完全不含價格、量、動能。
執行照市場現實:PIT 生效日重排名、T+1 open 成交、apex 全成本(引擎唯一真源
`apex.engine.simulate`,純日曆出場 = rebalance 日不在新名單即標記出場;對照變體
+trail35 披露「加價格出場」的增量)。

書:
- 成長書 G(月頻):rev_yoy_accel × rev_3m_yoy × ni_mom_ta 幾何 rank,N∈{10,20}
- 品質書 Q(季頻):f_score × gpoa × accruals_neg × cfo_ni × asset_g_neg 幾何 rank,N∈{10,20}

Run: uv run --project . python -m quantlib.apex.experiments.q02_pure_financial_books
依賴 cache:是。
"""
from __future__ import annotations

import time
from datetime import date as Date

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.apex.experiments.m02_refit_frequency import BOOT_SEED, _metrics, _rets

SIM_START, END = "2007-07-02", None   # 全史(tpex 價 2007-07 起);END=None → cache 最新
C = "company_code"


def kpi(nav: pl.DataFrame, a: str, b: str | None = None) -> dict:
    r = _rets(nav, Date.fromisoformat(a), Date.fromisoformat(b) if b else Date(2099, 1, 1))
    return _metrics(r, np.random.default_rng(BOOT_SEED))


def geo_rank(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    """rebalance 日截面幾何 rank(全部等權 ^1;缺值該檔剔除)。"""
    df = df.drop_nulls(subset=cols)
    expr = None
    for c_ in cols:
        term = (pl.col(c_).rank() / pl.len()).over("date")
        expr = term if expr is None else expr * term
    return df.with_columns(expr.alias("score")).select(["date", C, "score"])


def run_book(panel, elig_daily, sc: pl.DataFrame, n: int, trail: float | None,
             start: str) -> pl.DataFrame:
    """純日曆書:sc 只在 rebalance 日有列;出場 = 下期不在 top-N(exit_flags)。"""
    sc = (sc.join(elig_daily, on=["date", C], how="semi")
          .filter(pl.col("date") >= pl.lit(start).str.to_date()))
    top = (sc.sort(["date", "score"], descending=[False, True])
           .group_by("date", maintain_order=True).head(n))
    # 出場旗標:上一期名單 − 本期名單(名單層近似;未成交者無持倉即無動作)
    dates = top["date"].unique().sort().to_list()
    prev: set[str] = set()
    drops = []
    for d in dates:
        cur = set(top.filter(pl.col("date") == d)[C].to_list())
        drops += [(d, c) for c in prev - cur]
        prev = cur
    flags = pl.DataFrame({"date": [d for d, _ in drops], C: [c for _, c in drops]},
                         schema={"date": pl.Date, C: pl.Utf8})
    res = simulate(
        panel, top.select(["date", C, "score"]), exit_flags=flags,
        exec_spec=ExecSpec(),
        port_spec=PortSpec(n_slots=n, max_new_per_day=n),
        exit_spec=ExitSpec(trailing_stop=trail if trail else 9.99,
                           time_stop=10**6, loser_time_stop=None),
        start=Date.fromisoformat(start))
    return res.nav.select(["date", "nav"]).sort("date")


def main() -> None:
    t0 = time.time()
    con = data.connect()
    de = data.latest_date(con).isoformat()
    panel = data.common_stocks(data.load_panel(con, "2006-06-01", de, warmup_days=420))
    elig_daily = (data.eligibility(panel, min_adv=5_000_000.0)
                  .filter(pl.col("eligible")).select(["date", C]))
    trading_days = panel.select(pl.col("date").unique().sort()).get_column("date")
    td = pl.DataFrame({"td": trading_days}).sort("td")

    def snap(df, col):
        return (df.sort(col).join_asof(td, left_on=col, right_on="td", strategy="forward")
                .rename({"td": "date"}).drop_nulls(subset=["date"]))

    # ── 成長書原料(月營收 + 季報 NI 動能;PIT 慣例同 F02)────────────────
    rev = (data.load_monthly_revenue(con, de)
           .sort([C, "year", "month"])
           .with_columns([
               pl.date(pl.col("year") + pl.col("month") // 12,
                       pl.col("month") % 12 + 1, 10).alias("deadline"),
               pl.col("monthly_revenue_yoy").alias("rev_yoy"),
               ((pl.col("monthly_revenue").rolling_sum(3)
                 / pl.col("monthly_revenue").rolling_sum(3).shift(12) - 1) * 100
                ).over(C).alias("rev_3m_yoy"),
               (pl.col("monthly_revenue_yoy").rolling_mean(3)
                - pl.col("monthly_revenue_yoy").rolling_mean(12)
                ).over(C).alias("rev_yoy_accel"),
           ]))
    rq = pl.read_parquet(data.RAW_QUARTERLY_PARQUET).sort([C, "year", "quarter"])
    pos = lambda c: pl.when(pl.col(c) > 0).then(pl.col(c))
    rq = (rq.with_columns([
            pl.col("gross_pf_q").rolling_sum(4).over(C).alias("gp_ttm"),
            ((pl.col("ni_ttm") - pl.col("ni_ttm").shift(4).over(C))
             / pos("total_assets")).alias("ni_mom_ta"),
          ])
          .with_columns([
            (pl.col("gp_ttm") / pos("total_assets")).alias("gpoa"),
            (-(pl.col("ni_ttm") - pl.col("cfo_ttm")) / pos("total_assets")).alias("accruals_neg"),
            (-(pl.col("total_assets") / pos("total_assets").shift(4).over(C) - 1)).alias("asset_g_neg"),
            pl.col("f_score_raw").cast(pl.Float64),
            (-(pl.col("ni_q").rolling_std(8).over(C) / pos("total_assets"))).alias("ni_vol8_neg"),
            (-pl.col("gross_margin_q").rolling_std(8).over(C)).alias("gm_vol8_neg"),
            (pl.col("cfo_ttm") / pos("total_assets")).alias("cfo_ta"),
          ])
          .with_columns(
            pl.when(pl.col("quarter") == 1).then(pl.date(pl.col("year"), 5, 15))
            .when(pl.col("quarter") == 2).then(pl.date(pl.col("year"), 8, 14))
            .when(pl.col("quarter") == 3).then(pl.date(pl.col("year"), 11, 14))
            .otherwise(pl.date(pl.col("year") + 1, 3, 31)).alias("deadline")))

    rev_s = snap(rev, "deadline")
    rq_s = snap(rq, "deadline")
    # 季報值 as-of 到月營收 rebalance 日(成長書的 ni_mom_ta)
    g_raw = (rev_s.select(["date", C, "rev_yoy_accel", "rev_3m_yoy"])
             .sort("date")
             .join_asof(rq_s.select([C, "date", "ni_mom_ta"]).sort("date")
                        .rename({"date": "q_date"}),
                        left_on="date", right_on="q_date", by=C,
                        strategy="backward", tolerance="150d")
             .drop("q_date"))
    g_sc = geo_rank(g_raw, ["rev_yoy_accel", "rev_3m_yoy", "ni_mom_ta"])
    q_sc = geo_rank(
        rq_s.select(["date", C, "f_score_raw", "gpoa", "accruals_neg",
                     "cfo_ni_ratio_ttm", "asset_g_neg"]),
        ["f_score_raw", "gpoa", "accruals_neg", "cfo_ni_ratio_ttm", "asset_g_neg"])
    # 穩定性書(Q01 結果驅動之追加變體,post-hoc 標注):Q01 晉級的穩定性族
    s_sc = geo_rank(
        rq_s.select(["date", C, "gm_vol8_neg", "ni_vol8_neg", "cfo_ta"]),
        ["gm_vol8_neg", "ni_vol8_neg", "cfo_ta"])
    print(f"原料就緒 {time.time()-t0:.0f}s;成長書 rebal 日 {g_sc['date'].n_unique()}、"
          f"品質書 {q_sc['date'].n_unique()}")

    rows = []
    for label, sc in [("成長G", g_sc), ("品質Q", q_sc), ("穩定S*", s_sc)]:
        for n in [10, 20]:
            for trail, tl in [(None, "純日曆"), (0.35, "+trail35")]:
                nav = run_book(panel, elig_daily, sc, n, trail, SIM_START)
                full = kpi(nav, SIM_START)
                w3 = kpi(nav, "2023-07-20")
                rows.append({"書": label, "N": n, "出場": tl,
                             "全史CAGR": full.get("cagr"), "全史P5": full.get("p5"),
                             "全史MDD": full.get("mdd"), "Sharpe": full.get("sharpe"),
                             "W3_CAGR": w3.get("cagr"), "W3_P5": w3.get("p5")})
                print(f"  {label} N={n} {tl}: 全史 {full.get('cagr', float('nan')):.1%}"
                      f"/P5 {full.get('p5', float('nan')):.2f}/MDD {full.get('mdd', float('nan')):.1%}"
                      f" | W3 {w3.get('cagr', float('nan')):.1%} ({time.time()-t0:.0f}s)")
    out = pl.DataFrame(rows)
    out.write_parquet("src/quantlib/apex/ledger/q02_pure_financial.parquet")
    with pl.Config(tbl_rows=20, float_precision=3):
        print(out)
    print(f"\ntotal {time.time()-t0:.0f}s → ledger/q02_pure_financial.parquet")


if __name__ == "__main__":
    main()
