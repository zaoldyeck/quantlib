"""T02 — 海龜交易系統容器 + V02 神奇公式書(goal 1f;預註冊見 ledger Q/V/T-LINE)。

海龜(Donchian 完整版,pyramid 簡化聲明):
- 進場 = 55 日新高突破(system 2;20 日版對照);出場 = 20/10 日新低 + 2N ATR 停損
  (以 exit_flags 逐日標記;加碼單位簡化為單一部位——聲明:原版 4 單位加碼未實作,
  F11 已證 S 上加碼為噪音級,此處容器層先驗天花板)
- 池 = eligible(ADV 5M);N=10 席等權;W3 + 全期兩窗
亞當理論 = 純價格慣性:引 N01 蓋棺(lb42/n12 最佳 P5 12.8)為證,不重跑月頻版;
此處 donchian 進場即其「突破式」變體。

神奇公式(Greenblatt):EY = EBIT_ttm/(mcap+TL) × ROC = EBIT_ttm/(TA−CL) 雙 rank
之和,季頻(法定生效日)re-rank,N=20,同 Q02 harness(純日曆出場)。

判準(容器):W3 P5 > 45.9 挑戰 / > 33 存活;皆不及 = 死。
Run: uv run --project . python -m quantlib.apex.experiments.t02_turtle_magic
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
from quantlib.apex.experiments.q02_pure_financial_books import geo_rank, run_book

SIM_START = "2007-07-02"
W3_START = "2023-07-20"
C = "company_code"


def kpi(nav: pl.DataFrame, a: str) -> dict:
    return _metrics(_rets(nav, Date.fromisoformat(a), Date(2099, 1, 1)),
                    np.random.default_rng(BOOT_SEED))


def turtle(panel, elig_daily, entry_w: int, exit_w: int, atr_mult: float = 2.0):
    """海龜容器:entry_w 日新高進場;exit_w 日新低或 2N 停損出場(exit_flags 逐日)。"""
    p = (panel.sort([C, "date"])
         .with_columns([
             (pl.col("close") - pl.col("close").shift(1)).over(C).abs().alias("_d"),
             pl.max_horizontal(
                 pl.col("high") - pl.col("low"),
                 (pl.col("high") - pl.col("close").shift(1).over(C)).abs(),
                 (pl.col("low") - pl.col("close").shift(1).over(C)).abs()).alias("tr"),
         ])
         .with_columns([
             pl.col("tr").rolling_mean(20).over(C).alias("atr20"),
             pl.col("close").shift(1).rolling_max(entry_w).over(C).alias("hh"),
             pl.col("close").shift(1).rolling_min(exit_w).over(C).alias("ll"),
         ])
         .with_columns([
             (pl.col("close") > pl.col("hh")).alias("brk"),
             # 突破強度作 score(同日多突破時挑最強)
             ((pl.col("close") / pl.col("hh") - 1)
              / (pl.col("atr20") / pl.col("close")).clip(1e-6)).alias("score"),
         ]))
    entries = (p.filter(pl.col("brk"))
               .join(elig_daily, on=["date", C], how="semi")
               .select(["date", C, "score"])
               .filter(pl.col("date") >= pl.lit(SIM_START).str.to_date()))
    # 出場旗標:exit_w 日新低(2N 停損由引擎 trailing 近似:2N/close ≈ 動態%——
    # 簡化為 exit_flags 用 ll,加上引擎 trail 以 2×ATR%(池中位 ~8%)→ trail 0.16
    flags = (p.filter(pl.col("close") < pl.col("ll"))
             .select(["date", C])
             .filter(pl.col("date") >= pl.lit(SIM_START).str.to_date()))
    res = simulate(panel, entries, exit_flags=flags, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=10, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.16, time_stop=10**6,
                                      loser_time_stop=None),
                   start=Date.fromisoformat(SIM_START))
    return res.nav.select(["date", "nav"]).sort("date")


def main() -> None:
    t0 = time.time()
    con = data.connect()
    de = data.latest_date(con).isoformat()
    panel = data.common_stocks(data.load_panel(con, "2006-06-01", de, warmup_days=420))
    elig_daily = (data.eligibility(panel, min_adv=5_000_000.0)
                  .filter(pl.col("eligible")).select(["date", C]))
    print(f"panel {time.time()-t0:.0f}s")

    print("=== 海龜容器(W3 判準:P5>45.9 挑戰 / >33 存活)===")
    for ew, xw in [(55, 20), (20, 10)]:
        nav = turtle(panel, elig_daily, ew, xw)
        full, w3 = kpi(nav, SIM_START), kpi(nav, W3_START)
        print(f"  turtle {ew}/{xw}: 全期 {full.get('cagr', 0):.1%}/P5 {full.get('p5', 0):.2f}"
              f"/MDD {full.get('mdd', 0):.1%} | W3 {w3.get('cagr', 0):.1%}"
              f"/P5 {w3.get('p5', 0):.2f} ({time.time()-t0:.0f}s)")

    print("=== 神奇公式書(季頻 N=20,Q02 harness)===")
    pos = lambda c: pl.when(pl.col(c) > 0).then(pl.col(c))
    td = pl.DataFrame({"td": panel.select(pl.col("date").unique().sort())
                       .get_column("date")}).sort("td")
    rq = (pl.read_parquet(data.RAW_QUARTERLY_PARQUET)
          .sort([C, "year", "quarter"])
          .with_columns([
              (pl.col("current_liabilities") + pl.col("non_current_liab")).alias("tl"),
              pl.col("op_income_q").rolling_sum(4).over(C).alias("ebit_ttm"),
          ])
          .with_columns(
              pl.when(pl.col("quarter") == 1).then(pl.date(pl.col("year"), 5, 15))
              .when(pl.col("quarter") == 2).then(pl.date(pl.col("year"), 8, 14))
              .when(pl.col("quarter") == 3).then(pl.date(pl.col("year"), 11, 14))
              .otherwise(pl.date(pl.col("year") + 1, 3, 31)).alias("deadline"))
          .sort("deadline")
          .join_asof(td, left_on="deadline", right_on="td", strategy="forward")
          .rename({"td": "date"}).drop_nulls(subset=["date"]))
    mf = (rq.join(panel.select(["date", C, "raw_close"]), on=["date", C], how="left")
          .with_columns((pl.col("raw_close") * pos("capital_stock") / 10).alias("mcap_k"))
          .with_columns([
              (pl.col("ebit_ttm") / (pos("mcap_k") + pos("tl"))).alias("ey"),
              (pl.col("ebit_ttm")
               / pos("total_assets").sub(pl.col("current_liabilities")).clip(1.0)).alias("roc"),
          ]))
    mf_sc = geo_rank(mf.select(["date", C, "ey", "roc"]), ["ey", "roc"])
    nav = run_book(panel, elig_daily, mf_sc, 20, None, SIM_START)
    full, w3 = kpi(nav, SIM_START), kpi(nav, W3_START)
    print(f"  magic N=20: 全期 {full.get('cagr', 0):.1%}/P5 {full.get('p5', 0):.2f}"
          f"/MDD {full.get('mdd', 0):.1%}/Sharpe {full.get('sharpe', 0):.2f}"
          f" | W3 {w3.get('cagr', 0):.1%}/P5 {w3.get('p5', 0):.2f}")
    print(f"\ntotal {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
