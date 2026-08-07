"""N14 — 出場錨點:錨在「下次營收公布前」還是「進場後 N 日」?

**背景(N09 之後才看清的結構)**:`rev_fresh_days` 是**日曆天**(assemble.py:163
`(date - avail).dt.total_days()`),而月營收的 avail 固定為次月 10 日。所以 S 的真實
節奏是:**公布後 7 天內買進(10-17 日)→ 抱到公布後第 26 天出場(次月 5 日)→ 恰好
避開下一次公布**。N09 已證 26 是尖銳最佳(放寬到 30/34 = 抱過下次公布,前窗 −25~−48pp)。

**本批要分離的混淆**:上述設計同時做了兩件事——(a) 抱一段固定長度(約三週)、
(b) 把出場**錨定在下次公布之前**。26 天勝出,可能只是因為「三週」剛好是最佳持有長度,
與「避開公布事件」無關。兩者可以拆開測:

  E 軸(事件錨,現行):stale_days = 26,出場時點隨公布日浮動
  T 軸(進場錨):stale 關掉(設 99)、改用 time_stop = N 個交易日,出場時點隨進場日浮動

**關鍵差異**:現行設計下,10 日進場者抱 26 天、17 日進場者只抱 19 天——**晚進場的人
自動抱得短**,因為兩者都在同一天出場。若事件錨是有價值的,T 軸(每個人都抱一樣長、
出場日各自不同、必然有人抱過公布日)應該明顯較差。

**臂**:E0 現行(stale 26 + time 30);T13/T16/T19/T22(stale 99 + time_stop N 交易日,
N 取 13/16/19/22 ≈ 現行持有分佈的四分位附近,中位為 16)。
輸家時間止損一律維持 15(不動,避免混入第二個變因)。

**判準**:同前——CAGR 與 Sharpe 同時不劣於現行 + 前後窗一致性。
**預期**:若 T 軸最佳臂明顯劣於 E0,則「避開公布事件」本身有價值(而非只是持有長度);
若 T 軸打平,則現行設計的價值只是「抱三週」,事件錨可以簡化掉。

依賴 cache: 是。
run: uv run --project . python -m quantlib.apex.experiments.n14_exit_anchor
"""
from __future__ import annotations

import polars as pl

from quantlib import paths
from quantlib.apex import data, metrics
from quantlib.apex.engine import ExitSpec
from quantlib.apex.strategy_s import prep_cached, run_s_full

FULL = "2015-01-01"
SPLIT = "2021-07-01"
OUT = paths.OUT / "apex" / "n14_anchor"

ARMS: list[tuple[str, int, int]] = [
    ("E0 事件錨 stale26(現行)", 26, 30),
    ("T13 進場錨 time13", 999, 13),
    ("T16 進場錨 time16", 999, 16),
    ("T19 進場錨 time19", 999, 19),
    ("T22 進場錨 time22", 999, 22),
]


def _row(label: str, nav, trades) -> dict:
    st = metrics.summarize(nav, trades)
    reasons = st.get("exit_reasons", {}) or {}
    n = max(1, sum(reasons.values()))
    return {"arm": label, "cagr": st["cagr"], "sharpe": st["sharpe"],
            "sortino": st["sortino"], "mdd": st["mdd"], "calmar": st["calmar"],
            "n_trades": st.get("n_trades", 0),
            "win_rate": round(st.get("win_rate", 0), 3),
            "signal占比": round(reasons.get("signal", 0) / n, 3),
            "time占比": round(reasons.get("time", 0) / n, 3),
            "med_days": st.get("med_days_held")}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = data.connect()
    panel, feat, elig = prep_cached(con)

    rows_full, rows_a, rows_b = [], [], []
    for label, stale, tstop in ARMS:
        xs = ExitSpec(trailing_stop=0.35, time_stop=tstop, loser_time_stop=15)
        nav, tr = run_s_full(panel, feat, elig, FULL, _exit_spec=xs, _stale_days=stale)
        rows_full.append(_row(label, nav, tr))
        cut = pl.lit(SPLIT).str.to_date()
        na = nav.filter(pl.col("date") < cut)
        rows_a.append(_row(label, na.with_columns(pl.col("nav") / pl.col("nav").first()),
                           tr.filter(pl.col("entry_date") < cut)))
        nb, trb = run_s_full(panel, feat, elig, SPLIT, _exit_spec=xs, _stale_days=stale)
        rows_b.append(_row(label, nb, trb))
        print(f"  {label} 完成")

    cols = ["arm", "cagr", "sharpe", "sortino", "mdd", "calmar", "n_trades",
            "win_rate", "signal占比", "time占比", "med_days"]
    for tag, rows in (("全窗", rows_full), ("前窗", rows_a), ("後窗", rows_b)):
        t = pl.DataFrame(rows)
        base = t.filter(pl.col("arm").str.starts_with("E0"))
        t = t.with_columns([
            ((pl.col("cagr") - base["cagr"][0]) * 100).round(2).alias("ΔCAGR_pp"),
            (pl.col("sharpe") - base["sharpe"][0]).round(3).alias("ΔSharpe")])
        print("\n" + "=" * 82)
        print(f"【{tag}】")
        print("=" * 82)
        with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=200, float_precision=4):
            print(t.select(cols + ["ΔCAGR_pp", "ΔSharpe"]))
        t.write_parquet(OUT / f"arms_{tag}.parquet")
    print(f"\n產物 → {OUT}")


if __name__ == "__main__":
    main()
