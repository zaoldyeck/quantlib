"""N06 — σ 標準化 trailing stop:把「同一條線代表同樣的極端程度」做成規格。

**預註冊**:`ledger/batches.md` §N06(2026-08-07)。

**假設來源**:N04 量到固定 35% trail 對進場日 20 日日波動不同的股票是 8.1σ(高波)
到 37σ(低波)——**4.5 倍不同的極端程度**。這是「用對數收益率建模而不是價格」在
出場規則上的直接後果:百分比是價格尺度,σ 才是機率尺度。

**關鍵臂 K14**:σ 中位 3.10% × 14 ≈ 43%,與現行 35% 同量級。**平均鬆緊不變、
只把跨標的的不一致修掉**——這才是「標準化本身有沒有價值」的乾淨檢定;K06/K10/K20
是鬆緊敏感度,用來確認結論不是剛好卡在某個水準。

**PIT**:trail 欄 = k × σ20 且 **shift(1)**——引擎讀成交日那一列,而該倉的決策發生在
前一日盤後,故欄值只能由 ≤ 決策日的資料算出(ExitSpec.trailing_stop_col 的約定,
與 ExecSpec.buy_limit_col 同一條,見 engine.py 該欄註解)。

**判準**:CAGR 與 Sharpe 同時不劣於 A0,且勝出方向須過前後窗一致性。

依賴 cache: 是。
run: uv run --project . python -m quantlib.apex.experiments.n06_sigma_trail
"""
from __future__ import annotations

import polars as pl

from quantlib import paths
from quantlib.apex import data, metrics
from quantlib.apex.engine import ExitSpec
from quantlib.apex.strategy_s import prep_cached, run_s_full

C = "company_code"
FULL = "2015-01-01"
SPLIT = "2021-07-01"
TRAIL_LO, TRAIL_HI = 0.10, 0.90    # 物理與防呆界限:>90% 永不觸發、<10% 純換手
OUT = paths.OUT / "apex" / "n06_sigma_trail"


def panel_with_trail(panel: pl.DataFrame, k: float) -> pl.DataFrame:
    """加上 `trail_col` = k × σ20(shift(1),決策日當下可知)。"""
    return (panel.sort([C, "date"])
            .with_columns(
                (pl.col("close").log() - pl.col("close").log().shift(1)).over(C).alias("_r"))
            .with_columns(
                pl.col("_r").rolling_std(20).shift(1).over(C).alias("_sig"))
            .with_columns(
                (pl.col("_sig") * k).clip(TRAIL_LO, TRAIL_HI).alias("trail_col"))
            .drop(["_r", "_sig"]))


ARMS = [("A0 fixed35 固定 35%", None)] + [
    (f"K{k:02d} σ×{k:<2d} 逐倉 σ 標準化", float(k)) for k in (6, 10, 14, 20)
]


def _row(label: str, nav, trades) -> dict:
    st = metrics.summarize(nav, trades)
    reasons = st.get("exit_reasons", {}) or {}
    n = max(1, sum(reasons.values()))
    return {"arm": label, "cagr": st["cagr"], "sharpe": st["sharpe"],
            "sortino": st["sortino"], "mdd": st["mdd"], "calmar": st["calmar"],
            "n_trades": st.get("n_trades", 0),
            "trail占比": round(reasons.get("trail", 0) / n, 3),
            "med_days": st.get("med_days_held")}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = data.connect()
    panel, feat, elig = prep_cached(con)

    rows_full, rows_a, rows_b = [], [], []
    for label, k in ARMS:
        if k is None:
            pnl, xs = panel, ExitSpec(trailing_stop=0.35, time_stop=30, loser_time_stop=15)
        else:
            pnl = panel_with_trail(panel, k)
            xs = ExitSpec(trailing_stop_col="trail_col", time_stop=30, loser_time_stop=15)
            q = pnl["trail_col"].drop_nulls()
            print(f"  {label}: trail 分佈 P10={q.quantile(0.1):.1%} "
                  f"中位={q.median():.1%} P90={q.quantile(0.9):.1%}")
        nav, tr = run_s_full(panel if k is None else pnl, feat, elig, FULL, _exit_spec=xs)
        rows_full.append(_row(label, nav, tr))
        cut = pl.lit(SPLIT).str.to_date()
        na = nav.filter(pl.col("date") < cut)
        rows_a.append(_row(label, na.with_columns(pl.col("nav") / pl.col("nav").first()),
                           tr.filter(pl.col("entry_date") < cut)))
        nb, trb = run_s_full(panel if k is None else pnl, feat, elig, SPLIT, _exit_spec=xs)
        rows_b.append(_row(label, nb, trb))
        nav.write_parquet(OUT / f"nav_{'fixed35' if k is None else f'k{int(k)}'}.parquet")

    cols = ["arm", "cagr", "sharpe", "sortino", "mdd", "calmar",
            "n_trades", "trail占比", "med_days"]
    for tag, rows in (("全窗 2015-2026", rows_full), (f"前窗 2015~{SPLIT}", rows_a),
                      (f"後窗 {SPLIT}~2026", rows_b)):
        t = pl.DataFrame(rows)
        base = t.filter(pl.col("arm").str.starts_with("A0"))
        t = t.with_columns([
            ((pl.col("cagr") - base["cagr"][0]) * 100).round(2).alias("ΔCAGR_pp"),
            (pl.col("sharpe") - base["sharpe"][0]).round(3).alias("ΔSharpe"),
        ])
        print("\n" + "=" * 78)
        print(f"【{tag}】")
        print("=" * 78)
        with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=180, float_precision=4):
            print(t.select(cols + ["ΔCAGR_pp", "ΔSharpe"]))
        t.write_parquet(OUT / f"arms_{tag.split()[0]}.parquet")
    print(f"\n產物 → {OUT}")


if __name__ == "__main__":
    main()
