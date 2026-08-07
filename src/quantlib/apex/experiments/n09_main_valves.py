"""N09 — S 的三大主控閥:訊號死亡門檻 / 進場新鮮度 / 輸家時間止損。

**為什麼是這三個**:N07 出場普查顯示 S 的出場結構完全不是先前以為的樣子——
`signal`(營收訊號過期,`_stale_days=26`)佔 **69.9%** 的出場、貢獻 **+134.5%** 的報酬;
`time_loser`(輸家滿 15 日)佔 28.6%、貢獻 −35.9%;`time_stop 30` 只有 5 筆(0.7%)、
`trail` 只有 4 筆(0.6%)。**先前兩輪(N05 資金分配、N06 σ 標準化 trail)動的都是
只決定 1.3% 出場的旋鈕。** 真正的主控閥是 stale_days,而它從未被尋優過——
ledger P02 只做過 ±擾動的穩健性檢查(fresh 4/6、stale 18/26),那是「會不會一碰就倒」,
不是「最適值在哪」。

**條件機率的接續**:N04 地圖顯示水下部位的期望谷底在持有 11-20 日,而 21 日之後
**期望回正**(IS 92.0/26.0/59.6 bp、OOS 14.5/90.9/241.6 bp)。loser_time_stop=15
砍在谷底入口——砍在最差之前是對的,但也可能砍掉了後面會回來的那些。這是可直接
檢定的假設,不是猜測。

**臂**:三軸各自單獨掃(不做全網格,避免多重比較把雜訊掃成發現):
  S 軸 stale_days ∈ {18, 22, 26(現行), 30, 34, 40}
  F 軸 fresh_days ∈ {3, 5, 7(現行), 9, 12}
  L 軸 loser_time_stop ∈ {10, 15(現行), 21, 26, None}

**判準**:CAGR 與 Sharpe 同時不劣於現行,且勝出方向須過前後窗一致性(切點 2021-07-01)。
單窗贏、另一窗輸者一律判為雜訊。

依賴 cache: 是。
run: uv run --project . python -m quantlib.apex.experiments.n09_main_valves
"""
from __future__ import annotations

import polars as pl

from quantlib import paths
from quantlib.apex import data, metrics
from quantlib.apex.engine import ExitSpec
from quantlib.apex.strategy_s import prep_cached, run_s_full

FULL = "2015-01-01"
SPLIT = "2021-07-01"
BASE_EXIT = dict(trailing_stop=0.35, time_stop=30, loser_time_stop=15)
OUT = paths.OUT / "apex" / "n09_valves"


def _row(label: str, nav, trades) -> dict:
    st = metrics.summarize(nav, trades)
    reasons = st.get("exit_reasons", {}) or {}
    n = max(1, sum(reasons.values()))
    return {"arm": label, "cagr": st["cagr"], "sharpe": st["sharpe"],
            "sortino": st["sortino"], "mdd": st["mdd"], "calmar": st["calmar"],
            "n_trades": st.get("n_trades", 0),
            "signal占比": round(reasons.get("signal", 0) / n, 3),
            "loser占比": round(reasons.get("time_loser", 0) / n, 3),
            "med_days": st.get("med_days_held")}


def run_axis(panel, feat, elig, name: str, arms: list[tuple[str, dict]]) -> None:
    rows_full, rows_a, rows_b = [], [], []
    for label, kw in arms:
        run_kw = {k: v for k, v in kw.items() if k in ("_stale_days", "_fresh_days")}
        ex = {k: v for k, v in kw.items() if k not in run_kw}
        xs = ExitSpec(**{**BASE_EXIT, **ex})
        nav, tr = run_s_full(panel, feat, elig, FULL, _exit_spec=xs, **run_kw)
        rows_full.append(_row(label, nav, tr))
        cut = pl.lit(SPLIT).str.to_date()
        na = nav.filter(pl.col("date") < cut)
        rows_a.append(_row(label, na.with_columns(pl.col("nav") / pl.col("nav").first()),
                           tr.filter(pl.col("entry_date") < cut)))
        nb, trb = run_s_full(panel, feat, elig, SPLIT, _exit_spec=xs, **run_kw)
        rows_b.append(_row(label, nb, trb))
        print(f"  {label} 完成")

    cols = ["arm", "cagr", "sharpe", "sortino", "mdd", "calmar", "n_trades",
            "signal占比", "loser占比", "med_days"]
    for tag, rows in (("全窗", rows_full), ("前窗", rows_a), ("後窗", rows_b)):
        t = pl.DataFrame(rows)
        base = t.filter(pl.col("arm").str.contains("現行"))
        t = t.with_columns([
            ((pl.col("cagr") - base["cagr"][0]) * 100).round(2).alias("ΔCAGR_pp"),
            (pl.col("sharpe") - base["sharpe"][0]).round(3).alias("ΔSharpe"),
        ])
        print("\n" + "=" * 82)
        print(f"【{name} 軸 / {tag}】")
        print("=" * 82)
        with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=190, float_precision=4):
            print(t.select(cols + ["ΔCAGR_pp", "ΔSharpe"]))
        t.write_parquet(OUT / f"{name}_{tag}.parquet")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = data.connect()
    panel, feat, elig = prep_cached(con)

    print("\n### S 軸:訊號死亡門檻 stale_days(決定 70% 的出場)")
    run_axis(panel, feat, elig, "S_stale", [
        (f"stale {v}{'(現行)' if v == 26 else ''}", {"_stale_days": v})
        for v in (18, 22, 26, 30, 34, 40)])

    print("\n### F 軸:進場新鮮度 fresh_days(決定池的入口)")
    run_axis(panel, feat, elig, "F_fresh", [
        (f"fresh {v}{'(現行)' if v == 7 else ''}", {"_fresh_days": v})
        for v in (3, 5, 7, 9, 12)])

    print("\n### L 軸:輸家時間止損 loser_time_stop(決定 29% 的出場)")
    run_axis(panel, feat, elig, "L_loser", [
        (f"loser {v if v else 'None'}{'(現行)' if v == 15 else ''}",
         {"loser_time_stop": v}) for v in (10, 15, 21, 26, None)])

    print(f"\n產物 → {OUT}")


if __name__ == "__main__":
    main()
