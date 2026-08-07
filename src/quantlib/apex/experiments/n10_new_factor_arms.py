"""N10 — N08 過關因子的端到端對決:條件期望顯著 ≠ 組合可用,所以要真的跑一次。

**預註冊**:`ledger/batches.md` §N08 結果段之後(2026-08-07)。

**候選來源**:N08 在 S 的條件池內、用火箭機率當量尺重篩,浮出三個**不在現役六因子內**
的因子(賠率 = 火箭提升 ÷ 崩跌提升,取兩窗較差者):
  hvn_dist       火箭 2.65/2.61、崩跌 0.99/1.12 → 賠率 2.33(攻擊型,最佳)
  range_pos_60   火箭 2.31/2.31、崩跌 0.81/1.06 → 賠率 2.18(攻擊型)
  donchian_60    火箭 1.00/1.02(零鑑別)、崩跌 0.38/0.58 → **防禦型**:不提高勝算,
                 但強力避開崩跌。攻擊項與防禦項該分開評,用同一把尺會兩邊都埋沒。

三者在 F01 用「全市場 + 平均 IC」篩選時都沒能進入正選(hvn 版 Sharpe 低而落選、
range_pos_60 未晉級)。**換條件、換量尺就浮出來**——但那只是必要條件,組合可用性
要靠端到端裁決(F01 老教訓:IC 顯著 ≠ 可交易)。

**權重慣例**:新因子一律 0.5,與現役次要因子(mom_126_5 / rev_seq / accel_rel)同級,
不另調——**先問「加進去有沒有用」,不要一開始就調權重把雜訊擬合成發現**。

**臂**:A0 現行六因子;H/R/D 各加一個;HR 加兩個攻擊型;HRD 三個全加;
SWAP 把 close_pos_20(20 日收盤位置)換成 range_pos_60(60 日區間位置)——兩者同類
不同期,檢查是替代還是互補。

**判準**:CAGR 與 Sharpe 同時不劣於 A0,且勝出方向須過前後窗一致性(切點 2021-07-01)。

依賴 cache: 是。
run: uv run --project . python -m quantlib.apex.experiments.n10_new_factor_arms
"""
from __future__ import annotations

import polars as pl

from quantlib import paths
from quantlib.apex import data, metrics
from quantlib.apex.strategy_s import WREL, prep_cached, run_s_full

FULL = "2015-01-01"
SPLIT = "2021-07-01"
OUT = paths.OUT / "apex" / "n10_new_factors"

ARMS: list[tuple[str, dict]] = [
    ("A0 現行六因子", dict(WREL)),
    ("H  +hvn_dist", {**WREL, "hvn_dist": 0.5}),
    ("R  +range_pos_60", {**WREL, "range_pos_60": 0.5}),
    ("D  +donchian_60(防禦)", {**WREL, "donchian_60": 0.5}),
    ("HR +hvn+range", {**WREL, "hvn_dist": 0.5, "range_pos_60": 0.5}),
    ("HRD +三個全加", {**WREL, "hvn_dist": 0.5, "range_pos_60": 0.5,
                       "donchian_60": 0.5}),
    ("SWAP close_pos_20→range_pos_60",
     {k: v for k, v in WREL.items() if k != "close_pos_20"} | {"range_pos_60": 1.0}),
]


def _row(label: str, nav, trades) -> dict:
    st = metrics.summarize(nav, trades)
    reasons = st.get("exit_reasons", {}) or {}
    n = max(1, sum(reasons.values()))
    return {"arm": label, "cagr": st["cagr"], "sharpe": st["sharpe"],
            "sortino": st["sortino"], "mdd": st["mdd"], "calmar": st["calmar"],
            "n_trades": st.get("n_trades", 0),
            "win_rate": round(st.get("win_rate", 0), 3),
            "signal占比": round(reasons.get("signal", 0) / n, 3)}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = data.connect()
    panel, feat, elig = prep_cached(con)

    rows_full, rows_a, rows_b = [], [], []
    for label, wrel in ARMS:
        nav, tr = run_s_full(panel, feat, elig, FULL, _wrel=wrel)
        rows_full.append(_row(label, nav, tr))
        cut = pl.lit(SPLIT).str.to_date()
        na = nav.filter(pl.col("date") < cut)
        rows_a.append(_row(label, na.with_columns(pl.col("nav") / pl.col("nav").first()),
                           tr.filter(pl.col("entry_date") < cut)))
        nb, trb = run_s_full(panel, feat, elig, SPLIT, _wrel=wrel)
        rows_b.append(_row(label, nb, trb))
        nav.write_parquet(OUT / f"nav_{label.split()[0]}.parquet")
        print(f"  {label} 完成")

    cols = ["arm", "cagr", "sharpe", "sortino", "mdd", "calmar",
            "n_trades", "win_rate", "signal占比"]
    for tag, rows in (("全窗 2015-2026", rows_full), (f"前窗 2015~{SPLIT}", rows_a),
                      (f"後窗 {SPLIT}~2026", rows_b)):
        t = pl.DataFrame(rows)
        base = t.filter(pl.col("arm").str.starts_with("A0"))
        t = t.with_columns([
            ((pl.col("cagr") - base["cagr"][0]) * 100).round(2).alias("ΔCAGR_pp"),
            (pl.col("sharpe") - base["sharpe"][0]).round(3).alias("ΔSharpe"),
        ])
        print("\n" + "=" * 82)
        print(f"【{tag}】")
        print("=" * 82)
        with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=190, float_precision=4):
            print(t.select(cols + ["ΔCAGR_pp", "ΔSharpe"]))
        t.write_parquet(OUT / f"arms_{tag.split()[0]}.parquet")
    print(f"\n產物 → {OUT}")


if __name__ == "__main__":
    main()
