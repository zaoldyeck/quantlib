"""N03 — 反轉/動量進場擇時 overlay 對 apex_revcycle_S 的增量檢驗。

**接續 N02**:N02 的結論是「均值回歸在台股**是真的**(剔漲跌停日後 Spearman IC
−0.038、t −17.1),但毛邊際最多 8.0 bp/日,而來回成本門檻 35.7 bp/日」。截面統計成立
不等於加到 S 上有用——本檔直接在 S 自己的引擎、自己的池、自己的成本模型上做端到端
對決,不從 N02 的截面結論外推。

**設計**:唯一真源鐵律——不重寫 S,一律走 `strategy_s.run_s_full` 的 `_score_fn` hook。
baseline 的計分公式逐字保留(六因子 rank 幾何加權),overlay 只在**算完 canonical 分數後
過濾候選列**或**乘上一個額外 rank 項**,因此任何 KPI 差異都能歸因到 overlay 本身。

**臂**:
  A0  baseline          canonical S(對照組)
  B1  no_dip            排除進場日當天下跌者(不追跌)
  B2  dip1              只買進場日當天下跌者(貼文邏輯套進 S)
  B3  dip2              只買連跌 ≥2 日者(貼文的「跌久」加強版)
  C1  up1               只買進場日當天上漲者(動量側對照)
  D1  tilt_rev          計分乘上 rank(5 日反轉)^0.5(反轉當第七因子)
  D2  tilt_mom          計分乘上 rank(5 日動量)^0.5(動量側對照)

**PIT**:run length / ret5 由 ≤ 決策日 d 的收盤算出,引擎 ExecSpec 預設 fill_at=
"next_open"(d+1 開盤成交),故無前視。

**判準**:overlay 需在 CAGR 與 Sharpe 同時不劣於 baseline 才有討論價值;
單看 CAGR 上升而 Sharpe/MDD 惡化者視為換來的是槓桿不是 alpha。

依賴 cache: 是。前置:先跑 n02(本檔重用其 panel.parquet 的方向編碼,避免重算)。
run: uv run --project . python -m quantlib.apex.experiments.n03_meanrev_overlay_s
"""
from __future__ import annotations

import polars as pl

from quantlib import paths
from quantlib.apex import data, metrics
from quantlib.apex.strategy_s import WREL, prep_cached, run_s_full

C = "company_code"
START = "2015-01-01"
N02 = paths.OUT / "apex" / "n02_meanrev" / "panel.parquet"


def _canonical_score(df: pl.DataFrame) -> pl.DataFrame:
    """strategy_s.run_s_full 內建計分的逐字複製(六因子 rank 幾何加權)。

    ⚠ 這是 hook 介面的必要複製:_score_fn 的契約是「收過濾後的 df、回傳含 score 的 df」,
    引擎不會再幫忙算。與 strategy_s.py L124-128 同式,任一方改動須同步(本檔只做研究對照,
    不進生產路徑;生產計分的唯一真源仍是 strategy_s)。
    """
    expr = None
    for c_, wt in WREL.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    return df.with_columns(expr.alias("score"))


def _signals() -> pl.DataFrame:
    """(date, company_code, dn_run, up_run, ret5) — N02 方向編碼 + 5 日累計對數報酬。"""
    if not N02.exists():
        raise FileNotFoundError(f"{N02} 不存在——先跑 quantlib.apex.experiments.n02_meanrev_runlength")
    return (
        pl.read_parquet(N02)
        .select(["date", C, "r", "dn_run", "up_run"])
        .sort([C, "date"])
        .with_columns(pl.col("r").rolling_sum(5).over(C).alias("ret5"))
        .select(["date", C, "dn_run", "up_run", "ret5"])
    )


def make_score_fn(sig: pl.DataFrame, mode: str):
    """canonical 分數算完後再套 overlay,確保 baseline 分數逐位不變(乾淨歸因)。"""
    def f(df: pl.DataFrame) -> pl.DataFrame:
        d = _canonical_score(df).join(sig, on=["date", C], how="left")
        dn = pl.col("dn_run").fill_null(0)
        up = pl.col("up_run").fill_null(0)
        if mode == "baseline":
            return d
        if mode == "no_dip":
            return d.filter(dn == 0)
        if mode == "dip1":
            return d.filter(dn >= 1)
        if mode == "dip2":
            return d.filter(dn >= 2)
        if mode == "up1":
            return d.filter(up >= 1)
        if mode in ("tilt_rev", "tilt_mom"):
            d = d.drop_nulls("ret5")
            s = -pl.col("ret5") if mode == "tilt_rev" else pl.col("ret5")
            tilt = ((s.rank() / pl.len()).over("date")) ** 0.5
            return d.with_columns((pl.col("score") * tilt).alias("score"))
        raise ValueError(mode)
    return f


ARMS = [
    ("A0 baseline  canonical S", "baseline"),
    ("B1 no_dip    當日下跌不買", "no_dip"),
    ("B2 dip1      只買當日下跌", "dip1"),
    ("B3 dip2      只買連跌≥2日", "dip2"),
    ("C1 up1       只買當日上漲", "up1"),
    ("D1 tilt_rev  計分加反轉項", "tilt_rev"),
    ("D2 tilt_mom  計分加動量項", "tilt_mom"),
]


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    sig = _signals()
    print(f"訊號 panel {sig.height:,} 列 | S feat {feat.height:,} 列 | start={START}\n")

    rows = []
    for label, mode in ARMS:
        nav, trades = run_s_full(panel, feat, elig, START, _score_fn=make_score_fn(sig, mode))
        st = metrics.summarize(nav, trades)
        rows.append({"arm": label, **{k: st[k] for k in
                                      ("cagr", "sharpe", "mdd", "n_trades", "win_rate")
                                      if k in st}})
        print(f"{label:28s} CAGR {st.get('cagr', float('nan')):7.2%}  "
              f"Sharpe {st.get('sharpe', float('nan')):5.2f}  "
              f"MDD {st.get('mdd', float('nan')):7.2%}  "
              f"trades {st.get('n_trades', 0):5d}")
        nav.write_parquet(paths.OUT / "apex" / "n02_meanrev" / f"nav_{mode}.parquet")

    tab = pl.DataFrame(rows)
    out = paths.OUT / "apex" / "n02_meanrev" / "n03_overlay_arms.parquet"
    tab.write_parquet(out)
    base = tab.filter(pl.col("arm").str.starts_with("A0"))
    print("\n相對 baseline 的增量(pp / Sharpe 絕對差):")
    for r in tab.iter_rows(named=True):
        if r["arm"].startswith("A0"):
            continue
        print(f"  {r['arm']:28s} ΔCAGR {(r['cagr'] - base['cagr'][0]) * 100:+7.2f}pp   "
              f"ΔSharpe {r['sharpe'] - base['sharpe'][0]:+5.2f}")
    print(f"\n產物 → {out}")


if __name__ == "__main__":
    main()
