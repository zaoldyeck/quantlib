"""滾動「重新研發」對決:每年用近 N 年資料**重選因子**組策略 vs 固定 S 六因子。

## 使用者的問題(2026-07-24 澄清)
「不是用近期資料選參數,而是**用近期資料研發適合近期的策略,因子也會不同**。」
——這比參數 refit 更根本:如果 regime 真的會換,那**有效的因子本身**應該會換,
每年重選因子的策略應該勝過十年不變的六因子。本檔直接測。

## 設計(嚴格 walk-forward,PIT)
因子池 = 16 個(assemble 全特徵,排除池閘用的 rev_fresh_days):
  價格/動能:high_52w, mom_126_5, close_pos_20, donchian_60, range_pos_60, updays_20,
             hvn_dist, fvg_20, lowvol_60, frn_60
  營收/基本面:rev_yoy, rev_yoy_accel, rev_seq, accel_rel, cfo_ni_ratio_ttm, dy
對每個 OOS 年 y(2017-2026)× 回看窗 N ∈ {1,2,3,5}:
  1. train = [y−N, y):在 **S 的同一個池內**(營收新鮮 ≤7 + eligible + cfo 閘)算每個因子的
     h21 截面 IC(每日 rank 相關的均值);**只用 train 期資料**(無前視)。
  2. 選 IC 最高的 K=6 個因子(與 S 同數量),**等權**幾何 rank 合成分數。
  3. 用該分數跑 y 年回測(其餘結構全部 = canonical:5 slots/trail .35/time 30/15/stale 26),
     取該年報酬 = 真樣本外。
對照 A:S 固定六因子(canonical 權重 1/1/1/.5/.5/.5)。
對照 B:S 固定六因子**等權**(與重選版同計分形態的公平對照)。
另報:每年選出的因子集(看穩定性——若年年不同 = 在追雜訊)。

Run: uv run --project . python -m quantlib.strat_lab.rolling_refactor
依賴 cache:是(乾淨世代)。
"""
from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl

from quantlib.apex import data, factors
from quantlib.apex.strategy_s import C, WREL, prep_cached, run_s_full

POOL = ["high_52w", "mom_126_5", "close_pos_20", "donchian_60", "range_pos_60",
        "updays_20", "hvn_dist", "fvg_20", "lowvol_60", "frn_60",
        "rev_yoy", "rev_yoy_accel", "rev_seq", "accel_rel", "cfo_ni_ratio_ttm", "dy"]
K = 6
OOS_YEARS = tuple(range(2017, 2027))
WINDOWS = (1, 2, 3, 5)


def _s_pool(feat: pl.DataFrame, elig: pl.DataFrame) -> pl.DataFrame:
    """S 的選股池(與 run_s_full 內部同法):營收新鮮 ≤7 + eligible + cfo_ni 中位數以上。"""
    return (feat.filter(pl.col("rev_fresh_days") <= 7)
            .join(elig.filter(pl.col("eligible")).select(["date", C]), on=["date", C], how="semi")
            .filter(pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")))


def _ic(pool: pl.DataFrame, fwd: pl.DataFrame, col: str, lo: dt.date, hi: dt.date,
        metric: str = "ic") -> float:
    """train 期的因子品質分數。

    metric="ic"     :h21 截面 IC(每日 rank 相關均值)——**注意:IC 高 ≠ 可交易**
                     (candidate_edges 實測:dy/價值類 IC 顯著但 decile spread ≈ 0,
                      極端分位不分離 → 選不出可交易的股票)。
    metric="spread" :**top 五分位 − bottom 五分位的 fwd21 平均報酬差**(可交易性直接量測)
                     ——對「用近期資料重新研發」的想法更公平的選因子標準。
    """
    d = (pool.select(["date", C, col]).drop_nulls()
         .filter(pl.col(col).is_finite())
         .filter((pl.col("date") >= lo) & (pl.col("date") < hi))
         .join(fwd.select(["date", C, "fwd_21"]), on=["date", C], how="inner")
         .drop_nulls(subset=["fwd_21"]))
    if d.height < 500:
        return float("nan")
    if metric == "spread":
        q = (d.with_columns(((pl.col(col).rank("ordinal").over("date") * 5)
                             // (pl.len().over("date") + 1)).alias("_q"))
             .group_by("_q").agg(pl.col("fwd_21").mean().alias("m")).sort("_q"))
        if q.height < 5:
            return float("nan")
        return float(q["m"][-1] - q["m"][0])
    daily = (d.with_columns([pl.col(col).rank().over("date").alias("_a"),
                             pl.col("fwd_21").rank().over("date").alias("_b"),
                             pl.len().over("date").alias("_n")])
             .filter(pl.col("_n") >= 30)
             .group_by("date").agg(pl.corr("_a", "_b").alias("ic")).drop_nulls())
    return float(daily["ic"].mean()) if daily.height >= 20 else float("nan")


def _score_fn_for(cols: list[str], weights: dict | None = None):
    """回傳 _score_fn:等權(或指定權重)幾何 rank 合成。"""
    def fn(df: pl.DataFrame) -> pl.DataFrame:
        expr = None
        for c_ in cols:
            w = (weights or {}).get(c_, 1.0)
            term = ((pl.col(c_).rank() / pl.len()).over("date")) ** w
            expr = term if expr is None else expr * term
        return df.with_columns(expr.alias("score"))
    return fn


def _year_ret(panel, feat, elig, y: int, cols: list[str], weights=None) -> float | None:
    """該年報酬(start = 年初;取當年段)。因子集不全在 feat 時回 None。"""
    miss = [c for c in cols if c not in feat.columns]
    if miss:
        return None
    nav, _ = run_s_full(panel, feat, elig, f"{y}-01-01",
                        _score_fn=_score_fn_for(cols, weights),
                        _wrel={c: 1.0 for c in cols})   # _wrel 只用於 drop_nulls/finite 過濾集合
    seg = nav.sort("date").filter(pl.col("date") < dt.date(y + 1, 1, 1))
    if seg.height < 40:
        return None
    return float(seg["nav"][-1] / seg["nav"][0] - 1)


def main(metric: str = "ic") -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    fwd = factors.forward_returns(panel)
    pool = _s_pool(feat, elig)
    s_cols = list(WREL)

    print(f"[refactor] 因子池 {len(POOL)} 個,每年選 top-{K}(選因子標準={metric});"
          f"OOS {OOS_YEARS[0]}-{OOS_YEARS[-1]}", flush=True)
    rows, picks = [], {}
    for y in OOS_YEARS:
        base_a = _year_ret(panel, feat, elig, y, s_cols, WREL)
        base_b = _year_ret(panel, feat, elig, y, s_cols)          # 等權對照
        if base_a is None:
            continue
        for N in WINDOWS:
            lo, hi = dt.date(max(2015, y - N), 1, 1), dt.date(y, 1, 1)
            ics = {c: _ic(pool, fwd, c, lo, hi, metric) for c in POOL}
            top = [c for c, v in sorted(ics.items(), key=lambda kv: -(kv[1] if kv[1] == kv[1] else -9))
                   if v == v][:K]
            if len(top) < K:
                continue
            r = _year_ret(panel, feat, elig, y, top)
            if r is None:
                continue
            picks[(y, N)] = top
            rows.append({"year": y, "window": N, "refit_ret": r,
                         "s_ret": base_a, "s_eq_ret": base_b if base_b is not None else float("nan")})
        print(f"  {y} ✓", flush=True)

    df = pl.DataFrame(rows)
    print("\n=== 每年重選因子 vs 固定 S 六因子(真樣本外)===")
    print(f"  {'回看窗':>8}{'pooled 年化':>13}{'S(canonical)':>15}{'S(等權)':>11}{'勝年數':>9}")
    for N in WINDOWS:
        g = df.filter(pl.col("window") == N)
        if g.is_empty():
            continue
        gm = float(np.prod([1 + r for r in g["refit_ret"]]) ** (1 / g.height) - 1)
        sa = float(np.prod([1 + r for r in g["s_ret"]]) ** (1 / g.height) - 1)
        se_vals = [r for r in g["s_eq_ret"] if r == r]
        se = float(np.prod([1 + r for r in se_vals]) ** (1 / len(se_vals)) - 1) if se_vals else float("nan")
        wins = int((g["refit_ret"] > g["s_ret"]).sum())
        print(f"  {N:>6} 年{gm:>+12.1%}{sa:>+14.1%}{se:>+10.1%}{wins:>6}/{g.height}")

    print("\n=== 逐年:重選因子的報酬(各窗)vs S ===")
    print(f"  {'年':>6}" + "".join(f"{f'{N}y':>9}" for N in WINDOWS) + f"{'S':>9}{'S等權':>9}")
    for y in OOS_YEARS:
        g = df.filter(pl.col("year") == y)
        if g.is_empty():
            continue
        line = f"  {y:>6}"
        for N in WINDOWS:
            r = g.filter(pl.col("window") == N)
            line += f"{r['refit_ret'][0]:>+8.0%} " if r.height else f"{'--':>9}"
        line += f"{g['s_ret'][0]:>+8.0%} {g['s_eq_ret'][0]:>+8.0%}"
        print(line)

    print("\n=== 每年選出的因子(窗=3 年;看穩定性)===")
    for y in OOS_YEARS:
        t = picks.get((y, 3))
        if t:
            print(f"  {y}: {', '.join(t)}")
    print(f"\n  S 的固定六因子:{', '.join(s_cols)}")
    print("  判讀:若重選版 pooled 年化 > S 且勝年數過半 → 『用近期資料重新研發』成立;"
          "若因子集年年大變且績效不勝 → 在追雜訊。")


if __name__ == "__main__":
    import sys
    main("spread" if "--spread" in sys.argv else "ic")
