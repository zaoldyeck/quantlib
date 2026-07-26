"""用真實 1 分 K 驗證 S 的兩個執行假設:①「隔日開盤買」是最好的時點嗎?② 滑價 0.1% 合理嗎?

## 為什麼現在才能做(新資訊集第一個應用)
S 的回測假設 `fill_at="next_open"` + `slippage=0.001`,但日 K 資料裡**沒有盤中價格**,
這兩個假設從來只能用「合理」二字帶過。2026-06/07 的 1 分 K 已回補到**全市場**
(2,324 / 2,548 檔),第一次可以用真實盤中價格檢驗。

## 量什麼
對 S 在樣本期的每日候選(top-5),在**進場日**(決策日的次一交易日)讀 1 分 K:
1. **時點成本曲線**:相對 09:01 開盤價,在 09:05 / 09:15 / 09:30 / 10:00 / 11:00 / 13:30 買
   分別貴/便宜多少(正 = 更貴)。若開盤系統性偏貴 → 晚點買是免費的 alpha;
   若開盤後續漲 → 現行搶開盤是對的。
2. **開盤衝擊代理**:開盤後前 5 分鐘的價格區間(high−low)/開盤價 = 開盤瞬間的不確定性,
   與 0.1% 滑價假設對比。
3. **實際可成交性**:進場日開盤那根 1 分 K 的成交金額,對比 S 在各資金規模下的單筆下單金額
   (容量分析的實測版:開盤第一分鐘吃得下多少)。

## 誠實界線
樣本僅 2 個月(2026-06~07,約 37 個交易日)——足以量測**執行層的系統性偏差**(每天都有
觀察、與市場方向無關),但**不足以做策略層結論**(報酬層需要跨 regime 的長樣本)。

Run: uv run --project . python -m quantlib.intraday.entry_timing_1m
依賴:data/intraday/kbars_1m(2026-06+ 全市場)+ cache。
"""
from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl

from quantlib import paths
from quantlib.apex import data
from quantlib.apex.assemble import entries_and_flags
from quantlib.apex.strategy_s import C, DS, WREL, prep_cached

KB = paths.RAW_INTRADAY
MONTHS = ("2026-06", "2026-07")
MARKS = (5, 15, 30, 60, 120, 999)   # 距開盤分鐘;999 = 收盤


def _candidates(feat: pl.DataFrame, elig: pl.DataFrame) -> pl.DataFrame:
    """S 的每日 top-5 候選(與 run_s_full 同法)。"""
    pool = feat.filter(pl.col("rev_fresh_days") <= 7)
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]), on=["date", C], how="semi")
          .drop_nulls(subset=list(WREL))
          .filter(pl.all_horizontal([pl.col(c).is_finite() for c in WREL]))
          .filter(pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")))
    expr = None
    for c_, wt in WREL.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = (df.with_columns(expr.alias("score")).select(["date", C, "score"])
          .filter(pl.col("date") >= pl.lit(DS).str.to_date()))
    ent, _ = entries_and_flags(sc, 5, 10**9)
    return ent


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    cands = _candidates(feat, elig)
    days = sorted(panel["date"].unique().to_list())
    nxt = {d: days[i + 1] for i, d in enumerate(days[:-1])}

    lo, hi = dt.date(2026, 6, 1), dt.date(2026, 7, 24)
    sel = cands.filter((pl.col("date") >= lo) & (pl.col("date") <= hi))
    jobs = []
    for r in sel.to_dicts():
        nd = nxt.get(r["date"])
        if nd and lo <= nd <= hi:
            jobs.append((nd, r[C]))
    jobs = sorted(set(jobs))
    print(f"[1m] 樣本:{len(jobs)} 個 (進場日 × 標的),{lo}~{hi}", flush=True)

    rows = []
    for d, code in jobs:
        tag = f"{d.year:04d}-{d.month:02d}"
        f = KB / tag / f"{code}.parquet"
        if not f.exists():
            continue
        k = (pl.read_parquet(f)
             .filter(pl.col("dt").dt.date() == d)
             .sort("dt"))
        if k.height < 30:
            continue
        o = float(k["open"][0])
        if o <= 0:
            continue
        t0 = k["dt"][0]
        rec = {"date": d, C: code, "open": o,
               "first5_range": float((k["high"][:5].max() - k["low"][:5].min()) / o),
               "open_amt": float(k["amount"][0])}
        for m in MARKS:
            if m == 999:
                px = float(k["close"][-1])
            else:
                w = k.filter(pl.col("dt") <= t0 + dt.timedelta(minutes=m))
                px = float(w["close"][-1]) if w.height else float("nan")
            rec[f"d{m}"] = px / o - 1
        rows.append(rec)

    if not rows:
        print("  無可用 1 分 K 樣本(資料尚未回補?)")
        return
    df = pl.DataFrame(rows)
    print(f"  成功配對 {df.height} 筆\n")

    print("=== ① 時點成本曲線(相對 09:01 開盤價;正 = 買得更貴)===")
    print(f"  {'時點':>10}{'中位':>10}{'平均':>10}{'P25':>9}{'P75':>9}{'較貴比例':>10}")
    for m in MARKS:
        s = df[f"d{m}"].drop_nulls()
        lab = "收盤" if m == 999 else f"+{m} 分"
        print(f"  {lab:>10}{s.median():>+9.3%}{s.mean():>+9.3%}{s.quantile(0.25):>+8.2%}"
              f"{s.quantile(0.75):>+8.2%}{(s > 0).mean():>9.0%}")

    print("\n=== ② 開盤瞬間不確定性 vs 滑價假設 0.1% ===")
    r5 = df["first5_range"].drop_nulls()
    print(f"  開盤前 5 分鐘高低幅:中位 {r5.median():.3%}  平均 {r5.mean():.3%}  P90 {r5.quantile(0.9):.3%}")
    print(f"  → 現行假設單邊 0.10%;若半幅({r5.median()/2:.3%})可視為期望偏離,假設"
          f"{'偏保守(高估成本)' if r5.median()/2 < 0.001 else '可能偏樂觀(低估成本)'}")

    print("\n=== ③ 開盤第一分鐘可吃下多少(容量的實測版)===")
    amt = df["open_amt"].drop_nulls()
    print(f"  開盤首分成交額:中位 {amt.median()/1e6:.1f}M  P25 {amt.quantile(0.25)/1e6:.1f}M"
          f"  P75 {amt.quantile(0.75)/1e6:.1f}M")
    for cap in (10_000_000, 30_000_000, 100_000_000):
        q = cap / 5
        print(f"  資金 {cap/1e6:>4.0f}M(單筆 {q/1e6:.0f}M):首分吃得下的比例 "
              f"{(amt >= q * 5).mean():>5.0%}(以「單筆 ≤ 首分成交額 1/5」為可成交門檻)")
    print("\n  註:樣本 2 個月,足以量執行層系統性偏差,不足以做策略層結論。")


if __name__ == "__main__":
    main()
