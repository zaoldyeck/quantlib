"""限價單掛價研究:盤前該掛什麼價?——成交率 × 成交價 × 未成交機會成本的期望值權衡。

## 使用者的問題(2026-07-26)
「與其研究哪個時點下單,我倒希望可以掛限價單,幫助系統決定能掛什麼價格。」

## 關鍵設計:限價必須掛在**前一日收盤**上(不是開盤價)
S 的計畫在**盤前**產生(premarket 01:00 / 07:20),下單當下**開盤價還不存在**。
用開盤價當基準算限價 = 前視偏差。故本研究一律以「決策日收盤價 × (1+x)」為掛價,
x 掃描 −3% ~ +3%(含市價單對照)。

## 成交判定(用真實 1 分 K,保守假設)
- 當日 **最低價 ≤ 限價** → 視為成交;成交價 = min(限價, 當日開盤)
  (開盤即低於限價時以開盤成交——這是實務上限價單的行為,對我們有利但真實)
- 否則未成交 → 該筆機會作廢(資金留現金,報酬 0)

## 期望值(這才是判準,不是成交價本身)
    E[x] = P(成交|x) × E[買在 fill_price 的後續報酬 | 成交]
掛越低 → 成交價越好但成交率越低、且**會系統性錯過最強的股票**(開盤直接跳空的那些
正是動能最強者)。故必須看期望值,不能只看「省了多少價差」。
對照:市價單(以開盤價 100% 成交)= S 現行做法。

Run: uv run --project . python -m quantlib.intraday.limit_order_study
依賴:data/intraday/kbars_1m(2026-06+ 全市場)+ cache。樣本 2 個月,結論為初步。
"""
from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl

from quantlib import paths
from quantlib.apex import data, factors
from quantlib.apex.strategy_s import C, prep_cached
from quantlib.intraday.entry_timing_1m import _candidates

KB = paths.RAW_INTRADAY
OFFSETS = (-0.03, -0.02, -0.015, -0.01, -0.005, 0.0, 0.005, 0.01, 0.02, 0.03)
LO, HI = dt.date(2026, 6, 1), dt.date(2026, 7, 24)


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    fwd = factors.forward_returns(panel)
    cands = _candidates(feat, elig)
    days = sorted(panel["date"].unique().to_list())
    nxt = {d: days[i + 1] for i, d in enumerate(days[:-1])}
    px = panel.select([C, "date", "close"])

    # 決策日收盤(掛價基準)+ 進場日
    rows = []
    for r in cands.filter((pl.col("date") >= LO) & (pl.col("date") <= HI)).to_dicts():
        d0, code = r["date"], r[C]
        d1 = nxt.get(d0)
        if not d1 or not (LO <= d1 <= HI):
            continue
        pc = px.filter((pl.col(C) == code) & (pl.col("date") == d0))
        if pc.height == 0:
            continue
        rows.append({"d0": d0, "d1": d1, C: code, "prev_close": float(pc["close"][0])})
    rows = list({(r["d1"], r[C]): r for r in rows}.values())
    print(f"[limit] 樣本 {len(rows)} 個 (進場日 × 標的),{LO}~{HI}", flush=True)

    recs = []
    for r in rows:
        d1, code = r["d1"], r[C]
        f = KB / f"{d1.year:04d}-{d1.month:02d}" / f"{code}.parquet"
        if not f.exists():
            continue
        k = pl.read_parquet(f).filter(pl.col("dt").dt.date() == d1).sort("dt")
        if k.height < 30:
            continue
        o, lo_px = float(k["open"][0]), float(k["low"].min())
        fw = fwd.filter((pl.col(C) == code) & (pl.col("date") == d1))
        if fw.height == 0:
            continue
        f5 = fw["fwd_5"][0]
        f21 = fw["fwd_21"][0]
        recs.append({C: code, "d1": d1, "prev_close": r["prev_close"], "open": o,
                     "low": lo_px, "f5": f5, "f21": f21})
    if not recs:
        print("  無 1 分 K 樣本")
        return
    df = pl.DataFrame(recs).drop_nulls(subset=["f5"])
    n = df.height
    print(f"  可用 {n} 筆(含後續報酬)\n")

    pc = df["prev_close"].to_numpy()
    op = df["open"].to_numpy()
    lw = df["low"].to_numpy()
    f5 = df["f5"].to_numpy()
    f21 = df["f21"].to_numpy()
    # 市價單基準:開盤成交,後續報酬 = f5/f21(以進場日收盤為錨的前瞻報酬需換算成
    # 「以 fill 價買入」的報酬:R = (1+f) × close_d1 / fill − 1,close_d1 = open×(1+r_open_to_close)
    # 簡化並保守:用 fwd 相對進場日收盤,fill 價差直接加成(見下註)。
    cl = None  # 進場日收盤
    cls = []
    for r in df.to_dicts():
        c_ = px.filter((pl.col(C) == r[C]) & (pl.col("date") == r["d1"]))
        cls.append(float(c_["close"][0]) if c_.height else np.nan)
    cl = np.array(cls)

    def ret_from(fill: np.ndarray, f: np.ndarray) -> np.ndarray:
        """以 fill 價買入、持有到 (進場日收盤 × (1+f)) 的報酬。"""
        return (1 + f) * cl / fill - 1

    print("=== 掛價 vs 成交率 vs 期望報酬(基準 = 決策日收盤;市價單為對照)===")
    print(f"  {'掛價':>10}{'成交率':>9}{'成交價/開盤':>12}{'E[5日報酬]':>12}"
          f"{'E[21日報酬]':>13}{'成交者5日':>11}")
    base5 = np.nanmean(ret_from(op, f5))
    base21 = np.nanmean(ret_from(op, f21))
    print(f"  {'市價(開盤)':>10}{'100%':>9}{'1.000':>12}{base5:>+11.2%}{base21:>+12.2%}"
          f"{base5:>+10.2%}")
    for x in OFFSETS:
        limit = pc * (1 + x)
        filled = lw <= limit                      # 當日最低價觸及限價 → 成交
        fill_px = np.where(op <= limit, op, limit)  # 開盤即低於限價 → 開盤成交
        r5 = ret_from(fill_px, f5)
        r21 = ret_from(fill_px, f21)
        e5 = np.nanmean(np.where(filled, r5, 0.0))    # 未成交 = 0 報酬(資金閒置)
        e21 = np.nanmean(np.where(filled, r21, 0.0))
        c5 = np.nanmean(r5[filled]) if filled.any() else np.nan
        ratio = np.nanmean(fill_px[filled] / op[filled]) if filled.any() else np.nan
        print(f"  {x:>+9.1%}{filled.mean():>9.0%}{ratio:>12.3f}{e5:>+11.2%}{e21:>+12.2%}"
              f"{c5:>+10.2%}")

    print("\n  判讀:E[報酬] 欄已把「沒成交=賺 0」的機會成本算進去。")
    print("  若某個掛價的 E[報酬] > 市價單 → 值得改掛限價;若市價單最高 → 不要為省價差而錯過。")
    print(f"  ⚠ 樣本 {n} 筆(2 個月),結論為初步;資料每日累積,3-6 個月後重驗。")


if __name__ == "__main__":
    main()
