"""I01 — 出場語義校準:條件單「盤中觸發」vs 回測「收盤觸發」的實際差異。

**為什麼必須先做**:S 的 trail 35% 是**收盤價**規則(close ≤ peak_close×0.65 →
隔日開盤賣)。富邦條件單掛在券商端、以**成交價**每 3 分鐘比價 → 盤中就觸發。
兩者語義不同:盤中版會在「盤中破線但收盤拉回」時被洗出去(whipsaw),也會在
崩跌日更早出場(可能更好)。淨向不明 → **用真實分 K 讓資料說話,不憑感覺上線**。

方法(逐筆真實交易重放,資料 = P0 拉到的 S 實際持倉 1 分 K):
  A 現行(回測語義):peak_close 追蹤;close(d) ≤ peak×(1-trail) → open(d+1) 賣
  B 盤中條件單:每日盤前把停損掛在 peak_close(至 d-1)×(1-trail);
    當日任一分 K 成交價 ≤ 該價 → 當場以該價成交(保守:不假設更好的滑價)
  C 安全網版:同 B 但用較寬的 wide_pct(只接災難性崩跌,平時不干擾 A)
成本一致(賣出手續費+證交稅+滑價),per-trade 比較 + 總體彙總。

Run: uv run --project research python -m research.intraday.exit_calibration
依賴:research/data/intraday/kbars_1m(P0 已備);cache(daily close/open)。
"""
from __future__ import annotations

import time
from datetime import date as Date
from pathlib import Path

import numpy as np
import polars as pl

REPO = Path(__file__).resolve().parents[2]
KB = REPO / "research" / "data" / "intraday" / "kbars_1m"
TRAIL = 0.35            # S 規格
WIDE = 0.50             # 安全網變體
SLIP = 0.001            # apex 慣例單邊滑價
FEE = 0.0028 / 2 + 0.003  # 賣出:手續費(2折)+ 證交稅,與 apex ExecSpec 同尺


def _months(a: Date, b: Date) -> list[str]:
    out, cur = [], Date(a.year, a.month, 1)
    while cur <= b:
        out.append(f"{cur.year:04d}-{cur.month:02d}")
        cur = Date(cur.year + cur.month // 12, cur.month % 12 + 1, 1)
    return out


def _intraday(code: str, a: Date, b: Date) -> pl.DataFrame | None:
    """該檔 [a,b] 的 1 分 K(原始價);缺任一月 → None(不做半套推論)。"""
    frames = []
    for tag in _months(a, b):
        f = KB / tag / f"{code}.parquet"
        if not f.exists():
            if (KB / tag / f"{code}.empty").exists():
                continue
            return None
        frames.append(pl.read_parquet(f, columns=["dt", "close"]))
    if not frames:
        return None
    return (pl.concat(frames)
            .with_columns(pl.col("dt").dt.date().alias("d"))
            .filter(pl.col("d").is_between(a, b))
            .sort("dt"))


def main() -> None:
    t0 = time.time()
    from research.apex import data
    from research.apex.strategy_s import prep, run_s_full

    con = data.connect()
    try:
        panel, feat, elig = prep(con)
        _nav, trades = run_s_full(panel, feat, elig, "2020-03-02")
    finally:
        con.close()
    # 日線(原始價,與分 K 同基準;調整價只用於策略訊號,執行面比較用 raw)
    px = (panel.select(["date", "company_code", "raw_close", "open", "close", "adj_factor"])
          .rename({"company_code": "code"}))
    print(f"S 交易 {len(trades)} 筆(2020-03 起);準備日線+分K {time.time()-t0:.0f}s")

    rows = []
    n_skip = 0
    for tr in trades.iter_rows(named=True):
        code, ed, xd = tr["company_code"], tr["entry_date"], tr["exit_date"]
        if tr["exit_reason"] == "open":          # 尚未平倉,不比較
            continue
        day = (px.filter((pl.col("code") == code)
                         & pl.col("date").is_between(ed, xd))
               .sort("date"))
        if day.height < 2:
            n_skip += 1
            continue
        intr = _intraday(code, ed, xd)
        if intr is None:
            n_skip += 1
            continue
        # 還原因子:分 K 為原始價,日線 close 為調整價 → 用 raw_close 對齊
        d_dates = day["date"].to_list()
        d_raw = day["raw_close"].to_numpy()
        entry_raw = d_raw[0]

        def _sim(mode: str, pct: float) -> tuple[float, str, int]:
            """回 (毛出場價_raw, 出場原因, 持有天數index)。"""
            peak = d_raw[0]
            for k in range(1, len(d_dates)):
                lvl = peak * (1 - pct)
                if mode != "close":              # 盤中:當日分 K 破線即成交
                    bars = intr.filter(pl.col("d") == d_dates[k])["close"].to_numpy()
                    if bars.size and bars.min() <= lvl:
                        return lvl, "intraday_stop", k
                if d_raw[k] <= lvl:              # 收盤破線 → 隔日開盤(用次日 raw 近似)
                    j = min(k + 1, len(d_dates) - 1)
                    return d_raw[j], "close_stop", j
                peak = max(peak, d_raw[k])
            return d_raw[-1], "other_exit", len(d_dates) - 1   # 其他規則出場(time/stale/lts)

        res = {}
        for label, mode, pct in (("A_close", "close", TRAIL),
                                 ("B_intraday", "intra", TRAIL),
                                 ("C_wide", "intra", WIDE)):
            gross, why, k = _sim(mode, pct)
            net = (gross * (1 - SLIP) * (1 - FEE)) / (entry_raw * (1 + SLIP)) - 1
            res[label] = net
            res[label + "_why"] = why
        rows.append({"code": code, "entry": ed, "exit": xd, **res})

    df = pl.DataFrame(rows)
    print(f"可比較 {len(df)} 筆(缺分K/日線略過 {n_skip} 筆);{time.time()-t0:.0f}s\n")
    if df.is_empty():
        print("✗ 無可比較樣本"); return

    print(f"{'變體':<12s}{'平均報酬':>10s}{'中位':>9s}{'勝率':>8s}{'總複利':>10s}{'觸發次數':>10s}")
    for lab in ("A_close", "B_intraday", "C_wide"):
        r = df[lab].to_numpy()
        n_trig = int((df[lab + "_why"] == "intraday_stop").sum()) if lab != "A_close" \
            else int((df["A_close_why"] == "close_stop").sum())
        print(f"{lab:<12s}{r.mean():>10.2%}{np.median(r):>9.2%}"
              f"{(r > 0).mean():>8.1%}{np.prod(1 + r):>10.1f}x{n_trig:>10,}")

    d_ba = df["B_intraday"].to_numpy() - df["A_close"].to_numpy()
    d_ca = df["C_wide"].to_numpy() - df["A_close"].to_numpy()
    se_b = d_ba.std(ddof=1) / np.sqrt(len(d_ba))
    se_c = d_ca.std(ddof=1) / np.sqrt(len(d_ca))
    print(f"\n配對差(每筆交易報酬,B−A):{d_ba.mean():+.2%} ± {se_b:.2%}"
          f"(t={d_ba.mean()/max(se_b,1e-12):+.2f})")
    print(f"配對差(C−A):{d_ca.mean():+.2%} ± {se_c:.2%}"
          f"(t={d_ca.mean()/max(se_c,1e-12):+.2f})")
    # 被盤中洗出去的筆數(B 觸發但 A 沒觸發)
    whip = df.filter((pl.col("B_intraday_why") == "intraday_stop")
                     & (pl.col("A_close_why") != "close_stop"))
    print(f"\n盤中多殺(B 觸發但 A 未觸發):{whip.height} 筆,"
          f"平均差 {(whip['B_intraday'] - whip['A_close']).mean() if whip.height else 0:+.2%}")
    df.write_parquet(REPO / "research" / "apex" / "ledger" / "i01_exit_calibration.parquet")
    print(f"\ntotal {time.time()-t0:.0f}s → ledger/i01_exit_calibration.parquet")


if __name__ == "__main__":
    main()
