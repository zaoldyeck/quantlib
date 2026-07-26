"""護欄(collar)會不會把「撈價 + 收盤保底」變成「根本沒成交」?——量它。

## 背景
使用者要的預設是「盤中買最低、賣最高」,現行 profile 正是如此:結構錨定、盤中永不
因時間跨價、收盤未竟由盤後定價(14:30 撮合 = 收盤價)收尾。

但每個 profile 都有一道**絕對護欄**:買方上限 = 到達價 ×(1+cap)、賣方下限 =
到達價 ×(1−cap)。而護欄是鐵律,**收盤保底也受它約束**(policy.py:「收盤價破欄不掛」)。
於是「保底」只在**收盤價還在護欄內**時才生效;一旦當天走勢跑出護欄,就退化成
「今天沒成交」。

而 `limit_order_verdict.md` 的 12 年全史結論正是:**致命的從來不是掛價,是沒成交**
(買單沒保底時最深 −52%/年;賣單沒賣掉時 MDD 由 −34% 惡化到 −40%)。所以護欄多寬,
直接決定現行預設落在「§3 有保底(無害)」還是「§1 沒保底(災難)」。

## 量法(用 S 的真實交易日,不是全市場)
到達價 ≈ 當日開盤(執行器 09:00 啟動)。收盤保底被護欄擋掉的條件:
  買:收盤 > 開盤 × (1+cap)      賣:收盤 < 開盤 × (1−cap)
對每個 cap 算出「保底失效」的比例。這是**下界**——盤中若曾觸及結構錨就先成交了,
故實際沒成交率更低;但它精準衡量「護欄把保底關掉」的頻率,而那正是要決策的事。

Run: uv run --project . python -m quantlib.strat_lab.collar_fill_risk
依賴 cache: 是(prep_cached)。
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.strategy_s import C, DS, prep_cached, run_s_full
from quantlib.trading.execution.policy import PROFILES

CAPS = (0.005, 0.008, 0.01, 0.015, 0.02, 0.03, 0.05)


def _day_stats(panel: pl.DataFrame, days: pl.DataFrame) -> np.ndarray:
    """回傳 close/open − 1(每個 (code, date))。"""
    j = (days.join(panel.select([C, "date", "open", "close"]),
                   on=[C, "date"], how="inner")
         .filter((pl.col("open") > 0) & (pl.col("close") > 0)))
    return (j["close"] / j["open"] - 1.0).to_numpy()


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    _, trades = run_s_full(panel, feat, elig, DS)
    done = trades.filter(pl.col("exit_reason") != "open")

    buy_r = _day_stats(panel, trades.select([C, pl.col("entry_date").alias("date")]).unique())
    sell_r = _day_stats(panel, done.select([C, pl.col("exit_date").alias("date")]).unique())
    print(f"S 真實交易日:進場 {len(buy_r)} 天、出場 {len(sell_r)} 天(到達價以當日開盤代理)\n")

    print("=== 護欄把「收盤保底」關掉的比例(數字越高,越接近『沒成交』那條災難路徑)===")
    print(f"  {'護欄 cap':>10}{'買:保底失效':>14}{'賣:保底失效':>14}   使用該 cap 的 profile")
    by_cap: dict[float, list[str]] = {}
    for p in PROFILES.values():
        by_cap.setdefault(round(p.cap_pct, 4), []).append(p.name)
    for cap in CAPS:
        buy_fail = float((buy_r > cap).mean())
        sell_fail = float((sell_r < -cap).mean())
        who = ", ".join(by_cap.get(round(cap, 4), []))
        print(f"  {cap:>10.1%}{buy_fail:>13.1%}{sell_fail:>13.1%}   {who}")

    print("\n=== 白話結論 ===")
    c05 = float((buy_r > 0.005).mean()), float((sell_r < -0.005).mean())
    c30 = float((buy_r > 0.03).mean()), float((sell_r < -0.03).mean())
    print(f"  護欄 0.5%(buy_patient / sell_patient):買有 {c05[0]:.0%} 的日子、"
          f"賣有 {c05[1]:.0%} 的日子連收盤都掛不出去 → 當天沒成交")
    print(f"  護欄 3.0%(sell_exit / sell_stop / sell_open):買 {c30[0]:.0%}、賣 {c30[1]:.0%}"
          f" → 好很多,但**仍非零**")
    print("  → 只要護欄管得到收盤保底,「保底」就不是保底。護欄的本意是『盤中不追價』,")
    print("    而收盤價是市場的官方結算價、不是追價;兩者混用才產生這個失效率。")


if __name__ == "__main__":
    main()
