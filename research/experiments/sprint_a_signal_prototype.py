"""Sprint A signal prototype — 用 3 天的 SBL + QFII 資料做 pipeline demo + 回看相關性.

前置條件
========
- research/cache.duckdb 已 sync（Sprint A 擴充後）
- sbl_borrowing: TWSE + TPEx 3 天（2026-04-21~23）
- foreign_holding_ratio: TWSE + TPEx 3 天（2026-04-21~23）
- daily_quote: 2000-01-04 ~ 2026-04-21（max 比 SBL/QFII 晚一天）

3 天資料的限制（誠實說在前）
============================
Forward return 10-20d（使用者想驗證的目標）= T=2026-04-21 需要 close[2026-05-05+]，
資料尚未存在。此 prototype 因此只能做三件事：

1. **Pipeline smoke test** — 確認 signal 計算 + join daily_quote 的 SQL 跑得過、資料乾淨
2. **Backward (lookback) correlation** — signal[T] vs return[T-10, T]
   - 回答：「最近借券餘額暴增的股票，是不是最近一直跌」
   - 不是預測力測試，而是「同期徵兆一致性」測試
3. **Descriptive stats** — signal 分布、極值、stock 覆蓋率

若要做真正的 forward predictive IC 測試，需 ≥ 30 天 SBL + QFII 累積（見結尾 next-steps）

Usage
=====
    uv run --project research python research/experiments/sprint_a_signal_prototype.py
"""
from __future__ import annotations

import os
import sys

import polars as pl

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, ".."))
from db import connect  # noqa: E402


def main() -> None:
    con = connect()

    # ---- 1. Load data ----
    print("=" * 60)
    print("1. 資料載入與範圍檢查")
    print("=" * 60)

    sbl_range = con.sql("""
        SELECT market, MIN(date) AS min_d, MAX(date) AS max_d, COUNT(*) AS n
        FROM sbl_borrowing GROUP BY market ORDER BY market
    """).pl()
    print("\nSBL 借券餘額：")
    print(sbl_range)

    qfii_range = con.sql("""
        SELECT market, MIN(date) AS min_d, MAX(date) AS max_d, COUNT(*) AS n
        FROM foreign_holding_ratio GROUP BY market ORDER BY market
    """).pl()
    print("\nQFII 外資持股比率：")
    print(qfii_range)

    price_range = con.sql("""
        SELECT MIN(date) AS min_d, MAX(date) AS max_d, COUNT(*) AS n
        FROM daily_quote WHERE market IN ('twse', 'tpex')
    """).pl()
    print("\ndaily_quote 價格：")
    print(price_range)

    # ---- 2. Compute signals ----
    # Signal A: foreign_held_ratio 當日變化（簡單 diff 版）
    # Signal B: sbl daily_sold 除以當日餘額（借券強度 intensity）
    # Signal C: sbl daily_balance 相對前日變化率
    print("\n" + "=" * 60)
    print("2. Signal 計算（2026-04-22 → 使用 T-1 prior day 計算變化）")
    print("=" * 60)

    signals = con.sql("""
        WITH
          sbl_lag AS (
            SELECT
              market, date, company_code,
              daily_balance,
              daily_sold,
              LAG(daily_balance) OVER (
                PARTITION BY market, company_code ORDER BY date
              ) AS prev_balance
            FROM sbl_borrowing
          ),
          qfii_lag AS (
            SELECT
              market, date, company_code,
              foreign_held_ratio,
              LAG(foreign_held_ratio) OVER (
                PARTITION BY market, company_code ORDER BY date
              ) AS prev_foreign_ratio
            FROM foreign_holding_ratio
          )
        SELECT
          s.market,
          s.date,
          s.company_code,
          s.daily_balance,
          s.daily_sold,
          CASE
            WHEN s.prev_balance IS NULL OR s.prev_balance = 0 THEN NULL
            ELSE (s.daily_balance - s.prev_balance) / s.prev_balance::DOUBLE
          END AS sbl_balance_chg_pct,
          CASE
            WHEN s.daily_balance = 0 THEN NULL
            ELSE s.daily_sold / s.daily_balance::DOUBLE
          END AS sbl_intensity,
          q.foreign_held_ratio,
          q.foreign_held_ratio - q.prev_foreign_ratio AS foreign_ratio_chg_pp
        FROM sbl_lag s
        LEFT JOIN qfii_lag q
          USING (market, date, company_code)
        WHERE s.prev_balance IS NOT NULL
          AND s.date = (SELECT MAX(date) FROM sbl_borrowing)
    """).pl()
    print(f"\n樣本：{len(signals)} rows（2 個市場 × 最新 date，排除 prev_balance IS NULL）")
    print("\n分布：")
    print(signals.select(
        pl.col("sbl_balance_chg_pct").describe(),
        pl.col("sbl_intensity").describe(),
        pl.col("foreign_ratio_chg_pp").describe(),
    ) if False else signals.select(
        ["sbl_balance_chg_pct", "sbl_intensity", "foreign_ratio_chg_pp"]
    ).describe())

    # ---- 3. Backward 10d return ----
    # return[T-10, T] = close[T] / close[T-10] - 1
    # T = max signal date = 2026-04-23；T-10 trading days = 需要透過 ROW_NUMBER 取
    signal_date = signals["date"].max()
    print(f"\nSignal date T = {signal_date}")

    # Backward return via asof subquery — 先取每個 ticker 最後 11 筆價格 → 算 return
    returns = con.sql(f"""
        WITH ranked AS (
          SELECT
            market, company_code, date, closing_price,
            ROW_NUMBER() OVER (
              PARTITION BY market, company_code ORDER BY date DESC
            ) AS rn
          FROM daily_quote
          WHERE market IN ('twse', 'tpex')
            AND date <= DATE '{signal_date}'
            AND closing_price > 0
        )
        SELECT
          market, company_code,
          MAX(CASE WHEN rn = 1  THEN closing_price END) AS px_t,
          MAX(CASE WHEN rn = 11 THEN closing_price END) AS px_t_minus_10
        FROM ranked
        WHERE rn <= 11
        GROUP BY market, company_code
        HAVING COUNT(*) = 11
    """).pl().with_columns(
        ((pl.col("px_t") / pl.col("px_t_minus_10")) - 1).alias("ret_bwd_10d")
    ).filter(pl.col("ret_bwd_10d").is_not_null())
    print(f"Backward 10d return 計算：{len(returns)} stocks")

    # ---- 4. Join signals + returns ----
    joined = signals.join(
        returns, on=["market", "company_code"], how="inner"
    ).drop_nulls(subset=["sbl_balance_chg_pct", "ret_bwd_10d"])
    print(f"\n3. Join 後可用 pairs: {len(joined)}")

    # ---- 5. Cross-sectional correlation ----
    print("\n" + "=" * 60)
    print("4. Cross-sectional correlation (signal[T] vs backward return[T-10, T])")
    print("=" * 60)
    print("註：這是「同期一致性」測試，非 forward predictive IC。")
    print()

    # Spearman (rank) + Pearson
    for signal_col in ["sbl_balance_chg_pct", "sbl_intensity", "foreign_ratio_chg_pp"]:
        sub = joined.drop_nulls(subset=[signal_col])
        if len(sub) < 30:
            print(f"  {signal_col:25s}  n={len(sub)} → 樣本太少跳過")
            continue
        # 用 DuckDB CORR() 計算 pearson；spearman 手算 rank
        x = sub[signal_col].to_numpy()
        y = sub["ret_bwd_10d"].to_numpy()
        import numpy as np
        pearson = float(np.corrcoef(x, y)[0, 1])
        # Spearman: rank both
        from scipy.stats import rankdata, spearmanr  # type: ignore
        spearman, spearman_p = spearmanr(x, y)
        print(f"  {signal_col:25s}  n={len(sub):>5}  Pearson={pearson:+.4f}  "
              f"Spearman={spearman:+.4f} (p={spearman_p:.3g})")

    # Per-market breakdown
    print("\nPer-market breakdown:")
    for mkt in ["twse", "tpex"]:
        sub_mkt = joined.filter(pl.col("market") == mkt)
        if len(sub_mkt) < 30:
            continue
        for signal_col in ["sbl_balance_chg_pct", "sbl_intensity", "foreign_ratio_chg_pp"]:
            sub = sub_mkt.drop_nulls(subset=[signal_col])
            if len(sub) < 30:
                continue
            from scipy.stats import spearmanr  # type: ignore
            spearman, p = spearmanr(sub[signal_col], sub["ret_bwd_10d"])
            print(f"  {mkt:4s} × {signal_col:25s}  n={len(sub):>5}  Spearman={spearman:+.4f} (p={p:.3g})")

    # ---- 6. Diagnostics: top / bottom 10 by each signal ----
    print("\n" + "=" * 60)
    print("5. Top / Bottom 10 by each signal (sanity check — are these stocks making sense?)")
    print("=" * 60)
    for signal_col in ["sbl_balance_chg_pct", "foreign_ratio_chg_pp"]:
        sub = joined.drop_nulls(subset=[signal_col]).sort(signal_col)
        print(f"\n--- Bottom 10 by {signal_col} ---")
        print(sub.head(10).select("market", "company_code", signal_col, "ret_bwd_10d"))
        print(f"--- Top 10 by {signal_col} ---")
        print(sub.tail(10).select("market", "company_code", signal_col, "ret_bwd_10d"))

    # ---- 7. Conclusion + next steps ----
    print("\n" + "=" * 60)
    print("6. 結論 + next steps")
    print("=" * 60)
    print("""
[Pipeline 狀態]
- Signal 計算 + join daily_quote + cross-sectional correlation 全部可跑 ✓
- 3 天資料 → 每個 signal 僅能取 1 個 observation day（T=latest）→ 樣本 ~2000 stocks

[關鍵限制]
- Forward 10-20d return 目前不可算（signal 日期 + 10d > daily_quote max）
- Backward correlation 只能證明「同期徵兆一致性」，不是預測力

[何時能做真正的 forward IC 測試]
- 選 A: 等 15-20 個交易日再跑（此 prototype 重跑即可，不改 code）
- 選 B: 歷史 backfill SBL + QFII 30-90 天（20s × 日數 × 2 endpoint）：
      sbt "runMain Main pull sbl  --since 2026-03-01"   # ~10-15 min
      sbt "runMain Main pull qfii --since 2026-03-01"   # ~10-15 min
      sbt "runMain Main read sbl" && sbt "runMain Main read qfii"
      uv run python research/cache_tables.py
      # 然後重跑此 prototype，forward return 段改用 FUTURE dates
- 選 C: FinMind VIP NT$299 一次買完整歷史

[訊號是否有潛力的早期判斷]
- 若 Spearman |rho| > 0.05（p < 0.05 per-market）→ 值得投回測資源
- 若 |rho| < 0.02 且非顯著 → 訊號弱，考慮轉向其他資料源
""")


if __name__ == "__main__":
    main()
