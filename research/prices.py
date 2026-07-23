"""Total-return-equivalent OHLCV — single source of truth for ALL backtests.

Why this module exists
======================

歷史上 `research/v4.py` 用 SQL DRIP（forward-pass cash dividend reinvestment），
而 `iter_20.py` / `iter_24.py` 直接讀 raw `daily_quote.closing_price` 跑 daily
NAV simulation —— 後者系統性低估 NAV ~0.5-1.5pp CAGR 因為除息日 raw close
下跌但沒加回現金股利。**任何新策略都必須走這個模組**，禁止再讀 raw daily_quote
跑 NAV 模擬。

Architecture
============

  fetch_adjusted_panel()       ←   canonical entry point (OHLCV panel)
       │
       ├── _build_factor_table()    cash_div + cap_red + stock split → factors
       └── _apply_back_adjustment() reverse-cumprod + asof-join

  daily_returns_from_panel()   ←   panel  →  (date, code, ret) DRIP returns
  fetch_daily_returns()        ←   one-shot: panel + return conversion
  total_return_series()        ←   single ticker convenience (benchmark)

Algorithm: standard back-adjustment ("Yahoo-style adj_close")
  - Cash dividend: factor_e = (close_pre_ex - cash_div) / close_pre_ex   (< 1)
  - Capital reduction: factor_e = post_reduction_ref_price / close_pre   (any)
  - Stock split: factor_e = close_post_split / close_pre_split
    TWSE ex-right data has been incomplete for ETF split events since 2024, so
    we also detect suspension-gap split ratios heuristically, matching the Scala
    Backtester. This keeps 0050/0052 total-return curves continuous.
  - For each (code, date), adj_factor = ∏ factor_e for events e with ex_date(e) > date
  - adj_close[date] = raw_close[date] × adj_factor[date]
  - Same factor applied to open / high / low (preserves intraday ratios → ATR consistent)
  - Volume + trade_value KEPT RAW (volume = share count, dividends don't change it)

Properties
==========
  - On most-recent date: adj == raw (factor = 1, no future events)
  - For buy-at-t, sell-at-T: adj[T] / adj[t] - 1  ==  cumprod((close+div)/prev_close)
  - All OHLC ratios preserved → trailing-stop / ATR / breakout signals
    behave identically in adjusted vs raw space (only the level differs)
  - Vectorized (Polars + DuckDB); full TWSE 21y panel ≈ 3-5s
"""
from __future__ import annotations

import warnings

import duckdb
import polars as pl

# Polars 對 join_asof 的 `by` 參數會 emit "Sortedness of columns cannot be checked"
# UserWarning — 是保守提示（無法靜態驗證 group 內 sortedness），實際資料都已 .sort()
# 過。我們有 cross-implementation parity test (`test_prices.py`) 保障數學正確性，
# 這個 warning 在這個模組裡是純雜訊。
warnings.filterwarnings(
    "ignore",
    message=".*Sortedness of columns cannot be checked.*",
    category=UserWarning,
)


__all__ = [
    "fetch_adjusted_panel",
    "daily_returns_from_panel",
    "fetch_daily_returns",
    "total_return_series",
]


# ──────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────

def fetch_adjusted_panel(
    con: duckdb.DuckDBPyConnection,
    start: str,
    end: str,
    codes: list[str] | None = None,
    market: str = "twse",
    include_extra_history_days: int = 300,
) -> pl.DataFrame:
    """Pull back-adjusted daily OHLCV for the given universe.

    Args:
      con: DuckDB connection (typically `research.db.connect()`).
      start, end: 'YYYY-MM-DD' inclusive.
      codes: optional list of company_codes. If None, returns full market.
      market: 'twse' or 'tpex'. (For both markets, call twice and concat.)
      include_extra_history_days: pull extra calendar days of price history
          BEFORE `start` so signals using rolling windows (e.g. 60d max,
          200d MA) have warm-up data. Returned DataFrame is NOT trimmed;
          caller should filter by `date >= start` after computing signals.

    Returns:
      Polars DataFrame with columns:
        market, date, company_code,
        open, high, low, close,           ← adjusted (TR-equivalent)
        raw_close,                        ← original daily_quote.closing_price
        adj_factor,                       ← back-adjustment multiplier
        volume, trade_value               ← unchanged (raw share-count / NTD)

    Notes:
      - Events on dates outside [start, end] but with ex_date inside the
        extended history window are still applied (they affect older prices).
      - Suspicious dividend events (cash_div >= pre_close) are dropped.
      - Capital-reduction post_ref_price implied factor must satisfy
        0.05 < f < 5; out-of-range factors are dropped (data-quality guard).
    """
    if market not in ("twse", "tpex"):
        raise ValueError(f"market must be twse|tpex, got {market!r}")

    code_filter_sql = ""
    if codes is not None:
        if not codes:
            return _empty_panel()
        codes_sql = ",".join(f"'{c}'" for c in codes)
        code_filter_sql = f"AND company_code IN ({codes_sql})"

    px = con.sql(f"""
        SELECT market, date, company_code,
               opening_price AS open,
               highest_price AS high,
               lowest_price  AS low,
               closing_price AS close,
               trade_volume  AS volume,
               trade_value
        FROM daily_quote
        WHERE market = '{market}'
          AND closing_price > 0
          AND opening_price > 0
          {code_filter_sql}
          AND date BETWEEN DATE '{start}' - INTERVAL '{include_extra_history_days} days'
                       AND DATE '{end}'
        ORDER BY company_code, date
    """).pl()

    if px.is_empty():
        return _empty_panel()

    # **拉全部除權息事件,不再只認 cash_dividend > 0**(2026-07-23 修 FC1)。
    # 舊查詢用 cash_dividend > 0 過濾 → 純配股(2,304 筆「權」)與配股配息的股票腿
    # 全數落空 → 除權日原始收盤跳水卻無因子還原 → 幽靈崩跌(中位 -3.94%、最深
    # -23.76%),直接汙染所有 NAV 回測與 live S 的 exit_replay。
    # 交易所公告的「參考價 / 除權息前收盤」同時涵蓋配息+配股,是官方還原因子本身;
    # 對純配息事件與現行 cash 法實測僅差 4e-5(20,273 筆),故切換等價且更完整。
    #
    # **對兩種 cache 世代皆正確**:舊 cache 只同步了 cash_dividend 一欄(rebuild 前)。
    # 偵測欄位——有前收盤/參考價就用完整法,只有 cash 就退回舊法(對配息仍正確,
    # 配股待 cache rebuild 後自動修復)。不是 walkaround:函式本就不該假設 cache 世代。
    _erd_cols = {r[0] for r in con.sql(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'ex_right_dividend'").fetchall()}
    _has_ref = {"closing_price_before_ex_right_ex_dividend",
                "ex_right_ex_dividend_reference_price"} <= _erd_cols
    if _has_ref:
        divs = con.sql(f"""
            SELECT date AS ex_date, company_code, cash_dividend,
                   closing_price_before_ex_right_ex_dividend AS tbl_pre,
                   ex_right_ex_dividend_reference_price       AS tbl_ref
            FROM ex_right_dividend
            WHERE market = '{market}' {code_filter_sql} AND date <= DATE '{end}'
        """).pl()
    else:
        divs = con.sql(f"""
            SELECT date AS ex_date, company_code, cash_dividend,
                   CAST(0.0 AS DOUBLE) AS tbl_pre, CAST(0.0 AS DOUBLE) AS tbl_ref
            FROM ex_right_dividend
            WHERE market = '{market}' AND cash_dividend > 0
              {code_filter_sql} AND date <= DATE '{end}'
        """).pl()

    cr = con.sql(f"""
        SELECT date AS ex_date, company_code, post_reduction_reference_price AS post_ref
        FROM capital_reduction
        WHERE market = '{market}'
          AND post_reduction_reference_price > 0
          {code_filter_sql}
          AND date <= DATE '{end}'
    """).pl()

    factors = _build_factor_table(px, divs, cr)
    if factors.is_empty():
        return _select_output_columns(
            px.with_columns([
                pl.lit(1.0).alias("adj_factor"),
                pl.col("close").alias("raw_close"),
            ])
        )

    return _select_output_columns(
        _apply_back_adjustment(px, factors).sort(["company_code", "date"])
    )


def daily_returns_from_panel(panel: pl.DataFrame) -> pl.DataFrame:
    """Convert adjusted OHLCV panel → (date, company_code, ret) DRIP returns.

    Mathematically equivalent to forward-DRIP `(close[t] + cash_div[t]) /
    close[t-1] - 1` because the back-adjustment already encoded both terms.

    Returns:
      Polars DataFrame: date, company_code, ret  (filter NULLs out — first
      row per code has no prev_close).
    """
    return (panel
        .sort(["company_code", "date"])
        .with_columns(
            (pl.col("close") / pl.col("close").shift(1).over("company_code") - 1).alias("ret")
        )
        .filter(pl.col("ret").is_not_null())
        .select(["date", "company_code", "ret"])
    )


def fetch_daily_returns(
    con: duckdb.DuckDBPyConnection,
    start: str,
    end: str,
    codes: list[str] | None = None,
    market: str = "twse",
) -> pl.DataFrame:
    """Convenience: `fetch_adjusted_panel` + `daily_returns_from_panel`.

    Drop-in replacement for `v4.compute_daily_returns_sql(con, start, end, codes)`.
    Returned schema matches: (date, company_code, ret).

    No warm-up history needed for daily returns (just diff), so we override
    `include_extra_history_days=10` to keep the first day of `start` covered
    even after the LAG join drops one row.
    """
    panel = fetch_adjusted_panel(
        con, start, end, codes=codes, market=market,
        include_extra_history_days=10,
    )
    if panel.is_empty():
        return pl.DataFrame(schema={
            "date": pl.Date, "company_code": pl.Utf8, "ret": pl.Float64
        })
    rets = daily_returns_from_panel(panel)
    # Trim warm-up rows
    return rets.filter(
        (pl.col("date") >= pl.lit(start).str.to_date())
        & (pl.col("date") <= pl.lit(end).str.to_date())
    )


def total_return_series(
    con: duckdb.DuckDBPyConnection,
    code: str,
    start: str,
    end: str,
    market: str = "twse",
) -> pl.DataFrame:
    """Single-ticker convenience for benchmark series.

    Returns a (date, adj_close, raw_close) frame for `code`.
    Equivalent semantics to `active_etf_metrics.total_return_series` —
    use this for benchmark NAVs (0050, 0052, 2330, etc.).
    """
    panel = fetch_adjusted_panel(con, start, end, codes=[code], market=market,
                                  include_extra_history_days=0)
    return panel.sort("date").select(["date", "close", "raw_close"]).rename({"close": "adj_close"})


# ──────────────────────────────────────────────────────────────────────────
# Internals
# ──────────────────────────────────────────────────────────────────────────

def _empty_panel() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "market": pl.Utf8, "date": pl.Date, "company_code": pl.Utf8,
        "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
        "raw_close": pl.Float64, "adj_factor": pl.Float64,
        "volume": pl.Int64, "trade_value": pl.Float64,
    })


def _select_output_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.select([
        "market", "date", "company_code",
        "open", "high", "low", "close",
        "raw_close", "adj_factor",
        "volume", "trade_value",
    ])


def _build_factor_table(px: pl.DataFrame, divs: pl.DataFrame, cr: pl.DataFrame) -> pl.DataFrame:
    """Return (company_code, ex_date, factor) — adjustment multipliers."""
    # join_asof 契約:兩側都按 asof key「全域」排序。禁止對 (code, key) 排序的欄
    # set_sorted——謊 flag 會讓下游走有序快徑而損壞(見 apex B01 事故 2026-07-09)。
    pre_close_lookup = (
        px.select(["company_code", "date", "close"])
          .rename({"date": "px_date", "close": "pre_close"})
          .sort("px_date")
    )

    events: list[pl.DataFrame] = []

    if divs.height > 0:
        # Probe = ex_date - 1 → asof-backward finds largest px_date <= probe < ex_date.
        d = (divs.with_columns(
                (pl.col("ex_date") - pl.duration(days=1)).alias("probe")
             )
             .sort("probe")
             .join_asof(
                 pre_close_lookup,
                 left_on="probe", right_on="px_date",
                 by="company_code", strategy="backward",
             )
             .with_columns(
                 # 因子優先序(2026-07-23 FC1):
                 #  ① 交易所公告的參考價/前收盤 —— 官方還原因子本身,配息+配股一體涵蓋
                 #  ② 無參考價但有現金股利 → 沿用 (前收盤−現金股利)/前收盤(2,456 筆息)
                 #  ③ 兩者皆無 → null,下方過濾掉並回報(那些是 pre/ref/cash 全 0 的壞列,
                 #     須重解析 ex_right_dividend 補回,見 FC1-parse)
                 pl.when((pl.col("tbl_pre") > 0) & (pl.col("tbl_ref") > 0))
                   .then(pl.col("tbl_ref") / pl.col("tbl_pre"))
                   .when((pl.col("pre_close") > 0)
                         & (pl.col("pre_close") > pl.col("cash_dividend"))
                         & (pl.col("cash_dividend") > 0))
                   .then((pl.col("pre_close") - pl.col("cash_dividend"))
                         / pl.col("pre_close"))
                   .otherwise(None)
                   .alias("factor")
             )
             .filter(pl.col("factor").is_not_null()
                     & (pl.col("factor") > 0.05) & (pl.col("factor") < 5.0))
             .select(["company_code", "ex_date", "factor"])
        )
        events.append(d)

    if cr.height > 0:
        c = (cr.with_columns(
                (pl.col("ex_date") - pl.duration(days=1)).alias("probe")
             )
             .sort("probe")
             .join_asof(
                 pre_close_lookup,
                 left_on="probe", right_on="px_date",
                 by="company_code", strategy="backward",
             )
             .filter(
                 pl.col("pre_close").is_not_null()
                 & (pl.col("pre_close") > 0)
                 & (pl.col("post_ref") > 0)
             )
             .with_columns(
                 (pl.col("post_ref") / pl.col("pre_close")).alias("factor")
             )
             # 上限放寬 5.0 → 100.0(2026-07-23 FC1):台股「彌補虧損」型減資可減到
             # 只剩 2.5% 股本(factor 40),舊上限 5.0 把 16 筆真實大減資誤殺 →
             # 減資日原始價暴跳卻不還原 → 幽靈暴漲。100 與 _detect_stock_splits 的
             # 分割上限一致(同樣是大幅換股的量級)。下限 0.05 保留(現金減資不會更極端)。
             .filter((pl.col("factor") > 0.05) & (pl.col("factor") < 100.0))
             .select(["company_code", "ex_date", "factor"])
        )
        events.append(c)

    splits = _detect_stock_splits(px, divs, cr)
    if splits.height > 0:
        events.append(splits)

    if not events:
        return pl.DataFrame(schema={
            "company_code": pl.Utf8, "ex_date": pl.Date, "factor": pl.Float64,
        })

    # Combine; if same (code, ex_date) has both div + cap-red (rare), multiply
    return (pl.concat(events)
              .sort(["company_code", "ex_date"])
              .group_by(["company_code", "ex_date"])
              .agg(pl.col("factor").product().alias("factor"))
              .sort(["company_code", "ex_date"]))


def _detect_stock_splits(px: pl.DataFrame, divs: pl.DataFrame, cr: pl.DataFrame) -> pl.DataFrame:
    """Detect split / reverse-split events missing from TWSE ex-right tables.

    The rule mirrors `src/main/scala/strategy/Backtester.scala`:
      - close ratio implies a split factor between 2.5x and 100x, or reverse
        split factor between 0.01x and 0.4x;
      - the event follows a 3-14 calendar-day trading suspension gap;
      - no same-day dividend or capital-reduction record exists.

    Returns the same event schema as dividends/capital reductions, where
    `factor` is the multiplier applied to all pre-event prices.
    """
    split_events = (
        px.select(["company_code", "date", "close"])
        .sort(["company_code", "date"])
        .with_columns(
            [
                pl.col("date").shift(1).over("company_code").alias("prev_date"),
                pl.col("close").shift(1).over("company_code").alias("prev_close"),
            ]
        )
        .filter(pl.col("prev_close").is_not_null() & (pl.col("prev_close") > 0) & (pl.col("close") > 0))
        .with_columns(
            [
                (pl.col("date") - pl.col("prev_date")).dt.total_days().alias("gap_days"),
                (pl.col("prev_close") / pl.col("close")).alias("split_factor"),
            ]
        )
        .filter(
            pl.col("gap_days").is_between(3, 14)
            & (
                pl.col("split_factor").is_between(2.5, 100.0)
                | pl.col("split_factor").is_between(0.01, 0.4)
            )
        )
        .with_columns(
            [
                pl.col("date").alias("ex_date"),
                (pl.col("close") / pl.col("prev_close")).alias("factor"),
            ]
        )
        .select(["company_code", "ex_date", "factor"])
    )
    if split_events.is_empty():
        return split_events

    known_events: list[pl.DataFrame] = []
    if divs.height > 0:
        known_events.append(divs.select(["company_code", "ex_date"]))
    if cr.height > 0:
        known_events.append(cr.select(["company_code", "ex_date"]))
    if not known_events:
        return split_events

    known = pl.concat(known_events).unique()
    return split_events.join(known, on=["company_code", "ex_date"], how="anti")


def _apply_back_adjustment(px: pl.DataFrame, factors: pl.DataFrame) -> pl.DataFrame:
    """For each (code, date), multiply OHLC by ∏ factor_e where ex_date(e) > date.

    Implementation:
      1. Sort factors ascending by (code, ex_date).
      2. Compute reverse cumulative product per code:
           cum_factor_from_here[i] = ∏_{j >= i} factor[j]
      3. asof-join with strategy='forward': for each (code, date+1), find the
         smallest ex_date >= date+1 (= ex_date > date strictly) and use its
         cum_factor_from_here. No event > date → adj_factor = 1.

    On ex_date itself, raw close has already dropped → no further adjustment
    (probe = date+1 ensures we don't re-apply the current-day factor).
    """
    factors_with_cum = (factors
        .sort(["company_code", "ex_date"])
        .with_columns(
            pl.col("factor").cum_prod(reverse=True)
              .over("company_code")
              .alias("cum_factor_from_here")
        )
        .select(["company_code", "ex_date", "cum_factor_from_here"])
        .sort("ex_date")   # join_asof 契約:asof key 全域排序
    )

    px_with_factor = (px
        .with_columns(
            (pl.col("date") + pl.duration(days=1)).alias("probe")
        )
        .sort("probe")
        .join_asof(
            factors_with_cum,
            left_on="probe", right_on="ex_date",
            by="company_code", strategy="forward",
        )
        .with_columns(
            pl.col("cum_factor_from_here").fill_null(1.0).alias("adj_factor")
        )
        .drop(["probe", "cum_factor_from_here", "ex_date"])
    )

    return (px_with_factor
        .with_columns([
            pl.col("close").alias("raw_close"),
            (pl.col("open")  * pl.col("adj_factor")).alias("open"),
            (pl.col("high") * pl.col("adj_factor")).alias("high"),
            (pl.col("low")   * pl.col("adj_factor")).alias("low"),
            (pl.col("close") * pl.col("adj_factor")).alias("close"),
        ])
    )


# ──────────────────────────────────────────────────────────────────────────
# CLI sanity check
# ──────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import os
    import sys

    sys.path.insert(0, os.path.dirname(__file__))
    from db import connect

    con = connect()

    print("=== 0050 21y back-adjusted total return ===")
    panel = fetch_adjusted_panel(con, "2003-06-30", "2026-04-25",
                                  codes=["0050"], market="twse").sort("date")
    cum = panel["close"][-1] / panel["close"][0] - 1
    days = (panel["date"][-1] - panel["date"][0]).days
    cagr = (1 + cum) ** (365.25 / days) - 1
    print(f"window: {panel['date'][0]} → {panel['date'][-1]}  "
          f"({days/365.25:.2f} years, {panel.height} rows)")
    print(f"cum return: {cum*100:.2f}%   CAGR: {cagr*100:.2f}%")
    print(f"raw[0]={panel['raw_close'][0]:.2f} → adj[0]={panel['close'][0]:.2f} "
          f"(factor={panel['adj_factor'][0]:.4f})")
    print(f"raw[-1]={panel['raw_close'][-1]:.2f} → adj[-1]={panel['close'][-1]:.2f} "
          f"(factor={panel['adj_factor'][-1]:.4f})")
    print()
    print("expected: split-adjusted series should be continuous across 0050 2025-06-18 split")

    # Also verify the daily_returns helper round-trips correctly
    print("\n=== daily_returns_from_panel sanity ===")
    rets = daily_returns_from_panel(panel)
    cum_via_rets = (1 + rets["ret"]).product() - 1
    print(f"cum via daily-returns compound: {cum_via_rets*100:.2f}%")
    print(f"diff vs panel ratio: {abs(cum - cum_via_rets)*100:.4f}pp")
