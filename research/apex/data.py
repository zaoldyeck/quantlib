"""apex 資料層 — cache 直開連線 + 正典調整價 panel + universe/資格過濾。

全部純函式:輸入 con/DataFrame、輸出 DataFrame,無全域狀態。
NAV 模擬一律使用 `prices.fetch_adjusted_panel`(除息/減資/分割 total-return 等價)。
"""
from __future__ import annotations

import os
from datetime import date as Date

import duckdb
import polars as pl

from research import prices
from research import paths

CACHE_DB = str(paths.CACHE_DB)
RAW_QUARTERLY_PARQUET = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "raw_quarterly.parquet"
)

#: 漲跌幅制度切換日:此日(含)之後 10%,之前 7%
LIMIT_CHANGE_DATE = Date(2015, 6, 1)


def connect() -> duckdb.DuckDBPyConnection:
    """Read-only cache 連線(可多程序並行)。附掛 raw_quarterly 基本面 view(若存在)。"""
    con = duckdb.connect(CACHE_DB, read_only=True)
    con.sql(f"SET threads = {max(1, os.cpu_count() or 4)}")
    if os.path.exists(RAW_QUARTERLY_PARQUET):
        con.sql(
            f"CREATE OR REPLACE TEMP VIEW raw_quarterly AS "
            f"SELECT * FROM read_parquet('{RAW_QUARTERLY_PARQUET}')"
        )
    return con


def latest_date(con: duckdb.DuckDBPyConnection) -> Date:
    """cache 最新交易日(daily_quote 最大日期)——「最新」的單一真相來源。

    任何「跑到最新」的 LIVE 圖表/報告一律呼叫本函式,禁止在腳本寫死日期字面值:
    寫死的「cache 最新日」會在資料前進後靜靜凍結該線(2026-07-09 事故:pnl_dashboard
    的 apex_revcycle_S 線凍結,其他線照跑,肉眼難察)。凍結窗的研究實驗才用固定
    字面值(復現性),且應如實註明是凍結窗、不得偽稱「cache 最新」。
    """
    return con.sql("SELECT max(date) FROM daily_quote").pl().item()


def load_panel(
    con: duckdb.DuckDBPyConnection,
    start: str,
    end: str,
    markets: tuple[str, ...] = ("twse", "tpex"),
    warmup_days: int = 300,
) -> pl.DataFrame:
    """雙市場調整價 panel,轉板股跨市場拼接(同日重複取 twse)。

    warmup_days 為 `start` 之前额外抓取的日曆天數,供 rolling 訊號暖機;
    回傳不裁切,策略層算完訊號後自行 filter `date >= start`。
    """
    frames = [
        prices.fetch_adjusted_panel(
            con, start, end, market=m, include_extra_history_days=warmup_days
        )
        for m in markets
    ]
    frames = [f for f in frames if not f.is_empty()]
    if not frames:
        raise ValueError(f"no price data in [{start}, {end}] for markets={markets}")
    panel = (
        pl.concat(frames)
        .sort(["company_code", "date", "market"], descending=[False, False, True])
        .unique(subset=["company_code", "date"], keep="first", maintain_order=True)
        .sort(["company_code", "date"])
    )
    # 精準漲跌停鎖死訊號:收盤最佳賣/買價缺失(E01;near-limit 日缺失率 85-88%,
    # 正常日 0.06-0.24% 雜訊底)。引擎配合 |ref_ret| 接近停板位使用。
    mk = ",".join(f"'{m}'" for m in markets)
    ba = con.sql(
        f"""
        SELECT market, date, company_code,
               (last_best_ask_price IS NULL OR last_best_ask_price = 0) AS ask_missing,
               (last_best_bid_price IS NULL OR last_best_bid_price = 0) AS bid_missing
        FROM daily_quote
        WHERE market IN ({mk})
          AND date BETWEEN DATE '{start}' - INTERVAL '{warmup_days} days' AND DATE '{end}'
        """
    ).pl()
    return (
        panel.join(ba, on=["market", "date", "company_code"], how="left")
        .with_columns(
            [pl.col("ask_missing").fill_null(False), pl.col("bid_missing").fill_null(False)]
        )
    )


def common_stocks(panel: pl.DataFrame) -> pl.DataFrame:
    """普通股 universe:4 碼純數字、非 0 開頭(排 ETF/ETN)、非 91xx(排 TDR)。

    特別股(字尾字母)與受益證券(0 開頭)被 regex 排除;已下市股保留。
    """
    return panel.filter(
        pl.col("company_code").str.contains(r"^[1-9]\d{3}$")
        & ~pl.col("company_code").str.starts_with("91")
    )


def eligibility(
    panel: pl.DataFrame,
    min_adv: float = 20_000_000.0,
    min_price: float = 10.0,
    min_history: int = 60,
) -> pl.DataFrame:
    """決策日投資資格:(date, company_code, eligible, adv20)。

    20 日中位成交值 ≥ min_adv、raw_close ≥ min_price、掛牌滿 min_history 根 bar。
    """
    return (
        panel.sort(["company_code", "date"])
        .with_columns(
            [
                pl.col("trade_value").cast(pl.Float64).rolling_median(20)
                .over("company_code").alias("adv20"),
                pl.int_range(pl.len()).over("company_code").alias("_bar_idx"),
            ]
        )
        .with_columns(
            (
                (pl.col("adv20") >= min_adv)
                & (pl.col("raw_close") >= min_price)
                & (pl.col("_bar_idx") >= min_history)
            ).alias("eligible")
        )
        .select(["date", "company_code", "eligible", "adv20"])
    )


def benchmark_nav(
    con: duckdb.DuckDBPyConnection, start: str, end: str, code: str = "0050"
) -> pl.DataFrame:
    """基準 total-return NAV(起點 = 1.0):(date, nav)。"""
    tr = prices.total_return_series(con, code, start, end).sort("date")
    return tr.select(
        ["date", (pl.col("adj_close") / pl.col("adj_close").first()).alias("nav")]
    )


# ──────────────────────────────────────────────────────────────────────────
# 訊號原料 loaders(全部含 start 前暖機;呼叫端自行對齊)
# ──────────────────────────────────────────────────────────────────────────

def _daily_table(
    con: duckdb.DuckDBPyConnection, table: str, cols: str, start: str, end: str,
    warmup_days: int = 200,
) -> pl.DataFrame:
    return con.sql(
        f"""
        SELECT date, company_code, {cols}
        FROM {table}
        WHERE date BETWEEN DATE '{start}' - INTERVAL '{warmup_days} days' AND DATE '{end}'
        ORDER BY company_code, date
        """
    ).pl()


def load_flows(con, start: str, end: str, warmup_days: int = 200) -> pl.DataFrame:
    """三大法人日買賣超(股數):foreign_diff / trust_diff / dealer_diff。"""
    return _daily_table(
        con, "daily_trading_details",
        "foreign_investors_difference AS foreign_diff, "
        "trust_difference AS trust_diff, dealers_difference AS dealer_diff",
        start, end, warmup_days,
    )


def load_margin(con, start: str, end: str, warmup_days: int = 200) -> pl.DataFrame:
    """融資融券餘額(張)。"""
    return _daily_table(
        con, "margin_transactions",
        "margin_balance, short_balance, margin_quota, short_quota",
        start, end, warmup_days,
    )


def load_foreign_holding(con, start: str, end: str, warmup_days: int = 200) -> pl.DataFrame:
    """外資持股比 + 流通股數。"""
    return _daily_table(
        con, "foreign_holding_ratio",
        "outstanding_shares, foreign_held_shares, foreign_held_ratio",
        start, end, warmup_days,
    )


def load_sbl(con, start: str, end: str, warmup_days: int = 200) -> pl.DataFrame:
    """借券賣出餘額(股)。資料 2016-01-04 起。"""
    return _daily_table(
        con, "sbl_borrowing", "daily_balance", start, end, warmup_days
    )


def load_valuation(con, start: str, end: str, warmup_days: int = 2000) -> pl.DataFrame:
    """每日 PBR / PER / 殖利率。暖機預設拉長(供 5 年 rolling 分位)。"""
    return _daily_table(
        con, "stock_per_pbr",
        "price_book_ratio AS pbr, price_to_earning_ratio AS per, dividend_yield AS dy",
        start, end, warmup_days,
    )


def load_monthly_revenue(con, end: str) -> pl.DataFrame:
    """月營收(全歷史,PIT 生效日 = 次月 10 日後首個交易日,由呼叫端對齊)。

    同一 (code, year, month) 以 consolidated 優先、individual 補洞。
    """
    return con.sql(
        f"""
        WITH ranked AS (
          SELECT year, month, company_code, monthly_revenue, monthly_revenue_yoy,
                 ROW_NUMBER() OVER (
                   PARTITION BY company_code, year, month
                   ORDER BY CASE type WHEN 'consolidated' THEN 0 ELSE 1 END
                 ) AS rn
          FROM operating_revenue
          WHERE monthly_revenue IS NOT NULL
            AND make_date(year, month, 1) <= DATE '{end}'
        )
        SELECT year, month, company_code, monthly_revenue, monthly_revenue_yoy
        FROM ranked WHERE rn = 1
        ORDER BY company_code, year, month
        """
    ).pl()
