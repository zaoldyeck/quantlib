"""EV58 era-brief 支援:從 cache 量出某期別區間內大盤的實際轉折點與類股冷熱。

用途:era_brief 的 `macro_timeline` 要求「區間內每一次指數的顯著轉折都要有一條」。
轉折點必須從資料量出來(而不是憑印象回想「那年好像 10 月大跌」),再去找當日的
一手報導當出處。類股冷熱同理,供 `sector_context` 判斷循環位置時交叉檢查。

依賴 cache:是(讀 `market_index`,不需最新資料,歷史區間即可)。

Run:
    uv run --project . python -m quantlib.evergreen.ev58_era_index_pivots \
        --start 2018-01-01 --end 2019-12-31
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass

import polars as pl

from quantlib.db import connect

TAIEX = "發行量加權股價指數"


@dataclass(frozen=True)
class Pivot:
    date: str
    close: float
    kind: str  # peak | trough
    move_pct: float  # 自前一個轉折以來的漲跌幅


def _load(name: str, start: str, end: str) -> pl.DataFrame:
    """載入指數收盤序列。

    只取「當天真的有股票成交」的日期:`market_index` 含 TWSE 週六災難備援演練日
    (實測 2018-08-04、2018-09-15 有指數列但 `daily_quote` 零列),其加權指數收盤
    是演練值而非市場值——不濾掉會造出 -8.8% / +9.3% 的假單日轉折。
    """
    con = connect()
    df = con.execute(
        """
        SELECT i.date, i.close
        FROM market_index i
        WHERE i.market = 'twse' AND i.name = ? AND i.date BETWEEN ? AND ?
          AND EXISTS (
              SELECT 1 FROM daily_quote q
              WHERE q.market = 'twse' AND q.date = i.date
          )
        ORDER BY i.date
        """,
        [name, start, end],
    ).pl()
    return df


def zigzag(df: pl.DataFrame, threshold: float = 0.05) -> list[Pivot]:
    """標準 zigzag:只有反向幅度超過 threshold 才確認一個轉折。

    方向未定(direction == 0)時同時追蹤區間高點與低點,先跨過門檻的那一邊
    決定第一段的方向。**不可以只用單一 `ext_i` 兼管兩個方向**——那會讓極值
    索引每根 K 都被推到當根,反向幅度恆為 0,狀態機永遠停在未定向、一個轉折
    都吐不出來(2026-08-06 實測:含 -23% 崩跌的區間回傳空 list)。

    第一段方向確認時,該段的起點極值必然落在 index 0(視窗第一根)。這一點
    **不當成轉折輸出**:它的 leg 恆為 +0.0%,是視窗切邊的產物而不是量出來的
    轉折——真正的那個轉折可能落在視窗開始之前。輸出它會讓下游把「區間起點」
    誤讀成「當年的高/低點」而寫進時間軸。視窗端點資訊改由 CLI 的 open/high/
    low/close 表達。守護見 `tests/test_ev58_era_index_pivots.py`。
    """
    if df.is_empty():
        return []
    dates = df["date"].to_list()
    closes = df["close"].to_list()

    pivots: list[Pivot] = []
    last_pivot_i = 0
    hi_i = lo_i = 0  # 自上一個轉折以來的高/低點索引
    direction = 0  # 0 未定,1 上升(在找 peak),-1 下降(在找 trough)

    def _emit(ext_i: int, kind: str) -> None:
        nonlocal last_pivot_i
        if ext_i > 0:  # index 0 是視窗切邊,不是量出來的轉折(見 docstring)
            pivots.append(
                Pivot(
                    str(dates[ext_i]),
                    closes[ext_i],
                    kind,
                    closes[ext_i] / closes[last_pivot_i] - 1.0,
                )
            )
        last_pivot_i = ext_i

    for i in range(1, len(closes)):
        px = closes[i]
        if px > closes[hi_i]:
            hi_i = i
        if px < closes[lo_i]:
            lo_i = i

        if direction >= 0 and px / closes[hi_i] - 1.0 <= -threshold:
            # 自高點回落逾門檻 → 確認 peak,轉為找 trough
            _emit(hi_i, "peak")
            direction, lo_i, hi_i = -1, i, i
        elif direction <= 0 and px / closes[lo_i] - 1.0 >= threshold:
            # 自低點反彈逾門檻 → 確認 trough,轉為找 peak
            _emit(lo_i, "trough")
            direction, hi_i, lo_i = 1, i, i
    return pivots


def big_days(df: pl.DataFrame, n: int = 15) -> pl.DataFrame:
    return (
        df.with_columns((pl.col("close") / pl.col("close").shift(1) - 1.0).alias("ret"))
        .drop_nulls()
        .with_columns(pl.col("ret").abs().alias("absret"))
        .sort("absret", descending=True)
        .head(n)
        .sort("date")
        .select("date", "close", "ret")
    )


def sector_table(start: str, end: str) -> pl.DataFrame:
    con = connect()
    return con.execute(
        """
        WITH s AS (
            SELECT name, date, close,
                   first_value(close) OVER (PARTITION BY name ORDER BY date) AS c0,
                   last_value(close) OVER (
                       PARTITION BY name ORDER BY date
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                   ) AS c1,
                   max(close) OVER (PARTITION BY name) AS hi,
                   min(close) OVER (PARTITION BY name) AS lo
            FROM market_index
            WHERE market='twse' AND date BETWEEN ? AND ?
              AND name LIKE '%類指數'
        )
        SELECT name,
               round((c1/c0-1)*100, 1) AS period_pct,
               round((hi/c0-1)*100, 1) AS max_up_pct,
               round((lo/hi-1)*100, 1) AS max_dd_pct
        FROM s GROUP BY name, c0, c1, hi, lo
        ORDER BY period_pct DESC
        """,
        [start, end],
    ).pl()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--threshold", type=float, default=0.05)
    args = ap.parse_args()

    df = _load(TAIEX, args.start, args.end)
    print(f"# TAIEX {args.start}~{args.end}  rows={len(df)}")
    hi_row = df.filter(pl.col("close") == pl.col("close").max()).row(0, named=True)
    lo_row = df.filter(pl.col("close") == pl.col("close").min()).row(0, named=True)
    print(
        f"open={df['close'][0]:.2f}({df['date'][0]}) "
        f"close={df['close'][-1]:.2f}({df['date'][-1]}) "
        f"high={hi_row['close']:.2f}({hi_row['date']}) "
        f"low={lo_row['close']:.2f}({lo_row['date']})"
    )
    print("\n## zigzag pivots (>= %.0f%%)" % (args.threshold * 100))
    for p in zigzag(df, args.threshold):
        print(f"{p.date}  {p.kind:6s} {p.close:9.2f}  leg {p.move_pct * 100:+6.1f}%")

    print("\n## 單日最大波動 (top 15)")
    for r in big_days(df).iter_rows(named=True):
        print(f"{r['date']}  {r['close']:9.2f}  {r['ret'] * 100:+5.2f}%")

    print("\n## 各類指數區間表現")
    with pl.Config(tbl_rows=40):
        print(sector_table(args.start, args.end))


if __name__ == "__main__":
    main()
