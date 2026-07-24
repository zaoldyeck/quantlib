"""B1 從零線:新事件源 edge 初測——庫藏股公告/內部人申報轉讓/減資恢復買賣(事件研究)。

**這是舊 campaign 沒測過的角落**:insider(2007-2026,本輪才補齊)與庫藏股(全史)當時資料
不全。三源皆稀疏事件(日均個位數)→ 截面 IC 不適用,改**事件研究**:事件日後 fwd {5,21,63}d
報酬 vs 同日 eligible universe 均值的**異常報酬 AR**,報 n/mean/median/t/命中率。

thesis:庫藏股公告=公司自認低估+買盤(正);內部人申報轉讓=知情賣壓(負);減資按事由分
(彌補虧損=劣質訊號?現金減資=中性回錢)。AR 顯著者才有資格進策略構造(step ②→③)。

Run: uv run --project . python -m quantlib.strat_lab.e01_event_edges
依賴 cache:是。
"""
from __future__ import annotations

import math

import polars as pl

from quantlib.apex import data, factors
from quantlib.apex.strategy_s import C, prep_cached


def _ar_table(events: pl.DataFrame, fwd: pl.DataFrame, base: pl.DataFrame, tag: str) -> None:
    """events=(event_date, C);AR = fwd(事件) − 同日 eligible 均值。"""
    ev = (events.rename({"event_date": "date"})
          .join(fwd, on=["date", C], how="inner")
          .join(base, on="date", how="inner"))
    print(f"  {tag}(對上 fwd 的事件 n={ev.height})")
    for k in (5, 21, 63):
        col, b = f"fwd_{k}", f"base_{k}"
        d = ev.drop_nulls(subset=[col, b]).with_columns((pl.col(col) - pl.col(b)).alias("ar"))
        if d.height < 30:
            print(f"    h{k}: n<30 樣本不足")
            continue
        ar = d["ar"]
        m, sd, n = ar.mean(), ar.std(), d.height
        t = m / (sd / math.sqrt(n)) if sd and sd > 0 else 0.0
        hit = (ar > 0).mean()
        print(f"    h{k}: AR mean {m:+.2%} median {ar.median():+.2%} t {t:+.1f} 命中 {hit:.0%} (n={n})")


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    fwd = factors.forward_returns(panel)
    # 同日 eligible universe 的 fwd 均值(AR 基準)
    base = (fwd.join(elig.filter(pl.col("eligible")).select(["date", C]),
                     on=["date", C], how="semi")
            .group_by("date")
            .agg([pl.col(f"fwd_{k}").mean().alias(f"base_{k}") for k in (5, 21, 63)]))

    print("=== 新事件源 edge 初測(異常報酬 AR vs 同日 eligible 均值)===\n")

    tsb = con.sql("SELECT announce_date AS event_date, company_code FROM treasury_stock_buyback "
                  "WHERE announce_date IS NOT NULL").pl().unique()
    _ar_table(tsb, fwd, base, "① 庫藏股買回公告(thesis:正)")

    ins = con.sql("""
        SELECT report_date AS event_date, company_code, sum(transfer_shares) AS sh
        FROM insider_holding WHERE transfer_shares > 0 GROUP BY 1, 2
    """).pl().select(["event_date", C])
    _ar_table(ins, fwd, base, "② 內部人申報轉讓(thesis:負=知情賣壓)")

    for reason, tag in [("彌補虧損", "③a 減資恢復買賣-彌補虧損"), ("現金減資", "③b 減資恢復買賣-現金減資")]:
        cr = con.sql(f"SELECT date AS event_date, company_code FROM capital_reduction "
                     f"WHERE reason_for_capital_reduction LIKE '%{reason}%'").pl().unique()
        _ar_table(cr, fwd, base, tag)

    print("\n  判準:|t|>3 且 AR 量級 >1%/月 才有資格進策略構造;否則證偽落地。")


if __name__ == "__main__":
    main()
