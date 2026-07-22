"""「這檔為什麼不在 S 的進場池裡?」——逐關卡追蹤器。

出現時機:每當持股被判成「非本策略標的」,人的直覺往往是「它明明很好/我印象中
S 推薦過」。空口爭辯沒有用,要能指著關卡說「它卡在第幾關、卡了幾天」。

關卡順序**必須與 `advisors.pool_history` 完全同構**(同一份定義的逐關拆解,
不是另一套實作):
  ① 營收新鮮 rev_fresh_days ≤ 7   ② 流動性/上市資格 eligible
  ③ 六因子齊全(缺值即出局)       ④ 當日現金流品質閘(cfo/ni ≥ 當日池中位數)

Run:
  uv run --project research python -m research.tri.pool_trace 2059 6446
  uv run --project research python -m research.tri.pool_trace 2059 --since 2025-01-01
"""
from __future__ import annotations

import argparse
import os
from datetime import date as Date

import duckdb
import polars as pl

from research.apex import data
from research.apex.assemble import apply_avail_override, build_features
from research.tri.advisors import C, S_WTS
from research import paths

_GATES = ("① 營收新鮮 ≤7 日", "② 流動性/上市資格", "③ 六因子齊全", "④ 現金流品質閘")


def _inputs(con, today: Date):
    """重建 s_advisor 的特徵輸入(與 `s_advisor` 前半段同構)。"""
    ov = None
    fs = f"{paths.RECORDS}/revenue_first_seen.parquet"
    if os.path.exists(fs):
        ov = (pl.read_parquet(fs)
              .with_columns(pl.col("first_seen").str.to_date().alias("avail_date"))
              .select([C, "year", "month", "avail_date"]))
    ws = today.replace(year=today.year - 2).isoformat()
    panel, feat, elig = build_features(con, ws, today.isoformat(), avail_override=ov)
    rev = (data.load_monthly_revenue(con, today.isoformat())
           .sort([C, "year", "month"])
           .with_columns([
               pl.date(pl.col("year") + pl.col("month") // 12,
                       pl.col("month") % 12 + 1, 10).alias("avail"),
               (pl.col("monthly_revenue").rolling_sum(3)
                / pl.col("monthly_revenue").rolling_sum(3).shift(3) - 1)
               .over(C).alias("rev_seq"),
           ]))
    rev = (apply_avail_override(rev, ov)
           .select([C, "avail", "rev_seq"]).drop_nulls().sort("avail"))
    feat = (feat.sort("date")
            .join_asof(rev, left_on="date", right_on="avail", by=C,
                       strategy="backward", tolerance="70d").sort([C, "date"]))
    raw = duckdb.connect(f"{paths.CACHE_DB}", read_only=True)
    tax = raw.sql("SELECT company_code, effective_date, industry FROM "
                  "industry_taxonomy_pit WHERE industry IS NOT NULL "
                  "ORDER BY effective_date").pl()
    return feat, elig, tax


def trace(feat: pl.DataFrame, elig: pl.DataFrame, tax: pl.DataFrame,
          codes: list[str], since: Date | None = None) -> dict[str, list[int]]:
    """→ {code: [過①的天數, 過②, 過③, 過④(=入池天數)]}。"""
    day = feat.sort("date").join_asof(
        tax.sort("effective_date"), left_on="date", right_on="effective_date",
        by=C, strategy="backward")
    ind_med = (day.filter(pl.col("industry").is_not_null())
               .group_by(["date", "industry"])
               .agg(pl.col("rev_yoy_accel").median().alias("_im")))
    day = (day.join(ind_med, on=["date", "industry"], how="left")
           .with_columns((pl.col("rev_yoy_accel") - pl.col("_im")).alias("accel_rel")))
    if since is not None:
        day = day.filter(pl.col("date") >= since)

    g1 = day.filter(pl.col("rev_fresh_days") <= 7)
    g2 = g1.join(elig.filter(pl.col("eligible")).select(["date", C]),
                 on=["date", C], how="semi")
    g3 = g2.drop_nulls(subset=list(S_WTS))
    g3 = g3.with_columns([
        pl.col("cfo_ni_ratio_ttm").median().over("date").alias("_med"),
        pl.col("cfo_ni_ratio_ttm").is_not_null().sum().over("date").alias("_ncov"),
        pl.len().over("date").alias("_h"),
    ])
    gate = (pl.col("_ncov") >= 0.3 * pl.col("_h")) & pl.col("_med").is_not_null()
    g4 = g3.filter(~gate | (pl.col("cfo_ni_ratio_ttm") >= pl.col("_med")))

    out: dict[str, list[int]] = {}
    for code in codes:
        out[code] = [f.filter(pl.col(C) == code).height for f in (g1, g2, g3, g4)]
    return out


def render(res: dict[str, list[int]], names: dict[str, str] | None = None) -> str:
    names = names or {}
    lines = []
    for code, counts in res.items():
        nm = f" {names[code]}" if code in names else ""
        lines.append(f"{code}{nm}:")
        prev = None
        for gname, n in zip(_GATES, counts):
            drop = "" if prev is None else f"(刷掉 {prev - n} 天)"
            lines.append(f"   {gname}: 通過 {n} 天{drop}")
            prev = n
        lines.append("   → 入池 " + (f"{counts[-1]} 天" if counts[-1]
                                     else "0 天:**S 從未想買它**"))
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="追蹤某檔股票卡在 S 進場池的哪一關")
    ap.add_argument("codes", nargs="+", help="股票代碼")
    ap.add_argument("--today", default=None, help="決策日 YYYY-MM-DD(預設今天)")
    ap.add_argument("--since", default=None, help="只看此日之後")
    args = ap.parse_args()

    today = Date.fromisoformat(args.today) if args.today else Date.today()
    since = Date.fromisoformat(args.since) if args.since else None
    con = data.connect()
    try:
        feat, elig, tax = _inputs(con, today)
        res = trace(feat, elig, tax, [c.zfill(4) for c in args.codes], since)
    finally:
        con.close()
    from research.trading.execution.daily_context import lookup_names
    print(render(res, lookup_names(list(res))))


if __name__ == "__main__":
    main()
