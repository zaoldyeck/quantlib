"""F10 — S × 庫內籌碼軸(預註冊見 ledger/batches.md F10 段)。

Run: uv run --project . python -m quantlib.apex.experiments.f10_chip_axes
依賴 cache: 是
"""
from __future__ import annotations

import duckdb
import polars as pl

from quantlib.apex.experiments.f08_downmkt import (C, DEV0, DEV1, WREL, prep,
                                                   run_variant)


def main() -> None:
    panel, feat, elig = prep()
    raw = duckdb.connect("var/cache/cache.duckdb", read_only=True)
    mg = (raw.execute("SELECT date, company_code, margin_balance "
                      "FROM margin_transactions").pl()
          .sort([C, "date"])
          .with_columns((-(pl.col("margin_balance")
                           / pl.col("margin_balance").shift(5) - 1))
                        .over(C).alias("margin_inv5"))
          .select(["date", C, "margin_inv5"]))
    sb = (raw.execute("SELECT date, company_code, daily_balance "
                      "FROM sbl_borrowing").pl()
          .sort([C, "date"])
          .with_columns((-(pl.col("daily_balance")
                           / pl.col("daily_balance").shift(5).clip(1, None) - 1))
                        .over(C).alias("sbl_inv5"))
          .select(["date", C, "sbl_inv5"]))
    fh = (raw.execute("SELECT date, company_code, foreign_held_ratio "
                      "FROM foreign_holding_ratio").pl()
          .sort([C, "date"])
          .with_columns((pl.col("foreign_held_ratio")
                         - pl.col("foreign_held_ratio").shift(5))
                        .over(C).alias("fhold_chg5"))
          .select(["date", C, "fhold_chg5"]))
    fut = (raw.execute("SELECT date, foreign_tx_net_oi FROM "
                       "taifex_futures_daily_factors ORDER BY date").pl()
           .with_columns((pl.col("foreign_tx_net_oi") > 0).alias("fut_pos"))
           .select(["date", "fut_pos"]))
    feat = (feat.join(mg, on=["date", C], how="left")
            .join(sb, on=["date", C], how="left")
            .join(fh, on=["date", C], how="left")
            .join(fut, on="date", how="left")
            .with_columns([
                pl.col("margin_inv5").fill_null(0.0),
                pl.col("sbl_inv5").fill_null(0.0),
                pl.col("fhold_chg5").fill_null(0.0),
                pl.col("fut_pos").fill_null(False),
            ]))

    variants = [
        ("S 基準(重現)", dict(wts=WREL)),
        ("S + 融資降^0.5", dict(wts={**WREL, "margin_inv5": 0.5})),
        ("S + 借券降^0.5", dict(wts={**WREL, "sbl_inv5": 0.5})),
        ("S + 外資增持^0.5", dict(wts={**WREL, "fhold_chg5": 0.5})),
        ("S × 期貨多方 gate", dict(wts=WREL, dm_gate=False)),
    ]
    print(f"dev 窗 {DEV0}~{DEV1};S 基準 CAGR 120.9%/P5 ~67/MDD −32.6")
    for name, kw in variants[:-1]:
        k = run_variant(panel, feat, elig, **kw)
        flag = "★" if (k["p5"] > 0.744 and k["mdd"] > -0.376) else " "
        print(f"{flag} {name:16s} CAGR {k['cagr']:7.1%}  P5 {k['p5']:6.1%}  "
              f"MDD {k['mdd']:6.1%}  Martin {k['martin']:5.1f}")
    # 期貨 gate 變體:feat 過濾後照基準跑
    feat_g = feat.filter(pl.col("fut_pos"))
    k = run_variant(panel, feat_g, elig, wts=WREL)
    flag = "★" if (k["p5"] > 0.744 and k["mdd"] > -0.376) else " "
    print(f"{flag} {'S × 期貨多方 gate':16s} CAGR {k['cagr']:7.1%}  "
          f"P5 {k['p5']:6.1%}  MDD {k['mdd']:6.1%}  Martin {k['martin']:5.1f}")


if __name__ == "__main__":
    main()
