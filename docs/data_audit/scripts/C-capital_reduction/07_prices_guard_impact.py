"""C-capital_reduction 稽核 07:prices.py 的 0.05<f<5 護欄丟掉的減資事件實測衝擊。

research/prices.py::_build_factor_table 對減資因子 f = post_ref / pre_close 加了
`0.05 < f < 5.0` 的「資料品質護欄」。台股「彌補虧損」型減資的換股率可以到 1:40
(減資 97.5%),f 就會遠大於 5 → 因子被丟掉 → 還原價完全沒調整 → canonical
報酬序列在事件當天出現幾十倍的假報酬。

本腳本對每個被丟掉的事件,實跑 prices.fetch_daily_returns 看當天報酬。

Run: uv run --project research python docs/data_audit/scripts/C-capital_reduction/07_prices_guard_impact.py
"""
from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths, prices  # noqa: E402

PG_DSN = "host=localhost port=5432 dbname=quantlib user=zaoldyeck"


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")

    rows = con.sql("""
      SELECT market,date,company_code,company_name,
             closing_price_on_the_last_trading_date pre,
             post_reduction_reference_price post,
             post_reduction_reference_price/closing_price_on_the_last_trading_date f,
             reason_for_capital_reduction
      FROM pg.public.capital_reduction
      WHERE post_reduction_reference_price/closing_price_on_the_last_trading_date
            NOT BETWEEN 0.05 AND 5.0
      ORDER BY date
    """).fetchall()

    print(f"{'market':6} {'date':11} {'code':6} {'name':10} {'pre':>8} {'post':>8} "
          f"{'f':>8} {'報酬(當天)':>12} {'adj_factor 前一日':>16}")
    worst = 0.0
    for market, d, code, name, pre, post, f, reason in rows:
        panel = prices.fetch_adjusted_panel(
            con, str(d - timedelta(days=40)), str(d + timedelta(days=5)),
            codes=[code], market=market)
        ret = prices.daily_returns_from_panel(panel)
        r = ret.filter(ret["date"] == d)
        rv = float(r["ret"][0]) if r.height else float("nan")
        pre_af = panel.filter(panel["date"] < d)
        af = float(pre_af["adj_factor"][-1]) if pre_af.height else float("nan")
        worst = max(worst, rv if rv == rv else 0.0)
        print(f"{market:6} {str(d):11} {code:6} {name:10} {pre:8.2f} {post:8.2f} "
              f"{f:8.3f} {rv:12.2%} {af:16.4f}")
    print(f"\n最大假報酬:{worst:.2%}")

    print("\n== 對照:護欄內的減資事件(f<5)當天報酬應接近 0 ==")
    ok_rows = con.sql("""
      SELECT market,date,company_code,company_name,
             post_reduction_reference_price/closing_price_on_the_last_trading_date f
      FROM pg.public.capital_reduction
      WHERE post_reduction_reference_price/closing_price_on_the_last_trading_date BETWEEN 0.05 AND 5.0
      ORDER BY random() LIMIT 8
    """).fetchall()
    for market, d, code, name, f in ok_rows:
        panel = prices.fetch_adjusted_panel(
            con, str(d - timedelta(days=40)), str(d + timedelta(days=5)),
            codes=[code], market=market)
        ret = prices.daily_returns_from_panel(panel)
        r = ret.filter(ret["date"] == d)
        rv = float(r["ret"][0]) if r.height else float("nan")
        print(f"  {market} {d} {code} {name} f={f:.3f} ret={rv:.2%}")


if __name__ == "__main__":
    main()
