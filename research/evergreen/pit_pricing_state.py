"""PIT pricing-state + monthly-revenue snapshot for Evergreen qualitative labeling.

Measures, strictly as-of a given observation date (PIT), each candidate stock's:
  - close, 20d / 60d return, distance from 60d & 120d high (pricing state)
  - trailing 6 months of monthly-revenue YoY (near-term self-catalyst vs divergence)
  - PIT industry (industry_taxonomy_pit asof effective_date <= obs date)

Used by the mid-month labeling day to distinguish 醞釀待發 (unpriced, near-term
trigger) from 利多出盡 / sell-the-news (already run) and from divergence (price up
but current revenue down). Mirrors the ev28 2024-10 "定價狀態量測(PIT)" discipline.

Requires: research/cache.duckdb fresh (see CLAUDE.md Data Refresh Workflow).
Run: uv run --project research python -m research.evergreen.pit_pricing_state --date 2024-06-11 --codes 6187,5443,...
"""
from __future__ import annotations
import argparse
import duckdb

CACHE = "research/cache.duckdb"


def snapshot(codes: list[str], obs_date: str) -> None:
    con = duckdb.connect(CACHE, read_only=True)
    # nearest trading day <= obs_date
    asof = con.execute(
        "select max(date) from daily_quote where date <= ?", [obs_date]
    ).fetchone()[0]
    print(f"# PIT snapshot asof {asof} (obs_date={obs_date})\n")
    for code in codes:
        px = con.execute(
            """
            with p as (
                select date, closing_price as c, highest_price as h, trade_value as tv
                from daily_quote
                where company_code = ? and date <= ? order by date
            ), tail as (select * from p order by date desc limit 121)
            select
              (select c from tail order by date desc limit 1) as last_c,
              (select c from tail order by date desc limit 1 offset 20) as c20,
              (select c from tail order by date desc limit 1 offset 60) as c60,
              (select max(h) from (select h from tail order by date desc limit 60)) as hi60,
              (select max(h) from tail) as hi120,
              (select avg(tv) from (select tv from tail order by date desc limit 20)) as adv20
            """,
            [code, obs_date],
        ).fetchone()
        name = con.execute(
            "select company_name from daily_quote d join operating_revenue r using(company_code) where company_code=? limit 1",
            [code],
        ).fetchone()
        nm = name[0] if name else "?"
        ind = con.execute(
            "select industry from industry_taxonomy_pit where company_code=? and effective_date<=? order by effective_date desc limit 1",
            [code, obs_date],
        ).fetchone()
        inds = ind[0] if ind else "?"
        if not px or px[0] is None:
            print(f"{code} {nm}: NO PRICE DATA <= {obs_date}")
            continue
        last_c, c20, c60, hi60, hi120, adv20 = px
        r20 = (last_c / c20 - 1) * 100 if c20 else float("nan")
        r60 = (last_c / c60 - 1) * 100 if c60 else float("nan")
        d_hi60 = (last_c / hi60 - 1) * 100 if hi60 else float("nan")
        d_hi120 = (last_c / hi120 - 1) * 100 if hi120 else float("nan")
        advm = (adv20 or 0) / 1e8  # trade_value in NTD -> 億元
        # trailing 6 monthly revenue YoY, only months whose data is public by obs_date
        # (publish deadline = 10th of next month; require (year,month) publish <= obs_date)
        revs = con.execute(
            """
            select year, month, monthly_revenue_yoy
            from operating_revenue
            where company_code = ? and type='consolidated'
              and make_date(year, month, 1) + interval '40 days' <= ?
            order by year desc, month desc limit 6
            """,
            [code, obs_date],
        ).fetchall()
        rev_str = ", ".join(
            f"{y%100:02d}/{m:02d}:{('%+d' % round(yoy)) if yoy is not None else 'NA'}%"
            for (y, m, yoy) in revs
        )
        print(
            f"{code} {nm} [{inds}] close={last_c:.1f} | 20d={r20:+.1f}% 60d={r60:+.1f}% "
            f"| dHi60={d_hi60:+.1f}% dHi120={d_hi120:+.1f}% | ADV20={advm:.1f}億"
        )
        print(f"      revYoY(new→old): {rev_str}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--codes", required=True, help="comma-separated 4-digit codes")
    a = ap.parse_args()
    snapshot([c.strip() for c in a.codes.split(",") if c.strip()], a.date)


if __name__ == "__main__":
    main()
