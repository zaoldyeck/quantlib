"""Audit operating_revenue for bugs vs real data.

1. Monthly anomaly pattern: (year, month) with many 0/negative/extreme-YoY rows
2. Compare DB to raw CSV for suspicious months/rows
3. Identify reader bugs (column shift) vs real small-cap zero revenue

Usage: uv run python 05_revenue_audit.py
"""
from __future__ import annotations
import polars as pl
from db import connect


def main():
    con = connect()

    print("=== Monthly anomaly rate (2020+) ===")
    df = con.sql("""
        WITH by_month AS (
          SELECT year, month,
                 COUNT(*) FILTER (WHERE monthly_revenue < 0) AS neg,
                 COUNT(*) FILTER (WHERE monthly_revenue = 0) AS zero,
                 COUNT(*) FILTER (WHERE monthly_revenue > 0 AND last_year_monthly_revenue > 0
                                  AND (monthly_revenue::DOUBLE / last_year_monthly_revenue > 100
                                       OR monthly_revenue::DOUBLE / last_year_monthly_revenue < 0.01)) AS extreme,
                 COUNT(*) AS total
          FROM pg.public.operating_revenue
          GROUP BY year, month
        )
        SELECT year, month, neg, zero, extreme, total,
               ROUND((zero::DOUBLE / total * 100)::numeric, 1) AS zero_pct
        FROM by_month
        WHERE (neg + zero + extreme > 5) AND year >= 2020
        ORDER BY year, month
    """).pl()
    print(f"{len(df)} anomaly months")
    print(df)

    print("\n=== Companies with persistent zero revenue (2024+) — possibly real ===")
    df = con.sql("""
        SELECT company_code,
               COUNT(*) AS zero_months,
               MIN(year*100+month) AS earliest,
               MAX(year*100+month) AS latest
        FROM pg.public.operating_revenue
        WHERE year >= 2024 AND monthly_revenue = 0
        GROUP BY company_code
        HAVING COUNT(*) >= 3
        ORDER BY zero_months DESC
    """).pl()
    print(df.head(30))

    print("\n=== Stocks with sharp last_year → current drop (real restatement/delisting?) ===")
    df = con.sql("""
        SELECT company_code, year, month,
               monthly_revenue, last_year_monthly_revenue
        FROM pg.public.operating_revenue
        WHERE year >= 2024
          AND last_year_monthly_revenue > 1000  -- meaningful scale
          AND monthly_revenue = 0
        ORDER BY last_year_monthly_revenue DESC
        LIMIT 15
    """).pl()
    print(df)

    print("\n=== Negative monthly_revenue rows ===")
    df = con.sql("""
        SELECT company_code, year, month, monthly_revenue
        FROM pg.public.operating_revenue
        WHERE monthly_revenue < 0
        ORDER BY year DESC, month DESC
        LIMIT 20
    """).pl()
    print(df)


if __name__ == "__main__":
    main()
