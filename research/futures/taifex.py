"""TAIFEX futures continuous series and daily factor tables.

The raw tables are populated by the Scala crawler/reader:

    uv run --project research python -m research.crawl.rebuild --source taifex


This module then builds research-ready DuckDB tables from the raw TAIFEX rows.
"""

from __future__ import annotations

import os

import duckdb
from research import paths


TAIFEX_PRODUCTS = ("TX", "MTX", "TMF", "TE", "TF")
PRODUCT_SQL = ", ".join(f"'{product}'" for product in TAIFEX_PRODUCTS)


def build_taifex_futures_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Build near/next-month, return-spliced continuous, and factor tables.

    The continuous series uses the front contract by nearest listed contract
    month and computes roll-day return from the new contract's own previous
    close when available. This avoids injecting the old/new contract price gap
    into strategy research.
    """

    con.sql(f"""
        CREATE OR REPLACE TABLE taifex_futures_contract_rank AS
        WITH base AS (
            SELECT
                d.date,
                d.contract_code AS product,
                d.contract_month,
                TRY_CAST(regexp_extract(d.contract_month, '^(\\d{{6}})', 1) AS INTEGER) AS month_key,
                d.open,
                d.high,
                d.low,
                d.close,
                d.settlement_price,
                fs.final_settlement_price,
                d.volume,
                d.open_interest,
                d.trading_session
            FROM taifex_futures_daily d
            LEFT JOIN taifex_futures_final_settlement fs
              ON fs.date = d.date
             AND fs.contract_code = d.contract_code
             AND fs.contract_month = d.contract_month
            WHERE d.contract_code IN ({PRODUCT_SQL})
              AND d.trading_session = '一般'
              AND regexp_matches(d.contract_month, '^\\d{{6}}')
              AND (d.close IS NOT NULL OR d.settlement_price IS NOT NULL OR fs.final_settlement_price IS NOT NULL)
        )
        SELECT
            *,
            row_number() OVER (
                PARTITION BY date, product
                ORDER BY month_key, contract_month
            ) AS month_rank
        FROM base
        WHERE month_key IS NOT NULL
    """)

    con.sql("""
        CREATE OR REPLACE TABLE taifex_futures_continuous AS
        WITH ranked_with_prev AS (
            SELECT
                *,
                COALESCE(settlement_price, final_settlement_price, close) AS px,
                lag(COALESCE(settlement_price, final_settlement_price, close)) OVER (
                    PARTITION BY product, contract_month
                    ORDER BY date
                ) AS prev_contract_px
            FROM taifex_futures_contract_rank
        ),
        front AS (
            SELECT *
            FROM ranked_with_prev
            WHERE month_rank = 1
        ),
        returns AS (
            SELECT
                *,
                CASE
                    WHEN px > 0 AND prev_contract_px > 0 THEN px / prev_contract_px - 1.0
                    ELSE NULL
                END AS daily_return
            FROM front
        ),
        chained AS (
            SELECT
                *,
                1000.0 * exp(sum(ln(
                    CASE
                        WHEN daily_return IS NULL OR daily_return <= -0.999999 THEN 1.0
                        ELSE 1.0 + daily_return
                    END
                )) OVER (
                    PARTITION BY product
                    ORDER BY date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )) AS continuous_close
            FROM returns
        ),
        adjusted AS (
            SELECT
                *,
                CASE WHEN px > 0 THEN continuous_close / px ELSE NULL END AS adjustment_factor
            FROM chained
        )
        SELECT
            date,
            product,
            contract_month,
            open,
            high,
            low,
            close,
            settlement_price,
            final_settlement_price,
            volume,
            open_interest,
            daily_return,
            continuous_close,
            CASE WHEN adjustment_factor IS NOT NULL AND open IS NOT NULL THEN open * adjustment_factor END AS continuous_open,
            CASE WHEN adjustment_factor IS NOT NULL AND high IS NOT NULL THEN high * adjustment_factor END AS continuous_high,
            CASE WHEN adjustment_factor IS NOT NULL AND low IS NOT NULL THEN low * adjustment_factor END AS continuous_low,
            CASE
                WHEN adjustment_factor IS NOT NULL
                THEN COALESCE(settlement_price, final_settlement_price) * adjustment_factor
            END AS continuous_settlement,
            adjustment_factor
        FROM adjusted
    """)

    con.sql("""
        CREATE OR REPLACE TABLE taifex_futures_daily_factors AS
        WITH front AS (
            SELECT * FROM taifex_futures_contract_rank WHERE month_rank = 1
        ),
        second_month AS (
            SELECT * FROM taifex_futures_contract_rank WHERE month_rank = 2
        ),
        spot AS (
            SELECT date, close AS taiex_close
            FROM (
                SELECT
                    date,
                    close,
                    row_number() OVER (
                        PARTITION BY date
                        ORDER BY CASE WHEN name = '發行量加權股價指數' THEN 0 ELSE 1 END
                    ) AS rn
                FROM market_index
                WHERE market = 'twse'
                  AND name LIKE '%發行量加權股價指數%'
                  AND close IS NOT NULL
            )
            WHERE rn = 1
        ),
        inst AS (
            SELECT
                date,
                contract_code AS product,
                sum(CASE WHEN investor_type = '外資及陸資' THEN net_open_interest ELSE 0 END) AS foreign_net_oi,
                sum(CASE WHEN investor_type = '投信' THEN net_open_interest ELSE 0 END) AS trust_net_oi,
                sum(CASE WHEN investor_type = '自營商' THEN net_open_interest ELSE 0 END) AS dealer_net_oi,
                sum(CASE WHEN investor_type = '外資及陸資' THEN net_volume ELSE 0 END) AS foreign_net_volume,
                sum(CASE WHEN investor_type = '投信' THEN net_volume ELSE 0 END) AS trust_net_volume,
                sum(CASE WHEN investor_type = '自營商' THEN net_volume ELSE 0 END) AS dealer_net_volume
            FROM taifex_futures_institutional
            WHERE contract_code IN ('TX', 'MTX', 'TMF', 'TE', 'TF')
            GROUP BY date, contract_code
        )
        SELECT
            tx.date,
            tx.contract_month AS tx_contract_month,
            tx.close AS tx_close,
            tx.settlement_price AS tx_settlement_price,
            tx.final_settlement_price AS tx_final_settlement_price,
            tx.volume AS tx_volume,
            tx.open_interest AS tx_open_interest,
            tx2.contract_month AS tx_next_contract_month,
            COALESCE(tx2.settlement_price, tx2.final_settlement_price, tx2.close) - COALESCE(tx.settlement_price, tx.final_settlement_price, tx.close) AS tx_next_term_spread,
            (COALESCE(tx2.settlement_price, tx2.final_settlement_price, tx2.close) / NULLIF(COALESCE(tx.settlement_price, tx.final_settlement_price, tx.close), 0)) - 1.0 AS tx_next_term_spread_pct,
            mtx.contract_month AS mtx_contract_month,
            mtx.close AS mtx_close,
            mtx.settlement_price AS mtx_settlement_price,
            mtx.final_settlement_price AS mtx_final_settlement_price,
            mtx.open_interest AS mtx_open_interest,
            tmf.contract_month AS tmf_contract_month,
            tmf.close AS tmf_close,
            tmf.settlement_price AS tmf_settlement_price,
            tmf.final_settlement_price AS tmf_final_settlement_price,
            tmf.open_interest AS tmf_open_interest,
            te.contract_month AS te_contract_month,
            te.close AS te_close,
            te.settlement_price AS te_settlement_price,
            te.final_settlement_price AS te_final_settlement_price,
            te.open_interest AS te_open_interest,
            tf.contract_month AS tf_contract_month,
            tf.close AS tf_close,
            tf.settlement_price AS tf_settlement_price,
            tf.final_settlement_price AS tf_final_settlement_price,
            tf.open_interest AS tf_open_interest,
            mtx.close - tx.close AS tx_mtx_close_spread,
            (mtx.close / NULLIF(tx.close, 0)) - 1.0 AS tx_mtx_close_spread_pct,
            tmf.close - tx.close AS tx_tmf_close_spread,
            (tmf.close / NULLIF(tx.close, 0)) - 1.0 AS tx_tmf_close_spread_pct,
            spot.taiex_close,
            COALESCE(tx.settlement_price, tx.final_settlement_price, tx.close) - spot.taiex_close AS tx_spot_basis,
            (COALESCE(tx.settlement_price, tx.final_settlement_price, tx.close) / NULLIF(spot.taiex_close, 0)) - 1.0 AS tx_spot_basis_pct,
            inst_tx.foreign_net_oi AS foreign_tx_net_oi,
            inst_tx.trust_net_oi AS trust_tx_net_oi,
            inst_tx.dealer_net_oi AS dealer_tx_net_oi,
            inst_tx.foreign_net_volume AS foreign_tx_net_volume,
            inst_tx.trust_net_volume AS trust_tx_net_volume,
            inst_tx.dealer_net_volume AS dealer_tx_net_volume,
            inst_mtx.foreign_net_oi AS foreign_mtx_net_oi,
            inst_tmf.foreign_net_oi AS foreign_tmf_net_oi,
            inst_te.foreign_net_oi AS foreign_te_net_oi,
            inst_tf.foreign_net_oi AS foreign_tf_net_oi
        FROM front tx
        LEFT JOIN second_month tx2 ON tx2.date = tx.date AND tx2.product = 'TX'
        LEFT JOIN front mtx ON mtx.date = tx.date AND mtx.product = 'MTX'
        LEFT JOIN front tmf ON tmf.date = tx.date AND tmf.product = 'TMF'
        LEFT JOIN front te ON te.date = tx.date AND te.product = 'TE'
        LEFT JOIN front tf ON tf.date = tx.date AND tf.product = 'TF'
        LEFT JOIN spot ON spot.date = tx.date
        LEFT JOIN inst inst_tx ON inst_tx.date = tx.date AND inst_tx.product = 'TX'
        LEFT JOIN inst inst_mtx ON inst_mtx.date = tx.date AND inst_mtx.product = 'MTX'
        LEFT JOIN inst inst_tmf ON inst_tmf.date = tx.date AND inst_tmf.product = 'TMF'
        LEFT JOIN inst inst_te ON inst_te.date = tx.date AND inst_te.product = 'TE'
        LEFT JOIN inst inst_tf ON inst_tf.date = tx.date AND inst_tf.product = 'TF'
        WHERE tx.product = 'TX'
    """)

    for index_sql in [
        "CREATE INDEX IF NOT EXISTS idx_tfx_rank_product_date ON taifex_futures_contract_rank(product, date)",
        "CREATE INDEX IF NOT EXISTS idx_tfx_cont_product_date ON taifex_futures_continuous(product, date)",
        "CREATE INDEX IF NOT EXISTS idx_tfx_factors_date ON taifex_futures_daily_factors(date)",
    ]:
        con.sql(index_sql)


def main() -> None:
    db_path = str(paths.CACHE_DB)
    con = duckdb.connect(db_path)
    try:
        build_taifex_futures_tables(con)
        for table in [
            "taifex_futures_contract_rank",
            "taifex_futures_continuous",
            "taifex_futures_daily_factors",
        ]:
            n = con.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"{table}: {n:,} rows")
    finally:
        con.close()


if __name__ == "__main__":
    main()
