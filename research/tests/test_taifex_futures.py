import duckdb
import pytest
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "research"))
from futures.taifex import build_taifex_futures_tables


def test_taifex_continuous_uses_new_contract_previous_close_on_roll():
    con = duckdb.connect()
    con.sql(
        """
        CREATE TABLE taifex_futures_daily (
            date DATE,
            contract_code VARCHAR,
            contract_month VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            change DOUBLE,
            change_pct DOUBLE,
            volume BIGINT,
            settlement_price DOUBLE,
            open_interest BIGINT,
            best_bid DOUBLE,
            best_ask DOUBLE,
            historical_high DOUBLE,
            historical_low DOUBLE,
            trading_halt VARCHAR,
            trading_session VARCHAR,
            spread_single_volume BIGINT
        )
        """
    )
    con.sql(
        """
        INSERT INTO taifex_futures_daily VALUES
        ('2026-01-02','TX','202601',100,100,100,100,NULL,NULL,10,100,100,NULL,NULL,NULL,NULL,NULL,'一般',NULL),
        ('2026-01-02','TX','202602',105,105,105,105,NULL,NULL,2,105,10,NULL,NULL,NULL,NULL,NULL,'一般',NULL),
        ('2026-01-02','MTX','202601',100.5,100.5,100.5,100.5,NULL,NULL,10,100.5,100,NULL,NULL,NULL,NULL,NULL,'一般',NULL),
        ('2026-01-03','TX','202601',110,110,110,110,NULL,NULL,10,110,100,NULL,NULL,NULL,NULL,NULL,'一般',NULL),
        ('2026-01-03','TX','202602',115,115,115,115,NULL,NULL,3,115,20,NULL,NULL,NULL,NULL,NULL,'一般',NULL),
        ('2026-01-03','MTX','202601',110.25,110.25,110.25,110.25,NULL,NULL,10,110.25,100,NULL,NULL,NULL,NULL,NULL,'一般',NULL),
        ('2026-01-04','TX','202602',116,116,116,116,NULL,NULL,10,116,100,NULL,NULL,NULL,NULL,NULL,'一般',NULL),
        ('2026-01-04','MTX','202602',116.5,116.5,116.5,116.5,NULL,NULL,10,116.5,100,NULL,NULL,NULL,NULL,NULL,'一般',NULL)
        """
    )
    con.sql(
        """
        CREATE TABLE market_index (
            market VARCHAR,
            date DATE,
            name VARCHAR,
            close DOUBLE,
            change DOUBLE,
            change_pct DOUBLE
        )
        """
    )
    con.sql(
        """
        INSERT INTO market_index VALUES
        ('twse','2026-01-02','發行量加權股價指數',99,0,0),
        ('twse','2026-01-03','發行量加權股價指數',109,0,0),
        ('twse','2026-01-04','發行量加權股價指數',115,0,0)
        """
    )
    con.sql(
        """
        CREATE TABLE taifex_futures_final_settlement (
            date DATE,
            contract_code VARCHAR,
            contract_month VARCHAR,
            final_settlement_price DOUBLE
        )
        """
    )
    con.sql(
        """
        CREATE TABLE taifex_futures_institutional (
            date DATE,
            contract_code VARCHAR,
            product_name VARCHAR,
            investor_type VARCHAR,
            long_volume BIGINT,
            long_value_thousands BIGINT,
            short_volume BIGINT,
            short_value_thousands BIGINT,
            net_volume BIGINT,
            net_value_thousands BIGINT,
            long_open_interest BIGINT,
            long_oi_value_thousands BIGINT,
            short_open_interest BIGINT,
            short_oi_value_thousands BIGINT,
            net_open_interest BIGINT,
            net_oi_value_thousands BIGINT
        )
        """
    )
    con.sql(
        """
        INSERT INTO taifex_futures_institutional VALUES
        ('2026-01-04','TX','臺股期貨','外資及陸資',0,0,0,0,0,0,20,0,5,0,15,0)
        """
    )

    build_taifex_futures_tables(con)

    roll_close = con.sql(
        """
        SELECT continuous_close
        FROM taifex_futures_continuous
        WHERE product = 'TX' AND date = '2026-01-04'
        """
    ).fetchone()[0]
    assert roll_close == pytest.approx(1000.0 * (110 / 100) * (116 / 115))

    factors = con.sql(
        """
        SELECT tx_mtx_close_spread, tx_spot_basis, foreign_tx_net_oi
        FROM taifex_futures_daily_factors
        WHERE date = '2026-01-04'
        """
    ).fetchone()
    assert factors == pytest.approx((0.5, 1.0, 15))
