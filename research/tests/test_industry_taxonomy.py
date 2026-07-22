from datetime import date
import sys
from pathlib import Path

import duckdb
import polars as pl

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "research"))

from research.industry_taxonomy import attach_industry_asof, build_industry_taxonomy_pit, normalize_industry_name


def _seed_revenue(con: duckdb.DuckDBPyConnection) -> None:
    con.sql(
        """
        CREATE TABLE operating_revenue (
            market VARCHAR,
            type VARCHAR,
            year INTEGER,
            month INTEGER,
            company_code VARCHAR,
            company_name VARCHAR,
            industry VARCHAR
        )
        """
    )
    con.sql(
        """
        INSERT INTO operating_revenue VALUES
            ('twse', 'individual',   2020, 1, '2330', '台積電', '電子工業'),
            ('twse', 'consolidated', 2020, 1, '2330', '台積電', '半導體業'),
            ('twse', 'consolidated', 2020, 2, '2330', '台積電', '半導體業'),
            ('twse', 'consolidated', 2020, 1, '2801', '彰銀', '金融保險業（其中金控公司係控股公司，其申報之「營業收入」係認列所有子公司損益之合計數）'),
            ('tpex', 'consolidated', 2020, 1, '9999', '測試', '觀光事業'),
            ('twse', 'consolidated', 2020, 1, '0050', '元大台灣50', '半導體業')
        """
    )


def test_normalize_legacy_industry_labels() -> None:
    assert normalize_industry_name("建材營建") == "建材營造"
    assert normalize_industry_name("觀光事業") == "觀光餐旅"
    assert normalize_industry_name("金融保險業") == "金融保險"
    assert normalize_industry_name("金融保險（其中金控公司係控股公司，其申報之「營業收入」係認列所有子公司損益之合計數）") == "金融保險"
    assert normalize_industry_name("通訊網路") == "通信網路業"
    assert normalize_industry_name("生物科技") == "生技醫療業"
    assert normalize_industry_name("電子商務") == "數位雲端"
    assert normalize_industry_name(" 半導體業 ") == "半導體業"


def test_build_industry_taxonomy_uses_consolidated_rows_and_publish_dates() -> None:
    con = duckdb.connect()
    _seed_revenue(con)

    tax = build_industry_taxonomy_pit(con)
    row = tax.filter((pl.col("market") == "twse") & (pl.col("company_code") == "2330") & (pl.col("source_ym") == 202001)).row(0, named=True)

    assert row["industry"] == "半導體業"
    assert row["raw_industry"] == "半導體業"
    assert row["effective_date"] == date(2020, 2, 13)
    assert row["industry_source"] == "mops_operating_revenue"
    assert tax.filter(pl.col("company_code") == "0050").is_empty()


def test_attach_industry_asof_is_point_in_time() -> None:
    con = duckdb.connect()
    _seed_revenue(con)
    tax = build_industry_taxonomy_pit(con)
    con.register("_industry_taxonomy_pit", tax)
    con.sql("CREATE VIEW industry_taxonomy_pit AS SELECT * FROM _industry_taxonomy_pit")

    panel = pl.DataFrame(
        {
            "market": ["twse", "twse"],
            "company_code": ["2330", "2330"],
            "date": [date(2020, 2, 12), date(2020, 2, 13)],
            "close": [100.0, 101.0],
        }
    )

    out = attach_industry_asof(panel, con)

    before, after = out.sort("date").to_dicts()
    assert before["industry"] is None
    assert after["industry"] == "半導體業"
    assert after["broad_sector"] == "電子"
