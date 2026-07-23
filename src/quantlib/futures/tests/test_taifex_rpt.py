import io
import json
import sys
import zipfile
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src" / "quantlib"))

from futures.taifex_rpt import (  # noqa: E402
    latest_safe_intraday_date,
    dedupe_rpt_files,
    parse_embedded_entries,
    parse_rpt_file_to_parquet,
    RptDriveFile,
)


def test_parse_embedded_entries_finds_drive_files_and_folders():
    html = """
    <div class="flip-entry"><a href="https://drive.google.com/drive/folders/FOLDERID">
    <div class="flip-entry-title">2026</div></a></div>
    <div class="flip-entry"><a href="https://drive.google.com/file/d/FILEID/view?usp=drive_web">
    <div class="flip-entry-title">Daily_2026_05_19.zip</div></a></div>
    """
    entries = parse_embedded_entries(html)
    assert [(e.title, e.drive_id, e.kind) for e in entries] == [
        ("2026", "FOLDERID", "folder"),
        ("Daily_2026_05_19.zip", "FILEID", "file"),
    ]


def test_dedupe_prefers_folder_matching_source_year():
    duplicated = [
        RptDriveFile(
            source_date=__import__("datetime").date(2025, 12, 31),
            title="Daily_2025_12_31.zip",
            ext="zip",
            drive_file_id="next-year",
            drive_folder_id="2026-folder",
            listed_year=2026,
        ),
        RptDriveFile(
            source_date=__import__("datetime").date(2025, 12, 31),
            title="Daily_2025_12_31.zip",
            ext="zip",
            drive_file_id="source-year",
            drive_folder_id="2025-folder",
            listed_year=2025,
        ),
    ]
    result = dedupe_rpt_files(duplicated)
    assert len(result) == 1
    assert result[0].drive_file_id == "source-year"
    assert result[0].duplicate_count == 2


def test_dedupe_prefers_direct_rpt_over_zip_for_same_source_date():
    dt = __import__("datetime").date(2016, 1, 28)
    duplicated = [
        RptDriveFile(dt, "Daily_2016_01_28.zip", "zip", "zip-id", "folder", 2016),
        RptDriveFile(dt, "Daily_2016_01_28.rpt", "rpt", "rpt-id", "folder", 2016),
    ]
    result = dedupe_rpt_files(duplicated)
    assert len(result) == 1
    assert result[0].drive_file_id == "rpt-id"
    assert result[0].duplicate_count == 2


def test_parse_rpt_zip_filters_products_and_preserves_night_trade_date(tmp_path):
    rpt_text = "\n".join(
        [
            "成交日期,商品代號,到期月份(週別),成交時間,成交價格,成交數量(B+S),近月價格,遠月價格,開盤集合競價 ",
            "20260518,TX     ,202606     ,152652,21500,2,-,-, ",
            "20260519,TX     ,202606     ,084501,21510,4,-,-,*",
            "20260519,MTX    ,202606     ,084502,21511,6,-,-, ",
            "20260519,ABC    ,202606     ,084503,1,8,-,-, ",
            "",
        ]
    )
    raw = tmp_path / "Daily_2026_05_19.zip"
    with zipfile.ZipFile(raw, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("Daily_2026_05_19.rpt", rpt_text.encode("cp950"))

    out = tmp_path / "lake"
    written = parse_rpt_file_to_parquet(raw, out_dir=out, products=["TX", "MTX"])

    assert len(written) == 2
    tx = pl.read_parquet(out / "ticks/product=TX/year=2026/month=05/source_date=2026-05-19.parquet")
    assert tx.height == 2
    assert tx["source_date"].dt.to_string("%Y-%m-%d").to_list() == ["2026-05-19", "2026-05-19"]
    assert tx["trade_date"].dt.to_string("%Y-%m-%d").to_list() == ["2026-05-18", "2026-05-19"]
    assert tx["price"].to_list() == [21500.0, 21510.0]
    assert tx["quantity"].to_list() == [2, 4]

    mtx = pl.read_parquet(out / "ticks/product=MTX/year=2026/month=05/source_date=2026-05-19.parquet")
    assert mtx["product"].to_list() == ["MTX"]


def test_parse_rpt_zip_accepts_csv_payload_name(tmp_path):
    rpt_text = "\n".join(
        [
            "成交日期,商品代號,到期月份(週別),成交時間,成交價格,成交數量(B+S),近月價格,遠月價格,開盤集合競價 ",
            "20190102,TX     ,201901     ,084501,9500,2,-,-, ",
            "",
        ]
    )
    raw = tmp_path / "Daily_2019_01_02.zip"
    with zipfile.ZipFile(raw, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("Daily_2019_01_02.csv", rpt_text.encode("cp950"))

    out = tmp_path / "lake"
    written = parse_rpt_file_to_parquet(raw, out_dir=out, products=["TX"])

    assert len(written) == 1
    tx = pl.read_parquet(out / "ticks/product=TX/year=2019/month=01/source_date=2019-01-02.parquet")
    assert tx["contract_month"].to_list() == ["201901"]
    assert tx["price"].to_list() == [9500.0]


def test_parse_status_records_requested_products_even_when_product_has_no_rows(tmp_path):
    rpt_text = "\n".join(
        [
            "成交日期,商品代號,到期月份(週別),成交時間,成交價格,成交數量(B+S),近月價格,遠月價格,開盤集合競價 ",
            "20200102,TX     ,202001     ,084501,12000,2,-,-, ",
            "",
        ]
    )
    raw = tmp_path / "Daily_2020_01_02.zip"
    with zipfile.ZipFile(raw, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("Daily_2020_01_02.rpt", rpt_text.encode("cp950"))

    out = tmp_path / "lake"
    status_dir = tmp_path / "parse_status"
    written = parse_rpt_file_to_parquet(raw, out_dir=out, products=["TX", "TMF"], status_dir=status_dir)

    assert len(written) == 1
    status = json.loads((status_dir / "source_date=2020-01-02.json").read_text(encoding="utf-8"))
    assert status["products_requested"] == ["TMF", "TX"]
    assert status["products_written"] == ["TX"]

    written_again = parse_rpt_file_to_parquet(raw, out_dir=out, products=["TX", "TMF"], status_dir=status_dir)
    assert written_again == written


def test_latest_safe_intraday_date_excludes_today_before_safe_time():
    taipei = ZoneInfo("Asia/Taipei")

    assert latest_safe_intraday_date(now=datetime(2026, 5, 21, 12, 52, tzinfo=taipei)) == date(2026, 5, 20)
    assert latest_safe_intraday_date(now=datetime(2026, 5, 21, 16, 1, tzinfo=taipei)) == date(2026, 5, 21)
    assert latest_safe_intraday_date(now=datetime(2026, 5, 21, 12, 52, tzinfo=taipei), allow_today=True) == date(
        2026, 5, 21
    )


def test_higher_priority_official_parse_blocks_lower_priority_mirror_overwrite(tmp_path):
    def write_raw(path: Path, price: int) -> None:
        rpt_text = "\n".join(
            [
                "成交日期,商品代號,到期月份(週別),成交時間,成交價格,成交數量(B+S),近月價格,遠月價格,開盤集合競價 ",
                f"20260520,TX     ,202606     ,084501,{price},2,-,-, ",
                "",
            ]
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(path.name.replace(".zip", ".rpt"), rpt_text.encode("cp950"))

    official_raw = tmp_path / "official" / "Daily_2026_05_20.zip"
    mirror_raw = tmp_path / "mirror" / "Daily_2026_05_20.zip"
    write_raw(official_raw, 21000)
    write_raw(mirror_raw, 22000)

    out = tmp_path / "lake"
    status_dir = tmp_path / "parse_status"
    parse_rpt_file_to_parquet(
        official_raw,
        out_dir=out,
        products=["TX"],
        status_dir=status_dir,
        source_kind="taifex_official_recent",
        source_priority=20,
    )
    parse_rpt_file_to_parquet(
        mirror_raw,
        out_dir=out,
        products=["TX"],
        status_dir=status_dir,
        source_kind="mirror",
        source_priority=10,
    )

    tx = pl.read_parquet(out / "ticks/product=TX/year=2026/month=05/source_date=2026-05-20.parquet")
    assert tx["price"].to_list() == [21000.0]
    status = json.loads((status_dir / "source_date=2026-05-20.json").read_text(encoding="utf-8"))
    assert status["source_kind"] == "taifex_official_recent"
    assert status["source_priority"] == 20
