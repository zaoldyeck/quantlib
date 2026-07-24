"""TAIFEX daily RPT tick archive downloader and local data-lake builder.

This module intentionally bypasses PostgreSQL.  The canonical flow for the
long-history RPT mirror is:

    Google Drive public folder -> immutable raw zip archive -> Parquet tick lake
    -> DuckDB/Polars research views.

Raw files stay compressed.  Parsing reads each RPT from zip in streaming mode and
only materializes selected index-futures products into partitioned Parquet.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import shutil
import sys
import time
import zipfile
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time as dtime, timedelta
from html.parser import HTMLParser
from pathlib import Path
from threading import Lock
from typing import Iterable
from urllib.error import URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import duckdb
import polars as pl
from quantlib import paths


REPO_ROOT = paths.REPO
DEFAULT_ROOT_FOLDER_ID = "1mLvxQdqEQUty9EOeUQ33BoQcqxToM-SE"
DEFAULT_DATA_ROOT = REPO_ROOT / "data" / "taifex" / "rpt"
DEFAULT_RAW_DIR = DEFAULT_DATA_ROOT / "raw"
DEFAULT_LAKE_DIR = DEFAULT_DATA_ROOT / "lake"
DEFAULT_MANIFEST_PATH = DEFAULT_DATA_ROOT / "manifest.csv"
DEFAULT_JOURNAL_PATH = DEFAULT_DATA_ROOT / "download_journal.jsonl"
DEFAULT_DOWNLOADS_PATH = DEFAULT_DATA_ROOT / "downloads.csv"
DEFAULT_PARSE_STATUS_DIR = DEFAULT_DATA_ROOT / "parse_status"
DEFAULT_OFFICIAL_INTRADAY_DIR = REPO_ROOT / "data" / "taifex" / "intraday_raw" / "futures_sales"
DEFAULT_PRODUCTS = ("TX", "MTX", "TMF", "TE", "TF")
TAIPEI_ZONE = ZoneInfo("Asia/Taipei")
RPT_TITLE_RE = re.compile(r"^Daily_(\d{4})_(\d{2})_(\d{2})\.(zip|rpt)$", re.IGNORECASE)
PAYLOAD_SUFFIXES = (".rpt", ".csv")


class SourceUnavailableError(ValueError):
    """The mirror points to an upstream TAIFEX 404/unavailable payload."""


@dataclass(frozen=True)
class DriveEntry:
    title: str
    href: str
    drive_id: str | None
    kind: str


@dataclass(frozen=True)
class RptDriveFile:
    source_date: date
    title: str
    ext: str
    drive_file_id: str
    drive_folder_id: str
    listed_year: int
    duplicate_count: int = 1

    @property
    def download_url(self) -> str:
        return f"https://drive.google.com/uc?export=download&id={self.drive_file_id}"


@dataclass(frozen=True)
class DownloadResult:
    source_date: str
    title: str
    drive_file_id: str
    status: str
    local_path: str
    bytes: int
    sha256: str
    downloaded_at: str
    error: str = ""


@dataclass(frozen=True)
class ParseStatus:
    source_date: str
    source_file: str
    raw_path: str
    raw_size: int
    raw_mtime_ns: int
    source_kind: str
    source_priority: int
    products_requested: list[str]
    products_written: list[str]
    parquet_files: list[str]
    parsed_at: str


class EmbeddedFolderParser(HTMLParser):
    """Parse Google Drive `embeddedfolderview` entries.

    The public embedded view is deliberately simpler than the full Drive app and
    exposes all file/folder links in regular anchors for these RPT folders.
    """

    def __init__(self) -> None:
        super().__init__()
        self.entries: list[DriveEntry] = []
        self._href: str | None = None
        self._in_title = False
        self._title: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr = dict(attrs)
        if tag == "a" and attr.get("href"):
            self._href = attr["href"]
        if tag == "div" and "flip-entry-title" in (attr.get("class") or ""):
            self._in_title = True

    def handle_data(self, data: str) -> None:
        if self._in_title:
            title = data.strip()
            if title:
                self._title = title

    def handle_endtag(self, tag: str) -> None:
        if tag == "div" and self._in_title:
            self._in_title = False
            if self._title and self._href:
                drive_id, kind = parse_drive_href(self._href)
                self.entries.append(DriveEntry(self._title, self._href, drive_id, kind))
            self._title = None
            self._href = None


def parse_drive_href(href: str) -> tuple[str | None, str]:
    folder = re.search(r"/folders/([A-Za-z0-9_-]+)", href)
    if folder:
        return folder.group(1), "folder"
    file = re.search(r"/file/d/([A-Za-z0-9_-]+)", href)
    if file:
        return file.group(1), "file"
    return None, "unknown"


def parse_embedded_entries(html: str) -> list[DriveEntry]:
    parser = EmbeddedFolderParser()
    parser.feed(html)
    return parser.entries


def fetch_folder_entries(folder_id: str, timeout: int = 30) -> list[DriveEntry]:
    url = f"https://drive.google.com/embeddedfolderview?id={folder_id}#list"
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=timeout) as resp:
        html = resp.read().decode("utf-8", "replace")
    return parse_embedded_entries(html)


def _parse_rpt_title(title: str) -> tuple[date, str] | None:
    match = RPT_TITLE_RE.match(title)
    if not match:
        return None
    yyyy, mm, dd, ext = match.groups()
    return date(int(yyyy), int(mm), int(dd)), ext.lower()


def discover_rpt_files(
    root_folder_id: str = DEFAULT_ROOT_FOLDER_ID,
    *,
    start_year: int | None = None,
    end_year: int | None = None,
) -> list[RptDriveFile]:
    """Discover public Google Drive RPT files and de-duplicate year overlaps."""

    root_entries = fetch_folder_entries(root_folder_id)
    folders: list[tuple[int, str]] = []
    for entry in root_entries:
        if entry.kind == "folder" and entry.drive_id and entry.title.isdigit():
            year = int(entry.title)
            if start_year is not None and year < start_year:
                continue
            if end_year is not None and year > end_year:
                continue
            folders.append((year, entry.drive_id))

    discovered: list[RptDriveFile] = []
    for listed_year, folder_id in sorted(folders):
        for entry in fetch_folder_entries(folder_id):
            parsed = _parse_rpt_title(entry.title)
            if parsed is None or entry.kind != "file" or not entry.drive_id:
                continue
            source_date, ext = parsed
            if start_year is not None and source_date.year < start_year:
                continue
            if end_year is not None and source_date.year > end_year:
                continue
            discovered.append(
                RptDriveFile(
                    source_date=source_date,
                    title=entry.title,
                    ext=ext,
                    drive_file_id=entry.drive_id,
                    drive_folder_id=folder_id,
                    listed_year=listed_year,
                )
            )

    return dedupe_rpt_files(discovered)


def dedupe_rpt_files(files: Iterable[RptDriveFile]) -> list[RptDriveFile]:
    grouped: dict[date, list[RptDriveFile]] = {}
    for file in files:
        grouped.setdefault(file.source_date, []).append(file)

    selected: list[RptDriveFile] = []
    for _source_date, group in grouped.items():
        best = min(
            group,
            key=lambda f: (
                # If the mirror has both a direct .rpt and a corrupt .zip for
                # the same source date, keep the direct payload.
                0 if f.ext == "rpt" else 1,
                0 if f.listed_year == f.source_date.year else 1,
                abs(f.listed_year - f.source_date.year),
                f.listed_year,
                f.drive_file_id,
            ),
        )
        selected.append(
            RptDriveFile(
                source_date=best.source_date,
                title=best.title,
                ext=best.ext,
                drive_file_id=best.drive_file_id,
                drive_folder_id=best.drive_folder_id,
                listed_year=best.listed_year,
                duplicate_count=len(group),
            )
        )
    return sorted(selected, key=lambda f: (f.source_date, f.title))


def raw_path_for(file: RptDriveFile, raw_dir: Path = DEFAULT_RAW_DIR) -> Path:
    return raw_dir / f"year={file.source_date.year:04d}" / file.title


def write_discovery_manifest(files: list[RptDriveFile], path: Path = DEFAULT_MANIFEST_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for file in files:
        rows.append(
            {
                "source_date": file.source_date.isoformat(),
                "title": file.title,
                "ext": file.ext,
                "drive_file_id": file.drive_file_id,
                "drive_folder_id": file.drive_folder_id,
                "listed_year": file.listed_year,
                "duplicate_count": file.duplicate_count,
                "download_url": file.download_url,
                "local_path": str(raw_path_for(file)),
            }
        )
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        if rows:
            writer.writeheader()
            writer.writerows(rows)


def read_discovery_manifest(path: Path = DEFAULT_MANIFEST_PATH) -> list[RptDriveFile]:
    files: list[RptDriveFile] = []
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            files.append(
                RptDriveFile(
                    source_date=date.fromisoformat(row["source_date"]),
                    title=row["title"],
                    ext=row["ext"],
                    drive_file_id=row["drive_file_id"],
                    drive_folder_id=row["drive_folder_id"],
                    listed_year=int(row["listed_year"]),
                    duplicate_count=int(row.get("duplicate_count") or 1),
                )
            )
    return files


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def latest_safe_intraday_date(
    *,
    now: datetime | None = None,
    safe_after: str | None = None,
    allow_today: bool | None = None,
) -> date:
    local_now = now.astimezone(TAIPEI_ZONE) if now is not None else datetime.now(TAIPEI_ZONE)
    allow = (
        os.environ.get("QL_TAIFEX_INTRADAY_ALLOW_TODAY", "").lower() == "true"
        if allow_today is None
        else allow_today
    )
    safe_text = safe_after or os.environ.get("QL_TAIFEX_INTRADAY_SAFE_AFTER", "16:00:00")
    try:
        safe_time = dtime.fromisoformat(safe_text)
    except ValueError:
        safe_time = dtime(16, 0)
    return local_now.date() if allow or local_now.time() >= safe_time else local_now.date() - timedelta(days=1)


def _validate_raw_file(path: Path, ext: str) -> None:
    head = path.open("rb").read(512)
    normalized_head = head.lower().lstrip()
    if (
        b"<html" in normalized_head
        or b"<!doctype html" in normalized_head
        or normalized_head.startswith(b"<head")
    ):
        raise SourceUnavailableError(f"source returned HTML instead of {ext}: {path}")
    if ext == "zip":
        with zipfile.ZipFile(path) as zf:
            names = [name for name in zf.namelist() if name.lower().endswith(PAYLOAD_SUFFIXES)]
            if not names:
                raise ValueError(f"zip has no RPT/CSV tick payload: {path}")
            bad = zf.testzip()
            if bad is not None:
                raise ValueError(f"zip failed CRC at {bad}: {path}")


def _read_previous_downloads(path: Path = DEFAULT_DOWNLOADS_PATH) -> dict[tuple[str, str, str], DownloadResult]:
    if not path.exists():
        return {}
    results: dict[tuple[str, str, str], DownloadResult] = {}
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            result = DownloadResult(
                source_date=row["source_date"],
                title=row["title"],
                drive_file_id=row["drive_file_id"],
                status=row["status"],
                local_path=row["local_path"],
                bytes=int(row.get("bytes") or 0),
                sha256=row.get("sha256") or "",
                downloaded_at=row.get("downloaded_at") or "",
                error=row.get("error") or "",
            )
            results[(result.source_date, result.title, result.drive_file_id)] = result
    return results


def _should_retry_unavailable(source_date: date, retry_days: int) -> bool:
    if retry_days < 0:
        return True
    # The RPT mirror contains many calendar-date placeholders for weekends and
    # exchange holidays.  Keep recent unavailable files retryable for delayed
    # publication, but avoid permanent network churn for old non-trading dates.
    return source_date >= datetime.now().date() - timedelta(days=retry_days)


def download_one(
    file: RptDriveFile,
    *,
    raw_dir: Path = DEFAULT_RAW_DIR,
    force: bool = False,
    retries: int = 3,
    timeout: int = 120,
    prior_result: DownloadResult | None = None,
    unavailable_retry_days: int = 14,
) -> DownloadResult:
    local_path = raw_path_for(file, raw_dir)
    if local_path.exists() and not force:
        try:
            _validate_raw_file(local_path, file.ext)
            return DownloadResult(
                source_date=file.source_date.isoformat(),
                title=file.title,
                drive_file_id=file.drive_file_id,
                status="skipped",
                local_path=str(local_path),
                bytes=local_path.stat().st_size,
                sha256=_sha256_file(local_path),
                downloaded_at=_utc_now_iso(),
            )
        except Exception:
            local_path.unlink(missing_ok=True)

    if (
        prior_result is not None
        and not force
        and prior_result.status in {"source_unavailable", "source_unavailable_cached"}
        and not _should_retry_unavailable(file.source_date, unavailable_retry_days)
    ):
        return DownloadResult(
            source_date=file.source_date.isoformat(),
            title=file.title,
            drive_file_id=file.drive_file_id,
            status="source_unavailable_cached",
            local_path=str(local_path),
            bytes=0,
            sha256="",
            downloaded_at=_utc_now_iso(),
            error=prior_result.error,
        )

    local_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = local_path.with_name(local_path.name + f".tmp.{os.getpid()}")
    last_error = ""
    for attempt in range(1, retries + 1):
        try:
            req = Request(file.download_url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=timeout) as resp, tmp_path.open("wb") as out:
                while True:
                    chunk = resp.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
            _validate_raw_file(tmp_path, file.ext)
            tmp_path.replace(local_path)
            return DownloadResult(
                source_date=file.source_date.isoformat(),
                title=file.title,
                drive_file_id=file.drive_file_id,
                status="downloaded",
                local_path=str(local_path),
                bytes=local_path.stat().st_size,
                sha256=_sha256_file(local_path),
                downloaded_at=_utc_now_iso(),
            )
        except SourceUnavailableError as exc:
            tmp_path.unlink(missing_ok=True)
            return DownloadResult(
                source_date=file.source_date.isoformat(),
                title=file.title,
                drive_file_id=file.drive_file_id,
                status="source_unavailable",
                local_path=str(local_path),
                bytes=0,
                sha256="",
                downloaded_at=_utc_now_iso(),
                error=str(exc),
            )
        except (OSError, URLError, ValueError, zipfile.BadZipFile) as exc:
            last_error = f"attempt {attempt}/{retries}: {exc}"
            tmp_path.unlink(missing_ok=True)
            if attempt < retries:
                time.sleep(min(2**attempt, 10))

    return DownloadResult(
        source_date=file.source_date.isoformat(),
        title=file.title,
        drive_file_id=file.drive_file_id,
        status="error",
        local_path=str(local_path),
        bytes=0,
        sha256="",
        downloaded_at=_utc_now_iso(),
        error=last_error,
    )


def write_download_manifest(results: list[DownloadResult], path: Path = DEFAULT_DATA_ROOT / "downloads.csv") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(asdict(results[0]).keys()) if results else [
        "source_date",
        "title",
        "drive_file_id",
        "status",
        "local_path",
        "bytes",
        "sha256",
        "downloaded_at",
        "error",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for result in sorted(results, key=lambda r: (r.source_date, r.title)):
            writer.writerow(asdict(result))


def download_many(
    files: list[RptDriveFile],
    *,
    raw_dir: Path = DEFAULT_RAW_DIR,
    workers: int = 8,
    force: bool = False,
    journal_path: Path = DEFAULT_JOURNAL_PATH,
    downloads_path: Path = DEFAULT_DOWNLOADS_PATH,
    unavailable_retry_days: int = 14,
) -> list[DownloadResult]:
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    previous = _read_previous_downloads(downloads_path)
    lock = Lock()
    results: list[DownloadResult] = []
    with journal_path.open("a", encoding="utf-8") as journal, ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [
            pool.submit(
                download_one,
                file,
                raw_dir=raw_dir,
                force=force,
                prior_result=previous.get((file.source_date.isoformat(), file.title, file.drive_file_id)),
                unavailable_retry_days=unavailable_retry_days,
            )
            for file in files
        ]
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            with lock:
                journal.write(json.dumps(asdict(result), ensure_ascii=False) + "\n")
                journal.flush()
            if result.status == "error":
                print(f"[error] {result.title}: {result.error}", file=sys.stderr)
            else:
                print(f"[{result.status}] {result.title} {result.bytes:,} bytes")
    return results


def _parse_float(value: str) -> float | None:
    value = value.strip().replace(",", "")
    if not value or value == "-":
        return None
    return float(value)


def _parse_int(value: str) -> int | None:
    value = value.strip().replace(",", "")
    if not value or value == "-":
        return None
    return int(float(value))


def _source_date_from_path(path: Path) -> date:
    parsed = _parse_rpt_title(path.name)
    if parsed is None:
        raise ValueError(f"cannot infer source date from {path}")
    return parsed[0]


def _open_rpt_text(path: Path) -> io.TextIOBase:
    if path.suffix.lower() == ".zip":
        zf = zipfile.ZipFile(path)
        names = [name for name in zf.namelist() if name.lower().endswith(PAYLOAD_SUFFIXES)]
        if not names:
            zf.close()
            raise ValueError(f"zip has no RPT/CSV tick payload: {path}")
        raw = zf.open(names[0], "r")
        text = io.TextIOWrapper(raw, encoding="cp950", errors="replace", newline="")
        # Keep ZipFile alive through a close hook on the wrapper.
        original_close = text.close

        def close_with_zip() -> None:
            try:
                original_close()
            finally:
                zf.close()

        text.close = close_with_zip  # type: ignore[method-assign]
        return text
    return path.open("r", encoding="cp950", errors="replace", newline="")


def _tick_output_path(out_dir: Path, product: str, source_date: date) -> Path:
    return (
        out_dir
        / "ticks"
        / f"product={product}"
        / f"year={source_date.year:04d}"
        / f"month={source_date.month:02d}"
        / f"source_date={source_date.isoformat()}.parquet"
    )


def _parse_status_path(source_date: date, status_dir: Path = DEFAULT_PARSE_STATUS_DIR) -> Path:
    return status_dir / f"source_date={source_date.isoformat()}.json"


def _resolve_status_dir(out_dir: Path, status_dir: Path) -> Path:
    if status_dir == DEFAULT_PARSE_STATUS_DIR and out_dir != DEFAULT_LAKE_DIR:
        return out_dir.parent / "parse_status"
    return status_dir


def _raw_identity(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns


def _write_parse_status(
    raw_path: Path,
    *,
    source_date: date,
    products_requested: Iterable[str],
    parquet_files: Iterable[Path],
    status_dir: Path = DEFAULT_PARSE_STATUS_DIR,
    source_kind: str = "mirror",
    source_priority: int = 10,
) -> ParseStatus:
    parquet_paths = sorted(Path(path) for path in parquet_files)
    raw_size, raw_mtime_ns = _raw_identity(raw_path)
    status = ParseStatus(
        source_date=source_date.isoformat(),
        source_file=raw_path.name,
        raw_path=str(raw_path),
        raw_size=raw_size,
        raw_mtime_ns=raw_mtime_ns,
        source_kind=source_kind,
        source_priority=source_priority,
        products_requested=sorted({p.strip().upper() for p in products_requested}),
        products_written=sorted({path.parent.parent.parent.name.removeprefix("product=") for path in parquet_paths}),
        parquet_files=[str(path) for path in parquet_paths],
        parsed_at=_utc_now_iso(),
    )
    out = _parse_status_path(source_date, status_dir)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + f".tmp.{os.getpid()}")
    tmp.write_text(json.dumps(asdict(status), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(out)
    return status


def _read_parse_status(source_date: date, status_dir: Path = DEFAULT_PARSE_STATUS_DIR) -> ParseStatus | None:
    path = _parse_status_path(source_date, status_dir)
    if not path.exists():
        return None
    raw = json.loads(path.read_text(encoding="utf-8"))
    return ParseStatus(
        source_date=raw["source_date"],
        source_file=raw["source_file"],
        raw_path=raw["raw_path"],
        raw_size=int(raw["raw_size"]),
        raw_mtime_ns=int(raw["raw_mtime_ns"]),
        source_kind=raw.get("source_kind", "mirror"),
        source_priority=int(raw.get("source_priority", 10)),
        products_requested=list(raw["products_requested"]),
        products_written=list(raw["products_written"]),
        parquet_files=list(raw["parquet_files"]),
        parsed_at=raw["parsed_at"],
    )


def _parse_status_current(
    raw_path: Path,
    *,
    source_date: date,
    products: Iterable[str],
    status_dir: Path = DEFAULT_PARSE_STATUS_DIR,
    source_priority: int = 10,
) -> ParseStatus | None:
    status = _read_parse_status(source_date, status_dir)
    if status is None:
        return None
    requested = {p.strip().upper() for p in products}
    indexed = {p.strip().upper() for p in status.products_requested}
    if not requested.issubset(indexed):
        return None
    if not all(Path(path).exists() for path in status.parquet_files):
        return None
    if status.source_priority > source_priority:
        return status
    raw_size, raw_mtime_ns = _raw_identity(raw_path)
    if status.raw_path != str(raw_path) or status.raw_size != raw_size or status.raw_mtime_ns != raw_mtime_ns:
        return None
    return status


def parse_rpt_file_to_parquet(
    raw_path: Path,
    *,
    out_dir: Path = DEFAULT_LAKE_DIR,
    products: Iterable[str] = DEFAULT_PRODUCTS,
    status_dir: Path = DEFAULT_PARSE_STATUS_DIR,
    source_kind: str = "mirror",
    source_priority: int = 10,
    force: bool = False,
) -> list[Path]:
    """Parse one Daily_YYYY_MM_DD RPT archive into product-partitioned Parquet."""

    source_date = _source_date_from_path(raw_path)
    status_dir = _resolve_status_dir(out_dir, status_dir)
    product_set = {p.strip().upper() for p in products}
    target_paths = {product: _tick_output_path(out_dir, product, source_date) for product in product_set}
    if not force:
        status = _parse_status_current(
            raw_path,
            source_date=source_date,
            products=product_set,
            status_dir=status_dir,
            source_priority=source_priority,
        )
        if status is not None:
            return [Path(path) for path in status.parquet_files]

    rows_by_product: dict[str, list[dict[str, object]]] = {product: [] for product in product_set}
    with _open_rpt_text(raw_path) as text:
        reader = csv.reader(text)
        header = next(reader, None)
        if header is None or len(header) < 6:
            raise ValueError(f"invalid or empty RPT: {raw_path}")
        for row_number, row in enumerate(reader, start=2):
            if len(row) < 6:
                continue
            product = row[1].strip().upper()
            if product not in product_set:
                continue
            price = _parse_float(row[4])
            quantity = _parse_int(row[5])
            if price is None or quantity is None:
                continue
            trade_date_raw = row[0].strip()
            trade_time_raw = row[3].strip().zfill(6)
            try:
                trade_day = date(
                    int(trade_date_raw[0:4]),
                    int(trade_date_raw[4:6]),
                    int(trade_date_raw[6:8]),
                )
                trade_ts = datetime(
                    trade_day.year,
                    trade_day.month,
                    trade_day.day,
                    int(trade_time_raw[0:2]),
                    int(trade_time_raw[2:4]),
                    int(trade_time_raw[4:6]),
                )
            except (ValueError, IndexError):
                continue
            rows_by_product[product].append(
                {
                    "source_date": source_date,
                    "trade_date": trade_day,
                    "trade_ts": trade_ts,
                    "product": product,
                    "contract_month": row[2].strip(),
                    "price": float(price),
                    "quantity": int(quantity),
                    "near_month_price": _parse_float(row[6]) if len(row) > 6 else None,
                    "far_month_price": _parse_float(row[7]) if len(row) > 7 else None,
                    "opening_auction": row[8].strip() if len(row) > 8 else "",
                    "source_file": raw_path.name,
                    "file_row_number": row_number,
                }
            )

    written: list[Path] = []
    schema_overrides = {
        "source_date": pl.Date,
        "trade_date": pl.Date,
        "trade_ts": pl.Datetime,
        "product": pl.String,
        "contract_month": pl.String,
        "price": pl.Float64,
        "quantity": pl.Int64,
        "near_month_price": pl.Float64,
        "far_month_price": pl.Float64,
        "opening_auction": pl.String,
        "source_file": pl.String,
        "file_row_number": pl.Int64,
    }
    for product, rows in rows_by_product.items():
        out_path = target_paths[product]
        if not rows:
            if force:
                out_path.unlink(missing_ok=True)
            continue
        out_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = out_path.with_name(out_path.name + f".tmp.{os.getpid()}")
        frame = pl.from_dicts(
            rows,
            schema_overrides=schema_overrides,
            infer_schema_length=None,
        ).sort(["trade_ts", "file_row_number"])
        frame.write_parquet(tmp_path, compression="zstd", statistics=True)
        tmp_path.replace(out_path)
        written.append(out_path)
    written = sorted(written)
    _write_parse_status(
        raw_path,
        source_date=source_date,
        products_requested=product_set,
        parquet_files=written,
        status_dir=status_dir,
        source_kind=source_kind,
        source_priority=source_priority,
    )
    return written


def _parse_one_with_cache_info(
    raw_path: Path,
    *,
    out_dir: Path,
    products: tuple[str, ...],
    force: bool,
    status_dir: Path,
    source_kind: str = "mirror",
    source_priority: int = 10,
) -> tuple[list[Path], bool]:
    source_date = _source_date_from_path(raw_path)
    status_dir = _resolve_status_dir(out_dir, status_dir)
    cached = (
        not force
        and _parse_status_current(
            raw_path,
            source_date=source_date,
            products=products,
            status_dir=status_dir,
            source_priority=source_priority,
        )
        is not None
    )
    paths = parse_rpt_file_to_parquet(
        raw_path,
        out_dir=out_dir,
        products=products,
        force=force,
        status_dir=status_dir,
        source_kind=source_kind,
        source_priority=source_priority,
    )
    return paths, cached


def _parse_file_worker(args: tuple[str, str, tuple[str, ...], bool, str, str, int]) -> dict[str, object]:
    raw_path, out_dir, products, force, status_dir, source_kind, source_priority = args
    paths, cached = _parse_one_with_cache_info(
        Path(raw_path),
        out_dir=Path(out_dir),
        products=products,
        force=force,
        status_dir=Path(status_dir),
        source_kind=source_kind,
        source_priority=source_priority,
    )
    return {"paths": [str(path) for path in paths], "cached": cached}


def iter_raw_files_from_manifest(
    files: list[RptDriveFile],
    raw_dir: Path = DEFAULT_RAW_DIR,
    *,
    start_year: int | None = None,
    end_year: int | None = None,
) -> list[Path]:
    paths: list[Path] = []
    for file in files:
        if start_year is not None and file.source_date.year < start_year:
            continue
        if end_year is not None and file.source_date.year > end_year:
            continue
        path = raw_path_for(file, raw_dir)
        if path.exists():
            paths.append(path)
    return sorted(paths)


def parse_many_to_parquet(
    raw_files: list[Path],
    *,
    out_dir: Path = DEFAULT_LAKE_DIR,
    products: Iterable[str] = DEFAULT_PRODUCTS,
    status_dir: Path = DEFAULT_PARSE_STATUS_DIR,
    source_kind: str = "mirror",
    source_priority: int = 10,
    force: bool = False,
    limit: int | None = None,
    workers: int = 1,
) -> list[Path]:
    selected_files = raw_files[:limit] if limit else raw_files
    product_tuple = tuple(products)
    written: list[Path] = []
    if workers <= 1:
        for idx, raw_path in enumerate(selected_files, start=1):
            out_paths, cached = _parse_one_with_cache_info(
                raw_path,
                out_dir=out_dir,
                products=product_tuple,
                status_dir=status_dir,
                force=force,
                source_kind=source_kind,
                source_priority=source_priority,
            )
            written.extend(out_paths)
            label = "cached" if cached else "parsed"
            print(f"[{label}] {idx}/{len(selected_files)} {raw_path.name}: {len(out_paths)} product files")
        return written

    errors: list[str] = []
    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _parse_file_worker,
                (str(raw_path), str(out_dir), product_tuple, force, str(status_dir), source_kind, source_priority),
            ): raw_path
            for raw_path in selected_files
        }
        for idx, future in enumerate(as_completed(futures), start=1):
            raw_path = futures[future]
            try:
                worker_result = future.result()
                out_paths = [Path(path) for path in worker_result["paths"]]  # type: ignore[index]
                cached = bool(worker_result["cached"])
                written.extend(out_paths)
                label = "cached" if cached else "parsed"
                print(f"[{label}] {idx}/{len(selected_files)} {raw_path.name}: {len(out_paths)} product files")
            except Exception as exc:  # pragma: no cover - exercised in end-to-end runs.
                msg = f"{raw_path}: {exc}"
                errors.append(msg)
                print(f"[parse-error] {msg}", file=sys.stderr)
    if errors:
        raise RuntimeError(f"{len(errors)} RPT files failed to parse; first error: {errors[0]}")
    return written


def index_existing_parse_outputs(
    raw_files: list[Path],
    *,
    out_dir: Path = DEFAULT_LAKE_DIR,
    products: Iterable[str] = DEFAULT_PRODUCTS,
    status_dir: Path = DEFAULT_PARSE_STATUS_DIR,
) -> list[ParseStatus]:
    """Create parse-status files for an already-built Parquet lake.

    This is intentionally explicit: it is only for trusted migrations after a
    full parse has completed.  It makes future incremental parses idempotent
    even for years where newer products, such as TMF, did not yet exist.
    """

    product_tuple = tuple(p.strip().upper() for p in products)
    statuses: list[ParseStatus] = []
    for raw_path in raw_files:
        source_date = _source_date_from_path(raw_path)
        parquet_files = [
            path
            for product in product_tuple
            if (path := _tick_output_path(out_dir, product, source_date)).exists()
        ]
        if not parquet_files:
            continue
        statuses.append(
            _write_parse_status(
                raw_path,
                source_date=source_date,
                products_requested=product_tuple,
                parquet_files=parquet_files,
                status_dir=status_dir,
            )
        )
    print(f"[index-existing] wrote {len(statuses)} parse status files")
    return statuses


def iter_official_intraday_raw_files(
    official_dir: Path = DEFAULT_OFFICIAL_INTRADAY_DIR,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    prefer_csv: bool = True,
) -> list[Path]:
    """Return TAIFEX official free recent futures-sales tick archives.

    The official free download area keeps only a rolling recent window.  It uses
    the same `Daily_YYYY_MM_DD` payload schema as the long-history RPT mirror,
    so these files are a higher-priority recent overlay for missing or stale
    mirror dates.
    """

    candidates: list[tuple[date, int, Path]] = []
    for path in official_dir.glob("Dailydownload*/Daily_*.zip"):
        parsed = _parse_rpt_title(path.name)
        if parsed is None:
            continue
        source_date, _ext = parsed
        if start_date is not None and source_date < start_date:
            continue
        if end_date is not None and source_date > end_date:
            continue
        parent = path.parent.name
        priority = 0 if (prefer_csv and parent == "DailydownloadCSV") else 1
        candidates.append((source_date, priority, path))
    selected: dict[date, Path] = {}
    for source_date, _priority, path in sorted(candidates, key=lambda x: (x[0], x[1], str(x[2]))):
        selected.setdefault(source_date, path)
    return [selected[key] for key in sorted(selected)]


def _parse_timeframe_minutes(timeframe: str) -> int:
    match = re.fullmatch(r"(\d+)(m|min)", timeframe)
    if not match:
        raise ValueError(f"unsupported timeframe {timeframe}; use e.g. 1m, 5m, 15m, 60m")
    minutes = int(match.group(1))
    if minutes <= 0:
        raise ValueError(f"timeframe must be positive: {timeframe}")
    return minutes


def build_bars(
    *,
    lake_dir: Path = DEFAULT_LAKE_DIR,
    bars_dir: Path = DEFAULT_LAKE_DIR / "bars",
    timeframes: Iterable[str] = ("1m", "5m", "15m", "30m", "60m"),
    force: bool = False,
) -> list[Path]:
    """Build OHLCV bars from tick Parquet through DuckDB.

    Output is partitioned by timeframe/product/year/month for efficient local
    research scans.  Bar aggregation keeps actual contract_month separated.
    """

    tick_glob = str(lake_dir / "ticks" / "product=*" / "year=*" / "month=*" / "*.parquet")
    if not list((lake_dir / "ticks").glob("product=*/year=*/month=*/*.parquet")):
        raise FileNotFoundError(f"no tick parquet files under {lake_dir / 'ticks'}")

    con = duckdb.connect()
    outputs: list[Path] = []
    try:
        for timeframe in timeframes:
            minutes = _parse_timeframe_minutes(timeframe)
            out = bars_dir / f"timeframe={timeframe}"
            if out.exists():
                if force:
                    shutil.rmtree(out)
                else:
                    raise FileExistsError(f"{out} already exists; use --force to rebuild bars")
            out.mkdir(parents=True, exist_ok=True)
            query = f"""
                WITH ticks AS (
                    SELECT *
                    FROM read_parquet('{tick_glob}', hive_partitioning = true)
                ),
                bars AS (
                    SELECT
                        product,
                        contract_month,
                        time_bucket(INTERVAL '{minutes} minutes', trade_ts) AS bar_start,
                        first(price ORDER BY trade_ts, file_row_number) AS open,
                        max(price) AS high,
                        min(price) AS low,
                        last(price ORDER BY trade_ts, file_row_number) AS close,
                        sum(quantity)::BIGINT AS volume,
                        count(*)::BIGINT AS tick_count,
                        min(source_date) AS min_source_date,
                        max(source_date) AS max_source_date
                    FROM ticks
                    GROUP BY product, contract_month, bar_start
                )
                SELECT
                    product,
                    contract_month,
                    bar_start,
                    CAST(bar_start AS DATE) AS bar_date,
                    year(bar_start)::INTEGER AS year,
                    month(bar_start)::INTEGER AS month,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    tick_count,
                    min_source_date,
                    max_source_date
                FROM bars
                ORDER BY product, contract_month, bar_start
            """
            con.sql(
                f"""
                COPY ({query})
                TO '{out}'
                (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (product, year, month))
                """
            )
            outputs.append(out)
            print(f"[bars] {timeframe}: {out}")
    finally:
        con.close()
    return outputs


def verify_raw(files: list[RptDriveFile], raw_dir: Path = DEFAULT_RAW_DIR) -> dict[str, int]:
    summary = {"ok": 0, "missing": 0, "bad": 0}
    for file in files:
        path = raw_path_for(file, raw_dir)
        if not path.exists():
            summary["missing"] += 1
            continue
        try:
            _validate_raw_file(path, file.ext)
            summary["ok"] += 1
        except Exception as exc:
            summary["bad"] += 1
            print(f"[bad] {path}: {exc}", file=sys.stderr)
    return summary


def summarize_raw(files: list[RptDriveFile], raw_dir: Path = DEFAULT_RAW_DIR) -> dict[str, object]:
    sizes = []
    by_year: dict[int, int] = {}
    missing = 0
    for file in files:
        by_year[file.source_date.year] = by_year.get(file.source_date.year, 0) + 1
        path = raw_path_for(file, raw_dir)
        if path.exists():
            sizes.append(path.stat().st_size)
        else:
            missing += 1
    return {
        "files": len(files),
        "missing": missing,
        "downloaded": len(files) - missing,
        "bytes": sum(sizes),
        "gib": sum(sizes) / 1024 / 1024 / 1024,
        "start_date": min((f.source_date for f in files), default=None),
        "end_date": max((f.source_date for f in files), default=None),
        "by_year": by_year,
    }


def _load_or_discover(args: argparse.Namespace) -> list[RptDriveFile]:
    manifest = Path(args.manifest)
    if manifest.exists() and not args.refresh_manifest:
        files = read_discovery_manifest(manifest)
    else:
        files = discover_rpt_files(
            args.root_folder_id,
            start_year=args.start_year,
            end_year=args.end_year,
        )
        write_discovery_manifest(files, manifest)
    if args.start_year is not None or args.end_year is not None:
        files = [
            f
            for f in files
            if (args.start_year is None or f.source_date.year >= args.start_year)
            and (args.end_year is None or f.source_date.year <= args.end_year)
        ]
    if args.limit:
        files = files[: args.limit]
    return files


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="TAIFEX Daily RPT tick data-lake tool")
    parser.add_argument("--root-folder-id", default=DEFAULT_ROOT_FOLDER_ID)
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST_PATH))
    parser.add_argument("--raw-dir", default=str(DEFAULT_RAW_DIR))
    parser.add_argument("--lake-dir", default=str(DEFAULT_LAKE_DIR))
    parser.add_argument("--parse-status-dir", default=str(DEFAULT_PARSE_STATUS_DIR))
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--refresh-manifest", action="store_true")

    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("discover")

    download = sub.add_parser("download")
    download.add_argument("--workers", type=int, default=int(os.environ.get("QL_TAIFEX_RPT_WORKERS", "8")))
    download.add_argument("--force", action="store_true")
    download.add_argument(
        "--retry-unavailable-days",
        type=int,
        default=int(os.environ.get("QL_TAIFEX_RPT_RETRY_UNAVAILABLE_DAYS", "14")),
    )

    sub.add_parser("verify-raw")
    sub.add_parser("summary")

    parse = sub.add_parser("parse-ticks")
    parse.add_argument("--products", default=",".join(DEFAULT_PRODUCTS))
    parse.add_argument("--force", action="store_true")
    parse.add_argument("--workers", type=int, default=int(os.environ.get("QL_TAIFEX_RPT_PARSE_WORKERS", "4")))

    official = sub.add_parser("parse-official-intraday")
    official.add_argument("--products", default=",".join(DEFAULT_PRODUCTS))
    official.add_argument("--official-dir", default=str(DEFAULT_OFFICIAL_INTRADAY_DIR))
    official.add_argument("--start-date")
    official.add_argument("--end-date")
    official.add_argument("--allow-today", action="store_true")
    official.add_argument("--force", action="store_true")
    official.add_argument("--workers", type=int, default=int(os.environ.get("QL_TAIFEX_RPT_PARSE_WORKERS", "4")))

    bars = sub.add_parser("build-bars")
    bars.add_argument("--timeframes", default="1m,5m,15m,30m,60m")
    bars.add_argument("--force", action="store_true")

    index = sub.add_parser("index-existing")
    index.add_argument("--products", default=",".join(DEFAULT_PRODUCTS))

    sync = sub.add_parser("sync")
    sync.add_argument("--workers", type=int, default=int(os.environ.get("QL_TAIFEX_RPT_WORKERS", "8")))
    sync.add_argument("--parse-workers", type=int, default=int(os.environ.get("QL_TAIFEX_RPT_PARSE_WORKERS", "4")))
    sync.add_argument("--products", default=",".join(DEFAULT_PRODUCTS))
    sync.add_argument("--timeframes", default="1m,5m,15m,30m,60m")
    sync.add_argument("--force-download", action="store_true")
    sync.add_argument("--force-parse", action="store_true")
    sync.add_argument("--force-bars", action="store_true")
    sync.add_argument(
        "--retry-unavailable-days",
        type=int,
        default=int(os.environ.get("QL_TAIFEX_RPT_RETRY_UNAVAILABLE_DAYS", "14")),
    )

    args = parser.parse_args(argv)
    files = _load_or_discover(args)

    if args.command == "discover":
        print(json.dumps(summarize_raw(files, Path(args.raw_dir)), ensure_ascii=False, default=str, indent=2))
        return
    if args.command == "download":
        results = download_many(
            files,
            raw_dir=Path(args.raw_dir),
            workers=args.workers,
            force=args.force,
            unavailable_retry_days=args.retry_unavailable_days,
        )
        write_download_manifest(results)
        errors = sum(1 for result in results if result.status == "error")
        print(json.dumps({"files": len(results), "errors": errors}, ensure_ascii=False, indent=2))
        if errors:
            raise SystemExit(2)
        return
    if args.command == "verify-raw":
        print(json.dumps(verify_raw(files, Path(args.raw_dir)), ensure_ascii=False, indent=2))
        return
    if args.command == "summary":
        print(json.dumps(summarize_raw(files, Path(args.raw_dir)), ensure_ascii=False, default=str, indent=2))
        return
    if args.command == "parse-ticks":
        products = [item.strip().upper() for item in args.products.split(",") if item.strip()]
        raw_files = iter_raw_files_from_manifest(files, Path(args.raw_dir))
        written = parse_many_to_parquet(
            raw_files,
            out_dir=Path(args.lake_dir),
            products=products,
            status_dir=Path(args.parse_status_dir),
            force=args.force,
            limit=args.limit,
            workers=args.workers,
        )
        print(json.dumps({"raw_files": len(raw_files), "parquet_files": len(written)}, indent=2))
        return
    if args.command == "parse-official-intraday":
        products = [item.strip().upper() for item in args.products.split(",") if item.strip()]
        start_date = date.fromisoformat(args.start_date) if args.start_date else None
        end_date = date.fromisoformat(args.end_date) if args.end_date else latest_safe_intraday_date(allow_today=args.allow_today)
        raw_files = iter_official_intraday_raw_files(
            Path(args.official_dir),
            start_date=start_date,
            end_date=end_date,
        )
        written = parse_many_to_parquet(
            raw_files,
            out_dir=Path(args.lake_dir),
            products=products,
            status_dir=Path(args.parse_status_dir),
            source_kind="taifex_official_recent",
            source_priority=20,
            force=args.force,
            limit=args.limit,
            workers=args.workers,
        )
        print(json.dumps({"raw_files": len(raw_files), "parquet_files": len(written)}, indent=2))
        return
    if args.command == "index-existing":
        products = [item.strip().upper() for item in args.products.split(",") if item.strip()]
        raw_files = iter_raw_files_from_manifest(files, Path(args.raw_dir))
        statuses = index_existing_parse_outputs(
            raw_files,
            out_dir=Path(args.lake_dir),
            products=products,
            status_dir=Path(args.parse_status_dir),
        )
        print(json.dumps({"raw_files": len(raw_files), "indexed": len(statuses)}, indent=2))
        return
    if args.command == "build-bars":
        outputs = build_bars(
            lake_dir=Path(args.lake_dir),
            timeframes=[item.strip() for item in args.timeframes.split(",") if item.strip()],
            force=args.force,
        )
        print(json.dumps({"outputs": [str(path) for path in outputs]}, ensure_ascii=False, indent=2))
        return
    if args.command == "sync":
        products = [item.strip().upper() for item in args.products.split(",") if item.strip()]
        download_results = download_many(
            files,
            raw_dir=Path(args.raw_dir),
            workers=args.workers,
            force=args.force_download,
            unavailable_retry_days=args.retry_unavailable_days,
        )
        write_download_manifest(download_results)
        errors = sum(1 for result in download_results if result.status == "error")
        raw_summary = verify_raw(files, Path(args.raw_dir))
        if errors or raw_summary["bad"]:
            print(
                json.dumps(
                    {"download_errors": errors, "raw_summary": raw_summary},
                    ensure_ascii=False,
                    indent=2,
                )
            )
            raise SystemExit(2)
        raw_files = iter_raw_files_from_manifest(files, Path(args.raw_dir))
        written = parse_many_to_parquet(
            raw_files,
            out_dir=Path(args.lake_dir),
            products=products,
            status_dir=Path(args.parse_status_dir),
            force=args.force_parse,
            limit=args.limit,
            workers=args.parse_workers,
        )
        outputs = build_bars(
            lake_dir=Path(args.lake_dir),
            timeframes=[item.strip() for item in args.timeframes.split(",") if item.strip()],
            force=args.force_bars,
        )
        print(
            json.dumps(
                {
                    "downloads": len(download_results),
                    "download_errors": errors,
                    "raw_summary": raw_summary,
                    "raw_files": len(raw_files),
                    "parquet_files": len(written),
                    "bar_outputs": [str(path) for path in outputs],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    raise AssertionError(args.command)


if __name__ == "__main__":
    main()
