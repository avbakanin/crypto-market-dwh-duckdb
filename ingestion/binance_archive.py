"""USD-M futures history from the Binance public archive, data.binance.vision.

Layout (source notes 129-133): data/futures/um/{monthly,daily}/<path>/<file>.zip, a
.CHECKSUM (sha256) next to every zip, HTTP 404 for a file that does not exist (yet).
Monthly files reach the previous full month and can be published days late; daily files
reach yesterday, published 06:40-09:15 UTC the next day. Every zip holds one CSV; the
header is there in newer files only (klines), so a file is read by the dataset's column
list and a header, when present, must equal it. Records keep the CSV strings as they came,
keyed by column name.
"""

from __future__ import annotations

import csv
import hashlib
import io
import threading
import time
import zipfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from itertools import pairwise
from pathlib import Path
from typing import Protocol
from xml.etree import ElementTree

import requests

from ingestion.settings import LANDING_ZONE_DIR

ARCHIVE_URL = "https://data.binance.vision"
LISTING_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
UM_ROOT = "data/futures/um"
SPOT_ROOT = "data/spot"
HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS
METRICS_SLOTS_PER_DAY = 288  # one row every 5 minutes
# Spot files stamp times in microseconds from 2025-01-01 (note 143); anything this large is
# microseconds (milliseconds reach it in the year 5138).
MICROSECONDS_FROM = 100_000_000_000_000
NAIVE_TIME_COLUMNS = ("create_time", "timestamp")  # naive UTC strings, not Unix ms

Record = dict[str, str]


class ArchiveError(RuntimeError):
    """A file of the archive is not what it must be: checksum, layout or format."""


KLINE_COLUMNS = (
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
)
FUNDING_COLUMNS = ("calc_time", "funding_interval_hours", "last_funding_rate")
# Names as in the file; what they hold is in source note 130 (they are misnamed).
METRICS_COLUMNS = (
    "create_time",
    "symbol",
    "sum_open_interest",
    "sum_open_interest_value",
    "count_toptrader_long_short_ratio",
    "sum_toptrader_long_short_ratio",
    "count_long_short_ratio",
    "sum_taker_long_short_vol_ratio",
)
# A snapshot of the order book every ~30 s: per percentage band (-5 .. +5 % from the mid,
# the sign is the side), the cumulative depth in coins and in USDT (note 143). 10 bands
# (+-1 .. 5, written "-1") until early 2026, then 12 (+-0.20 added, written "-1.00").
BOOK_DEPTH_COLUMNS = ("timestamp", "percentage", "depth", "notional")
BOOK_DEPTH_LEVELS = (10, 12)


@dataclass(frozen=True)
class ArchiveDataset:
    """One series of the archive and the landing zone of its batches."""

    name: str  # landing zone directory
    path: str  # below monthly/ or daily/, with {symbol}
    stem: str  # file name before -<period>.zip, with {symbol}
    columns: tuple[str, ...]
    time_column: str
    first_day: date  # nothing older is published
    has_monthly: bool
    has_daily: bool
    step_ms: int | None  # grid step of a record (None: varies, funding)
    hour_rows_only: bool = False  # metrics: only the rows at minute 00 are kept
    first_snapshot_per_hour: bool = False  # book depth: only each hour's first snapshot
    root: str = UM_ROOT

    @property
    def output_dir(self) -> Path:
        return LANDING_ZONE_DIR / self.name

    def key(self, symbol: str, period: str) -> str:
        """Archive key of a file: period YYYY-MM is monthly, YYYY-MM-DD daily."""
        granularity = "monthly" if len(period) == 7 else "daily"
        path = self.path.format(symbol=symbol)
        stem = self.stem.format(symbol=symbol)
        return f"{self.root}/{granularity}/{path}/{stem}-{period}.zip"

    def record_ms(self, record: Record) -> int:
        """Time of a record in Unix ms UTC (create_time and timestamp are naive UTC
        strings; spot times are microseconds from 2025)."""
        value = record[self.time_column]
        if self.time_column in NAIVE_TIME_COLUMNS:
            parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
            return int(parsed.timestamp()) * 1000
        number = int(value)
        return number // 1000 if number >= MICROSECONDS_FROM else number

    def grid_key(self, record: Record) -> int:
        """Time of a record on its grid. Funding: the hour, calc_time lands a few ms after
        the settlement (note 132); book depth: the hour of its snapshot."""
        ms = self.record_ms(record)
        hourly = self.time_column == "calc_time" or self.first_snapshot_per_hour
        return ms // HOUR_MS * HOUR_MS if hourly else ms

    def row_key(self, record: Record) -> object:
        """Key of a row for duplicate counts; must match the loader's key_sql (datasets.py).
        Book depth: a snapshot has a row per band."""
        if self.first_snapshot_per_hour:
            return (record["timestamp"], record["percentage"])
        return self.grid_key(record)


UM_KLINES_1D = ArchiveDataset(
    name="archive_um_klines_1d",
    path="klines/{symbol}/1d",
    stem="{symbol}-1d",
    columns=KLINE_COLUMNS,
    time_column="open_time",
    first_day=date(2020, 1, 1),
    has_monthly=True,
    has_daily=True,
    step_ms=DAY_MS,
)
UM_KLINES_1H = ArchiveDataset(
    name="archive_um_klines_1h",
    path="klines/{symbol}/1h",
    stem="{symbol}-1h",
    columns=KLINE_COLUMNS,
    time_column="open_time",
    first_day=date(2020, 1, 1),
    has_monthly=True,
    has_daily=True,
    step_ms=HOUR_MS,
)
UM_FUNDING = ArchiveDataset(
    name="archive_um_funding_rate",
    path="fundingRate/{symbol}",
    stem="{symbol}-fundingRate",
    columns=FUNDING_COLUMNS,
    time_column="calc_time",
    first_day=date(2020, 1, 1),
    has_monthly=True,
    has_daily=False,  # the archive keeps funding monthly only: the API serves this month
    step_ms=None,
)
UM_METRICS = ArchiveDataset(
    name="archive_um_metrics_1h",
    path="metrics/{symbol}",
    stem="{symbol}-metrics",
    columns=METRICS_COLUMNS,
    time_column="create_time",
    first_day=date(2020, 9, 1),
    has_monthly=False,  # daily files only
    has_daily=True,
    step_ms=HOUR_MS,
    hour_rows_only=True,
)
UM_PREMIUM_INDEX_1H = ArchiveDataset(
    name="archive_um_premium_index_1h",
    path="premiumIndexKlines/{symbol}/1h",
    stem="{symbol}-1h",
    columns=KLINE_COLUMNS,  # volumes are 0; count is the number of 5 s samples (720)
    time_column="open_time",
    first_day=date(2020, 1, 1),
    has_monthly=True,
    has_daily=True,
    step_ms=HOUR_MS,
)
SPOT_KLINES_1D = ArchiveDataset(
    name="archive_spot_klines_1d",
    path="klines/{symbol}/1d",
    stem="{symbol}-1d",
    columns=KLINE_COLUMNS,
    time_column="open_time",
    first_day=date(2020, 1, 1),
    has_monthly=True,
    has_daily=True,
    step_ms=DAY_MS,
    root=SPOT_ROOT,
)
UM_BOOK_DEPTH = ArchiveDataset(
    name="archive_um_book_depth_1h",
    path="bookDepth/{symbol}",
    stem="{symbol}-bookDepth",
    columns=BOOK_DEPTH_COLUMNS,
    time_column="timestamp",
    first_day=date(2023, 1, 1),
    has_monthly=False,  # daily files only
    has_daily=True,
    step_ms=HOUR_MS,
    first_snapshot_per_hour=True,
)
ARCHIVE_DATASETS = (
    UM_KLINES_1D,
    UM_KLINES_1H,
    UM_FUNDING,
    UM_METRICS,
    UM_PREMIUM_INDEX_1H,
    SPOT_KLINES_1D,
    UM_BOOK_DEPTH,
)


def period_of(file_name: str) -> str:
    """YYYY-MM or YYYY-MM-DD from a file name like BTCUSDT-1h-2021-03.zip."""
    stem = file_name.removesuffix(".zip")
    parts = stem.split("-")
    if len(parts[-1]) == 2 and len(parts[-2]) == 2 and len(parts[-3]) == 4:
        return "-".join(parts[-3:])
    return "-".join(parts[-2:])


def period_days(period: str) -> list[date]:
    """The days a period covers: one for a daily file, the whole month for a monthly one."""
    if len(period) == 10:
        return [date.fromisoformat(period)]
    year, month = int(period[:4]), int(period[5:7])
    first = date(year, month, 1)
    following = date(year + month // 12, month % 12 + 1, 1)
    return [date.fromordinal(d) for d in range(first.toordinal(), following.toordinal())]


def verify_checksum(data: bytes, checksum_text: str, file_name: str) -> str:
    """sha256 of the zip, checked against its .CHECKSUM ("<sha256>  <file name>")."""
    parts = checksum_text.split()
    if len(parts) != 2 or parts[1] != file_name:
        raise ArchiveError(f"Unexpected checksum file for {file_name}: {checksum_text[:120]!r}")
    actual = hashlib.sha256(data).hexdigest()
    if actual != parts[0].lower():
        raise ArchiveError(f"Checksum mismatch for {file_name}: {actual} != {parts[0]}")
    return actual


def read_zip_csv(data: bytes, file_name: str) -> str:
    """The text of the single CSV in a zip."""
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        names = archive.namelist()
        if len(names) != 1 or not names[0].endswith(".csv"):
            raise ArchiveError(f"{file_name}: expected one CSV, found {names}")
        return archive.read(names[0]).decode("utf-8")


def read_rows(text: str, dataset: ArchiveDataset, file_name: str) -> list[list[str]]:
    """The CSV's rows, checked. A first row that does not start with a digit is a header and
    must equal the column list (a changed layout fails loudly)."""
    rows = [row for row in csv.reader(io.StringIO(text)) if row]
    if rows and not rows[0][0][:1].isdigit():
        if tuple(rows[0]) != dataset.columns:
            raise ArchiveError(f"{file_name}: unexpected header {rows[0]}")
        rows = rows[1:]
    width = len(dataset.columns)
    for number, row in enumerate(rows, start=1):
        if len(row) != width:
            raise ArchiveError(f"{file_name}: row {number} has {len(row)} fields, not {width}")
    return rows


def parse_csv(text: str, dataset: ArchiveDataset, file_name: str) -> list[Record]:
    """Rows keyed by the dataset's columns (read_rows)."""
    return [
        dict(zip(dataset.columns, row, strict=True)) for row in read_rows(text, dataset, file_name)
    ]


@dataclass(frozen=True)
class ArchiveFile:
    """One verified file: its records (metrics: the minute-00 rows, book depth: each hour's
    first snapshot) and the stats of the rows left out."""

    symbol: str
    period: str
    file_name: str
    sha256: str
    records: list[Record]
    stats: dict[str, int]


def metrics_slot_stats(dataset: ArchiveDataset, records: list[Record], days: int) -> dict[str, int]:
    """DQ of the 5-minute grid of a metrics file, before only minute 00 is kept (note 131)."""
    times = [dataset.record_ms(r) for r in records]
    slots = set(times)
    return {
        "rows_5m": len(times),
        "duplicates_5m": len(times) - len(slots),
        "unordered_5m": sum(1 for prev, curr in pairwise(times) if curr < prev),
        "missing_5m": METRICS_SLOTS_PER_DAY * days - len(slots),
    }


def book_depth_snapshots(
    rows: list[list[str]], file_name: str, days: int
) -> tuple[list[list[str]], dict[str, int]]:
    """The rows of each hour's first snapshot (all its bands, in file order) and the DQ of
    the file: snapshots, ones without 10 or 12 distinct bands (or with a band twice), hours
    with none, and hours whose first snapshot comes more than a minute after the hour.
    Timestamps stay strings ("YYYY-MM-DD HH:MM:SS": fixed width, so they sort as time and
    their first 13 characters are the hour): a day has ~34k rows and the backfill ~240k
    days, and parsing every row's time cost hours."""
    bands: dict[str, set[str]] = {}
    counts: dict[str, int] = {}
    first: dict[str, str] = {}
    for row in rows:
        stamp = row[0]
        seen = bands.get(stamp)
        if seen is None:
            if len(stamp) != 19 or stamp[13] != ":":
                raise ArchiveError(f"{file_name}: unexpected timestamp {stamp!r}")
            seen = bands[stamp] = set()
            counts[stamp] = 0
            hour = stamp[:13]
            if hour not in first or stamp < first[hour]:
                first[hour] = stamp
        seen.add(row[1])
        counts[stamp] += 1
    keep = set(first.values())
    stats = {
        "snapshots": len(bands),
        "odd_snapshots": sum(
            1
            for stamp, seen in bands.items()
            if len(seen) not in BOOK_DEPTH_LEVELS or counts[stamp] != len(seen)
        ),
        "missing_hours": 24 * days - len(first),
        "late_hours": sum(1 for stamp in first.values() if stamp[14:] > "01:00"),
    }
    return [row for row in rows if row[0] in keep], stats


class ArchiveSource(Protocol):
    """What the extract needs from the archive: the real client or a test fake."""

    def get(self, key: str) -> bytes | None:
        """Bytes of a key below the archive root; None when it does not exist (HTTP 404)."""
        ...

    def list_prefix(self, prefix: str) -> tuple[list[str], list[str]]:
        """Keys and sub-directories (common prefixes) directly below a prefix."""
        ...


def fetch_file(
    source: ArchiveSource, dataset: ArchiveDataset, symbol: str, period: str
) -> ArchiveFile | None:
    """Download, verify and parse one file; None when the archive does not have it."""
    key = dataset.key(symbol, period)
    file_name = key.rsplit("/", 1)[1]
    data = source.get(key)
    if data is None:
        return None
    checksum = source.get(f"{key}.CHECKSUM")
    if checksum is None:
        raise ArchiveError(f"{file_name} has no .CHECKSUM")
    sha256 = verify_checksum(data, checksum.decode("utf-8"), file_name)
    text = read_zip_csv(data, file_name)
    stats: dict[str, int] = {}
    if dataset.first_snapshot_per_hour:
        rows, stats = book_depth_snapshots(
            read_rows(text, dataset, file_name), file_name, len(period_days(period))
        )
        records = [dict(zip(dataset.columns, row, strict=True)) for row in rows]
        return ArchiveFile(symbol, period, file_name, sha256, records, stats)
    records = parse_csv(text, dataset, file_name)
    if dataset.hour_rows_only:
        stats = metrics_slot_stats(dataset, records, len(period_days(period)))
        records = [r for r in records if dataset.record_ms(r) % HOUR_MS == 0]
    return ArchiveFile(symbol, period, file_name, sha256, records, stats)


class ArchiveClient:
    """HTTP access to the archive: one Session per thread, retries on 5xx/429/network."""

    def __init__(self, timeout_s: float = 30.0, max_retries: int = 5) -> None:
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self._local = threading.local()

    def __enter__(self) -> ArchiveClient:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            self._local.session = session
        return session

    def _request(self, url: str, params: dict[str, str] | None = None) -> bytes | None:
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self._session().get(url, params=params, timeout=self.timeout_s)
            except (requests.ConnectionError, requests.Timeout):
                if attempt == self.max_retries:
                    raise
                time.sleep(2 ** (attempt - 1))
                continue
            if response.status_code == 404:
                return None
            if response.ok:
                return response.content
            if response.status_code in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                time.sleep(2 ** (attempt - 1))
                continue
            raise ArchiveError(f"HTTP {response.status_code} for {url}")
        raise AssertionError("unreachable")

    def get(self, key: str) -> bytes | None:
        return self._request(f"{ARCHIVE_URL}/{key}")

    def list_prefix(self, prefix: str) -> tuple[list[str], list[str]]:
        """S3 listing, 1000 keys a page (IsTruncated, NextMarker or the last key)."""
        keys: list[str] = []
        dirs: list[str] = []
        marker = ""
        while True:
            body = self._request(
                LISTING_URL, {"prefix": prefix, "delimiter": "/", "marker": marker}
            )
            if body is None:
                raise ArchiveError(f"Listing of {prefix} returned 404")
            page_keys, page_dirs, truncated, next_marker = parse_listing(body.decode("utf-8"))
            keys += page_keys
            dirs += page_dirs
            if not truncated:
                return keys, dirs
            marker = next_marker or max(page_keys + page_dirs)


def parse_listing(xml: str) -> tuple[list[str], list[str], bool, str | None]:
    """Keys, common prefixes, IsTruncated and NextMarker of one S3 ListBucket page."""
    root = ElementTree.fromstring(xml)
    ns = root.tag.split("}")[0] + "}" if root.tag.startswith("{") else ""
    keys = [e.text or "" for e in root.iter(f"{ns}Key")]
    dirs = [cp.findtext(f"{ns}Prefix") or "" for cp in root.iter(f"{ns}CommonPrefixes")]
    truncated = (root.findtext(f"{ns}IsTruncated") or "false").lower() == "true"
    return keys, dirs, truncated, root.findtext(f"{ns}NextMarker")
