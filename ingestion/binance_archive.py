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
HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS
METRICS_SLOTS_PER_DAY = 288  # one row every 5 minutes

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

    @property
    def output_dir(self) -> Path:
        return LANDING_ZONE_DIR / self.name

    def key(self, symbol: str, period: str) -> str:
        """Archive key of a file: period YYYY-MM is monthly, YYYY-MM-DD daily."""
        granularity = "monthly" if len(period) == 7 else "daily"
        path = self.path.format(symbol=symbol)
        stem = self.stem.format(symbol=symbol)
        return f"{UM_ROOT}/{granularity}/{path}/{stem}-{period}.zip"

    def record_ms(self, record: Record) -> int:
        """Time of a record in Unix ms UTC (create_time is a naive UTC string)."""
        value = record[self.time_column]
        if self.time_column == "create_time":
            parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
            return int(parsed.timestamp()) * 1000
        return int(value)

    def grid_key(self, record: Record) -> int:
        """Key of a record on its grid; must match the loader's key_sql (datasets.py).
        Funding: the hour, calc_time lands a few ms after the settlement (note 132)."""
        ms = self.record_ms(record)
        return ms // HOUR_MS * HOUR_MS if self.time_column == "calc_time" else ms


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
ARCHIVE_DATASETS = (UM_KLINES_1D, UM_KLINES_1H, UM_FUNDING, UM_METRICS)


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


def parse_csv(text: str, dataset: ArchiveDataset, file_name: str) -> list[Record]:
    """Rows keyed by the dataset's columns. A first row that does not start with a digit is
    a header and must equal the column list (a changed layout fails loudly)."""
    rows = [row for row in csv.reader(io.StringIO(text)) if row]
    if rows and not rows[0][0][:1].isdigit():
        if tuple(rows[0]) != dataset.columns:
            raise ArchiveError(f"{file_name}: unexpected header {rows[0]}")
        rows = rows[1:]
    width = len(dataset.columns)
    for number, row in enumerate(rows, start=1):
        if len(row) != width:
            raise ArchiveError(f"{file_name}: row {number} has {len(row)} fields, not {width}")
    return [dict(zip(dataset.columns, row, strict=True)) for row in rows]


@dataclass(frozen=True)
class ArchiveFile:
    """One verified file: its records (metrics: the minute-00 rows) and 5-minute stats."""

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
    records = parse_csv(read_zip_csv(data, file_name), dataset, file_name)
    stats: dict[str, int] = {}
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
