"""Extract USD-M history from data.binance.vision into the landing zone.

Run from repo root:
    python -m ingestion.extract_archive [--dataset NAME ...] [--backfill]
                                        [--nothing-new-exit-code N]

What is wanted, per dataset (binance_archive.py has the datasets):
- archive_um_klines_1d: every *USDT perpetual (dated contracts have a "_" and are left
  out). A contract new to the landing zone (every one with --backfill) is discovered by
  listing its files; a known one takes the days after its newest, while it is alive.
- archive_um_klines_1h, archive_um_funding_rate: contracts ever ranked within
  universe.DOWNLOAD_RANK, on the days the 1d klines have them.
- archive_um_metrics_1h: the days universe.metrics_days selects.
- archive_um_premium_index_1h: as the 1h klines.
- archive_spot_klines_1d: the spot pairs that may trade a ranked contract's coin
  (spot_candidates; dbt keeps the one whose price matches), on the contract's 1d days that
  the pair has in the archive.
- archive_um_book_depth_1h: as the metrics, from 2023 (the archive's first book depth).
What is there: the days the manifests' source files cover. A missing day is asked for again
only within RETRY_DAYS (all of them with --backfill, or for a contract with nothing yet),
so a file the archive never had costs a request a run for a month, not forever.

Which file: for a month before the current one the monthly file when the archive has
published it, else that month's daily files. Today is never asked for, yesterday only once
the archive has published it: the probe is PROBE_SYMBOL's file.

Batches: one per month of data with --backfill (bounded size), else one per dataset and
run. A batch is written only after all of its files are downloaded and verified, so a
failure (a checksum, a changed layout) leaves nothing behind; its id is the as-of moment,
a second after the previous batch's at least.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise
from pathlib import Path

from ingestion.batches import BatchWriter, complete_batches, read_manifest
from ingestion.binance_archive import (
    ARCHIVE_DATASETS,
    DAY_MS,
    HOUR_MS,
    KLINE_COLUMNS,
    SPOT_KLINES_1D,
    SPOT_ROOT,
    UM_BOOK_DEPTH,
    UM_FUNDING,
    UM_KLINES_1D,
    UM_KLINES_1H,
    UM_METRICS,
    UM_PREMIUM_INDEX_1H,
    UM_ROOT,
    ArchiveClient,
    ArchiveDataset,
    ArchiveFile,
    ArchiveSource,
    Record,
    fetch_file,
    period_days,
    period_of,
)
from ingestion.extract_cli import add_nothing_new_option, check_nothing_new_option
from ingestion.load_raw import SYMBOL_PATTERN
from ingestion.universe import Rank, ever_ranked, metrics_days, read_ranks

RETRY_DAYS = 40
ALIVE_DAYS = 15  # a contract with no 1d candle for this long is taken as delisted
WORKERS = 16  # parallel downloads; a backfill passes --workers
PROBE_SYMBOL = "BTCUSDT"
QUOTE = "USDT"
# Prefixes of contracts quoted per N coins, longest first (1000000MOGUSDT is not 1000...).
MULTIPLIER_PREFIXES = ("1000000", "1000", "1M")
BOOK_DEPTH_STATS = ("snapshots", "odd_snapshots", "missing_hours", "late_hours")


def month_of(day: date) -> str:
    return f"{day:%Y-%m}"


def shift_month(month: str, delta: int) -> str:
    index = int(month[:4]) * 12 + int(month[5:7]) - 1 + delta
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


def is_perpetual(symbol: str) -> bool:
    """A *USDT perpetual (dated contracts have "_"), with a name the loader accepts."""
    return symbol.endswith(QUOTE) and SYMBOL_PATTERN.fullmatch(symbol) is not None


def spot_candidates(contract: str) -> list[str]:
    """Spot pairs that may trade a contract's coin: the contract's own symbol, and for one
    quoted per 1000 or per million coins the symbol without the prefix (1000PEPEUSDT ->
    PEPEUSDT; 1000SATSUSDT trades on spot under its prefixed name)."""
    candidates = [contract]
    for prefix in MULTIPLIER_PREFIXES:
        if contract.startswith(prefix) and len(contract) > len(prefix) + len(QUOTE):
            candidates.append(contract[len(prefix) :])
            break
    return candidates


def covered_days(root: Path) -> dict[str, set[date]]:
    """Days per contract that the source files of complete batches cover."""
    covered: dict[str, set[date]] = defaultdict(set)
    for batch_dir in complete_batches(root):
        for symbol, files in read_manifest(batch_dir).get("sources", {}).items():
            for name in files:
                covered[symbol].update(period_days(period_of(name)))
    return covered


@dataclass(frozen=True)
class Request:
    symbol: str
    period: str  # YYYY-MM (monthly) or YYYY-MM-DD (daily)
    fallback: tuple[str, ...] = ()  # daily periods to ask for if the monthly file is missing


@dataclass
class Publication:
    """Whether the archive has published yesterday's daily file and last month's monthly
    file of a dataset; one probe each per run."""

    source: ArchiveSource
    today: date
    _seen: dict[str, bool] = field(default_factory=dict)

    def has(self, dataset: ArchiveDataset, period: str) -> bool:
        key = dataset.key(PROBE_SYMBOL, period)
        if key not in self._seen:
            self._seen[key] = self.source.get(key) is not None
        return self._seen[key]

    def daily_ready(self, dataset: ArchiveDataset, day: date) -> bool:
        return day < self.today - timedelta(days=1) or self.has(dataset, day.isoformat())

    def monthly_ready(self, dataset: ArchiveDataset, month: str) -> bool:
        current = month_of(self.today)
        if month >= current:
            return False
        return month < shift_month(current, -1) or self.has(dataset, month)


def plan(
    dataset: ArchiveDataset,
    wanted: dict[str, set[date]],
    covered: dict[str, set[date]],
    publication: Publication,
    backfill: bool,
) -> dict[str, list[Request]]:
    """Requests per month of data for the wanted days not covered yet."""
    today = publication.today
    oldest_retry = today - timedelta(days=RETRY_DAYS)
    by_month: dict[str, list[Request]] = defaultdict(list)
    for symbol in sorted(wanted):
        have = covered.get(symbol, set())
        missing = [
            d
            for d in sorted(wanted[symbol])
            if dataset.first_day <= d < today
            and d not in have
            and (backfill or not have or d >= oldest_retry)
        ]
        months: dict[str, list[date]] = defaultdict(list)
        for day in missing:
            months[month_of(day)].append(day)
        for month, days in months.items():
            daily = tuple(
                d.isoformat()
                for d in days
                if dataset.has_daily and publication.daily_ready(dataset, d)
            )
            if dataset.has_monthly and publication.monthly_ready(dataset, month):
                by_month[month].append(Request(symbol, month, daily))
            elif month < month_of(today) and dataset.has_monthly and not dataset.has_daily:
                continue  # funding: the month is not published yet, the API covers it
            elif daily:
                by_month[month] += [Request(symbol, p) for p in daily]
    return dict(by_month)


def download(
    source: ArchiveSource, dataset: ArchiveDataset, requests: list[Request]
) -> tuple[list[ArchiveFile], list[str]]:
    """Fetch every request (a missing monthly file falls back to its daily files); returns
    the files and the names of the ones the archive does not have."""

    def fetch(request: Request) -> tuple[Request, ArchiveFile | None]:
        return request, fetch_file(source, dataset, request.symbol, request.period)

    files: list[ArchiveFile] = []
    missing: list[str] = []
    retry: list[Request] = []
    thin: list[tuple[int, Request]] = []  # a monthly file's index, a day it lacks rows of
    with ThreadPoolExecutor(WORKERS) as pool:
        for request, found in pool.map(fetch, requests):
            if found is not None:
                files.append(found)
                index = len(files) - 1
                thin += [
                    (index, Request(request.symbol, d))
                    for d in thin_days(dataset, found, request.fallback)
                ]
            elif request.fallback:
                retry += [Request(request.symbol, p) for p in request.fallback]
            else:
                missing.append(dataset.key(request.symbol, request.period).rsplit("/", 1)[1])
        for request, found in pool.map(fetch, retry):
            if found is not None:
                files.append(found)
            else:
                missing.append(dataset.key(request.symbol, request.period).rsplit("/", 1)[1])
        for (index, _), (request, found) in zip(
            thin, pool.map(fetch, [r for _, r in thin]), strict=True
        ):
            monthly = files[index]
            keep = [r for r in monthly.records if day_of(dataset, r) != request.period]
            if found is not None and len(found.records) > len(monthly.records) - len(keep):
                files[index] = replace(monthly, records=keep)
                files.append(found)
    return files, missing


def day_of(dataset: ArchiveDataset, record: Record) -> str:
    return datetime.fromtimestamp(dataset.record_ms(record) / 1000, UTC).date().isoformat()


def thin_days(dataset: ArchiveDataset, found: ArchiveFile, wanted: Sequence[str]) -> list[str]:
    """Wanted days a monthly file has fewer rows of than a full day: it can lack whole days
    its daily files have (note 137). The first and the last day of its rows are partial
    when the contract lists or delists then, and stay. A daily file with more rows of the
    day takes the monthly file's rows of it."""
    if dataset.step_ms is None or not found.records:
        return []
    per_day = Counter(day_of(dataset, r) for r in found.records)
    first, last = min(per_day), max(per_day)
    full = DAY_MS // dataset.step_ms
    return [d for d in wanted if d not in (first, last) and per_day[d] < full]


def symbol_stats(dataset: ArchiveDataset, files: list[ArchiveFile]) -> dict[str, int]:
    """Manifest stats of one contract's file in a batch: rows and duplicate grid keys (the
    loader reconciles them), the time span, gaps on the grid, idle candles (count = 0, a
    delisted contract's tail, note 132) and the 5-minute grid of metrics (note 131)."""
    records = [r for f in files for r in f.records]
    unique = sorted({dataset.grid_key(r) for r in records})
    stats = {
        "rows": len(records),
        "duplicates": len(records) - len({dataset.row_key(r) for r in records}),
        "files": len(files),
        "first_ms": unique[0],
        "last_ms": unique[-1],
    }
    if dataset.step_ms is not None:
        stats["gaps"] = sum(1 for a, b in pairwise(unique) if b - a != dataset.step_ms)
    if dataset.columns == KLINE_COLUMNS:
        stats["idle"] = sum(1 for r in records if r["count"] == "0")
    if dataset.hour_rows_only:
        days = sum(len(period_days(f.period)) for f in files)
        for name in ("rows_5m", "duplicates_5m", "unordered_5m", "missing_5m"):
            stats[name] = sum(f.stats[name] for f in files)
        stats["missing_hours"] = 24 * days - len({k // HOUR_MS for k in unique})
    if dataset.first_snapshot_per_hour:
        for name in BOOK_DEPTH_STATS:
            stats[name] = sum(f.stats[name] for f in files)
    return stats


def write_batch(dataset: ArchiveDataset, files: list[ArchiveFile], as_of_ms: int) -> Path | None:
    """One batch of the files, a file per contract (records in period order)."""
    by_symbol: dict[str, list[ArchiveFile]] = defaultdict(list)
    for f in sorted(files, key=lambda f: (f.symbol, f.period)):
        if f.records:  # a contract with no row would not reconcile with RAW: left out
            by_symbol[f.symbol].append(f)
    writer = BatchWriter(dataset.output_dir, as_of_ms, dataset.name)
    for symbol, symbol_files in by_symbol.items():
        records = [r for f in symbol_files for r in f.records]
        sources = {f.file_name: f.sha256 for f in symbol_files}
        writer.add(symbol, records, symbol_stats(dataset, symbol_files), sources)
    return writer.finish()


def listed_days(
    source: ArchiveSource, dataset: ArchiveDataset, symbol: str, today: date
) -> set[date]:
    """Days the archive has for a contract: its monthly files, and the daily files of the
    months after the newest monthly one (none for a contract long gone)."""
    path = dataset.path.format(symbol=symbol)
    stem = dataset.stem.format(symbol=symbol)
    keys, _ = source.list_prefix(f"{dataset.root}/monthly/{path}/")
    months = sorted(period_of(k.rsplit("/", 1)[1]) for k in keys if k.endswith(".zip"))
    days = {d for m in months for d in period_days(m)}
    current = month_of(today)
    if not months:
        daily_months = [shift_month(current, -1), current]
    elif months[-1] >= shift_month(current, -3):
        last = months[-1]
        daily_months = [m for m in (shift_month(current, -i) for i in range(3, -1, -1)) if m > last]
    else:
        daily_months = []
    for month in daily_months:
        keys, _ = source.list_prefix(f"{dataset.root}/daily/{path}/{stem}-{month}")
        days |= {
            date.fromisoformat(period_of(k.rsplit("/", 1)[1])) for k in keys if k.endswith(".zip")
        }
    return {d for d in days if d < today}


def discover_symbols(source: ArchiveSource, backfill: bool) -> list[str]:
    """Perpetual contracts with a klines directory in the archive (daily, plus monthly with
    --backfill: a contract delisted before daily files began has monthly ones only)."""
    roots = ["daily", "monthly"] if backfill else ["daily"]
    names: set[str] = set()
    for root in roots:
        _, dirs = source.list_prefix(f"{UM_ROOT}/{root}/klines/")
        names |= {d.rstrip("/").rsplit("/", 1)[1] for d in dirs}
    return sorted(n for n in names if is_perpetual(n))


def discover_spot_pairs(source: ArchiveSource, backfill: bool) -> set[str]:
    """Spot pairs with a klines directory (daily, plus monthly with --backfill)."""
    roots = ["daily", "monthly"] if backfill else ["daily"]
    names: set[str] = set()
    for root in roots:
        _, dirs = source.list_prefix(f"{SPOT_ROOT}/{root}/klines/")
        names |= {d.rstrip("/").rsplit("/", 1)[1] for d in dirs}
    return names


def spot_wanted_days(
    source: ArchiveSource, ranks: list[Rank], today: date, backfill: bool
) -> dict[str, set[date]]:
    """Per spot pair: the 1d days of the ranked contracts it may trade, limited to the days
    the archive lists for a pair new to the landing zone (every pair with --backfill), and
    to the days from its first covered one for a known pair: a pair listed after its
    contract would otherwise ask for the contract's older days on every run."""
    contract_days = covered_days(UM_KLINES_1D.output_dir)
    pairs = discover_spot_pairs(source, backfill)
    wanted: dict[str, set[date]] = defaultdict(set)
    for contract in sorted(ever_ranked(ranks)):
        for pair in spot_candidates(contract):
            if pair in pairs and contract in contract_days and is_perpetual(pair):
                wanted[pair] |= contract_days[contract]
    covered = covered_days(SPOT_KLINES_1D.output_dir)
    to_list = [s for s in wanted if backfill or not covered.get(s)]
    listed = parallel_map(lambda s: listed_days(source, SPOT_KLINES_1D, s, today), to_list)
    result: dict[str, set[date]] = {}
    for pair, days in wanted.items():
        if pair in listed:
            days = days & listed[pair]
        else:
            first = min(covered[pair])
            days = {d for d in days if d >= first}
        if days:
            result[pair] = days
    return result


def parallel_map(func: Callable[[str], set[date]], items: Iterable[str]) -> dict[str, set[date]]:
    items = list(items)
    with ThreadPoolExecutor(WORKERS) as pool:
        return dict(zip(items, pool.map(func, items), strict=True))


def wanted_days(
    dataset: ArchiveDataset,
    source: ArchiveSource,
    ranks: list[Rank],
    today: date,
    backfill: bool,
) -> dict[str, set[date]]:
    if dataset is UM_KLINES_1D:
        covered = covered_days(dataset.output_dir)
        symbols = discover_symbols(source, backfill)
        to_list = [s for s in symbols if backfill or s not in covered]
        wanted = parallel_map(lambda s: listed_days(source, dataset, s, today), to_list)
        for symbol in symbols:
            if symbol in wanted or not covered.get(symbol):
                continue
            last = max(covered[symbol])
            if last >= today - timedelta(days=ALIVE_DAYS):
                wanted[symbol] = {last + timedelta(days=i) for i in range(1, (today - last).days)}
        return wanted
    if dataset in (UM_KLINES_1H, UM_FUNDING, UM_PREMIUM_INDEX_1H):
        listed = covered_days(UM_KLINES_1D.output_dir)
        return {s: listed[s] for s in ever_ranked(ranks) if s in listed}
    if dataset in (UM_METRICS, UM_BOOK_DEPTH):
        return metrics_days(ranks, dataset.first_day, today - timedelta(days=1))
    if dataset is SPOT_KLINES_1D:
        return spot_wanted_days(source, ranks, today, backfill)
    raise ValueError(f"Unknown dataset {dataset.name}")


def extract_dataset(
    source: ArchiveSource,
    dataset: ArchiveDataset,
    ranks: list[Rank],
    today: date,
    backfill: bool,
    next_as_of: Callable[[], int],
    refetch: frozenset[str] = frozenset(),
) -> list[Path]:
    print(f"\n== {dataset.name} ==")
    wanted = wanted_days(dataset, source, ranks, today, backfill)
    if not wanted:
        print("Nothing wanted (no contracts, or no universe yet)")
        return []
    covered = {
        symbol: {d for d in days if month_of(d) not in refetch}
        for symbol, days in covered_days(dataset.output_dir).items()
    }
    by_month = plan(dataset, wanted, covered, Publication(source, today), backfill)
    requests = sum(len(r) for r in by_month.values())
    print(f"{len(wanted)} contracts wanted, {requests} files to fetch in {len(by_month)} months")
    chunks = (
        [[r for m in sorted(by_month) for r in by_month[m]]]
        if not backfill
        else [by_month[m] for m in sorted(by_month)]
    )
    written: list[Path] = []
    missing_all: list[str] = []
    for chunk in chunks:
        if not chunk:
            continue
        files, missing = download(source, dataset, chunk)
        missing_all += missing
        batch_dir = write_batch(dataset, files, next_as_of()) if files else None
        if batch_dir is not None:
            manifest = read_manifest(batch_dir)
            rows = sum(s["rows"] for s in manifest["symbols"].values())
            print(
                f"  {batch_dir.name}: {len(files)} files, "
                f"{len(manifest['symbols'])} contracts, {rows} rows, missing {len(missing)}"
            )
            written.append(batch_dir)
    if missing_all:
        print(f"Not in the archive: {len(missing_all)} files, e.g. {sorted(missing_all)[:5]}")
    if not written:
        print("Nothing new: no batch written")
    return written


def as_of_clock() -> Callable[[], int]:
    """As-of moments for successive batches: now, and never the same second twice."""
    last = 0

    def next_as_of() -> int:
        nonlocal last
        now = int(time.time()) * 1000
        last = max(now, last + 1000)
        return last

    return next_as_of


def main(argv: Sequence[str] | None = None) -> int:
    global WORKERS
    by_name = {d.name: d for d in ARCHIVE_DATASETS}
    parser = argparse.ArgumentParser(description="Extract USD-M history from data.binance.vision.")
    parser.add_argument(
        "--dataset", action="append", choices=list(by_name), help="default: all, in order"
    )
    parser.add_argument(
        "--backfill", action="store_true", help="every missing day, a batch a month"
    )
    parser.add_argument(
        "--refetch-month",
        action="append",
        default=[],
        metavar="YYYY-MM",
        help="with --backfill: fetch the month again (a repair, e.g. of the days note 137 lacks)",
    )
    parser.add_argument("--workers", type=int, default=WORKERS, help=f"default {WORKERS}")
    add_nothing_new_option(parser, "no batch is written")
    args = parser.parse_args(argv)
    check_nothing_new_option(parser, args)
    if args.refetch_month and not args.backfill:
        parser.error("--refetch-month needs --backfill")
    WORKERS = args.workers

    datasets = [d for d in ARCHIVE_DATASETS if args.dataset is None or d.name in args.dataset]
    today = datetime.now(UTC).date()
    needs_universe = any(d is not UM_KLINES_1D for d in datasets)
    ranks = read_ranks() if needs_universe else []
    print(f"Archive extract as of {today}, {'backfill' if args.backfill else 'daily'} mode")
    next_as_of = as_of_clock()
    written: list[Path] = []
    with ArchiveClient() as source:
        for dataset in datasets:
            written += extract_dataset(
                source,
                dataset,
                ranks,
                today,
                args.backfill,
                next_as_of,
                frozenset(args.refetch_month),
            )
    print(f"\nBatches written: {len(written)}" + (f", last {written[-1].name}" if written else ""))
    return 0 if written else args.nothing_new_exit_code


if __name__ == "__main__":
    sys.exit(main())
