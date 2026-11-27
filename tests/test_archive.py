"""The archive extract: file checks, parsing, the plan of files, batches and their load."""

from __future__ import annotations

import dataclasses
import hashlib
import io
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from ingestion import extract_archive as ea
from ingestion import load_raw
from ingestion.batches import read_manifest
from ingestion.binance_archive import (
    FUNDING_COLUMNS,
    KLINE_COLUMNS,
    METRICS_COLUMNS,
    UM_FUNDING,
    UM_KLINES_1D,
    UM_KLINES_1H,
    UM_METRICS,
    ArchiveDataset,
    ArchiveError,
    fetch_file,
    parse_csv,
    parse_listing,
    period_days,
    period_of,
    verify_checksum,
)
from ingestion.datasets import UM_ARCHIVE_KLINES_1H, UM_ARCHIVE_METRICS_1H
from ingestion.warehouse import connect

TODAY = date(2026, 11, 27)
HOUR_MS = 3_600_000


def ms(text: str) -> int:
    return int(datetime.fromisoformat(text).replace(tzinfo=UTC).timestamp()) * 1000


def kline_row(open_ms: int, count: int = 10, step_ms: int = HOUR_MS) -> list[str]:
    return [
        str(open_ms), "1.0", "2.0", "0.5", "1.5", "100", str(open_ms + step_ms - 1),
        "150", str(count), "60", "90", "0",
    ]  # fmt: skip


def metrics_rows(day: date, slots: int = 288) -> list[list[str]]:
    start = ms(day.isoformat())
    return [
        [
            datetime.fromtimestamp((start + i * 300_000) / 1000, UTC).strftime("%Y-%m-%d %H:%M:%S"),
            "BTCUSDT",
            "10",
            "1000",
            "1.1",
            "1.2",
            "1.3",
            "0.9",
        ]  # fmt: skip
        for i in range(slots)
    ]


def make_zip(name: str, rows: list[list[str]], header: tuple[str, ...] | None) -> bytes:
    lines = ([",".join(header)] if header else []) + [",".join(r) for r in rows]
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name.replace(".zip", ".csv"), "\n".join(lines) + "\n")
    return buffer.getvalue()


class FakeArchive:
    """Serves zips and their .CHECKSUM from memory; lists keys like S3; records requests."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.requests: list[str] = []

    def put(
        self,
        dataset: ArchiveDataset,
        symbol: str,
        period: str,
        rows: list[list[str]],
        header: bool = True,
        checksum: str | None = None,
    ) -> None:
        key = dataset.key(symbol, period)
        name = key.rsplit("/", 1)[1]
        data = make_zip(name, rows, dataset.columns if header else None)
        self.objects[key] = data
        digest = checksum or hashlib.sha256(data).hexdigest()
        self.objects[f"{key}.CHECKSUM"] = f"{digest}  {name}\n".encode()

    def get(self, key: str) -> bytes | None:
        self.requests.append(key)
        return self.objects.get(key)

    def list_prefix(self, prefix: str) -> tuple[list[str], list[str]]:
        keys: list[str] = []
        dirs: set[str] = set()
        for key in self.objects:
            if key.startswith(prefix):
                rest = key[len(prefix) :]
                if "/" in rest:
                    dirs.add(prefix + rest.split("/")[0] + "/")
                else:
                    keys.append(key)
        return sorted(keys), sorted(dirs)


# --- files ---------------------------------------------------------------------------


def test_period_of_and_days() -> None:
    assert period_of("BTCUSDT-1h-2021-03.zip") == "2021-03"
    assert period_of("BTCUSDT-metrics-2021-03-05.zip") == "2021-03-05"
    assert period_of("1000PEPEUSDT-fundingRate-2024-02.zip") == "2024-02"
    assert len(period_days("2024-02")) == 29
    assert period_days("2026-12")[-1] == date(2026, 12, 31)
    assert period_days("2021-03-05") == [date(2021, 3, 5)]


def test_checksum_is_verified() -> None:
    data = b"zip bytes"
    good = hashlib.sha256(data).hexdigest()
    assert verify_checksum(data, f"{good}  a.zip\n", "a.zip") == good
    with pytest.raises(ArchiveError, match="mismatch"):
        verify_checksum(data + b"x", f"{good}  a.zip", "a.zip")
    with pytest.raises(ArchiveError, match="Unexpected checksum"):
        verify_checksum(data, f"{good}  b.zip", "a.zip")


def test_csv_with_and_without_header_reads_the_same() -> None:
    row = kline_row(ms("2020-01-01"))
    with_header = "\n".join([",".join(KLINE_COLUMNS), ",".join(row)])
    without = ",".join(row) + "\n\n"
    assert parse_csv(with_header, UM_KLINES_1H, "f") == parse_csv(without, UM_KLINES_1H, "f")
    assert parse_csv(without, UM_KLINES_1H, "f")[0]["count"] == "10"


def test_changed_layout_fails() -> None:
    with pytest.raises(ArchiveError, match="unexpected header"):
        parse_csv("calc_time,funding_rate\n1,2", UM_FUNDING, "f")
    with pytest.raises(ArchiveError, match="fields"):
        parse_csv("1,8", UM_FUNDING, "f")
    assert parse_csv(",".join(FUNDING_COLUMNS) + "\n1,8,0.0001", UM_FUNDING, "f") == [
        {"calc_time": "1", "funding_interval_hours": "8", "last_funding_rate": "0.0001"}
    ]


def test_fetch_file_checks_checksum_and_missing() -> None:
    archive = FakeArchive()
    archive.put(
        UM_KLINES_1H, "BTCUSDT", "2020-01", [kline_row(ms("2020-01-01"))], checksum="0" * 64
    )
    with pytest.raises(ArchiveError, match="mismatch"):
        fetch_file(archive, UM_KLINES_1H, "BTCUSDT", "2020-01")
    assert fetch_file(archive, UM_KLINES_1H, "BTCUSDT", "2020-02") is None
    archive.put(UM_KLINES_1H, "ETHUSDT", "2020-01", [kline_row(ms("2020-01-01"))])
    del archive.objects[UM_KLINES_1H.key("ETHUSDT", "2020-01") + ".CHECKSUM"]
    with pytest.raises(ArchiveError, match="no .CHECKSUM"):
        fetch_file(archive, UM_KLINES_1H, "ETHUSDT", "2020-01")


def test_metrics_keep_minute_zero_and_count_the_five_minute_grid() -> None:
    # Note 131: rows repeated and out of order; only the hour rows reach the landing zone.
    rows = metrics_rows(date(2021, 3, 1))
    rows = rows + rows  # every row twice
    rows[5], rows[6] = rows[6], rows[5]
    del rows[20]  # minute 40 of hour 1 once only: still one slot
    archive = FakeArchive()
    archive.put(UM_METRICS, "BTCUSDT", "2021-03-01", rows[:-12])  # the last hour lost once
    found = fetch_file(archive, UM_METRICS, "BTCUSDT", "2021-03-01")
    assert found is not None
    assert len(found.records) == 47  # 24 hours, twice, but the last hour once
    assert all(r["create_time"].endswith(":00:00") for r in found.records)
    assert found.stats == {
        "rows_5m": 575 - 12,
        "duplicates_5m": 575 - 12 - 288,
        "unordered_5m": 2,  # the swap, and the restart of the repeated day
        "missing_5m": 0,
    }


def test_parse_listing() -> None:
    xml = (
        '<?xml version="1.0"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        "<Prefix>data/futures/um/daily/klines/</Prefix><IsTruncated>true</IsTruncated>"
        "<Contents><Key>data/x.zip</Key></Contents>"
        "<CommonPrefixes><Prefix>data/futures/um/daily/klines/BTCUSDT/</Prefix></CommonPrefixes>"
        "<NextMarker>data/futures/um/daily/klines/BTCUSDT/</NextMarker></ListBucketResult>"
    )
    assert parse_listing(xml) == (
        ["data/x.zip"],
        ["data/futures/um/daily/klines/BTCUSDT/"],
        True,
        "data/futures/um/daily/klines/BTCUSDT/",
    )


# --- plan ----------------------------------------------------------------------------


def publication(archive: FakeArchive, today: date = TODAY) -> ea.Publication:
    return ea.Publication(archive, today)


def test_plan_takes_monthly_files_for_past_months_and_daily_ones_after() -> None:
    archive = FakeArchive()
    archive.put(UM_KLINES_1H, "BTCUSDT", "2026-11-26", [])  # yesterday is published
    archive.put(UM_KLINES_1H, "BTCUSDT", "2026-10", [])  # and last month
    wanted = {"ETHUSDT": {date(2026, 9, 30), date(2026, 10, 1), date(2026, 11, 26), TODAY}}
    by_month = ea.plan(UM_KLINES_1H, wanted, {}, publication(archive), backfill=True)
    assert by_month == {
        "2026-09": [ea.Request("ETHUSDT", "2026-09", ("2026-09-30",))],
        "2026-10": [ea.Request("ETHUSDT", "2026-10", ("2026-10-01",))],
        "2026-11": [ea.Request("ETHUSDT", "2026-11-26")],  # never today
    }


def test_plan_waits_for_publication_and_uses_daily_files_meanwhile() -> None:
    archive = FakeArchive()  # neither yesterday nor last month is published yet
    wanted = {"ETHUSDT": {date(2026, 10, 31), date(2026, 11, 25), date(2026, 11, 26)}}
    by_month = ea.plan(UM_KLINES_1H, wanted, {}, publication(archive), backfill=False)
    assert by_month == {
        "2026-10": [ea.Request("ETHUSDT", "2026-10-31")],
        "2026-11": [ea.Request("ETHUSDT", "2026-11-25")],
    }
    funding = ea.plan(
        UM_FUNDING, {"ETHUSDT": {date(2026, 10, 31)}}, {}, publication(archive), False
    )
    assert funding == {}  # no daily funding files: the API serves the month until then


def test_plan_skips_covered_days_and_retries_only_recent_ones() -> None:
    archive = FakeArchive()
    archive.put(UM_METRICS, "BTCUSDT", "2026-11-26", [])
    days = {date(2026, 9, 1), date(2026, 11, 1), date(2026, 11, 2), date(2026, 11, 26)}
    covered = {"ETHUSDT": {date(2026, 11, 2)}}
    daily = ea.plan(UM_METRICS, {"ETHUSDT": days}, covered, publication(archive), backfill=False)
    assert [r.period for rs in daily.values() for r in rs] == ["2026-11-01", "2026-11-26"]
    full = ea.plan(UM_METRICS, {"ETHUSDT": days}, covered, publication(archive), backfill=True)
    assert [r.period for rs in full.values() for r in rs] == [
        "2026-09-01",
        "2026-11-01",
        "2026-11-26",
    ]
    # a contract with nothing yet gets its whole history in daily mode too
    new = ea.plan(UM_METRICS, {"SOLUSDT": days}, covered, publication(archive), backfill=False)
    assert len([r for rs in new.values() for r in rs]) == 4


def test_plan_ignores_days_before_the_archive_begins() -> None:
    wanted = {"BTCUSDT": {date(2020, 8, 31), date(2020, 9, 1)}}
    by_month = ea.plan(UM_METRICS, wanted, {}, publication(FakeArchive()), backfill=True)
    assert by_month == {"2020-09": [ea.Request("BTCUSDT", "2020-09-01")]}


# --- download, batch, load -----------------------------------------------------------


@pytest.fixture
def landing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Archive datasets with their landing zones below tmp_path."""
    for name in ("UM_KLINES_1D", "UM_KLINES_1H", "UM_FUNDING", "UM_METRICS"):
        original = getattr(ea, name)
        monkeypatch.setattr(ea, name, _relocated(original, tmp_path))
    return tmp_path


def _relocated(dataset: ArchiveDataset, root: Path) -> ArchiveDataset:
    class Relocated(ArchiveDataset):
        @property
        def output_dir(self) -> Path:
            return root / self.name

    return Relocated(**{f.name: getattr(dataset, f.name) for f in dataclasses.fields(dataset)})


def test_missing_monthly_file_falls_back_to_daily_files(landing: Path) -> None:
    dataset = ea.UM_KLINES_1H
    archive = FakeArchive()
    archive.put(dataset, "BTCUSDT", "2026-09-30", [kline_row(ms("2026-09-30"))])
    requests = [ea.Request("BTCUSDT", "2026-09", ("2026-09-29", "2026-09-30"))]
    files, missing = ea.download(archive, dataset, requests)
    assert [f.period for f in files] == ["2026-09-30"]
    assert missing == ["BTCUSDT-1h-2026-09-29.zip"]


def test_days_a_monthly_file_lacks_come_from_daily_files(landing: Path) -> None:
    # Note 137: monthly files of 2022-02 lack 26 and 27 Feb that their daily files have.
    dataset = ea.UM_KLINES_1H
    archive = FakeArchive()

    def hours(day: str, count: int = 24) -> list[list[str]]:
        return [kline_row(ms(day) + i * HOUR_MS) for i in range(count)]

    days = [f"2022-02-{d:02d}" for d in range(24, 29)]
    monthly = hours(days[0], 12) + hours(days[1]) + hours(days[3], 1) + hours(days[4], 5)
    archive.put(dataset, "XRPUSDT", "2022-02", monthly)
    archive.put(dataset, "XRPUSDT", days[2], hours(days[2]))
    archive.put(dataset, "XRPUSDT", days[3], hours(days[3]))
    files, missing = ea.download(archive, dataset, [ea.Request("XRPUSDT", "2022-02", tuple(days))])
    assert missing == []
    assert [f.period for f in files] == ["2022-02", days[2], days[3]]
    # the monthly file gives up its one row of the 27th; the first and last days stay partial
    assert len(files[0].records) == 12 + 24 + 5
    fetched = [k.rsplit("/", 1)[1] for k in archive.requests if not k.endswith("CHECKSUM")]
    assert set(fetched) == {f"XRPUSDT-1h-{p}.zip" for p in ("2022-02", days[2], days[3])}


def test_a_daily_file_no_fuller_than_the_monthly_one_is_dropped(landing: Path) -> None:
    dataset = ea.UM_KLINES_1D
    archive = FakeArchive()
    rows = [kline_row(ms(f"2022-02-{d:02d}"), step_ms=86_400_000) for d in (1, 3)]
    archive.put(dataset, "XRPUSDT", "2022-02", rows)
    archive.put(dataset, "XRPUSDT", "2022-02-02", [])  # the daily file is empty too
    request = ea.Request("XRPUSDT", "2022-02", ("2022-02-01", "2022-02-02", "2022-02-03"))
    files, missing = ea.download(archive, dataset, [request])
    assert [f.period for f in files] == ["2022-02"] and missing == []


def test_batch_records_sources_and_loads_reconciled(landing: Path, tmp_path: Path) -> None:
    dataset = ea.UM_KLINES_1H
    archive = FakeArchive()
    start = ms("2020-01-01")
    jan = [kline_row(start + i * HOUR_MS) for i in range(24)]
    jan[-1] = kline_row(start + 23 * HOUR_MS, count=0)  # a delisted contract's idle candle
    archive.put(dataset, "FTTUSDT", "2020-01", jan, header=False)
    archive.put(dataset, "FTTUSDT", "2020-02-01", [kline_row(start + 26 * HOUR_MS)] * 2)
    files, _ = ea.download(
        archive, dataset, [ea.Request("FTTUSDT", "2020-01"), ea.Request("FTTUSDT", "2020-02-01")]
    )
    batch_dir = ea.write_batch(dataset, files, ms("2026-11-27"))
    assert batch_dir is not None and batch_dir.name == "20261127T000000Z"
    manifest = read_manifest(batch_dir)
    assert manifest["symbols"]["FTTUSDT"] == {
        "rows": 26,
        "duplicates": 1,
        "files": 2,
        "first_ms": start,
        "last_ms": start + 26 * HOUR_MS,
        "gaps": 1,
        "idle": 1,
    }
    assert set(manifest.get("sources", {})["FTTUSDT"]) == {
        "FTTUSDT-1h-2020-01.zip",
        "FTTUSDT-1h-2020-02-01.zip",
    }
    assert ea.covered_days(dataset.output_dir)["FTTUSDT"] >= {date(2020, 1, 31), date(2020, 2, 1)}

    raw = dataclasses.replace(UM_ARCHIVE_KLINES_1H, landing_dir=dataset.output_dir)
    with connect(tmp_path / "w.duckdb") as con:
        load_raw.ensure_raw_objects(con)
        assert load_raw.load_batch(con, raw, batch_dir, manifest) == []
        assert con.execute(f"select count(*) from {raw.table}").fetchall() == [(26,)]


def test_metrics_batch_reconciles_with_its_duplicates(landing: Path, tmp_path: Path) -> None:
    dataset = ea.UM_METRICS
    archive = FakeArchive()
    rows = metrics_rows(date(2021, 3, 1))
    archive.put(dataset, "BTCUSDT", "2021-03-01", rows + rows)
    files, _ = ea.download(archive, dataset, [ea.Request("BTCUSDT", "2021-03-01")])
    batch_dir = ea.write_batch(dataset, files, ms("2026-11-27"))
    assert batch_dir is not None
    manifest = read_manifest(batch_dir)
    stats = manifest["symbols"]["BTCUSDT"]
    assert (stats["rows"], stats["duplicates"], stats["missing_hours"]) == (48, 24, 0)
    assert (stats["rows_5m"], stats["duplicates_5m"], stats["missing_5m"]) == (576, 288, 0)
    raw = dataclasses.replace(UM_ARCHIVE_METRICS_1H, landing_dir=dataset.output_dir)
    with connect(tmp_path / "w.duckdb") as con:
        load_raw.ensure_raw_objects(con)
        assert load_raw.load_batch(con, raw, batch_dir, manifest) == []


def test_empty_file_leaves_the_contract_out(landing: Path) -> None:
    dataset = ea.UM_KLINES_1D
    archive = FakeArchive()
    archive.put(dataset, "AUSDT", "2020-01", [])
    archive.put(dataset, "BUSDT", "2020-01", [kline_row(ms("2020-01-01"), step_ms=86_400_000)])
    files, _ = ea.download(
        archive, dataset, [ea.Request("AUSDT", "2020-01"), ea.Request("BUSDT", "2020-01")]
    )
    batch_dir = ea.write_batch(dataset, files, ms("2026-11-27"))
    assert batch_dir is not None
    assert set(read_manifest(batch_dir)["symbols"]) == {"BUSDT"}


def test_as_of_clock_never_repeats_a_second() -> None:
    clock = ea.as_of_clock()
    first, second = clock(), clock()
    assert second - first >= 1000 and first % 1000 == 0


# --- discovery -----------------------------------------------------------------------


def test_discovery_lists_perpetuals_and_their_days(landing: Path) -> None:
    dataset = ea.UM_KLINES_1D
    archive = FakeArchive()
    archive.put(dataset, "BTCUSDT", "2026-09", [])
    archive.put(dataset, "BTCUSDT", "2026-11-25", [])
    archive.put(dataset, "BTCUSDT_250328", "2025-01", [])  # dated contract
    archive.put(dataset, "SRMUSDT", "2022-11", [])  # long gone: no daily listing
    archive.put(dataset, "NEWUSDT", "2026-11-26", [])  # listed this month
    assert ea.discover_symbols(archive, backfill=True) == ["BTCUSDT", "NEWUSDT", "SRMUSDT"]
    btc = ea.listed_days(archive, dataset, "BTCUSDT", TODAY)
    assert date(2026, 9, 30) in btc and date(2026, 11, 25) in btc and date(2026, 10, 1) not in btc
    assert ea.listed_days(archive, dataset, "NEWUSDT", TODAY) == {date(2026, 11, 26)}
    listed_srm = ea.listed_days(archive, dataset, "SRMUSDT", TODAY)
    assert min(listed_srm) == date(2022, 11, 1) and max(listed_srm) == date(2022, 11, 30)


def test_known_contracts_take_the_days_after_their_newest_while_alive(landing: Path) -> None:
    dataset = ea.UM_KLINES_1D
    archive = FakeArchive()
    day = 86_400_000
    archive.put(dataset, "BTCUSDT", "2026-11-20", [kline_row(ms("2026-11-20"), step_ms=day)])
    archive.put(dataset, "OLDUSDT", "2026-10-01", [kline_row(ms("2026-10-01"), step_ms=day)])
    files, _ = ea.download(
        archive,
        dataset,
        [ea.Request("BTCUSDT", "2026-11-20"), ea.Request("OLDUSDT", "2026-10-01")],
    )
    ea.write_batch(dataset, files, ms("2026-11-21"))
    wanted = ea.wanted_days(dataset, archive, [], TODAY, backfill=False)
    assert wanted == {"BTCUSDT": {date(2026, 11, d) for d in range(21, 27)}}  # OLDUSDT: gone


def test_hourly_klines_and_funding_follow_the_1d_days_of_ranked_contracts(landing: Path) -> None:
    archive = FakeArchive()
    day = 86_400_000
    for symbol in ("BTCUSDT", "XUSDT"):
        archive.put(
            ea.UM_KLINES_1D, symbol, "2026-11-20", [kline_row(ms("2026-11-20"), step_ms=day)]
        )
    files, _ = ea.download(
        archive,
        ea.UM_KLINES_1D,
        [ea.Request("BTCUSDT", "2026-11-20"), ea.Request("XUSDT", "2026-11-20")],
    )
    ea.write_batch(ea.UM_KLINES_1D, files, ms("2026-11-21"))
    ranks = [(date(2026, 11, 22), "BTCUSDT", 1)]
    assert ea.wanted_days(ea.UM_KLINES_1H, archive, ranks, TODAY, False) == {
        "BTCUSDT": {date(2026, 11, 20)}
    }
    assert ea.wanted_days(ea.UM_FUNDING, archive, [], TODAY, False) == {}


def test_metric_columns_are_the_files() -> None:
    # Misnamed in the file (note 130): mapped in staging, kept by name here.
    assert METRICS_COLUMNS[4:7] == (
        "count_toptrader_long_short_ratio",
        "sum_toptrader_long_short_ratio",
        "count_long_short_ratio",
    )
    assert UM_KLINES_1D.key("BTCUSDT", "2021-05") == (
        "data/futures/um/monthly/klines/BTCUSDT/1d/BTCUSDT-1d-2021-05.zip"
    )
    assert UM_METRICS.key("BTCUSDT", "2021-05-01") == (
        "data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2021-05-01.zip"
    )
