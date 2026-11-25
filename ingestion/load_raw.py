"""Load extracted batches into the RAW tables of the DuckDB warehouse.

Run from repo root: python -m ingestion.load_raw [--dataset NAME] [--until BATCH_ID]
Default: every dataset (klines, funding_rate, open_interest, the three long/short ratio
series) in one connection; per dataset, every complete batch of the landing zone that is
not in RAW yet, oldest first (the load watermark is the set of _batch_id values in its
table). --until loads only batches up to that id (a rebuild that matches the baseline).

RAW is a journal of the source: each record as it came (payload JSON) plus its lineage.
Each batch loads in one transaction: insert, then reconcile with the manifest (rows and
duplicate keys per symbol); a batch that does not reconcile is rolled back and its later
batches wait for a human, the other datasets still load. A batch already in the table is
not inserted again. Exit code 1 if a batch does not reconcile or nothing was extracted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import duckdb

from ingestion.batches import Manifest, complete_batches, is_batch_id, read_manifest
from ingestion.datasets import DATASETS, RawDataset
from ingestion.settings import warehouse_path
from ingestion.warehouse import connect

SYMBOL_PATTERN = re.compile(r"[A-Z0-9]+")

Connection = duckdb.DuckDBPyConnection


class LoadError(RuntimeError):
    """The batch cannot be loaded as it is; nothing was written."""


def check_symbol(symbol: str) -> str:
    """Symbols become file paths: allow [A-Z0-9]+ only (no '..', no separators)."""
    if SYMBOL_PATTERN.fullmatch(symbol) is None:
        raise LoadError(f"Unexpected symbol name {symbol!r}")
    return symbol


@dataclass(frozen=True)
class BatchFile:
    symbol: str
    text: str  # the file's JSON array, as written by the extract
    content_key: str  # md5 of the file's bytes


def read_local_files(batch_dir: Path, manifest: Manifest) -> list[BatchFile]:
    """The manifest's files - never what else is in the directory - each checked: it exists
    and still has the row count the manifest recorded."""
    files = []
    for symbol in sorted(manifest["symbols"]):
        path = batch_dir / f"{check_symbol(symbol)}.json"
        if not path.is_file():
            raise LoadError(f"{path.name} is missing")
        data = path.read_bytes()
        text = data.decode("utf-8")
        rows = len(json.loads(text))
        expected = manifest["symbols"][symbol]["rows"]
        if rows != expected:
            raise LoadError(f"{path.name} has {rows} rows, manifest says {expected}")
        files.append(BatchFile(symbol, text, hashlib.md5(data).hexdigest()))
    return files


def create_table_sql(dataset: RawDataset) -> str:
    return f"""CREATE TABLE IF NOT EXISTS {dataset.table} (
    symbol VARCHAR NOT NULL,
    payload JSON NOT NULL,
    _batch_id VARCHAR NOT NULL,
    _source_file VARCHAR NOT NULL,
    _file_row_number BIGINT NOT NULL,
    _file_content_key VARCHAR NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL
)"""


def ensure_raw_objects(con: Connection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    for dataset in DATASETS:
        con.execute(create_table_sql(dataset))


def insert_sql(dataset: RawDataset) -> str:
    """One row per element of a file's JSON array.

    Binds: symbol, batch id, source file, content key, the file's text. One file per
    statement: a lateral join of read_text and json_each over a year-long batch ran out of
    1 GiB (note 106). _file_row_number is the element's position in the file (json_each's
    key, from 0), not the order of the result; _loaded_at is the start of the transaction.
    """
    return f"""INSERT INTO {dataset.table}
SELECT
    ? AS symbol,
    e.value AS payload,
    ? AS _batch_id,
    ? AS _source_file,
    e.key::BIGINT + 1 AS _file_row_number,
    ? AS _file_content_key,
    now() AS _loaded_at
FROM json_each(?::JSON) AS e"""


def batch_stats_sql(dataset: RawDataset) -> str:
    return f"""SELECT
    symbol,
    count(*) AS row_count,
    count(*) - count(DISTINCT {dataset.key_sql}) AS duplicate_count
FROM {dataset.table}
WHERE _batch_id = ?
GROUP BY symbol"""


def batch_stats(con: Connection, dataset: RawDataset, batch_id: str) -> dict[str, dict[str, int]]:
    rows = con.execute(batch_stats_sql(dataset), [batch_id]).fetchall()
    return {symbol: {"rows": n, "duplicates": d} for symbol, n, d in rows}


def reconcile(stats: dict[str, dict[str, int]], manifest: Manifest) -> list[str]:
    """Compare the batch in the table with its manifest; return problems (empty = OK)."""
    expected = manifest["symbols"]
    problems = [f"{s}: not in table" for s in sorted(expected.keys() - stats.keys())]
    problems += [f"{s}: not in manifest" for s in sorted(stats.keys() - expected.keys())]
    for symbol in sorted(expected.keys() & stats.keys()):
        want, got = expected[symbol], stats[symbol]
        if got["rows"] != want["rows"]:
            problems.append(f"{symbol}: {got['rows']} rows, manifest {want['rows']}")
        if got["duplicates"] != want["duplicates"]:
            problems.append(
                f"{symbol}: {got['duplicates']} duplicate keys, manifest {want['duplicates']}"
            )
    return problems


def load_batch(
    con: Connection, dataset: RawDataset, batch_dir: Path, manifest: Manifest
) -> list[str]:
    """Load the batch unless it is already in the table; return reconciliation problems.

    One transaction: a batch that does not reconcile, or fails half way, is rolled back.
    """
    batch_id = batch_dir.name
    if not is_batch_id(batch_id) or manifest["batch_id"] != batch_id:
        raise LoadError(f"Not the batch its manifest describes: {batch_dir}")
    files = read_local_files(batch_dir, manifest)

    con.begin()
    try:
        stats = batch_stats(con, dataset, batch_id)
        if stats:
            print(f"Batch {batch_id} is already in {dataset.table}: not inserted again")
        else:
            sql = insert_sql(dataset)
            for f in files:
                source = f"{batch_id}/{f.symbol}.json"
                con.execute(sql, [f.symbol, batch_id, source, f.content_key, f.text])
            stats = batch_stats(con, dataset, batch_id)
        problems = reconcile(stats, manifest)
        if problems:
            con.rollback()
        else:
            con.commit()
    except BaseException:
        con.rollback()
        raise
    return problems


def pending_batches(root: Path, loaded: set[str], until: str | None = None) -> list[Path]:
    """Complete batches that are not in RAW yet, oldest first (the order they were
    extracted in), up to and including `until`."""
    return [
        d
        for d in complete_batches(root)
        if d.name not in loaded and (until is None or d.name <= until)
    ]


def loaded_batches(con: Connection, dataset: RawDataset) -> set[str]:
    rows = con.execute(f"SELECT DISTINCT _batch_id FROM {dataset.table}").fetchall()
    return {batch_id for (batch_id,) in rows}


def load_pending(
    con: Connection, dataset: RawDataset, until: str | None = None
) -> tuple[list[str], list[str]]:
    """Load every pending batch of a dataset, oldest first; stop at the first one that does
    not reconcile.

    Returns the ids of the batches loaded by this call, and the problems of the batch it
    stopped at, each prefixed with the batch id (empty = all reconciled).
    """
    pending = pending_batches(dataset.landing_dir, loaded_batches(con, dataset), until)
    if not pending:
        print(f"Every complete batch in {dataset.landing_dir} is in {dataset.table}")
        return [], []
    print(f"Batches not in {dataset.table} yet: {len(pending)}")
    loaded: list[str] = []
    for batch_dir in pending:
        manifest = read_manifest(batch_dir)
        problems = load_batch(con, dataset, batch_dir, manifest)
        if problems:
            return loaded, [f"{batch_dir.name}: {problem}" for problem in problems]
        rows = sum(s["rows"] for s in manifest["symbols"].values())
        print(f"  {batch_dir.name}: {len(manifest['symbols'])} symbols, {rows} rows, reconciled")
        loaded.append(batch_dir.name)
    return loaded, []


def outcome(loaded: list[str]) -> str:
    if not loaded:
        return "nothing to load"
    span = loaded[0] if len(loaded) == 1 else f"{loaded[0]} .. {loaded[-1]}"
    return f"loaded and reconciled {len(loaded)} batches ({span})"


def main(argv: Sequence[str] | None = None) -> int:
    by_name = {d.name: d for d in DATASETS}
    parser = argparse.ArgumentParser(description="Load extracted batches into DuckDB RAW.")
    parser.add_argument("--dataset", choices=sorted(by_name), help="default: every dataset")
    parser.add_argument("--until", help="newest batch id to load (default: all)")
    args = parser.parse_args(argv)
    if args.until is not None and not is_batch_id(args.until):
        parser.error(f"--until must look like 20261125T090706Z, got {args.until!r}")

    datasets = [by_name[args.dataset]] if args.dataset else list(DATASETS)
    # Fail before opening the warehouse when nothing was extracted at all.
    with_batches = [d for d in datasets if complete_batches(d.landing_dir)]
    for dataset in datasets:
        if dataset not in with_batches:
            print(f"{dataset.name}: no complete batch in {dataset.landing_dir}, skipped")
    if not with_batches:
        print("No complete batch to load: run the extracts first (python -m ingestion.extract_*)")
        return 1

    outcomes: list[str] = []
    problems: list[str] = []
    print(f"Warehouse: {warehouse_path()}")
    with connect() as con:
        ensure_raw_objects(con)
        for dataset in with_batches:
            print(f"\n== {dataset.name} ==")
            loaded, failed = load_pending(con, dataset, args.until)
            if failed:
                problems += [f"{dataset.name}: {p}" for p in failed]
            else:
                outcomes.append(f"{dataset.name} {outcome(loaded)}")

    if problems:
        print("\nFAIL: a batch does not reconcile with its manifest and was rolled back;")
        print("later batches of its dataset are not loaded:")
        for problem in problems:
            print(f"  {problem}")
        return 1
    print(f"\nOK: {'; '.join(outcomes)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
