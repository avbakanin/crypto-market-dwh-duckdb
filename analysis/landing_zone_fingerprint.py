"""The RAW fingerprint, recomputed from the landing zone (data/raw) alone: no Snowflake.

    python -m analysis.landing_zone_fingerprint [--until BATCH_ID] [--compare BASELINE_CSV]

RAW is the landing zone loaded as is: every complete batch, every file its manifest lists,
every record of the file. So the raw metrics of the baseline (docs/baseline/
snowflake_baseline_metrics.csv) can be computed from the files, up to the newest batch the
baseline had (`--until`). Equal numbers mean the landing zone rebuilds the same RAW - in
Snowflake or in another engine - which is what moving the project to DuckDB relies on.

Prints the metrics in the baseline's CSV layout; with --compare, also every difference
and exit code 1 when an exact metric differs.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from ingestion.batches import complete_batches, is_batch_id, read_manifest
from ingestion.datasets import BASELINE_DATASETS as DATASETS

HOUR_MS = 3_600_000


@dataclass(frozen=True)
class RawFields:
    """How the baseline query reads a record: its grid key and one summed value."""

    key: Callable[[Any], int]
    value: Callable[[Any], str]
    value_metric: str


def _timestamp(record: Any) -> int:
    return int(record["timestamp"])


def _long_short(record: Any) -> str:
    return record["longShortRatio"]


# The same expressions as the raw branches of docs/baseline/snowflake_baseline.sql.
FIELDS: dict[str, RawFields] = {
    "klines": RawFields(lambda r: int(r[0]), lambda r: r[4], "sum_close"),
    "funding_rate": RawFields(
        lambda r: int(r["fundingTime"]) // HOUR_MS * HOUR_MS,
        lambda r: r["fundingRate"],
        "sum_funding_rate",
    ),
    "open_interest": RawFields(_timestamp, lambda r: r["sumOpenInterest"], "sum_open_interest"),
    "top_account_ratio": RawFields(_timestamp, _long_short, "sum_long_short_ratio"),
    "top_position_ratio": RawFields(_timestamp, _long_short, "sum_long_short_ratio"),
    "global_account_ratio": RawFields(_timestamp, _long_short, "sum_long_short_ratio"),
}


def relation_name(table: str) -> str:
    """RAW.BINANCE_SPOT_KLINES_1H -> binance_spot_klines_1h, as the baseline names it."""
    return table.split(".")[-1].lower()


def loaded_records(root: Path, until: str | None) -> Iterator[tuple[str, str, Any]]:
    """(batch id, symbol, record) for every record the loader puts into RAW: complete
    batches up to `until` (inclusive), the files their manifests list."""
    for batch_dir in complete_batches(root):
        if until is not None and batch_dir.name > until:
            break
        for symbol in sorted(read_manifest(batch_dir)["symbols"]):
            records = json.loads((batch_dir / f"{symbol}.json").read_text(encoding="utf-8"))
            for record in records:
                yield batch_dir.name, symbol, record


def fingerprint(root: Path, fields: RawFields, until: str | None) -> dict[str, str]:
    """The raw metrics of one dataset, as strings comparable with the baseline's values."""
    rows = 0
    symbols: set[str] = set()
    batches: set[str] = set()
    keys: set[tuple[str, int]] = set()
    key_sum = 0
    value_sum = Decimal(0)
    for batch_id, symbol, record in loaded_records(root, until):
        key = fields.key(record)
        rows += 1
        symbols.add(symbol)
        batches.add(batch_id)
        keys.add((symbol, key))
        key_sum += key
        value_sum += Decimal(fields.value(record))
    if not rows:
        raise ValueError(f"no complete batch in {root} up to {until}")
    key_values = [k for _, k in keys]
    return {
        "rows": str(rows),
        "symbols": str(len(symbols)),
        "batches": str(len(batches)),
        "first_batch_id": min(batches),
        "last_batch_id": max(batches),
        "distinct_keys": str(len(keys)),
        "min_key_ms": str(min(key_values)),
        "max_key_ms": str(max(key_values)),
        "sum_key_ms": str(key_sum),
        fields.value_metric: str(value_sum),
    }


def same_value(ours: str, theirs: str) -> bool:
    """Numbers compare as numbers (Snowflake prints 0.10000000, Python 0.1); text as text."""
    try:
        return Decimal(ours) == Decimal(theirs)
    except ArithmeticError:
        return ours == theirs


def read_baseline(path: Path) -> dict[tuple[str, str], str]:
    """(relation, metric) -> value of the exact raw metrics of the baseline CSV."""
    with path.open(encoding="utf-8", newline="") as f:
        return {
            (row["relation"], row["metric"]): row["value"]
            for row in csv.DictReader(f)
            if row["layer"] == "raw" and row["grain"] == "all" and row["compare"] == "exact"
        }


def differences(
    computed: dict[str, dict[str, str]], baseline: dict[tuple[str, str], str]
) -> list[str]:
    """Every exact raw metric of the baseline that the landing zone does not reproduce."""
    problems = []
    for (relation, metric), expected in sorted(baseline.items()):
        actual = computed.get(relation, {}).get(metric)
        if actual is None:
            problems.append(f"{relation}.{metric}: not computed (baseline {expected})")
        elif not same_value(actual, expected):
            problems.append(f"{relation}.{metric}: landing zone {actual}, baseline {expected}")
    return problems


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="RAW fingerprint from the landing zone")
    parser.add_argument("--until", help="newest batch id to include (default: all)")
    parser.add_argument("--compare", type=Path, help="baseline CSV to compare with")
    args = parser.parse_args(argv)
    if args.until is not None and not is_batch_id(args.until):
        parser.error(f"--until must look like 20261125T090706Z, got {args.until!r}")

    computed = {
        relation_name(d.table): fingerprint(d.landing_dir, FIELDS[d.name], args.until)
        for d in DATASETS
    }
    out = csv.writer(sys.stdout, lineterminator="\n")
    out.writerow(["layer", "relation", "grain", "metric", "compare", "value"])
    for relation, metrics in computed.items():
        for metric, value in metrics.items():
            out.writerow(["raw", relation, "all", metric, "exact", value])

    if args.compare is None:
        return 0
    problems = differences(computed, read_baseline(args.compare))
    for problem in problems:
        print(f"DIFF {problem}", file=sys.stderr)
    checked = sum(len(m) for m in computed.values())
    if problems:
        print(f"FAIL: {len(problems)} raw metrics differ from {args.compare}", file=sys.stderr)
        return 1
    print(f"OK: {checked} raw metrics equal {args.compare}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
