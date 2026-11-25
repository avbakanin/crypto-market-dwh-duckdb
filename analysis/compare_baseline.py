"""Compare the DuckDB warehouse with the Snowflake baseline, layer by layer.

    python -m analysis.compare_baseline --layer raw [--layer ...] [--database PATH]
        [--baseline CSV] [--references CSV] [--report MARKDOWN]

The baseline (docs/baseline/snowflake_baseline_metrics.csv, docs/snowflake_baseline.md)
gives every metric a comparison class; analysis/baseline_duckdb/<layer>.sql computes the
same metrics in DuckDB. The warehouse must be built from the baseline's input only
(python -m ingestion.load_raw --until 20261125T090706Z, into its own file).

- exact: equal (numbers as numbers: Snowflake prints 0.10000000, DuckDB 0.1);
- tolerance: within 5 units of the baseline's last printed digit (division gives DOUBLE in
  DuckDB, exact DECIMAL in Snowflake);
- reference (check counts per type and status): compared like exact, but a difference is
  reported, not failed: the copy's set of checks may legitimately differ;
- engine_dependent, snowflake_only: listed, not compared here (engine_dependent with both
  values side by side).

A metric that differs from the baseline is a baseline error when an exact reference
(docs/baseline/exact_references.csv, python -m analysis.exact_reference) shows DuckDB within
tolerance of the true value and the baseline outside it; tolerance is still counted in units
of the baseline's last digit.

Exit code 1 when an exact or tolerance metric differs or is missing.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from ingestion.settings import PROJECT_ROOT, warehouse_path
from ingestion.warehouse import connect

SQL_DIR = Path(__file__).resolve().parent / "baseline_duckdb"
BASELINE_CSV = PROJECT_ROOT / "docs" / "baseline" / "snowflake_baseline_metrics.csv"
REFERENCES_CSV = PROJECT_ROOT / "docs" / "baseline" / "exact_references.csv"
COMPARED = ("exact", "tolerance")
REPORTED = ("reference",)  # compared, but a difference does not fail the run
TOLERANCE_UNITS = 5

Key = tuple[str, str, str, str]  # layer, relation, grain, metric


@dataclass(frozen=True)
class Outcome:
    key: Key
    compare: str
    baseline: str
    duckdb: str | None
    status: str  # equal | baseline error | differs | missing | not compared
    reference: str | None = None


def number(text: str) -> Decimal | None:
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def same_value(ours: str, theirs: str) -> bool:
    a, b = number(ours), number(theirs)
    return a == b if a is not None and b is not None else ours == theirs


def within_tolerance(ours: str, theirs: str, digits_of: str | None = None) -> bool:
    """|ours - theirs| <= 5 units of the last digit the baseline printed (`theirs`, or
    `digits_of` when theirs is an exact reference with more digits)."""
    a, b = number(ours), number(theirs)
    unit = number(digits_of if digits_of is not None else theirs)
    if a is None or b is None or unit is None:
        return ours == theirs
    exponent = unit.as_tuple().exponent
    if not isinstance(exponent, int):  # NaN or infinity
        return ours == theirs
    return abs(a - b) <= TOLERANCE_UNITS * Decimal(10) ** exponent


def read_baseline(path: Path, layers: Sequence[str]) -> dict[Key, tuple[str, str]]:
    """(layer, relation, grain, metric) -> (compare, value), for the given layers."""
    with path.open(encoding="utf-8", newline="") as f:
        return {
            (r["layer"], r["relation"], r["grain"], r["metric"]): (r["compare"], r["value"])
            for r in csv.DictReader(f)
            if r["layer"] in layers
        }


def read_references(path: Path) -> dict[Key, str]:
    """(layer, relation, grain, metric) -> exact reference; no file, no references."""
    if not path.is_file():
        return {}
    with path.open(encoding="utf-8", newline="") as f:
        return {
            (r["layer"], r["relation"], r["grain"], r["metric"]): r["reference"]
            for r in csv.DictReader(f)
        }


def duckdb_metrics(database: Path, layers: Sequence[str]) -> dict[Key, str]:
    metrics: dict[Key, str] = {}
    with connect(database, read_only=True) as con:
        for layer in layers:
            sql = (SQL_DIR / f"{layer}.sql").read_text(encoding="utf-8")
            for row_layer, relation, grain, metric, value in con.execute(sql).fetchall():
                metrics[(row_layer, relation, grain, metric)] = value
    return metrics


def compare(
    baseline: dict[Key, tuple[str, str]],
    ours: dict[Key, str],
    references: dict[Key, str] | None = None,
) -> list[Outcome]:
    references = references or {}
    outcomes = []
    for key, (cls, expected) in sorted(baseline.items()):
        actual = ours.get(key)
        reference = references.get(key)
        if cls not in COMPARED and cls not in REPORTED:
            status = "not compared"
        elif actual is None:
            status = "missing"
        elif (within_tolerance if cls == "tolerance" else same_value)(actual, expected):
            status = "equal"
        elif (
            reference is not None
            and within_tolerance(actual, reference, digits_of=expected)
            and not within_tolerance(expected, reference, digits_of=expected)
        ):
            status = "baseline error"
        else:
            status = "differs"
        outcomes.append(Outcome(key, cls, expected, actual, status, reference))
    return outcomes


def failed(o: Outcome) -> bool:
    """An exact or tolerance metric that differs or is missing; reference ones only report."""
    return o.compare in COMPARED and o.status in ("differs", "missing")


def side_by_side(o: Outcome) -> str:
    """engine_dependent: both values and the relative difference."""
    a, b = number(o.duckdb or ""), number(o.baseline)
    if a is None or b is None:
        return f"DuckDB {o.duckdb}, Snowflake {o.baseline}"
    delta = "" if b == 0 else f" ({(a - b) / abs(b) * 100:+.2f} %)"
    return f"DuckDB {o.duckdb}, Snowflake {o.baseline}{delta}"


def report(outcomes: list[Outcome], layers: Sequence[str], database: Path, baseline: Path) -> str:
    def rel(path: Path) -> str:
        return (
            path.relative_to(PROJECT_ROOT).as_posix()
            if path.is_relative_to(PROJECT_ROOT)
            else path.name
        )

    command = "python -m analysis.compare_baseline " + " ".join(f"--layer {x}" for x in layers)
    lines = [
        "# DuckDB port against the Snowflake baseline",
        "",
        f"Generated by `{command}` at {datetime.now(UTC):%Y-%m-%d %H:%M} UTC.",
        f"Warehouse `{rel(database)}`, baseline `{rel(baseline)}` (classes:"
        " [docs/snowflake_baseline.md](snowflake_baseline.md)).",
        "The warehouse holds the baseline's input only: `python -m ingestion.load_raw --until"
        " 20261125T090706Z`, into its own file (`DUCKDB_PATH`).",
        "",
        "| layer | class | metrics | equal | baseline error | differ | missing | not compared |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    counts = Counter((o.key[0], o.compare, o.status) for o in outcomes)
    statuses = ("equal", "baseline error", "differs", "missing", "not compared")
    for layer in layers:
        for cls in sorted({o.compare for o in outcomes if o.key[0] == layer}):
            total = sum(n for (lay, c, _), n in counts.items() if lay == layer and c == cls)
            cells = [counts[(layer, cls, s)] for s in statuses]
            lines.append(f"| {layer} | {cls} | {total} | " + " | ".join(map(str, cells)) + " |")
    problems = [o for o in outcomes if failed(o)]
    lines += ["", "## Differences", ""]
    if not problems:
        lines.append("None: every exact and tolerance metric of these layers matches.")
    for o in problems:
        layer, relation, grain, metric = o.key
        lines.append(
            f"- `{layer}.{relation}` [{grain}] `{metric}` ({o.compare}): "
            f"DuckDB {o.duckdb}, Snowflake {o.baseline}"
        )
    noted = [o for o in outcomes if o.compare in REPORTED and o.status in ("differs", "missing")]
    if noted:
        lines += ["", "## Reference differences (reported, not failed)", ""]
        for o in noted:
            layer, relation, grain, metric = o.key
            lines.append(
                f"- `{layer}.{relation}` [{grain}] `{metric}`: DuckDB {o.duckdb},"
                f" Snowflake {o.baseline}"
            )
    errors = [o for o in outcomes if o.status == "baseline error"]
    if errors:
        lines += [
            "",
            "## Baseline errors",
            "",
            "DuckDB is within tolerance of the exact value, the Snowflake baseline is not"
            " (docs/baseline/exact_references.csv, `python -m analysis.exact_reference`).",
            "",
        ]
        for o in errors:
            layer, relation, grain, metric = o.key
            lines.append(
                f"- `{layer}.{relation}` [{grain}] `{metric}` ({o.compare}): exact {o.reference},"
                f" DuckDB {o.duckdb}, Snowflake {o.baseline}"
            )
    skipped = [o for o in outcomes if o.status == "not compared"]
    if skipped:
        lines += ["", "## Not compared here", ""]
        for o in skipped:
            layer, relation, grain, metric = o.key
            values = f" - {side_by_side(o)}" if o.compare == "engine_dependent" else ""
            lines.append(f"- `{layer}.{relation}` [{grain}] `{metric}`: {o.compare}{values}")
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="DuckDB warehouse against the Snowflake baseline")
    parser.add_argument("--layer", action="append", required=True, help="e.g. raw; repeatable")
    parser.add_argument("--database", type=Path, help="default: DUCKDB_PATH")
    parser.add_argument("--baseline", type=Path, default=BASELINE_CSV)
    parser.add_argument("--references", type=Path, default=REFERENCES_CSV)
    parser.add_argument("--report", type=Path, help="write the comparison as Markdown")
    args = parser.parse_args(argv)
    for layer in args.layer:
        if not (SQL_DIR / f"{layer}.sql").is_file():
            parser.error(f"no metrics query for layer {layer!r} in {SQL_DIR}")
    database = args.database.resolve() if args.database else warehouse_path()

    outcomes = compare(
        read_baseline(args.baseline, args.layer),
        duckdb_metrics(database, args.layer),
        read_references(args.references),
    )
    for o in outcomes:
        if failed(o):
            print(f"DIFF {'.'.join(o.key)} ({o.compare}): DuckDB {o.duckdb}, baseline {o.baseline}")
        elif o.status in ("differs", "missing"):
            print(f"NOTE {'.'.join(o.key)} ({o.compare}): DuckDB {o.duckdb}, baseline {o.baseline}")
        elif o.status == "baseline error":
            print(
                f"BASELINE ERROR {'.'.join(o.key)}: exact {o.reference},"
                f" DuckDB {o.duckdb}, baseline {o.baseline}"
            )
    if args.report:
        args.report.write_bytes(report(outcomes, args.layer, database, args.baseline).encode())
    status = Counter((o.compare, o.status) for o in outcomes)
    print("  ".join(f"{c}/{s}: {n}" for (c, s), n in sorted(status.items())))
    bad = sum(failed(o) for o in outcomes)
    if bad:
        print(f"FAIL: {bad} metrics differ from the baseline")
        return 1
    print(f"OK: every compared metric of {', '.join(args.layer)} equals the baseline")
    return 0


if __name__ == "__main__":
    sys.exit(main())
