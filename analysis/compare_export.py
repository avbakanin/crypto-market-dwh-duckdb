"""Compare the DuckDB marts row by row with a Snowflake export of the same input.

    python -m analysis.compare_export --export DIR [--database PATH] [--report MARKDOWN]

DIR is an export snapshot of the original (data/marts/<export_id>/ with _manifest.json and
one Parquet file per table), taken on the same input as the warehouse: the baseline's
export marts_20261125T091007Z, built from batches up to 20261125T090706Z.

Per table, the rows are joined on the table's key (a full join both ways: rows only in
Snowflake, rows only in DuckDB) and every column of the export is compared:
- numbers: |DuckDB - Snowflake| <= 1e-9 + 1e-9 * |Snowflake| (DuckDB divides in DOUBLE,
  Snowflake kept decimals of scale 12-18);
- everything else: equal, NULL equal to NULL.
fct_pump_events.return_zscore is a DOUBLE statistic in both warehouses, and Snowflake's is
off by up to 9.7e-8 relative to an exact Decimal recomputation (DuckDB: 2.2e-13; source
note 113): it is compared within 1e-6 relative.
Columns that depend on the picked control hours (md5 in DuckDB, hash() in Snowflake) are
engine_dependent: not compared row by row, but summarised per metric as distributions of
the paired values (correlation, sign agreement, median |difference| against the IQR).

Exit code 1 when a key is missing on either side or a compared column differs.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from ingestion.settings import PROJECT_ROOT, warehouse_path
from ingestion.warehouse import connect

TOLERANCE = 1e-9
NUMERIC_PREFIXES = ("DECIMAL", "DOUBLE", "FLOAT", "REAL")


@dataclass(frozen=True)
class TableSpec:
    keys: tuple[str, ...]
    engine_dependent: tuple[str, ...] = ()
    tolerance: dict[str, float] = field(default_factory=dict)  # column -> relative tolerance
    group_by: str | None = None  # the grain of the distribution summary
    # The independent unit of the distribution test: rows of one event share its 30 control
    # hours at every offset, so the effective sample is the events. None: no KS (aggregates).
    sample_unit: str | None = None


TABLES: dict[str, TableSpec] = {
    "dim_symbol": TableSpec(keys=("symbol",)),
    "fct_pump_events": TableSpec(keys=("pump_event_id",), tolerance={"return_zscore": 1e-6}),
    "fct_pump_event_window": TableSpec(keys=("pump_event_id", "offset_hours")),
    "fct_pump_event_excess": TableSpec(
        keys=("pump_event_id", "offset_hours", "metric"),
        engine_dependent=(
            "control_median",
            "control_share_above_zero",
            "control_share_below_zero",
            "excess",
        ),
        group_by="metric",
        sample_unit="pump_event_id",
    ),
    "mart_pump_profile": TableSpec(
        keys=("event_scope", "offset_hours", "metric"),
        engine_dependent=("control_median", "median_excess"),
        group_by="metric",
    ),
    "mart_pump_signal_check": TableSpec(keys=("rule", "signal_date")),
}


@dataclass
class TableResult:
    table: str
    snowflake_rows: int
    duckdb_rows: int
    only_snowflake: int
    only_duckdb: int
    compared_columns: int
    differing: dict[str, int] = field(default_factory=dict)  # column -> rows that differ
    missing_columns: list[str] = field(default_factory=list)
    # (column, grain, pairs, units, median Snowflake, median DuckDB, KS, correlation)
    distributions: list[tuple[str, str, int, int | None, float, float, float | None, float]] = (
        field(default_factory=list)
    )

    @property
    def ok(self) -> bool:
        return not (
            self.only_snowflake
            or self.only_duckdb
            or self.missing_columns
            or any(self.differing.values())
        )


def quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def column_types(con: duckdb.DuckDBPyConnection, relation: str) -> dict[str, str]:
    return {row[0]: row[1] for row in con.execute(f"describe {relation}").fetchall()}


def same(column: str, numeric: bool, relative: float = TOLERANCE) -> str:
    s, d = f"s.{quoted(column)}", f"d.{quoted(column)}"
    if numeric:
        # Never NULL: a NULL on one side must count as a difference, and count_if(NULL) would
        # silently skip it.
        return (
            f"(({s} is null and {d} is null) or ({s} is not null and {d} is not null"
            f" and abs({d}::double - {s}::double) <= {TOLERANCE} + {relative} * abs({s}::double)))"
        )
    return f"({s} is not distinct from {d})"


def compare_table(
    con: duckdb.DuckDBPyConnection, table: str, snowflake: str, duck: str, spec: TableSpec
) -> TableResult:
    """snowflake and duck are relations of the same table; columns are the export's."""
    s_types = column_types(con, snowflake)
    d_types = column_types(con, duck)
    missing = [c for c in s_types if c not in d_types]
    columns = [c for c in s_types if c in d_types and c not in spec.keys]
    compared = [c for c in columns if c not in spec.engine_dependent]
    on = " and ".join(f"s.{quoted(k)} = d.{quoted(k)}" for k in spec.keys)
    first = quoted(spec.keys[0])
    both = f"s.{first} is not null and d.{first} is not null"
    counts = [
        f"count_if(d.{first} is null and s.{first} is not null)",
        f"count_if(s.{first} is null and d.{first} is not null)",
        f"count_if(s.{first} is not null)",
        f"count_if(d.{first} is not null)",
    ] + [
        f"count_if({both} and not "
        f"{same(c, s_types[c].startswith(NUMERIC_PREFIXES), spec.tolerance.get(c, TOLERANCE))})"
        for c in compared
    ]
    row = con.execute(
        f"select {', '.join(counts)} from {snowflake} as s full join {duck} as d on {on}"
    ).fetchone()
    assert row is not None
    only_s, only_d, n_s, n_d, *diffs = row
    result = TableResult(
        table=table,
        snowflake_rows=n_s,
        duckdb_rows=n_d,
        only_snowflake=only_s,
        only_duckdb=only_d,
        compared_columns=len(compared),
        differing=dict(zip(compared, diffs, strict=True)),
        missing_columns=missing,
    )
    for column in (c for c in spec.engine_dependent if c in d_types):
        result.distributions += [
            (column, *r) for r in distribution(con, snowflake, duck, spec, column, on)
        ]
    return result


def distribution(
    con: duckdb.DuckDBPyConnection,
    snowflake: str,
    duck: str,
    spec: TableSpec,
    column: str,
    on: str,
) -> list[tuple[str, int, int | None, float, float, float | None, float]]:
    """Per grain, over the rows with a value on both sides: pairs, independent units, Snowflake
    median, DuckDB median, the two-sample Kolmogorov-Smirnov statistic (max |F_snowflake -
    F_duckdb|; NULL without a sample unit) and the paired correlation."""
    group = f"s.{quoted(spec.group_by)}" if spec.group_by else "'all'"
    unit = f"s.{quoted(spec.sample_unit)}" if spec.sample_unit else "null"
    s, d = f"s.{quoted(column)}::double", f"d.{quoted(column)}::double"
    return con.execute(f"""
        with pairs as (
            select {group} as grain, {unit} as unit, {s} as sf, {d} as dk
            from {snowflake} as s join {duck} as d on {on}
            where {s} is not null and {d} is not null
        ),
        points as (
            select grain, sf as x, 1 as from_sf, 0 as from_dk from pairs
            union all
            select grain, dk, 0, 1 from pairs
        ),
        steps as (
            select grain, x, sum(from_sf) as n_sf, sum(from_dk) as n_dk
            from points group by grain, x
        ),
        cdf as (
            select
                grain,
                sum(n_sf) over (partition by grain order by x) / sum(n_sf) over (partition by grain)
                    as f_sf,
                sum(n_dk) over (partition by grain order by x) / sum(n_dk) over (partition by grain)
                    as f_dk
            from steps
        ),
        ks as (select grain, max(abs(f_sf - f_dk)) as ks from cdf group by grain)
        select
            pairs.grain, count(*), nullif(count(distinct unit), 0), median(sf), median(dk),
            if(count(unit) > 0, any_value(ks.ks), null), corr(dk, sf)
        from pairs join ks on pairs.grain = ks.grain
        group by pairs.grain order by pairs.grain
    """).fetchall()


def ks_critical(units: int | None) -> float | None:
    """5 % critical value of the two-sample KS statistic, two samples of `units` each."""
    return None if not units else 1.358 * (2 / units) ** 0.5


def compare_export(export: Path, database: Path) -> list[TableResult]:
    manifest = json.loads((export / "_manifest.json").read_text(encoding="utf-8"))
    results = []
    with connect(database, read_only=True) as con:
        for table, spec in TABLES.items():
            if table not in manifest["tables"]:
                raise ValueError(f"{export} has no table {table}")
            path = (export / f"{table}.parquet").as_posix().replace("'", "''")
            con.execute(
                f"create or replace temp view snowflake_{table} as"
                f" select * from read_parquet('{path}')"
            )
            results.append(compare_table(con, table, f"snowflake_{table}", f"marts.{table}", spec))
    return results


def report(results: list[TableResult], export: Path, database: Path) -> str:
    def rel(path: Path) -> str:
        return (
            path.relative_to(PROJECT_ROOT).as_posix()
            if path.is_relative_to(PROJECT_ROOT)
            else path.name
        )

    lines = [
        "# DuckDB marts against the Snowflake export, row by row",
        "",
        f"Generated by `python -m analysis.compare_export --export .../{export.name}` at"
        f" {datetime.now(UTC):%Y-%m-%d %H:%M} UTC. Warehouse `{rel(database)}`; export"
        f" `{export.name}` of the original, taken on the same input (batches up to"
        " 20261125T090706Z).",
        f"Numbers equal within {TOLERANCE:g} + {TOLERANCE:g} x |Snowflake|; other values equal,"
        " NULL = NULL; fct_pump_events.return_zscore within 1e-6 relative (Snowflake's float"
        " error, source note 113). Columns of the control hours are engine_dependent (md5 picks"
        " other hours than Snowflake's hash()): compared as distributions below.",
        "",
        "| table | Snowflake rows | DuckDB rows | only Snowflake | only DuckDB"
        " | columns compared | rows differing |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for r in results:
        lines.append(
            f"| {r.table} | {r.snowflake_rows} | {r.duckdb_rows} | {r.only_snowflake}"
            f" | {r.only_duckdb} | {r.compared_columns} | {sum(r.differing.values())} |"
        )
    problems = [(r.table, c, n) for r in results for c, n in r.differing.items() if n] + [
        (r.table, c, "missing in DuckDB") for r in results for c in r.missing_columns
    ]
    lines += ["", "## Differences", ""]
    lines += [f"- `{t}.{c}`: {n} rows" for t, c, n in problems] or [
        "None: every key is on both sides and every compared column is equal."
    ]
    lines += [
        "",
        "## Engine-dependent columns: paired distributions",
        "",
        "Rows with a value on both sides. Control medians are two independent samples of 30"
        " hours from the same pool, so row by row they differ by sampling noise (low paired"
        " correlation); the question is whether the distributions agree: medians, and the"
        " two-sample Kolmogorov-Smirnov statistic against its 5 % critical value"
        " 1.358 x sqrt(2 / events). Events, not rows, are the independent units: the rows of"
        " one event share its control hours at every offset. The profile aggregates all"
        " events (no KS); the paired correlation of excess and median_excess shows that the"
        " findings agree.",
        "",
        "| table | column | grain | pairs | events | median Snowflake | median DuckDB | KS"
        " | KS 5 % | correlation |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in results:
        for column, grain, pairs, units, sf, dk, ks, corr in r.distributions:
            lines.append(
                f"| {r.table} | {column} | {grain} | {pairs} | {units or ''} | {fmt(sf, 6)}"
                f" | {fmt(dk, 6)} | {fmt(ks)} | {fmt(ks_critical(units))} | {fmt(corr)} |"
            )
    return "\n".join(lines) + "\n"


def fmt(value: float | None, digits: int = 3) -> str:
    return "" if value is None or value != value else f"{value:.{digits}f}"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="DuckDB marts against a Snowflake export")
    parser.add_argument("--export", type=Path, required=True, help="export snapshot directory")
    parser.add_argument("--database", type=Path, help="default: DUCKDB_PATH")
    parser.add_argument("--report", type=Path, help="write the comparison as Markdown")
    args = parser.parse_args(argv)
    export = args.export.resolve()
    if not (export / "_manifest.json").is_file():
        parser.error(f"{export} is not an export snapshot (no _manifest.json)")
    database = args.database.resolve() if args.database else warehouse_path()
    results = compare_export(export, database)
    for r in results:
        bad = {c: n for c, n in r.differing.items() if n}
        print(
            f"{'OK  ' if r.ok else 'DIFF'} {r.table}: {r.snowflake_rows} / {r.duckdb_rows} rows,"
            f" only Snowflake {r.only_snowflake}, only DuckDB {r.only_duckdb},"
            f" {r.compared_columns} columns"
            + (f", differing {bad}" if bad else "")
            + (f", missing {r.missing_columns}" if r.missing_columns else "")
        )
    if args.report:
        args.report.write_bytes(report(results, export, database).encode())
    if all(r.ok for r in results):
        print("OK: the marts equal the export row by row (engine-dependent columns aside)")
        return 0
    print("FAIL: the marts differ from the export")
    return 1


if __name__ == "__main__":
    sys.exit(main())
