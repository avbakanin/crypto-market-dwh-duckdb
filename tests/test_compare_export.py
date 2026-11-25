"""Tests for the row-by-row comparison of the marts with a Snowflake export."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pytest

from analysis import compare_export as ce
from ingestion.warehouse import connect

SPEC = ce.TableSpec(keys=("id",))


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    with duckdb.connect() as c:
        c.execute("set TimeZone = 'UTC'")
        yield c


def tables(con: duckdb.DuckDBPyConnection, snowflake: str, duck: str) -> None:
    con.execute(f"create table s as select * from (values {snowflake}) t(id, label, x)")
    con.execute(f"create table d as select * from (values {duck}) t(id, label, x)")


def test_equal_rows_pass_and_double_noise_is_within_tolerance(
    con: duckdb.DuckDBPyConnection,
) -> None:
    tables(
        con,
        "(1, 'a', 0.1::decimal(38, 12)), (2, 'b', null)",
        "(1, 'a', 1.1::double / 1.0 - 1), (2, 'b', null)",  # 0.10000000000000009
    )
    r = ce.compare_table(con, "t", "s", "d", SPEC)
    assert (r.snowflake_rows, r.duckdb_rows, r.only_snowflake, r.only_duckdb) == (2, 2, 0, 0)
    assert r.differing == {"label": 0, "x": 0}
    assert r.ok


@pytest.mark.parametrize(
    ("duck", "column"),
    [
        ("(1, 'a', 0.1000001), (2, 'b', null)", "x"),  # beyond 1e-9
        ("(1, 'a', 0.1), (2, 'b', 0)", "x"),  # a value where Snowflake has NULL
        ("(1, 'A', 0.1), (2, 'b', null)", "label"),  # text is exact
    ],
)
def test_a_differing_value_is_counted_in_its_column(
    con: duckdb.DuckDBPyConnection, duck: str, column: str
) -> None:
    tables(con, "(1, 'a', 0.1), (2, 'b', null)", duck)
    r = ce.compare_table(con, "t", "s", "d", SPEC)
    assert {c: n for c, n in r.differing.items() if n} == {column: 1}
    assert not r.ok


def test_keys_missing_on_either_side_are_counted(con: duckdb.DuckDBPyConnection) -> None:
    tables(con, "(1, 'a', 0.1), (2, 'b', 0.2)", "(1, 'a', 0.1), (3, 'c', 0.3)")
    r = ce.compare_table(con, "t", "s", "d", SPEC)
    assert (r.only_snowflake, r.only_duckdb) == (1, 1)
    assert r.differing == {"label": 0, "x": 0}  # only the matched row is compared
    assert not r.ok


def test_a_column_missing_in_duckdb_fails(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("create table s as select 1 as id, 'a' as label")
    con.execute("create table d as select 1 as id")
    r = ce.compare_table(con, "t", "s", "d", SPEC)
    assert r.missing_columns == ["label"]
    assert not r.ok


def test_a_column_can_have_its_own_relative_tolerance(con: duckdb.DuckDBPyConnection) -> None:
    tables(con, "(1, 'a', 2.0)", "(1, 'a', 2.000001)")  # 5e-7 relative
    assert ce.compare_table(con, "t", "s", "d", SPEC).differing["x"] == 1
    loose = ce.TableSpec(keys=("id",), tolerance={"x": 1e-6})
    assert ce.compare_table(con, "t", "s", "d", loose).differing["x"] == 0


def test_engine_dependent_columns_become_distributions(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        "create table s as select i as id, 'm' || (i % 2) as label, i::double as x"
        " from range(10) r(i)"
    )
    con.execute(
        "create table d as select i as id, 'm' || (i % 2) as label,"
        " if(i % 2 = 0, i::double, i + 100.0) as x from range(10) r(i)"
    )
    spec = ce.TableSpec(keys=("id",), engine_dependent=("x",), group_by="label", sample_unit="id")
    r = ce.compare_table(con, "t", "s", "d", spec)
    assert r.differing == {"label": 0}  # x is not compared row by row
    assert r.ok
    by_grain = {row[1]: row for row in r.distributions}
    # m0: identical values -> KS 0; m1: every DuckDB value above every Snowflake one -> KS 1.
    assert by_grain["m0"][:6] == ("x", "m0", 5, 5, 4.0, 4.0)
    assert by_grain["m0"][6] == 0
    assert by_grain["m1"][4:7] == (5.0, 105.0, 1.0)
    assert by_grain["m1"][7] == pytest.approx(1.0)


def test_events_not_rows_are_the_independent_units(con: duckdb.DuckDBPyConnection) -> None:
    # 3 events x 4 offsets: 12 pairs, but the KS critical value must use the 3 events.
    rows = ", ".join(f"({e * 10 + o}, {e}, {o}.0)" for e in range(3) for o in range(4))
    con.execute(f"create table s as select * from (values {rows}) t(id, event, x)")
    con.execute("create table d as select * from s")
    spec = ce.TableSpec(keys=("id",), engine_dependent=("x",), sample_unit="event")
    ((_, _, pairs, units, *_),) = ce.compare_table(con, "t", "s", "d", spec).distributions
    assert (pairs, units) == (12, 3)


def test_no_ks_without_a_sample_unit(con: duckdb.DuckDBPyConnection) -> None:
    tables(con, "(1, 'a', 0.1), (2, 'a', 0.3)", "(1, 'a', 0.2), (2, 'a', 0.4)")
    spec = ce.TableSpec(keys=("id",), engine_dependent=("x",))
    ((column, grain, pairs, units, _, _, ks, corr),) = ce.compare_table(
        con, "t", "s", "d", spec
    ).distributions
    assert (column, grain, pairs, units, ks) == ("x", "all", 2, None, None)
    assert corr == pytest.approx(1.0)
    assert ce.ks_critical(None) is None
    assert ce.ks_critical(198) == pytest.approx(0.1365, abs=1e-4)


def make_export(root: Path, rows: str) -> Path:
    export = root / "20261125T091007Z"
    export.mkdir()
    with duckdb.connect() as c:
        c.execute(
            f"copy (select * from (values {rows}) t(id, label, x)) to '{export / 't.parquet'}'"
        )
    manifest = {"export_id": export.name, "tables": {"t": {"rows": 2}}}
    (export / "_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return export


@pytest.fixture
def warehouse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    database = tmp_path / "w.duckdb"
    with connect(database) as c:
        c.execute("create schema marts")
        c.execute(
            "create table marts.t as select * from (values (1, 'a', 0.5), (2, 'b', 1.5))"
            " t(id, label, x)"
        )
    monkeypatch.setattr(ce, "TABLES", {"t": SPEC})
    return database


def test_main_passes_on_an_equal_export_and_reports(
    warehouse: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    export = make_export(tmp_path, "(1, 'a', 0.5), (2, 'b', 1.5)")
    report = tmp_path / "r.md"
    args = ["--export", str(export), "--database", str(warehouse), "--report", str(report)]
    assert ce.main(args) == 0
    assert "OK: the marts equal the export row by row" in capsys.readouterr().out
    text = report.read_text(encoding="utf-8")
    assert "| t | 2 | 2 | 0 | 0 | 2 | 0 |" in text
    assert "None: every key is on both sides" in text


def test_main_fails_and_names_the_column(
    warehouse: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    export = make_export(tmp_path, "(1, 'a', 0.5), (2, 'b', 1.6)")
    assert ce.main(["--export", str(export), "--database", str(warehouse)]) == 1
    out = capsys.readouterr().out
    assert (
        "DIFF t: 2 / 2 rows, only Snowflake 0, only DuckDB 0, 2 columns, differing {'x': 1}" in out
    )


def test_main_refuses_a_directory_without_a_manifest(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as exc:
        ce.main(["--export", str(tmp_path), "--database", str(tmp_path / "w.duckdb")])
    assert exc.value.code == 2


def test_a_table_missing_from_the_export_is_an_error(warehouse: Path, tmp_path: Path) -> None:
    export = make_export(tmp_path, "(1, 'a', 0.5), (2, 'b', 1.5)")
    (export / "_manifest.json").write_text(json.dumps({"tables": {}}), encoding="utf-8")
    with pytest.raises(ValueError, match="has no table t"):
        ce.compare_export(export, warehouse)
