"""Check that this process has what the pipeline needs, without contacting Binance.

Run from repo root: python -m ingestion.check_environment
Exit code 1 on any problem. All checks are local and print names, never values:

- the variables of dbt, of the DuckDB warehouse and the pipeline mode are set;
- the directory of the warehouse file exists and is writable (DuckDB does not create it);
- the landing zone of every dataset (under LANDING_ZONE_DIR) exists, is readable and has
  complete batches; in mode "own" it must be writable too (the extracts write there), in
  mode "follow" it may be read-only (the original owns it and the copy mounts it so).

In Airflow it runs first: a broken environment fails here, before any other task.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from ingestion.batches import high_water_marks
from ingestion.datasets import DATASETS
from ingestion.extract_klines import ms_to_utc
from ingestion.settings import LANDING_ZONE_DIR, OWN, pipeline_mode, warehouse_path

REQUIRED_VARS = (
    "DBT_PROJECT_DIR",
    "DBT_PROFILES_DIR",
    "DUCKDB_PATH",  # the warehouse file: dbt's profile and the loader
    "PIPELINE_MODE",  # follow / own: who writes the landing zone
)


def missing_variables(env: Mapping[str, str], names: Sequence[str]) -> list[str]:
    """Names that are unset or empty, in the given order: an empty value counts as missing."""
    return [name for name in names if not env.get(name)]


def directory_problem(path: Path, writable: bool) -> str | None:
    if not path.is_dir():
        return "directory not found"
    if not os.access(path, os.R_OK):
        return "directory not readable"
    if writable and not os.access(path, os.W_OK):
        return "directory not writable"
    return None


def landing_zone_problem(root: Path, writable: bool) -> str | None:
    """Why a dataset's landing zone cannot serve the pipeline, or None."""
    problem = directory_problem(root, writable)
    if problem:
        return problem
    if not high_water_marks(root):
        return "no complete batch"
    return None


def main(env: Mapping[str, str] | None = None) -> int:
    if env is None:
        env = os.environ  # ingestion.settings has loaded .env: it never overrides the environment
    problems: list[str] = []
    print(f"Python {sys.version.split()[0]}: {sys.executable}")

    missing = missing_variables(env, REQUIRED_VARS)
    print(f"Variables: {len(REQUIRED_VARS) - len(missing)} of {len(REQUIRED_VARS)} set")
    problems += [f"variable {name} is not set" for name in missing]

    writable = True  # without a valid mode, ask for the most
    try:
        mode = pipeline_mode(env)
    except ValueError as error:
        if "PIPELINE_MODE" not in missing:  # set, but to something else
            problems.append(str(error))
    else:
        writable = mode == OWN
        access = "read and write" if writable else "read only"
        print(f"Pipeline mode: {mode} (landing zone: {access})")

    warehouse = warehouse_path(env)
    problem = directory_problem(warehouse.parent, writable=True)
    print(f"Warehouse: {warehouse}" + (f": {problem}" if problem else ""))
    if problem:
        problems.append(f"warehouse directory ({warehouse.parent}): {problem}")

    print(f"Landing zone: {LANDING_ZONE_DIR}")
    for dataset in DATASETS:
        problem = landing_zone_problem(dataset.landing_dir, writable)
        if problem:
            print(f"Landing zone {dataset.name}: {problem}")
            problems.append(f"landing zone {dataset.name} ({dataset.landing_dir}): {problem}")
            continue
        marks = high_water_marks(dataset.landing_dir)
        print(
            f"Landing zone {dataset.name}: {len(marks)} symbols, "
            f"newest {ms_to_utc(max(marks.values())):%Y-%m-%d %H:%M} UTC"
        )

    if problems:
        print("\nFAIL:\n" + "\n".join(f"  {p}" for p in problems))
        return 1
    print("\nOK: environment is ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
