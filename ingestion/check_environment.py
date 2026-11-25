"""Check that this process has what the pipeline needs, without contacting Binance.

Run from repo root: python -m ingestion.check_environment
Exit code 1 on any problem. All checks are local and print names, never values:

- the variables of dbt and of the DuckDB warehouse are set;
- the landing zone of every dataset exists, is writable and has complete batches.

In Airflow it runs first: a broken environment fails here, before any other task.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from dotenv import load_dotenv

from ingestion.batches import high_water_marks
from ingestion.datasets import DATASETS
from ingestion.extract_klines import PROJECT_ROOT, ms_to_utc

ENV_PATH = PROJECT_ROOT / ".env"

REQUIRED_VARS = (
    "DBT_PROJECT_DIR",
    "DBT_PROFILES_DIR",
    "DUCKDB_PATH",  # the warehouse file: dbt's profile and the loader
)


def missing_variables(env: Mapping[str, str], names: Sequence[str]) -> list[str]:
    """Names that are unset or empty, in the given order: an empty value counts as missing."""
    return [name for name in names if not env.get(name)]


def landing_zone_problem(root: Path) -> str | None:
    """Why a dataset's landing zone cannot serve as the extract's watermark, or None."""
    if not root.is_dir():
        return "directory not found"
    if not os.access(root, os.W_OK):
        return "directory not writable"
    if not high_water_marks(root):
        return "no complete batch"
    return None


def main(env: Mapping[str, str] | None = None) -> int:
    if env is None:
        load_dotenv(ENV_PATH)  # never overrides the environment
        env = os.environ
    problems: list[str] = []
    print(f"Python {sys.version.split()[0]}: {sys.executable}")

    missing = missing_variables(env, REQUIRED_VARS)
    print(f"Variables: {len(REQUIRED_VARS) - len(missing)} of {len(REQUIRED_VARS)} set")
    problems += [f"variable {name} is not set" for name in missing]

    for dataset in DATASETS:
        problem = landing_zone_problem(dataset.landing_dir)
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
