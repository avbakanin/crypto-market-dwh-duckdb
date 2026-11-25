"""Where the project keeps its files.

Both locations come from the environment, or from .env in the repo root (which never
overrides a variable the environment already has: Airflow, Docker, tests). Unset or empty
means the default in the repo; a relative path is resolved against the repo root, not the
working directory; an absolute path is kept.

- LANDING_ZONE_DIR: the extracts' immutable batches and their watermark. Read once, at
  import (the extracts' output directories are module constants). Default data/raw; the
  DuckDB port first reads the Snowflake original's data/raw.
- DUCKDB_PATH: the warehouse file. Read on each call, so a command can point at another
  file (DUCKDB_PATH=... python -m ingestion.load_raw). Default warehouse/crypto_dwh.duckdb.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"
LANDING_ZONE_VAR = "LANDING_ZONE_DIR"
DEFAULT_LANDING_ZONE = PROJECT_ROOT / "data" / "raw"
WAREHOUSE_VAR = "DUCKDB_PATH"
DEFAULT_WAREHOUSE = PROJECT_ROOT / "warehouse" / "crypto_dwh.duckdb"


def project_path(env: Mapping[str, str], var: str, default: Path) -> Path:
    value = env.get(var)
    if not value:
        return default
    path = Path(value).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


def landing_zone_dir(env: Mapping[str, str]) -> Path:
    return project_path(env, LANDING_ZONE_VAR, DEFAULT_LANDING_ZONE)


def warehouse_path(env: Mapping[str, str] | None = None) -> Path:
    return project_path(os.environ if env is None else env, WAREHOUSE_VAR, DEFAULT_WAREHOUSE)


load_dotenv(ENV_PATH)
LANDING_ZONE_DIR = landing_zone_dir(os.environ)
