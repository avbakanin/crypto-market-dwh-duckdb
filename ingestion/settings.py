"""Where the project keeps its files. Read once, at import.

LANDING_ZONE_DIR (environment, or .env in the repo root) moves the landing zone - the
extracts' immutable batches and their watermark - out of the repo:

- unset or empty: data/raw in the repo;
- relative: resolved against the repo root, not the working directory;
- absolute: as is (the DuckDB port first reads the Snowflake original's data/raw).

.env never overrides a variable the environment already has (Airflow, Docker, tests).
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


def landing_zone_dir(env: Mapping[str, str]) -> Path:
    value = env.get(LANDING_ZONE_VAR)
    if not value:
        return DEFAULT_LANDING_ZONE
    path = Path(value).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


load_dotenv(ENV_PATH)
LANDING_ZONE_DIR = landing_zone_dir(os.environ)
