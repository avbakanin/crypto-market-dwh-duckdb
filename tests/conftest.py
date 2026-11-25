"""Keep every test away from the real landing zone, whatever LANDING_ZONE_DIR .env sets.

pytest imports this file before any test module imports ingestion; ingestion.settings
reads the variable once, at import, and .env never overrides a variable that is set.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

SANDBOX_LANDING_ZONE = Path(tempfile.gettempdir()) / "crypto-dwh-tests" / "landing-zone"
os.environ["LANDING_ZONE_DIR"] = str(SANDBOX_LANDING_ZONE)
