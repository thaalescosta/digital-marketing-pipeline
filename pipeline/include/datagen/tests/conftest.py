"""Test bootstrap: make the src-layout package importable and pin env defaults."""

from __future__ import annotations

import os
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

os.environ.setdefault("SIM_START_DATE", "2026-01-01")
os.environ.setdefault("GEN_SEED", "42")
os.environ.setdefault("DAILY_SESSION_VOLUME", "550")