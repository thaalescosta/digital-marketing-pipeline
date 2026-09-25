"""Environment-driven configuration for the datagen package.

No wall-clock defaults: dates and run timestamps are explicit parameters
supplied by Airflow or the wrapper CLIs (PRD D5).
"""

from __future__ import annotations

import os
from datetime import date

GEN_SEED = int(os.environ.get("GEN_SEED", "42"))
DAILY_SESSION_VOLUME = int(os.environ.get("DAILY_SESSION_VOLUME", "550"))
GCS_BUCKET = os.environ.get("GCS_BUCKET", "thaalescosta_marketing")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "digital-marketing-509604")
BQ_RAW_DATASET = os.environ.get("BQ_RAW_DATASET", "digital_marketing_raw")
BQ_STAGING_DATASET = os.environ.get("BQ_STAGING_DATASET", "digital_marketing_staging")
BQ_MARTS_DATASET = os.environ.get("BQ_MARTS_DATASET", "digital_marketing_marts")
GCP_LOCATION = os.environ.get("GCP_LOCATION", "US")
SIM_START_DATE = date.fromisoformat(os.environ.get("SIM_START_DATE", "2026-01-01"))

EVENT_NAMES: tuple[str, ...] = (
    "session_start",
    "page_view",
    "scroll",
    "add_to_cart",
    "begin_checkout",
    "purchase",
)

CHANNELS: dict[int, tuple[str, str]] = {
    1: ("paid_search", "Search"),
    2: ("organic_search", "Search"),
    3: ("direct", "Direct"),
    4: ("social", "Social"),
    5: ("email", "Email"),
    6: ("referral", "Referral"),
    7: ("organic_video", "Organic Video"),
}

CAMPAIGN_TYPES: tuple[str, ...] = ("Search", "Display", "Shopping", "Video")