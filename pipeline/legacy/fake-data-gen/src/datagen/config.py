"""Environment-driven configuration for the datagen package."""

from __future__ import annotations

import calendar
import os
from datetime import date

import pandas as pd

GEN_SEED = int(os.environ.get("GEN_SEED", "42"))
HISTORY_MONTHS = int(os.environ.get("HISTORY_MONTHS", "5"))
DAILY_EVENT_VOLUME = int(os.environ.get("DAILY_EVENT_VOLUME", "550"))
GCS_BUCKET = os.environ.get("GCS_BUCKET", "")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
BIGQUERY_RAW_DATASET = os.environ.get("BIGQUERY_RAW_DATASET", "raw_marketing")
BIGQUERY_STG_DATASET = os.environ.get("BIGQUERY_STG_DATASET", "stg_marketing")
BIGQUERY_MARTS_DATASET = os.environ.get("BIGQUERY_MARTS_DATASET", "marts_marketing")
GCP_REGION = os.environ.get("GCP_REGION", "US")

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


def add_months(day: date, months: int) -> date:
    """Return the calendar date shifted by a whole number of months."""
    month_index = day.year * 12 + (day.month - 1) + months
    year, month_zero = divmod(month_index, 12)
    month = month_zero + 1
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(day.day, last_day))


def _months_ago(months: int) -> date:
    return add_months(date.today(), -months)


BACKFILL_START = os.environ.get("BACKFILL_START", _months_ago(HISTORY_MONTHS).isoformat())


def _utc_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


INSERTED_AT: pd.Timestamp = _utc_now()


def inserted_at() -> pd.Timestamp:
    """Return the fixed load timestamp shared by every row in this run."""
    return INSERTED_AT