"""Shared Parquet writer regression tests (D2/D3, M1 AC: timestamp[us] UTC)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from datagen import generate, generate_backfill
from datagen.parquet_writer import write_partition


def test_timestamp_precision_us_utc(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {"inserted_at": [pd.Timestamp("2026-01-15T12:34:56.123456+05:30")]}
    )
    path = write_partition(frame, tmp_path, "test_table", date(2026, 1, 15))
    assert path is not None

    written = pd.read_parquet(path)
    assert written["inserted_at"].dtype == np.dtype("datetime64[us]")
    expected = pd.Timestamp("2026-01-15T07:04:56.123456").tz_localize(None)
    pd.testing.assert_series_equal(
        written["inserted_at"], pd.Series([expected], dtype="datetime64[us]", name="inserted_at")
    )


def test_empty_frame_writes_nothing(tmp_path: Path) -> None:
    path = write_partition(pd.DataFrame(), tmp_path, "test_table", date(2026, 1, 15))
    assert path is None
    assert not (tmp_path / "test_table" / "test_table_2026-01-15.parquet").exists()


def test_write_partition_layout(tmp_path: Path) -> None:
    frame = pd.DataFrame({"value": [1]})
    path = write_partition(frame, tmp_path, "ads_campaign_daily", date(2026, 7, 1))
    expected = tmp_path / "ads_campaign_daily" / "ads_campaign_daily_2026-07-01.parquet"
    assert path == expected
    assert path.exists()


def test_daily_and_backfill_same_dtypes(tmp_path: Path) -> None:
    day = date(2026, 5, 15)
    inserted = pd.Timestamp("2026-01-01T00:00:00+00:00")
    daily_root = tmp_path / "daily"
    backfill_root = tmp_path / "backfill"
    generate("ga4", day, day, daily_root, inserted, gen_seed=42)
    generate_backfill(day, day, backfill_root, inserted, gen_seed=42)

    daily = pd.read_parquet(
        daily_root / "ga4_events" / f"ga4_events_{day.isoformat()}.parquet"
    )
    backfill = pd.read_parquet(
        backfill_root / "ga4_events" / f"ga4_events_{day.isoformat()}.parquet"
    )
    assert (daily.dtypes == backfill.dtypes).all()
    pd.testing.assert_frame_equal(daily, backfill)