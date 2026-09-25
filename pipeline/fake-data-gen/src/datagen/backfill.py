"""Historical backfill CLI that writes deterministic daily Parquet tables."""

from __future__ import annotations

import argparse
import dataclasses
from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from . import config, dimensions, generate_campaigns, generate_events, generate_youtube


def _iter_days(start: date, end_exclusive: date) -> Iterator[date]:
    current = start
    while current < end_exclusive:
        yield current
        current += timedelta(days=1)


def _write_rows(rows: list[Any], path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame([dataclasses.asdict(row) for row in rows])
    if frame.empty:
        return 0
    _coerce_timestamps_to_us(frame)
    frame.to_parquet(path, index=False)
    return len(frame)


def _coerce_timestamps_to_us(frame: pd.DataFrame) -> None:
    """Downcast datetime/datetime64 columns to microsecond precision in place.

    BigQuery's native Parquet loader only accepts timestamp precision at or
    below microseconds (``TIMESTAMP_MICROS``/``TIMESTAMP_MILLIS``/``TIMESTAMP``).
    pandas writes ``datetime64[ns]`` (parquet ``TIMESTAMP(NANOS)``) by default,
    which BigQuery rejects with a 400 on load (``Invalid timestamp nanoseconds
    value ... of logical type TIMESTAMP_NANOS``). Coercing to ``us`` is the
    canonical, single-point fix applied to every emitted Parquet file
    (daily raw tables and dimension tables alike).
    """
    for column_name in frame.columns:
        column = frame[column_name]
        if pd.api.types.is_datetime64_any_dtype(column.dtype):
            frame[column_name] = column.astype("datetime64[us]")
        elif isinstance(column.dtype, pd.DatetimeTZDtype):
            # Timezone-aware datetimes need a roundtrip to a naive microsecond
            # UTC dtype that pyarrow can write as TIMESTAMP(MICROS, UTC).
            frame[column_name] = column.dt.tz_convert("UTC").dt.tz_localize(None).astype("datetime64[us]")


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill deterministic fake marketing data as partitioned Parquet files."
    )
    parser.add_argument("--output-dir", default=".data/parquet")
    parser.add_argument("--start-date", default=config.BACKFILL_START)
    parser.add_argument("--months", type=int, default=config.HISTORY_MONTHS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the historical backfill and print per-source row counts."""
    args = _parse_args(argv)
    output_dir = Path(args.output_dir)
    start = date.fromisoformat(args.start_date)
    end_exclusive = config.add_months(start, args.months)
    root_rng = np.random.RandomState(config.GEN_SEED)
    campaigns, channels, videos = dimensions.generate_dimensions(
        root_rng, start, end_exclusive - timedelta(days=1)
    )
    counts: dict[str, int] = {"ads": 0, "ga4_events": 0, "youtube": 0}
    for day in _iter_days(start, end_exclusive):
        day_rng = np.random.RandomState(config.GEN_SEED + day.toordinal())
        ads_rows = generate_campaigns.generate_ads_campaign_daily(day, campaigns, day_rng)
        event_rows = generate_events.generate_ga4_events(day, day_rng, config.DAILY_EVENT_VOLUME)
        youtube_rows = generate_youtube.generate_youtube_video_daily(day, videos, day_rng)
        day_label = day.isoformat()
        counts["ads"] += _write_rows(
            ads_rows, output_dir / "ads" / f"dt={day_label}" / "part.parquet"
        )
        counts["ga4_events"] += _write_rows(
            event_rows, output_dir / "ga4_events" / f"dt={day_label}" / "part.parquet"
        )
        counts["youtube"] += _write_rows(
            youtube_rows, output_dir / "youtube" / f"dt={day_label}" / "part.parquet"
        )
    dims_dir = output_dir / "dims"
    dims_dir.mkdir(parents=True, exist_ok=True)
    _write_rows(campaigns, dims_dir / "dim_campaign.parquet")
    _write_rows(channels, dims_dir / "dim_channel.parquet")
    _write_rows(videos, dims_dir / "dim_video.parquet")
    total = sum(counts.values())
    for source in ("ads", "ga4_events", "youtube"):
        print(f"{source}: {counts[source]} rows")
    print(f"total: {total} rows")
    return 0