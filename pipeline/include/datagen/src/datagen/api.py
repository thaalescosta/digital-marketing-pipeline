"""Importable API for deterministic datagen generation."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from . import (
    config,
    dimensions,
    generate_campaigns,
    generate_events,
    generate_youtube,
    parquet_writer,
)
from .distributions import rng_for

logger = logging.getLogger(__name__)

SOURCE_TABLES: dict[str, tuple[str, ...]] = {
    "ga4": ("raw_ga4_events",),
    "google_ads": ("raw_ads_campaign_daily",),
    "youtube": ("raw_youtube_video_daily",),
    "dimensions": ("raw_dim_campaign", "raw_dim_channel", "raw_dim_video"),
}

# Directory name written under ``out_dir`` and used as the GCS ``raw/<dir>/``
# key segment. The loader (dags/common/config.py RAW_TABLES -> TABLE_BY_GCS_DIR)
# derives the BigQuery table from these names: ga4_events -> raw_ga4_events.
# The flat short names follow the DESIGN diagram, not PRD's nested
# ``raw/<source>/<entity>/`` (airflow-specialist decision, logged in config.py).
TABLE_DIRS: dict[str, tuple[str, ...]] = {
    "ga4": ("ga4_events",),
    "google_ads": ("ads_campaign_daily",),
    "youtube": ("youtube_video_daily",),
    "dimensions": ("dim_campaign", "dim_channel", "dim_video"),
}


def generate(
    source: str,
    start_date: date,
    end_date: date,
    out_dir: Path,
    inserted_at: pd.Timestamp,
    gen_seed: int = 42,
) -> list[Path]:
    """Generate deterministic Parquet partitions for one source and date range.

    Writes one file per day per table under ``out_dir/<table>/dt=YYYY-MM-DD/``
    and returns the written paths. Rows depend only on ``(gen_seed, date,
    source)``; ``inserted_at`` is run metadata stamped by the caller so the
    library never reads the wall clock (D5).
    """
    if source not in SOURCE_TABLES:
        raise ValueError(
            f"Unknown source {source!r}; expected one of "
            f"{sorted(SOURCE_TABLES)}"
        )
    if start_date > end_date:
        raise ValueError(
            f"start_date {start_date.isoformat()} must not be after "
            f"end_date {end_date.isoformat()}"
        )
    out_dir = Path(out_dir)
    inserted = (
        inserted_at
        if isinstance(inserted_at, pd.Timestamp)
        else pd.Timestamp(inserted_at)
    )
    written: list[Path] = []
    for day in _iter_days(start_date, end_date):
        for table, frame in _frames_for(source, day, inserted, gen_seed):
            path = parquet_writer.write_partition(frame, out_dir, table, day)
            if path is None:
                logger.warning(
                    "empty %s partition on %s; no file written",
                    table,
                    day.isoformat(),
                )
                continue
            logger.info(
                "wrote %s rows to %s for %s",
                len(frame),
                table,
                day.isoformat(),
            )
            written.append(path)
    return written


def generate_backfill(
    start_date: date,
    end_date: date,
    out_dir: Path,
    inserted_at: pd.Timestamp,
    gen_seed: int = 42,
    source: str | None = None,
) -> list[Path]:
    """Generate deterministic Parquet partitions across an inclusive date range.

    If ``source`` is provided, generate only that source. If ``source`` is None
    (default), generate all sources in ``SOURCE_TABLES``.
    """
    written: list[Path] = []
    sources = [source] if source is not None else list(SOURCE_TABLES)
    if source is not None and source not in SOURCE_TABLES:
        raise ValueError(
            f"Unknown source {source!r}; expected one of "
            f"{sorted(SOURCE_TABLES)}"
        )
    for src in sources:
        written.extend(
            generate(src, start_date, end_date, out_dir, inserted_at, gen_seed)
        )
    return written


def _frames_for(
    source: str,
    day: date,
    inserted_at: pd.Timestamp,
    gen_seed: int,
) -> Iterator[tuple[str, pd.DataFrame]]:
    if source == "ga4":
        rng = rng_for(source, day, gen_seed)
        rows = generate_events.generate_ga4_events(
            day, rng, config.DAILY_SESSION_VOLUME, inserted_at
        )
        yield "ga4_events", parquet_writer.frame_from_rows(rows)
    elif source == "google_ads":
        snapshot = dimensions.dimensions_as_of(day, config)
        rng = rng_for(source, day, gen_seed)
        rows = generate_campaigns.generate_ads_campaign_daily(
            day, snapshot.campaigns, rng, inserted_at
        )
        yield "ads_campaign_daily", parquet_writer.frame_from_rows(rows)
    elif source == "youtube":
        snapshot = dimensions.dimensions_as_of(day, config)
        rng = rng_for(source, day, gen_seed)
        rows = generate_youtube.generate_youtube_video_daily(
            day, snapshot.videos, rng, inserted_at
        )
        yield "youtube_video_daily", parquet_writer.frame_from_rows(rows)
    else:
        snapshot = dimensions.dimensions_as_of(day, config)
        yield "dim_campaign", parquet_writer.frame_from_rows(
            dimensions.campaign_rows(snapshot, day, inserted_at)
        )
        yield "dim_channel", parquet_writer.frame_from_rows(
            dimensions.channel_rows(snapshot, day, inserted_at)
        )
        yield "dim_video", parquet_writer.frame_from_rows(
            dimensions.video_rows(snapshot, day, inserted_at)
        )


def _iter_days(start: date, end: date) -> Iterator[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)