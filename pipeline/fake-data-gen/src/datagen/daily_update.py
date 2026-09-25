"""Single-day update CLI for the most recent marketing day."""

from __future__ import annotations

import argparse
import dataclasses
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from . import config, dimensions, generate_campaigns, generate_events, generate_youtube
from .schemas import DimCampaignRow, DimChannelRow, DimVideoRow


def _as_date(value: date | pd.Timestamp) -> date:
    return value.date() if isinstance(value, pd.Timestamp) else value


def _read_dim_campaigns(path: Path) -> list[DimCampaignRow]:
    frame = pd.read_parquet(path)
    return [
        DimCampaignRow(
            campaign_id=int(record["campaign_id"]),
            campaign_name=str(record["campaign_name"]),
            campaign_type=str(record["campaign_type"]),
            status=str(record["status"]),
            start_date=_as_date(record["start_date"]),
            end_date=_as_date(record["end_date"]),
            daily_budget_usd=float(record["daily_budget_usd"]),
            inserted_at=pd.Timestamp(record["inserted_at"]),
        )
        for record in frame.to_dict(orient="records")
    ]


def _read_dim_channels(path: Path) -> list[DimChannelRow]:
    frame = pd.read_parquet(path)
    return [
        DimChannelRow(
            channel_id=int(record["channel_id"]),
            channel_name=str(record["channel_name"]),
            channel_group=str(record["channel_group"]),
            inserted_at=pd.Timestamp(record["inserted_at"]),
        )
        for record in frame.to_dict(orient="records")
    ]


def _read_dim_videos(path: Path) -> list[DimVideoRow]:
    frame = pd.read_parquet(path)
    return [
        DimVideoRow(
            video_id=str(record["video_id"]),
            video_title=str(record["video_title"]),
            published_at=_as_date(record["published_at"]),
            video_duration_min=int(record["video_duration_min"]),
            inserted_at=pd.Timestamp(record["inserted_at"]),
        )
        for record in frame.to_dict(orient="records")
    ]


def _load_dimensions(
    output_dir: Path,
    day: date,
) -> tuple[list[DimCampaignRow], list[DimChannelRow], list[DimVideoRow]]:
    """Load existing dimension Parquet files or regenerate them when absent."""
    dims_dir = output_dir / "dims"
    campaign_path = dims_dir / "dim_campaign.parquet"
    channel_path = dims_dir / "dim_channel.parquet"
    video_path = dims_dir / "dim_video.parquet"
    if campaign_path.exists() and channel_path.exists() and video_path.exists():
        return (
            _read_dim_campaigns(campaign_path),
            _read_dim_channels(channel_path),
            _read_dim_videos(video_path),
        )
    rng = np.random.RandomState(config.GEN_SEED)
    start = config.add_months(day, -config.HISTORY_MONTHS)
    return dimensions.generate_dimensions(rng, start, day)


def _write_rows(rows: list[Any], path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame([dataclasses.asdict(row) for row in rows])
    if frame.empty:
        return 0
    frame.to_parquet(path, index=False)
    return len(frame)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write deterministic fake marketing Parquet partitions for a single date."
    )
    parser.add_argument("--output-dir", default=".data/parquet")
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    parser.add_argument("--date", default=yesterday)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the single-day update and print per-source row counts."""
    args = _parse_args(argv)
    output_dir = Path(args.output_dir)
    day = date.fromisoformat(args.date)
    campaigns, _, videos = _load_dimensions(output_dir, day)
    day_rng = np.random.RandomState(config.GEN_SEED + day.toordinal())
    ads_rows = generate_campaigns.generate_ads_campaign_daily(day, campaigns, day_rng)
    event_rows = generate_events.generate_ga4_events(day, day_rng, config.DAILY_EVENT_VOLUME)
    youtube_rows = generate_youtube.generate_youtube_video_daily(day, videos, day_rng)
    day_label = day.isoformat()
    ads_count = _write_rows(ads_rows, output_dir / "ads" / f"dt={day_label}" / "part.parquet")
    event_count = _write_rows(
        event_rows, output_dir / "ga4_events" / f"dt={day_label}" / "part.parquet"
    )
    youtube_count = _write_rows(
        youtube_rows, output_dir / "youtube" / f"dt={day_label}" / "part.parquet"
    )
    total = ads_count + event_count + youtube_count
    print(f"ads: {ads_count} rows")
    print(f"ga4_events: {event_count} rows")
    print(f"youtube: {youtube_count} rows")
    print(f"total: {total} rows")
    return 0