"""YouTube Analytics-style daily video metrics generation."""

from __future__ import annotations

import zlib
from datetime import date

import numpy as np

from . import config
from .distributions import youtube_daily_views
from .schemas import DimVideoRow, YoutubeVideoDailyRow


def _base_popularity(video_id: str) -> int:
    return 500 + zlib.crc32(video_id.encode("utf-8")) % 7500


def _watch_time_mean(video_id: str) -> float:
    return 2.0 + (zlib.crc32(video_id.encode("utf-8")) % 200) / 100.0


def generate_youtube_video_daily(
    day: date,
    videos: list[DimVideoRow],
    rng: np.random.RandomState,
) -> list[YoutubeVideoDailyRow]:
    """Generate one YoutubeVideoDailyRow per published video for the day."""
    ts = config.inserted_at()
    rows: list[YoutubeVideoDailyRow] = []
    for video in videos:
        if day < video.published_at:
            continue
        views = youtube_daily_views(rng, video.published_at, day, _base_popularity(video.video_id))
        avg_watch_min = _watch_time_mean(video.video_id)
        rows.append(
            YoutubeVideoDailyRow(
                video_id=video.video_id,
                video_date=day,
                views=views,
                watch_time_min=int(views * avg_watch_min),
                likes=int(views * rng.uniform(0.02, 0.06)),
                comments=int(views * rng.uniform(0.001, 0.01)),
                shares=int(views * rng.uniform(0.001, 0.02)),
                subscribers_gained=int(views * rng.uniform(0.01, 0.05)),
                inserted_at=ts,
                _load_date=day,
            )
        )
    return rows