"""Deterministic dimension state machine: pure replay per snapshot date.

``dimensions_as_of`` is a pure function of ``(seed, date)``: it replays the
campaign/video launch schedule from ``SIM_START_DATE`` up to the target date
and returns the exact snapshot that existed on that date. No files and no
shared mutable state, so any day can be generated in any order (D1, DESIGN
decision 6, PRD 6.1.2).
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Protocol

import numpy as np
import pandas as pd

from .distributions import rng_for
from .schemas import DimCampaignRow, DimChannelRow, DimVideoRow

_MIN_ACTIVE_CAMPAIGNS = 2
_CAMPAIGN_LAUNCH_PROBABILITY = 0.10
_MIN_INITIAL_VIDEOS = 8
_VIDEO_PUBLISH_PROBABILITY = 0.07
_VIDEO_ID_WIDTH = 4

_CAMPAIGN_SOURCE = "dimensions:campaigns"
_VIDEO_SOURCE = "dimensions:videos"

_CAMPAIGN_TEMPLATES: tuple[tuple[str, str], ...] = (
    ("Search - Brand {value}", "Search"),
    ("Search - Non-brand {value}", "Search"),
    ("Display - Prospecting {value}", "Display"),
    ("Display - Retargeting", "Display"),
    ("Shopping - {value}", "Shopping"),
    ("Video - Awareness {value}", "Video"),
    ("Video - Remarketing", "Video"),
)

_VIDEO_TITLES: tuple[str, ...] = (
    "How we built our data pipeline",
    "Product walkthrough",
    "5 tips for marketing analysts",
    "Customer story: Acme Corp",
    "Feature spotlight: agent routing",
    "Behind the scenes at the data team",
    "Tutorial: getting started with dbt",
    "Webinar replay: Spec-Driven Development",
    "Build vs buy: choosing a warehouse",
    "Campaign measurement best practices",
    "Migration guide: legacy to modern stack",
    "Q&A with the founders",
)


@dataclass(frozen=True, slots=True)
class CampaignEntity:
    campaign_id: int
    campaign_name: str
    campaign_type: str
    start_date: date
    end_date: date
    daily_budget_usd: float


@dataclass(frozen=True, slots=True)
class ChannelEntity:
    channel_id: int
    channel_name: str
    channel_group: str


@dataclass(frozen=True, slots=True)
class VideoEntity:
    video_id: str
    video_title: str
    published_at: date
    video_duration_min: int


@dataclass(frozen=True, slots=True)
class DimSnapshot:
    campaigns: tuple[CampaignEntity, ...]
    channels: tuple[ChannelEntity, ...]
    videos: tuple[VideoEntity, ...]


class DatagenConfig(Protocol):
    SIM_START_DATE: date
    GEN_SEED: int
    CHANNELS: dict[int, tuple[str, str]]


def dimensions_as_of(target_date: date, config: DatagenConfig) -> DimSnapshot:
    """Return the deterministic dimension snapshot that existed on target_date."""
    if target_date < config.SIM_START_DATE:
        return DimSnapshot(campaigns=(), channels=(), videos=())
    campaigns = tuple(
        campaign
        for campaign in _campaigns_through(target_date, config)
        if campaign.start_date <= target_date
    )
    channels = _channel_entities(config)
    videos = tuple(
        video
        for video in _videos_through(target_date, config)
        if video.published_at <= target_date
    )
    return DimSnapshot(campaigns=campaigns, channels=channels, videos=videos)


def campaign_rows(
    snapshot: DimSnapshot,
    snapshot_date: date,
    inserted_at: pd.Timestamp,
) -> list[DimCampaignRow]:
    """Stamp campaign entities into raw contract rows for the snapshot date."""
    return [
        DimCampaignRow(
            campaign_id=campaign.campaign_id,
            campaign_name=campaign.campaign_name,
            campaign_type=campaign.campaign_type,
            status="Active" if campaign.end_date >= snapshot_date else "Ended",
            start_date=campaign.start_date,
            end_date=campaign.end_date,
            daily_budget_usd=campaign.daily_budget_usd,
            inserted_at=inserted_at,
            _snapshot_date=snapshot_date,
        )
        for campaign in snapshot.campaigns
    ]


def channel_rows(
    snapshot: DimSnapshot,
    snapshot_date: date,
    inserted_at: pd.Timestamp,
) -> list[DimChannelRow]:
    """Stamp channel entities into raw contract rows for the snapshot date."""
    return [
        DimChannelRow(
            channel_id=channel.channel_id,
            channel_name=channel.channel_name,
            channel_group=channel.channel_group,
            inserted_at=inserted_at,
            _snapshot_date=snapshot_date,
        )
        for channel in snapshot.channels
    ]


def video_rows(
    snapshot: DimSnapshot,
    snapshot_date: date,
    inserted_at: pd.Timestamp,
) -> list[DimVideoRow]:
    """Stamp video entities into raw contract rows for the snapshot date."""
    return [
        DimVideoRow(
            video_id=video.video_id,
            video_title=video.video_title,
            published_at=video.published_at,
            video_duration_min=video.video_duration_min,
            inserted_at=inserted_at,
            _snapshot_date=snapshot_date,
        )
        for video in snapshot.videos
    ]


def _iter_days(start: date, end_exclusive: date) -> Iterator[date]:
    current = start
    while current < end_exclusive:
        yield current
        current += timedelta(days=1)


def _new_campaign(
    campaign_id: int,
    start_date: date,
    rng: np.random.Generator,
) -> CampaignEntity:
    template_index = int(rng.integers(0, len(_CAMPAIGN_TEMPLATES)))
    template, campaign_type = _CAMPAIGN_TEMPLATES[template_index]
    lifetime_days = 30 + int(rng.integers(0, 121))
    end_date = start_date + timedelta(days=lifetime_days)
    daily_budget_usd = round(float(rng.uniform(25.0, 500.0)), 2)
    campaign_name = template.format(value=f"Campaign {campaign_id:02d}")
    return CampaignEntity(
        campaign_id=campaign_id,
        campaign_name=campaign_name,
        campaign_type=campaign_type,
        start_date=start_date,
        end_date=end_date,
        daily_budget_usd=daily_budget_usd,
    )


def _new_video(
    video_id: int,
    published_at: date,
    rng: np.random.Generator,
) -> VideoEntity:
    title_index = int(rng.integers(0, len(_VIDEO_TITLES)))
    duration_min = 3 + int(rng.integers(0, 38))
    return VideoEntity(
        video_id=f"vid_{video_id:0{_VIDEO_ID_WIDTH}d}",
        video_title=_VIDEO_TITLES[title_index],
        published_at=published_at,
        video_duration_min=duration_min,
    )


def _campaigns_through(
    target_date: date,
    config: DatagenConfig,
) -> tuple[CampaignEntity, ...]:
    campaigns: list[CampaignEntity] = []
    next_id = 1
    for day in _iter_days(config.SIM_START_DATE, target_date + timedelta(days=1)):
        rng = rng_for(_CAMPAIGN_SOURCE, day, config.GEN_SEED)
        if day == config.SIM_START_DATE:
            initial_count = 8 + int(rng.integers(0, 4))
            for _ in range(initial_count):
                campaigns.append(_new_campaign(next_id, day, rng))
                next_id += 1
            continue
        active_count = sum(
            1
            for campaign in campaigns
            if campaign.start_date <= day <= campaign.end_date
        )
        if active_count < _MIN_ACTIVE_CAMPAIGNS:
            launches = _MIN_ACTIVE_CAMPAIGNS - active_count
        elif rng.random() < _CAMPAIGN_LAUNCH_PROBABILITY:
            launches = 1
        else:
            launches = 0
        for _ in range(launches):
            campaigns.append(_new_campaign(next_id, day, rng))
            next_id += 1
    return tuple(campaigns)


def _videos_through(
    target_date: date,
    config: DatagenConfig,
) -> tuple[VideoEntity, ...]:
    videos: list[VideoEntity] = []
    next_id = 1
    for day in _iter_days(config.SIM_START_DATE, target_date + timedelta(days=1)):
        rng = rng_for(_VIDEO_SOURCE, day, config.GEN_SEED)
        if day == config.SIM_START_DATE:
            initial_count = _MIN_INITIAL_VIDEOS + int(rng.integers(0, 3))
            for _ in range(initial_count):
                videos.append(_new_video(next_id, day, rng))
                next_id += 1
            continue
        if rng.random() < _VIDEO_PUBLISH_PROBABILITY:
            videos.append(_new_video(next_id, day, rng))
            next_id += 1
    return tuple(videos)


def _channel_entities(config: DatagenConfig) -> tuple[ChannelEntity, ...]:
    return tuple(
        ChannelEntity(
            channel_id=channel_id,
            channel_name=channel_name,
            channel_group=channel_group,
        )
        for channel_id, (channel_name, channel_group) in sorted(config.CHANNELS.items())
    )