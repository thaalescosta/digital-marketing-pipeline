"""Row models matching the raw BigQuery table contracts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd


@dataclass(frozen=True, slots=True)
class AdsCampaignDailyRow:
    campaign_id: int
    channel_id: int
    ad_group_id: int
    ad_id: int
    spend_date: date
    spend_usd: float
    impressions: int
    clicks: int
    conversions: int
    avg_order_value: float
    inserted_at: pd.Timestamp
    _load_date: date


@dataclass(frozen=True, slots=True)
class Ga4EventRow:
    event_id: str
    event_date: date
    event_timestamp: pd.Timestamp
    event_name: str
    user_pseudo_id: str | None
    session_id: str
    channel_id: int
    page_location: str | None
    event_value: float | None
    inserted_at: pd.Timestamp


@dataclass(frozen=True, slots=True)
class YoutubeVideoDailyRow:
    video_id: str
    video_date: date
    views: int
    watch_time_min: int
    likes: int
    comments: int
    shares: int
    subscribers_gained: int
    inserted_at: pd.Timestamp
    _load_date: date


@dataclass(frozen=True, slots=True)
class DimCampaignRow:
    campaign_id: int
    campaign_name: str
    campaign_type: str
    status: str
    start_date: date
    end_date: date
    daily_budget_usd: float
    inserted_at: pd.Timestamp
    _snapshot_date: date


@dataclass(frozen=True, slots=True)
class DimChannelRow:
    channel_id: int
    channel_name: str
    channel_group: str
    inserted_at: pd.Timestamp
    _snapshot_date: date


@dataclass(frozen=True, slots=True)
class DimVideoRow:
    video_id: str
    video_title: str
    published_at: date
    video_duration_min: int
    inserted_at: pd.Timestamp
    _snapshot_date: date