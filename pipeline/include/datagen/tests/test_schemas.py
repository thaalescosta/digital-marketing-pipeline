"""Row-model contract tests against PRD section 5 raw columns (D10)."""

from __future__ import annotations

import dataclasses

from datagen.schemas import (
    AdsCampaignDailyRow,
    DimCampaignRow,
    DimChannelRow,
    DimVideoRow,
    Ga4EventRow,
    YoutubeVideoDailyRow,
)


def _field_names(row_model: type) -> set[str]:
    return {field.name for field in dataclasses.fields(row_model)}


def test_dim_rows_have_snapshot_date() -> None:
    for row_model in (DimCampaignRow, DimChannelRow, DimVideoRow):
        assert "_snapshot_date" in _field_names(row_model)


def test_ga4_row_matches_contract() -> None:
    assert _field_names(Ga4EventRow) == {
        "event_id",
        "event_date",
        "event_timestamp",
        "event_name",
        "user_pseudo_id",
        "session_id",
        "channel_id",
        "page_location",
        "event_value",
        "inserted_at",
    }


def test_ads_row_matches_contract() -> None:
    assert _field_names(AdsCampaignDailyRow) == {
        "campaign_id",
        "channel_id",
        "ad_group_id",
        "ad_id",
        "spend_date",
        "spend_usd",
        "impressions",
        "clicks",
        "conversions",
        "avg_order_value",
        "inserted_at",
        "_load_date",
    }


def test_youtube_row_matches_contract() -> None:
    assert _field_names(YoutubeVideoDailyRow) == {
        "video_id",
        "video_date",
        "views",
        "watch_time_min",
        "likes",
        "comments",
        "shares",
        "subscribers_gained",
        "inserted_at",
        "_load_date",
    }


def test_dim_campaign_row_matches_contract() -> None:
    assert _field_names(DimCampaignRow) == {
        "campaign_id",
        "campaign_name",
        "campaign_type",
        "status",
        "start_date",
        "end_date",
        "daily_budget_usd",
        "inserted_at",
        "_snapshot_date",
    }


def test_dim_channel_row_matches_contract() -> None:
    assert _field_names(DimChannelRow) == {
        "channel_id",
        "channel_name",
        "channel_group",
        "inserted_at",
        "_snapshot_date",
    }


def test_dim_video_row_matches_contract() -> None:
    assert _field_names(DimVideoRow) == {
        "video_id",
        "video_title",
        "published_at",
        "video_duration_min",
        "inserted_at",
        "_snapshot_date",
    }