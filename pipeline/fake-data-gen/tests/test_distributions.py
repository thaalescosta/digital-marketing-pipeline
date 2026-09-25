"""Unit tests for datagen distribution helpers and generated shapes."""

import dataclasses
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from datagen import config
from datagen.dimensions import generate_dimensions
from datagen.distributions import campaign_lifecycle, weekday_seasonality
from datagen.generate_campaigns import generate_ads_campaign_daily
from datagen.generate_youtube import generate_youtube_video_daily
from datagen.schemas import AdsCampaignDailyRow, Ga4EventRow, YoutubeVideoDailyRow


def _month_rows() -> tuple[list[AdsCampaignDailyRow], list[YoutubeVideoDailyRow]]:
    start = date(2026, 5, 1)
    end = date(2026, 5, 31)
    campaigns, _, videos = generate_dimensions(
        np.random.RandomState(config.GEN_SEED), start, end
    )
    ads_rows: list[AdsCampaignDailyRow] = []
    youtube_rows: list[YoutubeVideoDailyRow] = []
    for offset in range((end - start).days + 1):
        day = start + timedelta(days=offset)
        day_rng = np.random.RandomState(config.GEN_SEED + day.toordinal())
        ads_rows.extend(generate_ads_campaign_daily(day, campaigns, day_rng))
        youtube_rows.extend(generate_youtube_video_daily(day, videos, day_rng))
    return ads_rows, youtube_rows


def test_ctr_within_unit_interval() -> None:
    ads_rows, _ = _month_rows()
    assert ads_rows
    for row in ads_rows:
        assert row.clicks >= 0
        assert row.clicks <= row.impressions


def test_metrics_nonnegative() -> None:
    ads_rows, youtube_rows = _month_rows()
    assert ads_rows
    assert youtube_rows
    for row in ads_rows:
        assert row.spend_usd >= 0
        assert row.impressions >= 0
        assert row.clicks >= 0
        assert row.conversions >= 0
    for row in youtube_rows:
        assert row.views >= 0
        assert row.watch_time_min >= 0
        assert row.likes >= 0
        assert row.comments >= 0
        assert row.shares >= 0
        assert row.subscribers_gained >= 0


def test_weekday_greater_than_weekend() -> None:
    monday = date(2026, 5, 4)
    saturday = date(2026, 5, 9)
    assert monday.weekday() == 0
    assert saturday.weekday() == 5
    assert weekday_seasonality(monday) > weekday_seasonality(saturday)

    weekday_samples = (date(2026, 5, 1), date(2026, 5, 4), date(2026, 5, 8))
    for day in weekday_samples:
        assert day.weekday() < 5
        assert weekday_seasonality(day) > 1.0

    weekend_samples = (date(2026, 5, 9), date(2026, 5, 10))
    for day in weekend_samples:
        assert day.weekday() >= 5
        assert weekday_seasonality(day) < 1.0


def test_campaign_lifecycle_boundaries() -> None:
    start = date(2026, 5, 1)
    end = date(2026, 5, 30)
    assert campaign_lifecycle(start - timedelta(days=1), start, end) == 0.0
    assert campaign_lifecycle(end + timedelta(days=1), start, end) == 0.0
    assert campaign_lifecycle(start, start, end) == pytest.approx(0.4)
    assert campaign_lifecycle(start + timedelta(days=14), start, end) == 1.0
    assert campaign_lifecycle(end - timedelta(days=2), start, end) < 1.0
    assert campaign_lifecycle(end, start, end) < 1.0


def test_schema_shape() -> None:
    ts = pd.Timestamp.now(tz="UTC")
    ads = AdsCampaignDailyRow(
        campaign_id=1,
        channel_id=1,
        ad_group_id=123456789,
        ad_id=1234567890,
        spend_date=date(2026, 5, 1),
        spend_usd=123.45,
        impressions=1000,
        clicks=25,
        conversions=2,
        avg_order_value=45.60,
        inserted_at=ts,
        _load_date=date(2026, 5, 1),
    )
    ga4 = Ga4EventRow(
        event_id="123e4567-e89b-12d3-a456-426614174000",
        event_date=date(2026, 5, 1),
        event_timestamp=pd.Timestamp("2026-05-01 10:00:00"),
        event_name="session_start",
        user_pseudo_id=None,
        session_id="20260501-1234567890",
        channel_id=1,
        page_location=None,
        event_value=None,
        inserted_at=ts,
    )
    assert {field.name for field in dataclasses.fields(ads)} == {
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
    assert {field.name for field in dataclasses.fields(ga4)} == {
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