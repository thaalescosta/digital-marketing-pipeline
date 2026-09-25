"""Distribution-range and D7/D8 invariant tests (PRD 6.1.3, M1 AC, AT-003/AT-004)."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from datagen import config, generate
from datagen.dimensions import dimensions_as_of

INSERTED_AT = pd.Timestamp("2026-01-01T00:00:00+00:00")

ADS_START = date(2026, 1, 1)
ADS_END = date(2026, 9, 30)
GA4_START = date(2026, 1, 5)
GA4_END = date(2026, 1, 11)
YT_START = date(2026, 1, 1)
YT_END = date(2026, 1, 31)


def _days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def _read_partition(root: Path, table: str, day: date) -> pd.DataFrame:
    path = root / table / f"{table}_{day.isoformat()}.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


@pytest.fixture(scope="module")
def ads_frames(tmp_path_factory: pytest.TempPathFactory) -> list[pd.DataFrame]:
    root = tmp_path_factory.mktemp("ads")
    generate("google_ads", ADS_START, ADS_END, root, INSERTED_AT, gen_seed=42)
    return [_read_partition(root, "ads_campaign_daily", day) for day in _days(ADS_START, ADS_END)]


@pytest.fixture(scope="module")
def ga4_frames(tmp_path_factory: pytest.TempPathFactory) -> list[pd.DataFrame]:
    root = tmp_path_factory.mktemp("ga4")
    generate("ga4", GA4_START, GA4_END, root, INSERTED_AT, gen_seed=42)
    return [_read_partition(root, "ga4_events", day) for day in _days(GA4_START, GA4_END)]


@pytest.fixture(scope="module")
def youtube_frames(tmp_path_factory: pytest.TempPathFactory) -> list[pd.DataFrame]:
    root = tmp_path_factory.mktemp("youtube")
    generate("youtube", YT_START, YT_END, root, INSERTED_AT, gen_seed=42)
    return [_read_partition(root, "youtube_video_daily", day) for day in _days(YT_START, YT_END)]


def test_ads_rows_present_every_day(ads_frames: list[pd.DataFrame]) -> None:
    for day, frame in zip(_days(ADS_START, ADS_END), ads_frames):
        assert not frame.empty, f"ads rows missing on {day.isoformat()}"


def test_max_spend_budget_ratio_le_1_3(ads_frames: list[pd.DataFrame]) -> None:
    for day, frame in zip(_days(ADS_START, ADS_END), ads_frames):
        if frame.empty:
            continue
        snapshot = dimensions_as_of(day, config)
        budgets = {
            campaign.campaign_id: campaign.daily_budget_usd
            for campaign in snapshot.campaigns
        }
        for campaign_id, spend in frame.groupby("campaign_id")["spend_usd"].sum().items():
            assert spend / budgets[campaign_id] <= 1.3


def test_ad_ids_stable_within_campaign(ads_frames: list[pd.DataFrame]) -> None:
    daily_pairs: dict[int, list[frozenset[tuple[int, int]]]] = defaultdict(list)
    for frame in ads_frames:
        if frame.empty:
            continue
        for campaign_id, group in frame.groupby("campaign_id"):
            pairs = frozenset(zip(group["ad_group_id"], group["ad_id"]))
            assert pairs
            daily_pairs[campaign_id].append(pairs)
    assert daily_pairs
    for campaign_id, pairs_by_day in daily_pairs.items():
        assert len(set(pairs_by_day)) == 1, f"campaign {campaign_id} changed ad ids"


def test_clicks_le_impressions(ads_frames: list[pd.DataFrame]) -> None:
    merged = pd.concat(ads_frames, ignore_index=True)
    assert len(merged) > 0
    assert (merged["clicks"] <= merged["impressions"]).all()


def test_conversions_le_clicks(ads_frames: list[pd.DataFrame]) -> None:
    merged = pd.concat(ads_frames, ignore_index=True)
    assert len(merged) > 0
    assert (merged["conversions"] <= merged["clicks"]).all()
    assert (merged["spend_usd"] >= 0).all()


def test_budget_positive() -> None:
    for day in _days(ADS_START, ADS_END):
        snapshot = dimensions_as_of(day, config)
        for campaign in snapshot.campaigns:
            assert campaign.daily_budget_usd > 0


def test_event_names_in_enum(ga4_frames: list[pd.DataFrame]) -> None:
    merged = pd.concat(ga4_frames, ignore_index=True)
    assert len(merged) > 0
    assert set(merged["event_name"]) <= set(config.EVENT_NAMES)


def test_channel_ids_in_1_7(ga4_frames: list[pd.DataFrame]) -> None:
    merged = pd.concat(ga4_frames, ignore_index=True)
    assert len(merged) > 0
    assert merged["channel_id"].between(1, 7).all()


def test_purchase_event_value_only(ga4_frames: list[pd.DataFrame]) -> None:
    merged = pd.concat(ga4_frames, ignore_index=True)
    non_purchase = merged.loc[merged["event_name"] != "purchase", "event_value"]
    assert non_purchase.isna().all()
    purchase_value = merged.loc[merged["event_name"] == "purchase", "event_value"]
    assert not purchase_value.isna().all()
    assert (purchase_value >= 0).all()


def test_session_crossing_midnight(ga4_frames: list[pd.DataFrame]) -> None:
    merged = pd.concat(ga4_frames, ignore_index=True)
    event_date = pd.to_datetime(merged["event_date"])
    event_timestamp = pd.to_datetime(merged["event_timestamp"])
    assert (event_timestamp >= event_date).all()
    assert (event_timestamp < event_date + pd.Timedelta(days=2)).all()


def test_counts_non_negative(youtube_frames: list[pd.DataFrame]) -> None:
    merged = pd.concat(youtube_frames, ignore_index=True)
    assert len(merged) > 0
    for column in ("views", "watch_time_min", "likes", "comments", "shares", "subscribers_gained"):
        assert (merged[column] >= 0).all()


def test_likes_le_views(youtube_frames: list[pd.DataFrame]) -> None:
    merged = pd.concat(youtube_frames, ignore_index=True)
    assert (merged["likes"] <= merged["views"]).all()


def test_subscribers_le_views(youtube_frames: list[pd.DataFrame]) -> None:
    merged = pd.concat(youtube_frames, ignore_index=True)
    assert (merged["subscribers_gained"] <= merged["views"]).all()


def test_days_since_published_ge_0(youtube_frames: list[pd.DataFrame]) -> None:
    for day, frame in zip(_days(YT_START, YT_END), youtube_frames):
        if frame.empty:
            continue
        snapshot = dimensions_as_of(day, config)
        published = {video.video_id: video.published_at for video in snapshot.videos}
        video_date = pd.to_datetime(frame["video_date"])
        published_date = pd.to_datetime(frame["video_id"].map(published))
        assert (video_date >= published_date).all()