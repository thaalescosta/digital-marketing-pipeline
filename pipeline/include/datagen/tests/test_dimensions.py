"""PRD 6.1.2 dimension-evolution invariant tests (fixes D1/D7, D10)."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from datagen import config
from datagen.dimensions import DimSnapshot, campaign_rows, dimensions_as_of

END_DATE = date(2026, 9, 30)


def _days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


@pytest.fixture(scope="module")
def snapshots() -> list[tuple[date, DimSnapshot]]:
    return [
        (day, dimensions_as_of(day, config))
        for day in _days(config.SIM_START_DATE, END_DATE)
    ]


def test_at_least_two_active_campaigns_every_day(
    snapshots: list[tuple[date, DimSnapshot]],
) -> None:
    for day, snapshot in snapshots:
        active = [
            campaign
            for campaign in snapshot.campaigns
            if campaign.start_date <= day <= campaign.end_date
        ]
        assert len(active) >= 2, f"{day.isoformat()} has {len(active)} active campaigns"


def test_at_least_eight_videos_on_start() -> None:
    snapshot = dimensions_as_of(config.SIM_START_DATE, config)
    assert len(snapshot.videos) >= 8


def test_ids_never_change(snapshots: list[tuple[date, DimSnapshot]]) -> None:
    for (day, snapshot), (next_day, next_snapshot) in zip(snapshots, snapshots[1:]):
        assert next_day == day + timedelta(days=1)
        campaign_ids = {campaign.campaign_id for campaign in snapshot.campaigns}
        next_campaign_ids = {campaign.campaign_id for campaign in next_snapshot.campaigns}
        assert campaign_ids.issubset(next_campaign_ids)
        by_id = {campaign.campaign_id: campaign for campaign in snapshot.campaigns}
        next_by_id = {campaign.campaign_id: campaign for campaign in next_snapshot.campaigns}
        for campaign_id in campaign_ids:
            assert by_id[campaign_id] == next_by_id[campaign_id]
        video_ids = {video.video_id for video in snapshot.videos}
        next_video_ids = {video.video_id for video in next_snapshot.videos}
        assert video_ids.issubset(next_video_ids)


def test_status_derived(snapshots: list[tuple[date, DimSnapshot]]) -> None:
    inserted = pd.Timestamp("2026-01-01T00:00:00+00:00")
    for day, snapshot in snapshots:
        for row in campaign_rows(snapshot, day, inserted):
            expected = "Active" if row.end_date >= day else "Ended"
            assert row.status == expected


def test_only_entities_started_by_date(
    snapshots: list[tuple[date, DimSnapshot]],
) -> None:
    for day, snapshot in snapshots:
        assert all(campaign.start_date <= day for campaign in snapshot.campaigns)
        assert all(video.published_at <= day for video in snapshot.videos)


def test_dimension_snapshot_shape() -> None:
    snapshot = dimensions_as_of(config.SIM_START_DATE, config)
    assert len(snapshot.channels) == 7
    assert {channel.channel_id for channel in snapshot.channels} == set(range(1, 8))


def test_dimensions_before_sim_start_empty() -> None:
    snapshot = dimensions_as_of(config.SIM_START_DATE - timedelta(days=1), config)
    assert snapshot.campaigns == ()
    assert snapshot.videos == ()
    assert len(snapshot.channels) == 7