"""Dimension table generation for campaigns, channels, and videos."""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np

from . import config
from .schemas import DimCampaignRow, DimChannelRow, DimVideoRow

_CAMPAIGN_TEMPLATES = (
    ("Search - Brand {value}", "Search"),
    ("Search - Non-brand {value}", "Search"),
    ("Display - Prospecting {value}", "Display"),
    ("Display - Retargeting", "Display"),
    ("Shopping - {value}", "Shopping"),
    ("Video - Awareness {value}", "Video"),
    ("Video - Remarketing", "Video"),
)

_VIDEO_TITLES = (
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


def _random_date(rng: np.random.RandomState, start: date, end: date) -> date:
    span = (end - start).days
    return start + timedelta(days=int(rng.randint(0, span + 1)))


def generate_dimensions(
    rng: np.random.RandomState,
    start: date,
    end: date,
) -> tuple[list[DimCampaignRow], list[DimChannelRow], list[DimVideoRow]]:
    """Generate campaign, channel, and video dimension rows for the window."""
    ts = config.inserted_at()
    campaign_rows: list[DimCampaignRow] = []
    campaign_count = int(rng.randint(8, 15))
    for index in range(campaign_count):
        template, campaign_type = _CAMPAIGN_TEMPLATES[int(rng.randint(0, len(_CAMPAIGN_TEMPLATES)))]
        campaign_name = template.format(value=f"Campaign {index + 1:02d}")
        span_days = max((end - start).days - 14, 0)
        campaign_start = start + timedelta(days=int(rng.randint(0, span_days + 1)))
        max_life = (end - campaign_start).days
        life = int(rng.randint(14, max(max_life, 14) + 1))
        campaign_end = min(end, campaign_start + timedelta(days=life))
        campaign_rows.append(
            DimCampaignRow(
                campaign_id=index + 1,
                campaign_name=campaign_name,
                campaign_type=campaign_type,
                status="Active" if campaign_end >= end else "Ended",
                start_date=campaign_start,
                end_date=campaign_end,
                daily_budget_usd=round(float(rng.uniform(25, 500)), 2),
                inserted_at=ts,
            )
        )
    channel_rows = [
        DimChannelRow(
            channel_id=channel_id,
            channel_name=channel_name,
            channel_group=channel_group,
            inserted_at=ts,
        )
        for channel_id, (channel_name, channel_group) in sorted(config.CHANNELS.items())
    ]
    video_rows: list[DimVideoRow] = []
    video_count = int(rng.randint(10, 19))
    for index in range(video_count):
        video_rows.append(
            DimVideoRow(
                video_id=f"vid_{index + 1:04d}",
                video_title=str(rng.choice(_VIDEO_TITLES)),
                published_at=_random_date(rng, start, end),
                video_duration_min=int(rng.randint(3, 41)),
                inserted_at=ts,
            )
        )
    return campaign_rows, channel_rows, video_rows