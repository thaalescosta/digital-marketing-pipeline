"""Daily Google Ads-style campaign performance generation."""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from . import config
from .distributions import ads_campaign_daily, campaign_lifecycle, weekday_seasonality
from .schemas import AdsCampaignDailyRow, DimCampaignRow


def inserted_at() -> pd.Timestamp:
    """Return this run's fixed load timestamp."""
    return config.inserted_at()


def generate_ads_campaign_daily(
    day: date,
    campaigns: list[DimCampaignRow],
    rng: np.random.RandomState,
    inserted_at: pd.Timestamp | None = None,
) -> list[AdsCampaignDailyRow]:
    """Generate one AdsCampaignDailyRow for each ad running on the day."""
    ts = inserted_at if inserted_at is not None else config.inserted_at()
    seasonality = weekday_seasonality(day)
    rows: list[AdsCampaignDailyRow] = []
    for campaign in campaigns:
        if day < campaign.start_date or day > campaign.end_date:
            continue
        lifecycle = campaign_lifecycle(day, campaign.start_date, campaign.end_date)
        ad_group_count = int(rng.randint(2, 4))
        for _ in range(ad_group_count):
            ad_group_id = int(rng.randint(100_000_000, 999_999_999))
            ad_count = int(rng.randint(2, 5))
            shares = rng.dirichlet(np.ones(ad_count) * 2.0)
            for _, share in enumerate(shares):
                ad_id = int(rng.randint(1_000_000_000, 9_999_999_999, dtype=np.int64))
                is_video_campaign = campaign.campaign_type == "Video" and rng.random() < 0.3
                channel_id = 4 if is_video_campaign else 1
                factor = float(share) * seasonality * lifecycle
                metrics = ads_campaign_daily(rng, campaign.daily_budget_usd, factor)
                rows.append(
                    AdsCampaignDailyRow(
                        campaign_id=campaign.campaign_id,
                        channel_id=channel_id,
                        ad_group_id=ad_group_id,
                        ad_id=ad_id,
                        spend_date=day,
                        spend_usd=float(metrics["spend_usd"]),
                        impressions=int(metrics["impressions"]),
                        clicks=int(metrics["clicks"]),
                        conversions=int(metrics["conversions"]),
                        avg_order_value=float(metrics["avg_order_value"]),
                        inserted_at=ts,
                        _load_date=day,
                    )
                )
    return rows