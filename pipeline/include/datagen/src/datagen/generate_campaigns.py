"""Daily Google Ads-style campaign performance generation."""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from .dimensions import CampaignEntity
from .distributions import ads_campaign_daily, campaign_lifecycle, weekday_seasonality
from .schemas import AdsCampaignDailyRow


def generate_ads_campaign_daily(
    day: date,
    campaigns: tuple[CampaignEntity, ...],
    rng: np.random.Generator,
    inserted_at: pd.Timestamp,
) -> list[AdsCampaignDailyRow]:
    """Generate one ads row per ad running on the day for each active campaign.

    Every campaign owns a fixed set of ad groups and ads whose IDs derive
    deterministically from ``campaign_id`` and the group/ad indices (D7).
    Each ad's spend is its share of a per-group Dirichlet split divided by the
    ad-group count, so campaign-day spend stays near
    ``budget * weekday * lifecycle`` and below ~1.3x budget (D8).
    """
    rows: list[AdsCampaignDailyRow] = []
    for campaign in campaigns:
        if not (campaign.start_date <= day <= campaign.end_date):
            continue
        seasonality = weekday_seasonality(day)
        lifecycle = campaign_lifecycle(day, campaign.start_date, campaign.end_date)
        ad_group_count = 2 + (campaign.campaign_id % 3)
        for group_index in range(ad_group_count):
            ad_group_id = campaign.campaign_id * 100 + group_index
            ad_count = 2 + ((campaign.campaign_id + group_index) % 3)
            shares = rng.dirichlet(np.full(ad_count, 2.0))
            for ad_index, share in enumerate(shares):
                ad_id = ad_group_id * 100 + ad_index
                is_video_campaign = (
                    campaign.campaign_type == "Video" and rng.random() < 0.3
                )
                channel_id = 4 if is_video_campaign else 1
                factor = float(share) * seasonality * lifecycle / ad_group_count
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
                        inserted_at=inserted_at,
                        _load_date=day,
                    )
                )
    return rows