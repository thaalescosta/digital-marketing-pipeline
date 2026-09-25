"""Seeded probability distributions for deterministic fake marketing metrics.

Each generator derives its own RNG from ``(seed, date, source)`` so backfill
and daily runs produce identical rows for the same inputs and sources stay
independent of one another (PRD D6, DESIGN decision 4).
"""

from __future__ import annotations

import hashlib
from datetime import date

import numpy as np

_WEEKDAY_MULTIPLIERS: dict[int, float] = {
    0: 1.05,
    1: 1.00,
    2: 1.10,
    3: 1.15,
    4: 1.20,
    5: 0.75,
    6: 0.65,
}


def rng_for(source: str, day: date, gen_seed: int) -> np.random.Generator:
    """Return an independent, reproducible RNG for ``(seed, date, source)``.

    Uses a stable SHA-256 hash rather than the builtin ``hash()`` so the seed
    is identical across interpreter launches (Python string hashing is salted
    per process and would break cross-run determinism, AT-005).
    """
    key = f"{gen_seed}:{day.isoformat()}:{source}"
    digest = hashlib.sha256(key.encode("utf-8")).digest()[:8]
    seed = int.from_bytes(digest, "big") & 0x7FFFFFFF
    return np.random.default_rng(seed=seed)


def weekday_seasonality(day: date) -> float:
    """Return a multiplier that lifts weekday traffic and depresses weekends."""
    return _WEEKDAY_MULTIPLIERS[day.weekday()]


def campaign_lifecycle(day: date, start: date, end: date) -> float:
    """Return the campaign lifecycle multiplier for a given day."""
    if day < start or day > end:
        return 0.0
    total_days = (end - start).days + 1
    if total_days <= 0:
        return 0.0
    elapsed = (day - start).days
    ramp_len = max(int(total_days * 0.10), 1)
    decay_len = max(int(total_days * 0.15), 1)
    if elapsed < ramp_len:
        if ramp_len <= 1:
            return 0.4
        return 0.4 + 0.6 * (elapsed / (ramp_len - 1))
    if elapsed >= total_days - decay_len:
        progress = (elapsed - (total_days - decay_len)) / max(decay_len - 1, 1)
        return max(0.3, 1.0 - 0.7 * progress)
    return 1.0


def ads_campaign_daily(
    rng: np.random.Generator,
    budget_usd: float,
    factor: float,
) -> dict[str, float | int]:
    """Return ad-level performance metrics derived from a daily budget.

    ``factor`` already contains the ad's share of the campaign day: share of
    the ad group's Dirichlet split, divided by the ad-group count so that the
    whole campaign sums to roughly ``budget * weekday * lifecycle`` (D8).
    """
    spend_usd = round(budget_usd * factor, 2)
    impressions = int(spend_usd * rng.uniform(80.0, 120.0))
    ctr = rng.beta(2, 25)
    clicks = int(impressions * ctr)
    cvr = rng.beta(1, 12)
    conversions = int(clicks * cvr)
    avg_order_value = round(float(rng.gamma(shape=12, scale=3.8)), 2)
    return {
        "spend_usd": spend_usd,
        "impressions": impressions,
        "clicks": clicks,
        "conversions": conversions,
        "avg_order_value": avg_order_value,
    }


def youtube_daily_views(
    rng: np.random.Generator,
    published_at: date,
    video_date: date,
    base_popularity: int,
) -> int:
    """Return daily views from an age-decay curve times weekday uplift."""
    age_days = max((video_date - published_at).days, 0)
    decay = 1.0 / (1.0 + age_days / 45.0)
    noise = rng.uniform(0.6, 1.4)
    uplift = weekday_seasonality(video_date)
    return max(0, int(base_popularity * decay * noise * uplift))