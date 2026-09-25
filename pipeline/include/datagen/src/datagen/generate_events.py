"""GA4-style web event generation."""

from __future__ import annotations

import uuid
from datetime import date

import numpy as np
import pandas as pd

from .schemas import Ga4EventRow

_CHANNEL_IDS = np.arange(1, 8)
_CHANNEL_WEIGHTS = np.array([0.35, 0.15, 0.10, 0.15, 0.10, 0.10, 0.05])

_FOLLOW_ON_EVENTS = ("page_view", "scroll", "add_to_cart", "begin_checkout")
_FOLLOW_ON_WEIGHTS = np.array([0.45, 0.27, 0.16, 0.12])

_SESSION_ID_RANGE = (1_000_000_000, 10_000_000_000)

_PAGE_POOL = (
    "/home",
    "/products",
    "/products/analytics",
    "/products/etl",
    "/pricing",
    "/blog",
    "/blog/spec-driven-development",
    "/docs",
    "/checkout",
)


def _page_location(rng: np.random.Generator) -> str | None:
    path = str(rng.choice(_PAGE_POOL))
    if rng.random() < 0.05:
        return None
    return f"https://www.example.com{path}"


def generate_ga4_events(
    day: date,
    rng: np.random.Generator,
    n_sessions: int,
    inserted_at: pd.Timestamp,
) -> list[Ga4EventRow]:
    """Generate GA4-style events for the day across the requested sessions.

    Each session emits ``session_start``, 1-4 follow-on events, and a
    ``purchase`` with probability ~2%. Sessions can cross midnight: the event
    timestamp may land on the next day while ``event_date`` stays the session
    start date (PRD 3.3).
    """
    rows: list[Ga4EventRow] = []
    for _ in range(n_sessions):
        session_id = (
            f"{day:%Y%m%d}-{int(rng.integers(*_SESSION_ID_RANGE, dtype=np.int64))}"
        )
        user_draw = int(rng.integers(*_SESSION_ID_RANGE, dtype=np.int64))
        user_pseudo_id = None if rng.random() < 0.02 else f"{user_draw}.{day:%Y%m%d}"
        channel_id = int(rng.choice(_CHANNEL_IDS, p=_CHANNEL_WEIGHTS))
        hour = int(rng.integers(0, 24))
        minute = int(rng.integers(0, 60))
        current = pd.Timestamp(day.year, day.month, day.day, hour, minute)

        rows.append(
            Ga4EventRow(
                event_id=str(uuid.UUID(int=int.from_bytes(rng.bytes(16), "big"))),
                event_date=day,
                event_timestamp=current,
                event_name="session_start",
                user_pseudo_id=user_pseudo_id,
                session_id=session_id,
                channel_id=channel_id,
                page_location=None,
                event_value=None,
                inserted_at=inserted_at,
            )
        )

        follow_on_count = int(rng.integers(1, 5))
        for _ in range(follow_on_count):
            current += pd.Timedelta(minutes=int(rng.integers(1, 31)))
            event_name = str(rng.choice(_FOLLOW_ON_EVENTS, p=_FOLLOW_ON_WEIGHTS))
            rows.append(
                Ga4EventRow(
                    event_id=str(uuid.UUID(int=int.from_bytes(rng.bytes(16), "big"))),
                    event_date=day,
                    event_timestamp=current,
                    event_name=event_name,
                    user_pseudo_id=user_pseudo_id,
                    session_id=session_id,
                    channel_id=channel_id,
                    page_location=_page_location(rng),
                    event_value=None,
                    inserted_at=inserted_at,
                )
            )

        if rng.random() < 0.02:
            current += pd.Timedelta(minutes=int(rng.integers(1, 11)))
            rows.append(
                Ga4EventRow(
                    event_id=str(uuid.UUID(int=int.from_bytes(rng.bytes(16), "big"))),
                    event_date=day,
                    event_timestamp=current,
                    event_name="purchase",
                    user_pseudo_id=user_pseudo_id,
                    session_id=session_id,
                    channel_id=channel_id,
                    page_location=_page_location(rng),
                    event_value=round(float(rng.gamma(shape=8, scale=6)), 2),
                    inserted_at=inserted_at,
                )
            )
    return rows