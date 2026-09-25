"""AT-005 determinism tests for the datagen API (DESIGN decision 4, PRD 6.1.1)."""

from __future__ import annotations

import datetime
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from datagen import generate, generate_backfill
from datagen.api import TABLE_DIRS

INSERTED_AT = pd.Timestamp("2026-01-01T00:00:00+00:00")
META_COLUMNS = ("inserted_at", "_load_date")


def _read_partition(root: Path, table: str, day: date) -> pd.DataFrame:
    path = root / table / f"{table}_{day.isoformat()}.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _without_meta(frame: pd.DataFrame) -> pd.DataFrame:
    columns = [column for column in frame.columns if column not in META_COLUMNS]
    return frame[columns].reset_index(drop=True)


def test_generate_deterministic_same_seed_date_source(tmp_path: Path) -> None:
    day = date(2026, 5, 15)
    generate("ga4", day, day, tmp_path / "first", INSERTED_AT, gen_seed=42)
    generate("ga4", day, day, tmp_path / "second", INSERTED_AT, gen_seed=42)

    first = _read_partition(tmp_path / "first", "ga4_events", day)
    second = _read_partition(tmp_path / "second", "ga4_events", day)

    assert len(first) == len(second)
    pd.testing.assert_frame_equal(_without_meta(first), _without_meta(second))


def test_backfill_equals_daily(tmp_path: Path) -> None:
    start = date(2026, 5, 1)
    end = date(2026, 5, 7)
    sample_day = date(2026, 5, 4)
    backfill_root = tmp_path / "backfill"
    daily_root = tmp_path / "daily"

    generate_backfill(start, end, backfill_root, INSERTED_AT, gen_seed=42)
    for source, tables in TABLE_DIRS.items():
        generate(source, sample_day, sample_day, daily_root, INSERTED_AT, gen_seed=42)
        for table in tables:
            backfill_frame = _read_partition(backfill_root, table, sample_day)
            daily_frame = _read_partition(daily_root, table, sample_day)
            assert len(backfill_frame) == len(daily_frame)
            pd.testing.assert_frame_equal(
                _without_meta(backfill_frame),
                _without_meta(daily_frame),
            )


def test_different_dates_differ(tmp_path: Path) -> None:
    first_day = date(2026, 5, 15)
    second_day = date(2026, 5, 16)
    generate("ga4", first_day, first_day, tmp_path / "a", INSERTED_AT, gen_seed=42)
    generate("ga4", second_day, second_day, tmp_path / "b", INSERTED_AT, gen_seed=42)

    first = _without_meta(_read_partition(tmp_path / "a", "ga4_events", first_day))
    second = _without_meta(_read_partition(tmp_path / "b", "ga4_events", second_day))

    assert not first.equals(second)


def test_different_seed_produces_different_rows(tmp_path: Path) -> None:
    day = date(2026, 5, 15)
    generate("ga4", day, day, tmp_path / "seed42", INSERTED_AT, gen_seed=42)
    generate("ga4", day, day, tmp_path / "seed7", INSERTED_AT, gen_seed=7)

    with_seed_42 = _without_meta(_read_partition(tmp_path / "seed42", "ga4_events", day))
    with_seed_7 = _without_meta(_read_partition(tmp_path / "seed7", "ga4_events", day))

    assert not with_seed_42.equals(with_seed_7)


def test_inserted_at_injected_not_wallclock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def raise_on_wall_clock(*args: object, **kwargs: object) -> object:
        raise AssertionError("wall clock accessed during generate")

    day = date(2026, 5, 15)
    with monkeypatch.context() as context:
        context.setattr(pd.Timestamp, "now", raise_on_wall_clock)
        context.setattr(datetime.date, "today", raise_on_wall_clock)
        generate("ga4", day, day, tmp_path / "out", INSERTED_AT, gen_seed=42)

    frame = _read_partition(tmp_path / "out", "ga4_events", day)
    assert len(frame) > 0
    assert frame["inserted_at"].notna().all()