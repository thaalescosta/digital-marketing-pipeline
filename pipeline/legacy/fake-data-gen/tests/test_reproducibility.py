"""Reproducibility tests for the deterministic datagen generators."""

import hashlib
import shutil
from datetime import date
from pathlib import Path

import numpy as np
import pytest

from datagen import backfill, config, daily_update
from datagen.generate_events import generate_ga4_events


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _parquet_tree(root: Path) -> dict[str, Path]:
    return {
        path.relative_to(root).as_posix(): path
        for path in sorted(root.rglob("*.parquet"))
        if path.is_file()
    }


def test_backfill_same_seed_identical_files(tmp_path: Path) -> None:
    out_a = tmp_path / "a"
    out_b = tmp_path / "b"
    backfill.main(
        ["--output-dir", str(out_a), "--start-date", "2026-05-01", "--months", "1"]
    )
    backfill.main(
        ["--output-dir", str(out_b), "--start-date", "2026-05-01", "--months", "1"]
    )

    files_a = _parquet_tree(out_a)
    files_b = _parquet_tree(out_b)
    assert files_a
    assert set(files_a) == set(files_b)

    for relative in files_a:
        assert _sha256(files_a[relative].read_bytes()) == _sha256(
            files_b[relative].read_bytes()
        )

    assert "ga4_events/dt=2026-05-15/part.parquet" in files_a
    assert "ads/dt=2026-05-15/part.parquet" in files_a
    assert "youtube/dt=2026-05-31/part.parquet" in files_a
    assert "dims/dim_campaign.parquet" in files_a
    assert "dims/dim_channel.parquet" in files_a
    assert "dims/dim_video.parquet" in files_a


def test_daily_update_matches_backfill_day(tmp_path: Path) -> None:
    backfill_dir = tmp_path / "backfill"
    daily_dir = tmp_path / "daily"
    backfill.main(
        [
            "--output-dir",
            str(backfill_dir),
            "--start-date",
            "2026-05-01",
            "--months",
            "1",
        ]
    )

    day = "2026-05-15"
    sources = ("ads", "ga4_events", "youtube")
    original = {
        source: (backfill_dir / source / f"dt={day}" / "part.parquet").read_bytes()
        for source in sources
    }

    daily_update.main(["--output-dir", str(backfill_dir), "--date", day])

    shutil.copytree(backfill_dir / "dims", daily_dir / "dims")
    daily_update.main(["--output-dir", str(daily_dir), "--date", day])

    for source in sources:
        overwritten = (backfill_dir / source / f"dt={day}" / "part.parquet").read_bytes()
        independent = (daily_dir / source / f"dt={day}" / "part.parquet").read_bytes()
        assert overwritten == original[source]
        assert independent == overwritten


def test_different_seed_changes_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out_default = tmp_path / "seed-42"
    out_other = tmp_path / "seed-7"
    backfill.main(
        [
            "--output-dir",
            str(out_default),
            "--start-date",
            "2026-05-01",
            "--months",
            "1",
        ]
    )

    monkeypatch.setenv("GEN_SEED", "7")
    monkeypatch.setattr(config, "GEN_SEED", 7)
    backfill.main(
        [
            "--output-dir",
            str(out_other),
            "--start-date",
            "2026-05-01",
            "--months",
            "1",
        ]
    )

    files_default = _parquet_tree(out_default)
    files_other = _parquet_tree(out_other)
    day = "2026-05-01"
    assert f"ga4_events/dt={day}/part.parquet" in files_default
    assert f"ga4_events/dt={day}/part.parquet" in files_other
    default_events = (out_default / "ga4_events" / f"dt={day}" / "part.parquet").read_bytes()
    other_events = (out_other / "ga4_events" / f"dt={day}" / "part.parquet").read_bytes()
    assert default_events != other_events


def test_event_volume_scales_with_sessions() -> None:
    day = date(2026, 5, 4)
    few = generate_ga4_events(day, np.random.RandomState(42), 10)
    many = generate_ga4_events(day, np.random.RandomState(43), 100)
    assert len(few) >= 10
    assert len(many) > len(few)