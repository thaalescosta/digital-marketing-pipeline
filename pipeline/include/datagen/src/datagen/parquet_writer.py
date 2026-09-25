"""Shared Parquet writer: UTC, microsecond timestamps, consistent dtypes.

Both the backfill and daily paths write through this single writer so the
resulting files have identical dtypes (D3). Timezone-aware columns are
normalized to UTC and downcast to a naive ``datetime64[us]`` column that
pyarrow writes as ``TIMESTAMP(MICROS)``, which BigQuery accepts (D2).
"""

from __future__ import annotations

import dataclasses
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

_TIMESTAMP_PRECISION = "datetime64[us]"


def frame_from_rows(rows: list[Any]) -> pd.DataFrame:
    """Build a DataFrame from a list of frozen row dataclasses."""
    return pd.DataFrame([dataclasses.asdict(row) for row in rows])


def _coerce_timestamps_to_us(frame: pd.DataFrame) -> pd.DataFrame:
    for column_name in frame.columns:
        column = frame[column_name]
        if isinstance(column.dtype, pd.DatetimeTZDtype):
            frame[column_name] = (
                column.dt.tz_convert("UTC")
                .dt.tz_localize(None)
                .astype(_TIMESTAMP_PRECISION)
            )
        elif pd.api.types.is_datetime64_any_dtype(column.dtype):
            frame[column_name] = column.astype(_TIMESTAMP_PRECISION)
    return frame


def write_partition(
    frame: pd.DataFrame,
    out_dir: Path,
    table: str,
    day: date,
) -> Path | None:
    """Write one day's frame to ``out_dir/<table>/<table>_YYYY-MM-DD.parquet``.

    Returns the written path, or ``None`` when the frame is empty so the
    caller can log a clear line instead of leaving a zero-row file.
    """
    if frame.empty:
        return None
    path = out_dir / table / f"{table}_{day.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    _coerce_timestamps_to_us(frame)
    frame.to_parquet(path, index=False, engine="pyarrow")
    return path