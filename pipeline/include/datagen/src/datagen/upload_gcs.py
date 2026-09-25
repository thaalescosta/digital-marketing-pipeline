"""Thin GCS upload helpers for generated Parquet partitions.

Object names are deterministic and overwrite-safe: re-uploading the same
``(table, date)`` replaces the same object (PRD 6.2).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from google.cloud import storage


def raw_object_name(table: str, day: date) -> str:
    """Return the deterministic GCS object name for one day's partition."""
    return f"raw/{table}/dt={day.isoformat()}/part.parquet"


def upload_parquet_file(
    local_path: Path,
    bucket: str,
    table: str,
    day: date,
    project_id: str | None = None,
) -> str:
    """Upload one local Parquet file to GCS and return the object name."""
    client = storage.Client(project=project_id)
    blob = client.bucket(bucket).blob(raw_object_name(table, day))
    blob.upload_from_filename(str(local_path))
    return blob.name


def upload_generated_files(
    out_dir: Path,
    bucket: str,
    project_id: str | None = None,
) -> list[str]:
    """Upload every partition under ``out_dir`` and return the object names.

    Derives ``table`` and ``day`` from the standard
    ``out_dir/<table>/dt=YYYY-MM-DD/part.parquet`` layout written by
    :func:`datagen.api.generate`.
    """
    uploaded: list[str] = []
    for local_path in sorted(out_dir.rglob("part.parquet")):
        table = local_path.parent.parent.name
        day_label = local_path.parent.name.removeprefix("dt=")
        day = date.fromisoformat(day_label)
        uploaded.append(upload_parquet_file(local_path, bucket, table, day, project_id))
    return uploaded