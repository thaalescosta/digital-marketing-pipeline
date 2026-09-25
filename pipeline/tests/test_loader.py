"""AT-006 loader callable tests: partition-scoped WRITE_TRUNCATE, mocked BigQuery/GCS."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest
from google.cloud import bigquery, storage

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dags.common import tasks  # noqa: E402
from dags.common.config import (  # noqa: E402
    BQ_RAW_DATASET,
    GCP_PROJECT_ID,
    RAW_SCHEMAS,
    gcs_uri_for_date,
    gcs_uri_wildcard,
)


class FakeLoadJob:
    output_rows = 17

    def result(self) -> None:
        return None


class FakeBigQueryClient:
    def __init__(self, project: str | None = None) -> None:
        self.project = project or "test-project"
        self.loads: list[tuple[str, str, Any]] = []

    def load_table_from_uri(
        self, uri: str, destination: str, job_config: Any
    ) -> FakeLoadJob:
        self.loads.append((uri, destination, job_config))
        return FakeLoadJob()


class FakeBucket:
    def __init__(self, name: str) -> None:
        self.name = name

    def get_blob(self, object_name: str) -> None:
        return None

    def list_blobs(self, prefix: str = "", max_results: int | None = None) -> list[str]:
        return []


class FakeStorageClient:
    def __init__(self, project: str | None = None) -> None:
        self.project = project or "test-project"

    def bucket(self, name: str) -> FakeBucket:
        return FakeBucket(name)


@pytest.fixture()
def bq_client(monkeypatch: pytest.MonkeyPatch) -> FakeBigQueryClient:
    client = FakeBigQueryClient()
    monkeypatch.setattr(bigquery, "Client", lambda *args, **kwargs: client)
    monkeypatch.setattr(tasks, "_require_object", lambda *args, **kwargs: None)
    monkeypatch.setattr(tasks, "_require_any_object", lambda *args, **kwargs: None)
    return client


def test_daily_load_partition_decorator(bq_client: FakeBigQueryClient) -> None:
    result = tasks.load_to_bigquery("raw_ga4_events", None, "2026-07-01", "daily")

    uri, destination, job_config = bq_client.loads[0]
    assert destination == f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.raw_ga4_events$20260701"
    assert "$20260701" in destination
    assert uri == gcs_uri_for_date("raw_ga4_events", "2026-07-01")
    assert uri.endswith("/raw/ga4_events/ga4_events_2026-07-01.parquet")
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert job_config.source_format == bigquery.SourceFormat.PARQUET
    assert job_config.autodetect is False
    assert {field.name for field in job_config.schema} == {
        name for name, _, _ in RAW_SCHEMAS["raw_ga4_events"]
    }
    assert result == {
        "table": "raw_ga4_events",
        "date": "2026-07-01",
        "rows_loaded": 17,
        "mode": "daily",
    }


def test_daily_dim_load_uses_snapshot_partition(bq_client: FakeBigQueryClient) -> None:
    tasks.load_to_bigquery("raw_dim_campaign", None, "2026-07-01", "daily")

    _, destination, job_config = bq_client.loads[0]
    assert destination.endswith("raw_dim_campaign$20260701")
    schema_names = {field.name for field in job_config.schema}
    assert schema_names == {name for name, _, _ in RAW_SCHEMAS["raw_dim_campaign"]}
    assert "_snapshot_date" in schema_names


def test_initial_load_wildcard_and_truncate(bq_client: FakeBigQueryClient) -> None:
    result = tasks.load_to_bigquery("raw_ga4_events", None, "2026-06-30", "initial")

    uri, destination, job_config = bq_client.loads[0]
    assert destination == f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.raw_ga4_events"
    assert "$" not in destination
    assert uri == gcs_uri_wildcard("raw_ga4_events")
    assert uri.endswith("/raw/ga4_events/ga4_events_*.parquet")
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert job_config.autodetect is False
    assert job_config.time_partitioning.field == "event_date"
    assert job_config.time_partitioning.type_ == bigquery.TimePartitioningType.DAY
    assert result["rows_loaded"] == 17


def test_missing_daily_file_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_bq = FakeBigQueryClient()
    monkeypatch.setattr(bigquery, "Client", lambda *args, **kwargs: fake_bq)
    monkeypatch.setattr(storage, "Client", lambda *args, **kwargs: FakeStorageClient())

    with pytest.raises(RuntimeError, match="required file missing"):
        tasks.load_to_bigquery("raw_ga4_events", None, "2026-07-01", "daily")
    assert fake_bq.loads == []


def test_missing_initial_files_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_bq = FakeBigQueryClient()
    monkeypatch.setattr(bigquery, "Client", lambda *args, **kwargs: fake_bq)
    monkeypatch.setattr(storage, "Client", lambda *args, **kwargs: FakeStorageClient())

    with pytest.raises(RuntimeError, match="required files missing"):
        tasks.load_to_bigquery("raw_ga4_events", None, "2026-06-30", "initial")
    assert fake_bq.loads == []


def test_load_rejects_invalid_mode(bq_client: FakeBigQueryClient) -> None:
    with pytest.raises(ValueError, match="mode"):
        tasks.load_to_bigquery("raw_ga4_events", None, "2026-07-01", "append")