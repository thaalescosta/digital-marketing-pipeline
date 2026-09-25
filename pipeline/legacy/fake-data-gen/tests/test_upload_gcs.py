"""Mocked GCS and BigQuery tests for the datagen upload module."""

from pathlib import Path
from typing import Any

import pytest
from google.cloud import bigquery

from datagen import config, upload_gcs
from datagen.upload_gcs import RAW_SCHEMAS, load_daily_to_bigquery, upload_parquet


class FakeBlob:
    def __init__(self, name: str, uploads: list[tuple[str, str]]) -> None:
        self.name = name
        self._uploads = uploads

    def upload_from_filename(self, filename: str) -> None:
        self._uploads.append((self.name, filename))


class FakeBucket:
    def __init__(
        self, name: str, uploads: list[tuple[str, str]], blobs: list[str] | None = None
    ) -> None:
        self.name = name
        self._uploads = uploads
        self._blobs = blobs

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(name, self._uploads)

    def list_blobs(self, prefix: str = "") -> list[str]:
        if self._blobs is None:
            return ["part.parquet"]
        return [blob for blob in self._blobs if blob.startswith(prefix)]


class FakeStorageClient:
    def __init__(self, project: str | None = None, blobs: list[str] | None = None) -> None:
        self.uploads: list[tuple[str, str]] = []
        self._blobs = blobs

    def bucket(self, name: str) -> FakeBucket:
        return FakeBucket(name, self.uploads, blobs=self._blobs)

    def list_blobs(self, bucket: str, prefix: str = "") -> list[str]:
        if self._blobs is None:
            return ["part.parquet"]
        return [blob for blob in self._blobs if blob.startswith(prefix)]


def test_upload_parquet_dims(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dims_dir = tmp_path / "dims"
    dims_dir.mkdir()
    (dims_dir / "dim_campaign.parquet").write_bytes(b"campaign-bytes")
    (dims_dir / "dim_channel.parquet").write_bytes(b"channel-bytes")

    fake_client = FakeStorageClient()
    monkeypatch.setattr(
        upload_gcs.storage, "Client", lambda *args, **kwargs: fake_client
    )

    remote = upload_parquet(str(tmp_path), "test-bucket", "dims", None)

    assert remote == ["dims/dim_campaign.parquet", "dims/dim_channel.parquet"]
    recorded = {name for name, _ in fake_client.uploads}
    assert recorded == {"dims/dim_campaign.parquet", "dims/dim_channel.parquet"}
    assert len(fake_client.uploads) == 2


def test_upload_parquet_daily_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    partition = tmp_path / "ads" / "dt=2026-05-01"
    partition.mkdir(parents=True)
    local_file = partition / "part.parquet"
    local_file.write_bytes(b"parquet-bytes")

    fake_client = FakeStorageClient()
    monkeypatch.setattr(
        upload_gcs.storage, "Client", lambda *args, **kwargs: fake_client
    )

    remote = upload_parquet(str(tmp_path), "test-bucket", "ads", "2026-05-01")

    assert remote == ["ads/dt=2026-05-01/part.parquet"]
    assert fake_client.uploads == [
        ("ads/dt=2026-05-01/part.parquet", str(local_file))
    ]


def test_upload_requires_date_for_daily_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_client = FakeStorageClient()
    monkeypatch.setattr(
        upload_gcs.storage, "Client", lambda *args, **kwargs: fake_client
    )

    with pytest.raises(ValueError):
        upload_parquet(str(tmp_path), "b", "ads", None)


def test_raw_schemas_match_contract() -> None:
    assert set(RAW_SCHEMAS) == {
        "raw_ads_campaign_daily",
        "raw_ga4_events",
        "raw_youtube_video_daily",
        "raw_dim_campaign",
        "raw_dim_channel",
        "raw_dim_video",
    }

    ads_fields = {field.name: field for field in RAW_SCHEMAS["raw_ads_campaign_daily"]}
    assert ads_fields["campaign_id"].field_type == "INT64"
    assert ads_fields["campaign_id"].mode == "REQUIRED"

    ga4_fields = {field.name: field for field in RAW_SCHEMAS["raw_ga4_events"]}
    assert ga4_fields["user_pseudo_id"].field_type == "STRING"
    assert ga4_fields["user_pseudo_id"].mode == "NULLABLE"


class FakeLoadJob:
    output_rows = 5

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


def test_load_daily_to_bigquery_partitioning(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("GCS_BUCKET", "test-bucket")
    monkeypatch.setattr(config, "GCS_BUCKET", "test-bucket")
    fake_client = FakeBigQueryClient()
    monkeypatch.setattr(
        upload_gcs.bigquery, "Client", lambda *args, **kwargs: fake_client
    )
    monkeypatch.setattr(
        upload_gcs.storage, "Client", lambda *args, **kwargs: FakeStorageClient()
    )

    rows = load_daily_to_bigquery(
        "ads/dt=2026-05-01", "raw_marketing", "raw_ads_campaign_daily"
    )

    assert rows == 5
    assert len(fake_client.loads) == 1
    uri, destination, job_config = fake_client.loads[0]
    assert uri == "gs://test-bucket/ads/dt=2026-05-01/*.parquet"
    assert destination == "test-project.raw_marketing.raw_ads_campaign_daily"
    assert job_config.source_format == bigquery.SourceFormat.PARQUET
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_APPEND
    assert job_config.time_partitioning.field == "_load_date"


def test_load_daily_to_bigquery_skips_empty_prefix(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("GCS_BUCKET", "test-bucket")
    monkeypatch.setattr(config, "GCS_BUCKET", "test-bucket")
    fake_client = FakeBigQueryClient()
    monkeypatch.setattr(
        upload_gcs.bigquery, "Client", lambda *args, **kwargs: fake_client
    )
    monkeypatch.setattr(
        upload_gcs.storage,
        "Client",
        lambda *args, **kwargs: FakeStorageClient(blobs=[]),
    )

    rows = load_daily_to_bigquery(
        "ads/dt=2026-05-02", "raw_marketing", "raw_ads_campaign_daily"
    )

    assert rows == 0
    assert fake_client.loads == []