"""Upload generated Parquet files to Google Cloud Storage and BigQuery."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from google.cloud import bigquery
from google.cloud import storage

from . import config

RAW_SCHEMAS: dict[str, list[bigquery.SchemaField]] = {
    "raw_ads_campaign_daily": [
        bigquery.SchemaField("campaign_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("channel_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ad_group_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ad_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("spend_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("spend_usd", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("impressions", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("clicks", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("conversions", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("avg_order_value", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("inserted_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("_load_date", "DATE", mode="REQUIRED"),
    ],
    "raw_ga4_events": [
        bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("event_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("user_pseudo_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("session_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("channel_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("page_location", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("event_value", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("inserted_at", "TIMESTAMP", mode="REQUIRED"),
    ],
    "raw_youtube_video_daily": [
        bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("video_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("views", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("watch_time_min", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("likes", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("comments", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("shares", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("subscribers_gained", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("inserted_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("_load_date", "DATE", mode="REQUIRED"),
    ],
    "raw_dim_campaign": [
        bigquery.SchemaField("campaign_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("campaign_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("campaign_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("start_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("end_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("daily_budget_usd", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("inserted_at", "TIMESTAMP", mode="REQUIRED"),
    ],
    "raw_dim_channel": [
        bigquery.SchemaField("channel_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("channel_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("channel_group", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("inserted_at", "TIMESTAMP", mode="REQUIRED"),
    ],
    "raw_dim_video": [
        bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("video_title", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("published_at", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("video_duration_min", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("inserted_at", "TIMESTAMP", mode="REQUIRED"),
    ],
}

_TABLE_BY_SOURCE: dict[str, str] = {
    "ads": "raw_ads_campaign_daily",
    "ga4_events": "raw_ga4_events",
    "youtube": "raw_youtube_video_daily",
}

_PARTITION_FIELD_BY_TABLE: dict[str, str] = {
    "raw_ads_campaign_daily": "_load_date",
    "raw_youtube_video_daily": "_load_date",
    "raw_ga4_events": "event_date",
}

_DIMS_FILE_BY_TABLE: dict[str, str] = {
    "raw_dim_campaign": "dims/dim_campaign.parquet",
    "raw_dim_channel": "dims/dim_channel.parquet",
    "raw_dim_video": "dims/dim_video.parquet",
}


def upload_parquet(local_dir: str, bucket: str, source: str, date: str | None) -> list[str]:
    """Upload local Parquet partitions to GCS and return the object names."""
    client = storage.Client(project=config.GCP_PROJECT_ID or None)
    bucket_client = client.bucket(bucket)
    local_root = Path(local_dir)
    if source == "dims":
        pattern = "dims/*.parquet"
        remote_dir = "dims"
    elif date is not None:
        pattern = f"{source}/dt={date}/*.parquet"
        remote_dir = f"{source}/dt={date}"
    else:
        raise ValueError("date is required when source is not 'dims'")
    blob_names: list[str] = []
    for local_path in sorted(local_root.glob(pattern)):
        remote_name = f"{remote_dir}/{local_path.name}"
        bucket_client.blob(remote_name).upload_from_filename(str(local_path))
        blob_names.append(remote_name)
    return blob_names


def load_daily_to_bigquery(prefix: str, dataset: str, table: str) -> int:
    """Load one daily Parquet prefix into a partitioned BigQuery raw table."""
    bucket = config.GCS_BUCKET
    if not bucket:
        raise ValueError("GCS_BUCKET must be set to load into BigQuery")
    client = bigquery.Client(project=config.GCP_PROJECT_ID or None)
    project = config.GCP_PROJECT_ID or client.project
    if prefix == "dims":
        uri = f"gs://{bucket}/{_DIMS_FILE_BY_TABLE[table]}"
        blob_prefix = f"{_DIMS_FILE_BY_TABLE[table].rsplit('/', 1)[0]}/"
    else:
        uri = f"gs://{bucket}/{prefix}/*.parquet"
        blob_prefix = f"{prefix}/"
    storage_client = storage.Client(project=config.GCP_PROJECT_ID or None)
    if not list(storage_client.list_blobs(bucket, prefix=blob_prefix)):
        logging.getLogger(__name__).warning(
            "No objects match prefix gs://%s/%s - skipping load into %s.%s",
            bucket,
            blob_prefix,
            dataset,
            table,
        )
        return 0
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,
        schema=RAW_SCHEMAS[table],
    )
    partition_field = _PARTITION_FIELD_BY_TABLE.get(table)
    if partition_field is not None:
        job_config.time_partitioning = bigquery.TimePartitioning(field=partition_field)
    job = client.load_table_from_uri(uri, f"{project}.{dataset}.{table}", job_config=job_config)
    job.result()
    return job.output_rows


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload local datagen Parquet files to GCS and load raw BigQuery tables."
    )
    parser.add_argument("--local-dir", default=".data/parquet")
    parser.add_argument("--bucket", default=config.GCS_BUCKET)
    parser.add_argument("--source", choices=("ads", "ga4_events", "youtube", "dims"), required=True)
    parser.add_argument("--date", default=None)
    args = parser.parse_args(argv)
    if args.source != "dims" and args.date is None:
        parser.error("--date is required when --source is not 'dims'")
    return args


def main(argv: list[str] | None = None) -> int:
    """Run the GCS upload and optional BigQuery load for one source."""
    args = _parse_args(argv)
    bucket = args.bucket or config.GCS_BUCKET
    if not bucket:
        raise SystemExit("a GCS bucket is required via --bucket or GCS_BUCKET")
    blob_names = upload_parquet(args.local_dir, bucket, args.source, args.date)
    for blob_name in blob_names:
        print(f"uploaded gs://{bucket}/{blob_name}")
    print(f"uploaded {len(blob_names)} object(s)")
    if args.source != "dims" and args.date is not None:
        table = _TABLE_BY_SOURCE[args.source]
        prefix = f"{args.source}/dt={args.date}"
        rows = load_daily_to_bigquery(prefix, config.BIGQUERY_RAW_DATASET, table)
        print(f"loaded {rows} rows into {config.BIGQUERY_RAW_DATASET}.{table}")
    return 0