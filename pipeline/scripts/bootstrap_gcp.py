"""Idempotent GCP bootstrap for the digital-marketing ELT pipeline.

Creates the GCS bucket and BigQuery datasets only when missing, never
deletes existing resources, and reports a per-resource status of
"created" or "skipped". The module is importable so the Airflow
initial-load DAG can call bootstrap() directly before running loads.
"""

import os

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import storage

DEFAULT_PROJECT_ID = "digital-marketing-509604"
DEFAULT_BUCKET = "thaalescosta_marketing"
DEFAULT_LOCATION = "US"
DEFAULT_RAW_DATASET = "digital_marketing_raw"
DEFAULT_STAGING_DATASET = "digital_marketing_staging"
DEFAULT_MARTS_DATASET = "digital_marketing_marts"


def bootstrap(
    gcp_project_id: str,
    gcp_location: str,
    gcs_bucket: str,
    datasets: list[str],
) -> dict[str, str]:
    """Create missing GCP resources and return {resource: status}."""
    storage_client = storage.Client(project=gcp_project_id)
    bigquery_client = bigquery.Client(project=gcp_project_id)
    statuses: dict[str, str] = {}

    bucket = storage_client.bucket(gcs_bucket)
    bucket_uri = f"gs://{gcs_bucket}"
    if bucket.exists():
        statuses[bucket_uri] = "skipped"
    else:
        storage_client.create_bucket(bucket, location=gcp_location)
        statuses[bucket_uri] = "created"

    for dataset_name in datasets:
        dataset = bigquery.Dataset(f"{gcp_project_id}.{dataset_name}")
        dataset.location = gcp_location
        dataset_key = f"bigquery:{gcp_project_id}.{dataset_name}"
        try:
            bigquery_client.get_dataset(dataset)
            statuses[dataset_key] = "skipped"
        except NotFound:
            bigquery_client.create_dataset(dataset)
            statuses[dataset_key] = "created"

    return statuses


def main() -> int:
    """Read configuration from the environment and run bootstrap."""
    statuses = bootstrap(
        gcp_project_id=os.environ.get("GCP_PROJECT_ID", DEFAULT_PROJECT_ID),
        gcp_location=os.environ.get("GCP_LOCATION", DEFAULT_LOCATION),
        gcs_bucket=os.environ.get("GCS_BUCKET", DEFAULT_BUCKET),
        datasets=[
            os.environ.get("BQ_RAW_DATASET", DEFAULT_RAW_DATASET),
            os.environ.get("BQ_STAGING_DATASET", DEFAULT_STAGING_DATASET),
            os.environ.get("BQ_MARTS_DATASET", DEFAULT_MARTS_DATASET),
        ],
    )
    for resource, status in statuses.items():
        print(f"{status} {resource}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())