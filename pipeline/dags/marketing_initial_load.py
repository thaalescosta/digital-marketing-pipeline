"""Initial one-time backfill of the raw layer (GCS + BigQuery), full-refresh dbt run.

DAG graph
    bootstrap_gcp
      ├── ga4.generate_backfill_and_upload  →  ga4.load_raw_ga4_events
      ├── google_ads.generate_backfill_and_upload  →  google_ads.load_raw_ads_campaign_daily
      ├── youtube.generate_backfill_and_upload  →  youtube.load_raw_youtube_video_daily
      └── dimensions.generate_backfill_and_upload
            ├── dimensions.load_raw_dim_campaign
            ├── dimensions.load_raw_dim_channel
            └── dimensions.load_raw_dim_video
      gx_validate_raw (scope=full)
      dbt_transform (full refresh, run_date=BACKFILL_END)
      gx_validate_marts (scope=full)

This DAG is manually triggered (no schedule). It is paused on creation because the
folder is deployed with the live DAGs, and after it succeeds the daily pipeline owns
the raw layer.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup

from dags.common.config import (
    BACKFILL_END,
    GCS_BUCKET,
    GCP_PROJECT_ID,
    RAW_TABLES,
    SIM_START_DATE,
    SOURCES,
    OUT_DIR,
    cosmos_execution_config,
    cosmos_operator_args,
    cosmos_profile_config,
    cosmos_project_config,
    cosmos_render_config,
)
from dags.common.tasks import bootstrap_gcp, compute_inserted_at, generate_backfill_and_upload, load_to_bigquery, run_gx


@dag(
    schedule=None,
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["digital-marketing", "backfill", "initial-load"],
    params={"target_date": Param(default=None, type=["null", "string"], description="Data date override (ISO yyyy-mm-dd).")},
    doc_md=__doc__,
)
def marketing_initial_load() -> None:
    """One-time raw backfill over SIM_START_DATE..BACKFILL_END, then full-refresh dbt."""

    bootstrap = task(bootstrap_gcp, task_id="bootstrap_gcp")()
    inserted_at = task(compute_inserted_at, task_id="compute_inserted_at")()

    with TaskGroup("load_raw") as load_raw:
        for source in SOURCES:
            with TaskGroup(f"{source}") as source_group:
                source_out_dir = OUT_DIR / source
                generated = task(
                    generate_backfill_and_upload,
                    task_id="generate_backfill_and_upload",
                    retries=2,
                    retry_delay=timedelta(seconds=120),
                    execution_timeout=timedelta(minutes=30),
                )(
                    source,
                    start_date=SIM_START_DATE.isoformat(),
                    end_date=BACKFILL_END.isoformat(),
                    out_dir=str(source_out_dir),
                    inserted_at=inserted_at,
                )
                for table, meta in RAW_TABLES.items():
                    if meta.source != source:
                        continue
                    task(
                        load_to_bigquery,
                        task_id=f"load_{meta.bq_table}",
                        retries=2,
                        retry_delay=timedelta(seconds=120),
                        execution_timeout=timedelta(seconds=900),
                    )(
                        table=table,
                        gcs_uri=None,
                        ds=BACKFILL_END.isoformat(),
                        mode="initial",
                    ) << generated

    gx_raw = task(run_gx, task_id="gx_validate_raw", retries=0, execution_timeout=timedelta(seconds=3600))(
        ds=BACKFILL_END.isoformat(), scope="full", dataset="raw"
    )

    from cosmos import DbtTaskGroup

    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=cosmos_project_config(),
        profile_config=cosmos_profile_config(),
        execution_config=cosmos_execution_config(),
        render_config=cosmos_render_config(),
        operator_args=cosmos_operator_args(full_refresh=True),
    )

    gx_marts = task(run_gx, task_id="gx_validate_marts", retries=0, execution_timeout=timedelta(seconds=3600))(
        ds=BACKFILL_END.isoformat(), scope="full", dataset="marts"
    )

    bootstrap >> load_raw >> gx_raw >> dbt_transform >> gx_marts
    inserted_at >> load_raw


marketing_initial_load()

__all__ = ["marketing_initial_load"]