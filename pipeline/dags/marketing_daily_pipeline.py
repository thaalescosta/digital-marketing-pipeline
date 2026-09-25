"""Daily incremental pipeline: raw generation/load, quality gates, dbt marts, summary.

DAG graph
    resolve_data_date
    assert_initial_load_done
      ├── ga4.generate_and_upload  →  ga4.load_raw_ga4_events
      ├── google_ads.generate_and_upload  →  google_ads.load_raw_ads_campaign_daily
      ├── youtube.generate_and_upload  →  youtube.load_raw_youtube_video_daily
      └── dimensions.generate_and_upload
            ├── dimensions.load_raw_dim_campaign
            ├── dimensions.load_raw_dim_channel
            └── dimensions.load_raw_dim_video
      gx_validate_raw (scope=partition)
      dbt_transform (incremental, run_date from resolve_data_date)
      gx_validate_marts (scope=partition)
      log_run_summary (all_done)

The start_date is FIRST_INCREMENTAL_DATE with catchup=True, so on first deploy the
DAG backfills 2026-07-01..today. A manual run with params.target_date overrides the
data date end-to-end (datagen, load URIs, dbt var, GX partition), mirroring PRD 7.3.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from dags.common.config import (
    BQ_RAW_DATASET,
    GCS_BUCKET,
    GCP_PROJECT_ID,
    RAW_TABLES,
    SOURCES,
    OUT_DIR,
    cosmos_execution_config,
    cosmos_operator_args,
    cosmos_profile_config,
    cosmos_project_config,
    cosmos_render_config,
)
from dags.common.tasks import (
    assert_initial_load_done,
    compute_inserted_at,
    generate_and_upload,
    load_to_bigquery,
    log_run_summary,
    resolve_data_date,
    run_gx,
)


@dag(
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=True,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["digital-marketing", "daily"],
    params={"target_date": Param(default=None, type=["null", "string"], description="Data date override (ISO yyyy-mm-dd).")},
    doc_md=__doc__,
)
def marketing_daily_pipeline() -> None:
    """Daily run: generate+load raw for the data date, validate, dbt marts, summarize."""

    @task(task_id="resolve_data_date")
    def _resolve_data_date(ds=None, params=None):
        return resolve_data_date(ds=ds or "", target_date=(params or {}).get("target_date"))

    @task(task_id="log_run_summary", trigger_rule=TriggerRule.ALL_DONE)
    def _log_run_summary(dag_run=None):
        return log_run_summary({"dag_run": dag_run})

    resolved = _resolve_data_date()

    required_partition = task(
        assert_initial_load_done,
        task_id="assert_initial_load_done",
        retries=0,
        execution_timeout=timedelta(seconds=600),
    )(ds=resolved)

    inserted_at = task(compute_inserted_at, task_id="compute_inserted_at")()

    with TaskGroup("load_raw") as load_raw:
        for source in SOURCES:
            with TaskGroup(f"{source}") as source_group:
                source_out_dir = OUT_DIR / source
                generated = task(
                    generate_and_upload,
                    task_id="generate_and_upload",
                    retries=2,
                    retry_delay=timedelta(seconds=120),
                    execution_timeout=timedelta(minutes=5),
                )(source, ds=resolved, out_dir=str(source_out_dir), inserted_at=inserted_at)
                for table, meta in RAW_TABLES.items():
                    if meta.source != source:
                        continue
                    task(
                        load_to_bigquery,
                        task_id=f"load_{meta.bq_table}",
                        retries=2,
                        retry_delay=timedelta(seconds=120),
                        execution_timeout=timedelta(seconds=900),
                    )(table=table, gcs_uri=None, ds=resolved, mode="daily") << generated

    gx_raw = task(run_gx, task_id="gx_validate_raw", retries=0, execution_timeout=timedelta(seconds=3600))(
        ds=resolved, scope="partition", dataset="raw"
    )

    from cosmos import DbtTaskGroup

    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=cosmos_project_config(),
        profile_config=cosmos_profile_config(),
        execution_config=cosmos_execution_config(),
        render_config=cosmos_render_config(),
        operator_args=cosmos_operator_args(
            full_refresh=False,
            run_date_task_id="resolve_data_date",
        ),
    )

    gx_marts = task(run_gx, task_id="gx_validate_marts", retries=0, execution_timeout=timedelta(seconds=3600))(
        ds=resolved, scope="partition", dataset="marts"
    )

    resolved >> required_partition >> load_raw >> gx_raw >> dbt_transform >> gx_marts
    inserted_at >> load_raw
    gx_marts >> _log_run_summary()


marketing_daily_pipeline()

__all__ = ["marketing_daily_pipeline"]