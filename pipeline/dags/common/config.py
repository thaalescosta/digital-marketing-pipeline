"""Shared environment-driven configuration for the digital-marketing DAGs.

This module imports only the standard library at top level. Google Cloud and Cosmos
imports happen inside the factory functions so that importing this module — or the
task callables that use it — never requires a live GCP environment.

GCS object layout (documented contract with the datagen port, PRD section 4):
    gs://<GCS_BUCKET>/raw/<gcs_table>/dt=YYYY-MM-DD/part.parquet
The task callables derive object keys from ``RAW_TABLES``; the datagen library must
write local files as ``<out_dir>/<gcs_table>/dt=YYYY-MM-DD/part.parquet`` so the two
sides agree. PRD section 4 shows a nested ``raw/<source>/<table>/`` layout; the flat
``raw/<gcs_table>/`` layout follows the DESIGN diagram and keeps load URIs derivable
from the table contract alone. Reverting to the nested layout is a one-line change in
``gcs_prefix`` per table.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path


def _env_str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _env_date(name: str, default: date) -> date:
    return date.fromisoformat(os.environ.get(name, default.isoformat()))


PROJECT_ROOT = Path(__file__).resolve().parents[2]

GCP_PROJECT_ID = _env_str("GCP_PROJECT_ID", "digital-marketing-509604")
GCP_LOCATION = _env_str("GCP_LOCATION", "US")
GCS_BUCKET = _env_str("GCS_BUCKET", "thaalescosta_marketing")

BQ_RAW_DATASET = _env_str("BQ_RAW_DATASET", "digital_marketing_raw")
BQ_STAGING_DATASET = _env_str("BQ_STAGING_DATASET", "digital_marketing_staging")
BQ_MARTS_DATASET = _env_str("BQ_MARTS_DATASET", "digital_marketing_marts")

GEN_SEED = _env_int("GEN_SEED", 42)
DAILY_SESSION_VOLUME = _env_int("DAILY_SESSION_VOLUME", 550)
HISTORY_MONTHS = _env_int("HISTORY_MONTHS", 6)

SIM_START_DATE = _env_date("SIM_START_DATE", date(2026, 1, 1))
BACKFILL_END = date(2026, 6, 30)
FIRST_INCREMENTAL_DATE = date(2026, 7, 1)
INITIAL_PARTITION = BACKFILL_END

GCS_PREFIX = "raw"
OUT_DIR = Path(_env_str("DATAGEN_OUT_DIR", "/tmp/datagen"))

DBT_DIR = PROJECT_ROOT / "include" / "dbt" / "marketing"
DBT_PROFILES_DIR = DBT_DIR
DBT_VENV = Path(_env_str("DBT_VENV", "/usr/local/airflow/dbt_venv"))
_dbt_executable = DBT_VENV / "bin" / "dbt"
if not _dbt_executable.exists():
    import shutil
    _dbt_executable = Path(shutil.which("dbt") or str(DBT_VENV / "bin" / "dbt"))
DBT_EXECUTABLE = _dbt_executable
GX_DIR = PROJECT_ROOT / "include" / "gx"
GX_VENV = Path(_env_str("GX_VENV", "/usr/local/airflow/gx_venv"))
# Fall back to current Python if dedicated venv doesn't exist (local dev)
_gx_python = GX_VENV / "bin" / "python"
if not _gx_python.exists():
    import sys
    _gx_python = Path(sys.executable)
GX_PYTHON = _gx_python

DBT_PROFILE_NAME = "marketing"
DBT_TARGET_NAME = "dev"
GCP_CONN_ID = "google_cloud_default"

SOURCES: tuple[str, ...] = ("ga4", "google_ads", "youtube", "dimensions")


@dataclass(frozen=True)
class RawTable:
    """Contract for one raw BigQuery table.

    ``gcs_table`` is both the local datagen output directory and the GCS key segment
    under ``raw/``. ``schema`` mirrors PRD section 5 including ``_snapshot_date`` on
    the three dimension tables (defect D10).
    """

    bq_table: str
    source: str
    gcs_table: str
    partition_col: str
    schema: tuple[tuple[str, str, str], ...]


RAW_SCHEMAS: dict[str, tuple[tuple[str, str, str], ...]] = {
    "raw_ga4_events": (
        ("event_id", "STRING", "REQUIRED"),
        ("event_date", "DATE", "REQUIRED"),
        ("event_timestamp", "TIMESTAMP", "REQUIRED"),
        ("event_name", "STRING", "REQUIRED"),
        ("user_pseudo_id", "STRING", "NULLABLE"),
        ("session_id", "STRING", "REQUIRED"),
        ("channel_id", "INT64", "REQUIRED"),
        ("page_location", "STRING", "NULLABLE"),
        ("event_value", "FLOAT64", "NULLABLE"),
        ("inserted_at", "TIMESTAMP", "REQUIRED"),
    ),
    "raw_ads_campaign_daily": (
        ("campaign_id", "INT64", "REQUIRED"),
        ("channel_id", "INT64", "REQUIRED"),
        ("ad_group_id", "INT64", "REQUIRED"),
        ("ad_id", "INT64", "REQUIRED"),
        ("spend_date", "DATE", "REQUIRED"),
        ("spend_usd", "FLOAT64", "REQUIRED"),
        ("impressions", "INT64", "REQUIRED"),
        ("clicks", "INT64", "REQUIRED"),
        ("conversions", "INT64", "REQUIRED"),
        ("avg_order_value", "FLOAT64", "REQUIRED"),
        ("inserted_at", "TIMESTAMP", "REQUIRED"),
        ("_load_date", "DATE", "REQUIRED"),
    ),
    "raw_youtube_video_daily": (
        ("video_id", "STRING", "REQUIRED"),
        ("video_date", "DATE", "REQUIRED"),
        ("views", "INT64", "REQUIRED"),
        ("watch_time_min", "INT64", "REQUIRED"),
        ("likes", "INT64", "REQUIRED"),
        ("comments", "INT64", "REQUIRED"),
        ("shares", "INT64", "REQUIRED"),
        ("subscribers_gained", "INT64", "REQUIRED"),
        ("inserted_at", "TIMESTAMP", "REQUIRED"),
        ("_load_date", "DATE", "REQUIRED"),
    ),
    "raw_dim_campaign": (
        ("campaign_id", "INT64", "REQUIRED"),
        ("campaign_name", "STRING", "REQUIRED"),
        ("campaign_type", "STRING", "REQUIRED"),
        ("status", "STRING", "REQUIRED"),
        ("start_date", "DATE", "REQUIRED"),
        ("end_date", "DATE", "REQUIRED"),
        ("daily_budget_usd", "FLOAT64", "REQUIRED"),
        ("inserted_at", "TIMESTAMP", "REQUIRED"),
        ("_snapshot_date", "DATE", "REQUIRED"),
    ),
    "raw_dim_channel": (
        ("channel_id", "INT64", "REQUIRED"),
        ("channel_name", "STRING", "REQUIRED"),
        ("channel_group", "STRING", "REQUIRED"),
        ("inserted_at", "TIMESTAMP", "REQUIRED"),
        ("_snapshot_date", "DATE", "REQUIRED"),
    ),
    "raw_dim_video": (
        ("video_id", "STRING", "REQUIRED"),
        ("video_title", "STRING", "REQUIRED"),
        ("published_at", "DATE", "REQUIRED"),
        ("video_duration_min", "INT64", "REQUIRED"),
        ("inserted_at", "TIMESTAMP", "REQUIRED"),
        ("_snapshot_date", "DATE", "REQUIRED"),
    ),
}

RAW_TABLES: dict[str, RawTable] = {
    "raw_ga4_events": RawTable(
        bq_table="raw_ga4_events",
        source="ga4",
        gcs_table="ga4_events",
        partition_col="event_date",
        schema=RAW_SCHEMAS["raw_ga4_events"],
    ),
    "raw_ads_campaign_daily": RawTable(
        bq_table="raw_ads_campaign_daily",
        source="google_ads",
        gcs_table="ads_campaign_daily",
        partition_col="spend_date",
        schema=RAW_SCHEMAS["raw_ads_campaign_daily"],
    ),
    "raw_youtube_video_daily": RawTable(
        bq_table="raw_youtube_video_daily",
        source="youtube",
        gcs_table="youtube_video_daily",
        partition_col="video_date",
        schema=RAW_SCHEMAS["raw_youtube_video_daily"],
    ),
    "raw_dim_campaign": RawTable(
        bq_table="raw_dim_campaign",
        source="dimensions",
        gcs_table="dim_campaign",
        partition_col="_snapshot_date",
        schema=RAW_SCHEMAS["raw_dim_campaign"],
    ),
    "raw_dim_channel": RawTable(
        bq_table="raw_dim_channel",
        source="dimensions",
        gcs_table="dim_channel",
        partition_col="_snapshot_date",
        schema=RAW_SCHEMAS["raw_dim_channel"],
    ),
    "raw_dim_video": RawTable(
        bq_table="raw_dim_video",
        source="dimensions",
        gcs_table="dim_video",
        partition_col="_snapshot_date",
        schema=RAW_SCHEMAS["raw_dim_video"],
    ),
}

TABLE_BY_GCS_DIR: dict[str, str] = {meta.gcs_table: table for table, meta in RAW_TABLES.items()}


def gcs_object_for_date(table: str, ds: date) -> str:
    """Full GCS object key for one table on one data date: raw/<gcs_table>/<gcs_table>_<ds>.parquet."""
    meta = RAW_TABLES[table]
    return f"{GCS_PREFIX}/{meta.gcs_table}/{meta.gcs_table}_{ds.isoformat()}.parquet"


def gcs_uri_for_date(table: str, ds: str) -> str:
    """gs:// URI for the single daily object."""
    return f"gs://{GCS_BUCKET}/{gcs_object_for_date(table, date.fromisoformat(ds))}"


def gcs_uri_wildcard(table: str) -> str:
    """gs:// URI for the initial-load wildcard over every partition."""
    meta = RAW_TABLES[table]
    return f"gs://{GCS_BUCKET}/{GCS_PREFIX}/{meta.gcs_table}/{meta.gcs_table}_*.parquet"


def cosmos_project_config():
    """Cosmos ProjectConfig pointing at include/dbt/marketing."""
    from cosmos import ProjectConfig

    return ProjectConfig(dbt_project_path=str(DBT_DIR))


def cosmos_profile_config():
    """Cosmos ProfileConfig built from the google_cloud_default Airflow connection.

    Uses GoogleCloudServiceAccountDictionaryProfileMapping so dbt never needs the
    standalone profiles.yml. M5 VERIFY: profile_args keys (project/dataset/location)
    against cosmos 1.15.1's BigQuery mapping.
    """
    from cosmos import ProfileConfig
    from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

    return ProfileConfig(
        profile_name=DBT_PROFILE_NAME,
        target_name=DBT_TARGET_NAME,
        profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
            conn_id=GCP_CONN_ID,
            profile_args={
                "project": GCP_PROJECT_ID,
                "dataset": BQ_STAGING_DATASET,
                "location": GCP_LOCATION,
                "threads": 4,
            },
        ),
    )


def cosmos_profile_config_fallback() -> object:
    """Fallback ProfileConfig using the standalone profiles.yml (M3 standalone runs)."""
    from cosmos import ProfileConfig

    return ProfileConfig(
        profile_name=DBT_PROFILE_NAME,
        target_name=DBT_TARGET_NAME,
        profiles_yml_filepath=str(DBT_PROFILES_DIR / "profiles.yml"),
    )


def cosmos_execution_config():
    """Cosmos ExecutionConfig: LOCAL mode against the dedicated dbt virtualenv.

    M5 VERIFY: prefer ExecutionMode.LOCAL + dbt_executable_path; if the pinned
    cosmos 1.15.1 / Airflow 3.2 combination has issues, switch to
    ExecutionMode.VIRTUALENV (virtualenv=/usr/local/airflow/dbt_venv) — or use the
    run_dbt shell fallback in dags/common/tasks.py.
    """
    from cosmos import ExecutionConfig
    from cosmos.constants import ExecutionMode

    return ExecutionConfig(
        execution_mode=ExecutionMode.LOCAL,
        dbt_executable_path=str(DBT_EXECUTABLE),
    )


def cosmos_render_config():
    """Cosmos RenderConfig: one task per dbt model, tests once after the run.

    Model-level visibility per PRD 7.4; TestBehavior.AFTER_ALL chosen to keep the
    graph compact (AFTER_EACH adds a test task per model and is the alternative).
    LoadMode.DBT_LS resolves the node graph at DAG parse; switch to
    LoadMode.DBT_MANIFEST if parse time becomes a problem.
    """
    from cosmos import RenderConfig
    from cosmos.constants import LoadMode, TestBehavior

    return RenderConfig(
        load_method=LoadMode.DBT_LS,
        test_behavior=TestBehavior.AFTER_ALL,
        select=["marts"],
    )


def cosmos_operator_args(full_refresh: bool, run_date_task_id: str | None = None) -> dict:
    """Operator args passed to every Cosmos task.

    run_date is the dbt var the incremental models key on (PRD 8.1). For the daily
    DAG it is pulled from the resolve_data_date task so a manual target_date override
    flows into dbt; for the initial DAG the backfill end date is used directly.
    """
    env = {"lookback_days": "3"}
    if run_date_task_id:
        env["run_date"] = "{{ ti.xcom_pull(task_ids='" + run_date_task_id + "', key='return_value') }}"
    else:
        env["run_date"] = BACKFILL_END.isoformat()
    return {"full_refresh": full_refresh, "env": env}