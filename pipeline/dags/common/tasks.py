"""Plain task callables for the digital-marketing DAGs.

Every function here is an environment-free plain callable (no Airflow imports) so it
can be unit-tested and imported anywhere. The DAG files wrap these with ``@task`` /
``DbtTaskGroup``; wiring lives only in the DAG files. Google Cloud clients are
constructed inside the functions, never at module import time, and all credentials
flow through Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS env var
set in the container) — never through code.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from dags.common.config import (
    BACKFILL_END,
    BQ_RAW_DATASET,
    BQ_STAGING_DATASET,
    BQ_MARTS_DATASET,
    DBT_DIR,
    DBT_EXECUTABLE,
    DBT_PROFILES_DIR,
    GCS_BUCKET,
    GCS_PREFIX,
    GCP_LOCATION,
    GCP_PROJECT_ID,
    GX_DIR,
    GX_PYTHON,
    GX_VENV,
    RAW_TABLES,
    SOURCES,
    TABLE_BY_GCS_DIR,
    gcs_uri_for_date,
    gcs_uri_wildcard,
)

logger = logging.getLogger(__name__)


def bootstrap_gcp() -> dict[str, bool]:
    """Idempotently create the GCS bucket and the three BigQuery datasets.

    Mirrors scripts/bootstrap_gcp.py so the DAG can call it in-process. Returns which
    resources were actually created; never deletes anything.
    """
    from google.cloud import bigquery
    from google.cloud import storage
    from google.cloud.exceptions import NotFound

    created: dict[str, bool] = {}

    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET)
    if bucket.exists():
        created[GCS_BUCKET] = False
    else:
        storage_client.create_bucket(GCS_BUCKET, location=GCP_LOCATION)
        created[GCS_BUCKET] = True
        logger.info("created bucket gs://%s in %s", GCS_BUCKET, GCP_LOCATION)

    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    for dataset_id in (BQ_RAW_DATASET, BQ_STAGING_DATASET, BQ_MARTS_DATASET):
        dataset_ref = bigquery.Dataset(f"{GCP_PROJECT_ID}.{dataset_id}")
        dataset_ref.location = GCP_LOCATION
        try:
            bq_client.get_dataset(dataset_ref)
            created[dataset_id] = False
        except NotFound:
            bq_client.create_dataset(dataset_ref)
            created[dataset_id] = True
            logger.info("created dataset %s in %s", dataset_id, GCP_LOCATION)

    return created


def compute_inserted_at() -> str:
    """UTC timestamp for one DAG run, computed once and shared across generator tasks."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def resolve_data_date(ds: str, target_date: str | None) -> str:
    """Return the effective data date.

    The optional target_date param overrides ds for manual triggers (PRD 7.3);
    templates render an absent param as the string "None", which is treated as unset.
    """
    if target_date and target_date.strip().lower() != "none":
        date.fromisoformat(target_date)
        return target_date
    return ds


def _upload_paths(run_dir: Path) -> list[dict[str, Any]]:
    """Upload every Parquet under run_dir to GCS raw/ and return per-file info.

    Uses parallel uploads via transfer_manager for speed, with fallback to sequential
    if parallel upload fails (e.g., due to file access race conditions).
    """
    from google.cloud import storage
    from google.cloud.storage import transfer_manager

    import pyarrow.parquet as pq

    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET)

    # Collect all parquet files with their metadata
    file_infos: list[tuple[Path, str, str]] = []  # (local_path, table_id, gcs_key)
    for path in sorted(run_dir.rglob("*.parquet")):
        # Defensive: file might have been deleted between rglob and here
        if not path.exists():
            logger.warning("skipping missing file: %s", path)
            continue
        rel = path.relative_to(run_dir).parts
        table_dir = rel[0]
        table_id = TABLE_BY_GCS_DIR.get(table_dir)
        if table_id is None:
            raise RuntimeError(
                f"generated table directory {table_dir!r} is not a known raw table "
                f"(expected one of {sorted(TABLE_BY_GCS_DIR)})"
            )
        # New flat format: <table>/<table>_YYYY-MM-DD.parquet
        filename = rel[1] if len(rel) > 1 else path.name
        key = "/".join((GCS_PREFIX, table_dir, filename))
        file_infos.append((path, table_id, key))

    if not file_infos:
        logger.warning("no parquet files found to upload in %s", run_dir)
        return []

    # Prepare parallel upload: list of source filenames and destination blob names
    source_filenames = [str(info[0]) for info in file_infos]
    blob_names = [info[2] for info in file_infos]

    # Try parallel upload first, fall back to sequential on failure
    try:
        results = transfer_manager.upload_many_from_filenames(
            bucket=bucket,
            filenames=source_filenames,
            source_directory="",
            blob_name_prefix="",
            skip_if_exists=False,
            max_workers=8,
            worker_type="process",  # processes avoid GIL/file-handle contention
            raise_exception=True,
        )
    except Exception as e:
        logger.warning("parallel upload failed (%s), falling back to sequential", e)
        # Sequential fallback
        results = []
        for src, dst in zip(source_filenames, blob_names):
            if not Path(src).exists():
                logger.warning("skipping missing file during sequential upload: %s", src)
                results.append(FileNotFoundError(src))
                continue
            blob = bucket.blob(dst)
            blob.upload_from_filename(src)
            results.append(None)

    # Build return value with row counts
    uploaded: list[dict[str, Any]] = []
    for (path, table_id, key), result in zip(file_infos, results):
        if isinstance(result, Exception):
            logger.error("upload failed for %s: %s", key, result)
            raise result
        rows = pq.ParquetFile(path).metadata.num_rows
        file_date = key.split("_")[-1].replace(".parquet", "") if "_" in key else "unknown"
        uri = f"gs://{GCS_BUCKET}/{key}"
        uploaded.append({"table": table_id, "gcs_uri": uri, "rows": rows})
        logger.info("uploaded table=%s date=%s uri=%s rows=%s", table_id, file_date, uri, rows)

    return uploaded


def generate_and_upload(
    source: str,
    ds: str,
    out_dir: str | Path,
    inserted_at: str,
) -> list[dict[str, Any]]:
    """Generate one source for one date and upload every Parquet to GCS (Decision 3).

    Datagen is deterministic per (seed, date, source); retrying this task rewrites the
    same objects. Returns one dict per file with keys table, gcs_uri, rows.
    """
    from datagen import generate

    if source not in SOURCES:
        raise ValueError(f"unknown source {source!r}, expected one of {SOURCES}")

    day = date.fromisoformat(ds)
    run_dir = Path(out_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    try:
        paths = generate(
            source=source,
            start_date=day,
            end_date=day,
            out_dir=run_dir,
            inserted_at=inserted_at,
        )
        if not paths:
            logger.warning("datagen returned no files for source=%s date=%s", source, ds)
        return _upload_paths(run_dir)
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)


def generate_backfill_and_upload(
    source: str,
    start_date: str,
    end_date: str,
    out_dir: str | Path,
    inserted_at: str,
) -> list[dict[str, Any]]:
    """Generate a source over the backfill window and upload every Parquet to GCS.

    One call per source handles the full 181-day window; files are tiny so chunking is
    unnecessary locally and can be added later if run memory becomes a concern. The
    datagen port must expose generate_backfill with the same signature as generate.
    """
    from datagen import generate_backfill

    if source not in SOURCES:
        raise ValueError(f"unknown source {source!r}, expected one of {SOURCES}")

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    run_dir = Path(out_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    try:
        paths = generate_backfill(
            source=source,
            start_date=start,
            end_date=end,
            out_dir=run_dir,
            inserted_at=inserted_at,
        )
        if not paths:
            logger.warning("datagen returned no files for source=%s range=%s..%s", source, start, end)
        return _upload_paths(run_dir)
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)


def _require_object(uri: str, table: str, ds: str) -> None:
    from google.cloud import storage

    from google.cloud.exceptions import NotFound

    storage_client = storage.Client(project=GCP_PROJECT_ID)
    blob_path = uri.split("gs://", 1)[1]
    bucket_name, _, object_name = blob_path.partition("/")
    try:
        blob = storage_client.bucket(bucket_name).get_blob(object_name)
    except NotFound:
        blob = None
    if blob is None:
        raise RuntimeError(f"required file missing for table={table} date={ds}: {uri}")


def _require_any_object(meta, table: str) -> None:
    from google.cloud import storage
    import time

    storage_client = storage.Client(project=GCP_PROJECT_ID)
    # New flat format: raw/<table>/<table>_*.parquet
    prefix = f"{GCS_PREFIX}/{meta.gcs_table}/{meta.gcs_table}_"
    bucket = storage_client.bucket(GCS_BUCKET)
    
    # Retry with backoff for GCS eventual consistency
    max_retries = 5
    base_delay = 2  # seconds
    for attempt in range(max_retries):
        blobs = list(bucket.list_blobs(prefix=prefix, max_results=1))
        if blobs:
            return
        if attempt < max_retries - 1:
            delay = base_delay * (2 ** attempt)
            logger.warning(
                "required files not yet visible for table=%s (attempt %d/%d), waiting %ds",
                table, attempt + 1, max_retries, delay
            )
            time.sleep(delay)
    
    raise RuntimeError(
        f"required files missing for table={table}: no objects under gs://{GCS_BUCKET}/{prefix} "
        f"after {max_retries} retries (GCS eventual consistency)"
    )


def load_to_bigquery(
    table: str,
    gcs_uri: str | None,
    ds: str,
    mode: str,
) -> dict[str, Any]:
    """Load one raw table from GCS into BigQuery.

    mode="daily": partition decorator ``table$YYYYMMDD`` + WRITE_TRUNCATE so a re-run
    replaces only the run's partition (facts partition on business date, dims on
    _snapshot_date). mode="initial": wildcard ``dt=*`` + WRITE_TRUNCATE on the whole
    table with the explicit schema and day partitioning on the contract column
    (PRD 6.3). Fails fast when the required object(s) do not exist. Returns
    {table, date, rows_loaded, mode}.
    """
    from google.cloud import bigquery

    if table not in RAW_TABLES:
        raise ValueError(f"unknown raw table {table!r}, expected one of {sorted(RAW_TABLES)}")
    if mode not in ("daily", "initial"):
        raise ValueError(f"mode must be 'daily' or 'initial', got {mode!r}")

    meta = RAW_TABLES[table]
    uri = gcs_uri or (gcs_uri_for_date(table, ds) if mode == "daily" else gcs_uri_wildcard(table))
    client = bigquery.Client(project=GCP_PROJECT_ID)
    schema = [bigquery.SchemaField(name, field_type, field_mode) for name, field_type, field_mode in meta.schema]

    if mode == "daily":
        _require_object(uri, table, ds)
        destination = f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{table}${ds.replace('-', '')}"
    else:
        _require_any_object(meta, table)
        destination = f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{table}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=False,
        schema=schema,
    )
    if mode == "initial":
        job_config.time_partitioning = bigquery.TimePartitioning(
            field=meta.partition_col,
            type_=bigquery.TimePartitioningType.DAY,
        )

    job = client.load_table_from_uri(uri, destination, job_config=job_config)
    job.result()
    rows = job.output_rows
    logger.info("table=%s date=%s rows_loaded=%s mode=%s", table, ds, rows, mode)
    return {"table": table, "date": ds, "rows_loaded": rows, "mode": mode}


def run_gx(ds: str, scope: str, suite: str | None = None, dataset: str | None = None) -> dict[str, Any]:
    """Run the Great Expectations runner in the dedicated GX virtualenv.

    Either ``suite`` (single suite name) or ``dataset`` ("raw" or "marts") must be provided.
    scope is "partition" (daily WHERE partition_col = ds) or "full" (initial load).
    The runner exits non-zero for CRITICAL failures only — a non-zero exit raises here,
    blocking downstream dbt (PRD 9.5); WARN failures log and continue.
    """
    if scope not in ("partition", "full"):
        raise ValueError(f"scope must be 'partition' or 'full', got {scope!r}")
    if not suite and not dataset:
        raise ValueError("either suite or dataset must be provided")

    env = os.environ.copy()
    # Ensure GX venv's site-packages are in PYTHONPATH for dependencies like pyyaml
    gx_venv_site_packages = GX_VENV / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
    pythonpath_parts = [str(GX_DIR)]
    if gx_venv_site_packages.exists():
        pythonpath_parts.insert(0, str(gx_venv_site_packages))
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)
    
    runner_path = GX_DIR / "runner.py"
    if dataset:
        cmd = [str(GX_PYTHON), str(runner_path), "--dataset", dataset, "--date", ds, "--scope", scope]
        label = f"dataset={dataset}"
    else:
        cmd = [str(GX_PYTHON), str(runner_path), "--suite", suite, "--date", ds, "--scope", scope]
        label = f"suite={suite}"
    result = subprocess.run(
        cmd,
        cwd=str(GX_DIR),
        env=env,
        capture_output=True,
        text=True,
        timeout=3600,
    )
    for line in (result.stdout or "").splitlines():
        logger.info("gx %s: %s", label, line)
    for line in (result.stderr or "").splitlines():
        logger.warning("gx %s stderr: %s", label, line)
    if result.returncode != 0:
        raise RuntimeError(
            f"gx validation failed {label} date={ds} scope={scope} exit_code={result.returncode}"
        )
    logger.info("gx validation passed %s date=%s scope=%s", label, ds, scope)
    return {"suite": suite, "dataset": dataset, "date": ds, "scope": scope, "exit_code": result.returncode}


def run_dbt(
    profile: str = "marketing",
    dbt_args: list[str] | None = None,
    run_date: str | None = None,
) -> dict[str, Any]:
    """Run dbt standalone with the dedicated dbt virtualenv.

    Not used by the DAGs — Cosmos DbtTaskGroup is the production path (PRD 7.4). This
    helper exists for M3 standalone runs, debugging, and the M5 fallback if Cosmos is
    unavailable: document which dbt_args are supported before relying on it.
    """
    args = dbt_args or ["build"]
    cmd = [str(DBT_EXECUTABLE), *args]
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(DBT_PROFILES_DIR)
    vars_payload = json.dumps(
        {"run_date": run_date or BACKFILL_END.isoformat(), "lookback_days": "3"}, sort_keys=True
    )
    result = subprocess.run(
        [*cmd, "--vars", vars_payload],
        cwd=str(DBT_DIR),
        env=env,
        capture_output=True,
        text=True,
        timeout=7200,
    )
    for line in (result.stdout or "").splitlines():
        logger.info("dbt profile=%s: %s", profile, line)
    for line in (result.stderr or "").splitlines():
        logger.warning("dbt profile=%s stderr: %s", profile, line)
    if result.returncode != 0:
        raise RuntimeError(f"dbt failed profile={profile} command={' '.join(cmd)} exit_code={result.returncode}")
    return {"profile": profile, "command": cmd, "exit_code": result.returncode}


def log_run_summary(context: dict[str, Any]) -> str:
    """Log one line per raw load task for the run (PRD 10 observability).

    Wired with trigger_rule=all_done so a failed critical gate still produces the
    summary. Reads the return_value XCom of every ``<source>.load_<table>`` task and
    lists not_run entries for tasks that never completed.
    """
    dag_run = context.get("dag_run")
    if dag_run is None:
        return "log_run_summary: dag_run missing from context"

    expected: dict[str, str] = {
        f"{meta.source}.load_{table}": table for table, meta in RAW_TABLES.items()
    }
    tis = dag_run.get_task_instances() if hasattr(dag_run, "get_task_instances") else list(dag_run.task_instances)
    lines = [f"dag_run={dag_run.run_id} dag_id={dag_run.dag_id}"]
    for task_id, table in expected.items():
        ti = next((item for item in tis if item.task_id == task_id), None)
        if ti is None or ti.state != "success":
            lines.append(f"stage=load table={table} status=not_run")
            continue
        info = ti.xcom_pull(task_ids=task_id, key="return_value")
        if isinstance(info, dict):
            lines.append(
                f"stage=load table={info.get('table', table)} date={info.get('date')} "
                f"rows_loaded={info.get('rows_loaded')} mode={info.get('mode')}"
            )
        else:
            lines.append(f"stage=load table={table} status=success rows_loaded=unknown")

    summary = "\n".join(lines)
    for line in lines:
        logger.info("summary: %s", line)
    return summary


def assert_initial_load_done(ds: str) -> dict[str, Any]:
    """Fail fast unless the backfill-end partition exists in every raw table.

    Queries INFORMATION_SCHEMA.PARTITIONS for the required partition
    (BACKFILL_END, i.e. 2026-06-30) per table; the daily DAG depends on the initial
    load having landed before any incremental work starts (PRD 7.3).
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT_ID)
    partition_id = BACKFILL_END.strftime("%Y%m%d")
    checked: list[str] = []

    for table in RAW_TABLES:
        query = (
            f"SELECT COUNT(1) AS n FROM `{GCP_PROJECT_ID}.{BQ_RAW_DATASET}`.INFORMATION_SCHEMA.PARTITIONS "
            "WHERE table_name = @table_name AND partition_id = @partition_id"
        )
        job = client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("table_name", "STRING", table),
                    bigquery.ScalarQueryParameter("partition_id", "STRING", partition_id),
                ]
            ),
        )
        row = next(iter(job.result()))
        if row["n"] == 0:
            raise RuntimeError(
                f"initial load not done: {table} is missing partition {partition_id} "
                "(run marketing_initial_load before the daily pipeline)"
            )
        checked.append(table)

    logger.info("initial load verified tables=%s partition=%s", checked, partition_id)
    return {"date": ds, "required_partition": partition_id, "tables": checked}