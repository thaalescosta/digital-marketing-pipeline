"""Structural integrity tests for the marketing DAGs.

These run with the standard library plus logical assertions against the DAG object
model — no Airflow server, no GCP. DagBag eagerly imports the DAG files; fixtures
mirror the container layout (pipeline/ on sys.path, AIRFLOW_HOME pointing at an empty
temp dir so no airflow.db/cfg is needed).
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from airflow.models.dagbag import DagBag  # noqa: E402

from dags.common.config import (
    BACKFILL_END,
    FIRST_INCREMENTAL_DATE,
    RAW_TABLES,
    SOURCES,
    gcs_object_for_date,
)  # noqa: E402


@pytest.fixture()
def dagbag(tmp_path, monkeypatch):
    """DagBag that imports only the two marketing DAGs from a clean AIRFLOW_HOME."""
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path))
    return DagBag(dag_folder=str(PROJECT_ROOT / "dags"), include_examples=False)


def _all_task_ids(dag) -> set[str]:
    return set(dag.task_dict.keys())


def test_dagbag_contains_both_dags(dagbag):
    assert dagbag.import_errors == {}
    dag_ids = set(dagbag.dag_ids)
    assert dag_ids == {"marketing_initial_load", "marketing_daily_pipeline"}


def test_initial_load_dag_properties(dagbag):
    dag = dagbag.get_dag("marketing_initial_load")
    assert dag.schedule is None
    assert dag.start_date == datetime(2026, 7, 1)
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.is_paused_upon_creation is True
    assert "digital-marketing" in dag.tags
    assert "backfill" in dag.tags


def test_initial_load_graph_shape(dagbag):
    dag = dagbag.get_dag("marketing_initial_load")

    task_ids = _all_task_ids(dag)
    expected_ids = {
        "bootstrap_gcp",
        "compute_inserted_at",
        "gx_validate_raw",
        "dbt_transform",
        "gx_validate_marts",
    }
    for source in SOURCES:
        expected_ids.add(f"load_raw.{source}.generate_backfill_and_upload")
    for meta in RAW_TABLES.values():
        expected_ids.add(f"load_raw.{meta.source}.load_{meta.bq_table}")
    assert task_ids == expected_ids

    assert "dbt_transform" in dag.task_group_dict
    for meta in RAW_TABLES.values():
        load = dag.get_task(f"load_raw.{meta.source}.load_{meta.bq_table}")
        expected_direct = {"load_raw." + meta.source + ".generate_backfill_and_upload"}
        actual_upstream = set(load.upstream_task_ids)
        assert actual_upstream == expected_direct, (meta.bq_table, actual_upstream)

    bootstrap = dag.get_task("bootstrap_gcp")
    assert "load_raw.ga4.generate_backfill_and_upload" in bootstrap.downstream_task_ids
    assert "load_raw.dimensions.generate_backfill_and_upload" in bootstrap.downstream_task_ids

    gen_ga4 = dag.get_task("load_raw.ga4.generate_backfill_and_upload")
    assert gen_ga4.upstream_task_ids == {"bootstrap_gcp", "compute_inserted_at"}

    gx_raw = dag.get_task("gx_validate_raw")
    assert gx_raw.upstream_task_ids.issuperset({"load_raw.dimensions.load_raw_dim_video"})
    assert dag.get_task("dbt_transform").upstream_task_ids == {"gx_validate_raw"}
    assert dag.get_task("gx_validate_marts").upstream_task_ids == {"dbt_transform"}


def test_initial_load_has_no_daily_only_tasks(dagbag):
    dag = dagbag.get_dag("marketing_initial_load")
    task_ids = _all_task_ids(dag)
    assert "assert_initial_load_done" not in task_ids
    assert not any("resolve_data_date" in tid for tid in task_ids)
    assert not any("log_run_summary" in tid for tid in task_ids)


def test_daily_dag_properties(dagbag):
    dag = dagbag.get_dag("marketing_daily_pipeline")
    assert dag.schedule == "@daily"
    assert dag.start_date == datetime(2026, 7, 1)
    assert dag.catchup is True
    assert dag.max_active_runs == 1
    assert dag.is_paused_upon_creation is True
    assert "daily" in dag.tags


def test_daily_graph_shape(dagbag):
    dag = dagbag.get_dag("marketing_daily_pipeline")

    task_ids = _all_task_ids(dag)
    expected_ids = {
        "resolve_data_date",
        "assert_initial_load_done",
        "compute_inserted_at",
        "gx_validate_raw",
        "dbt_transform",
        "gx_validate_marts",
        "log_run_summary",
    }
    for source in SOURCES:
        expected_ids.add(f"load_raw.{source}.generate_and_upload")
    for meta in RAW_TABLES.values():
        expected_ids.add(f"load_raw.{meta.source}.load_{meta.bq_table}")
    assert task_ids == expected_ids

    resolved = dag.get_task("resolve_data_date")
    gate = dag.get_task("assert_initial_load_done")
    assert gate.upstream_task_ids == {"resolve_data_date"}

    gen_ga4 = dag.get_task("load_raw.ga4.generate_and_upload")
    assert gen_ga4.upstream_task_ids == {"assert_initial_load_done", "compute_inserted_at"}
    assert not ("resolve_data_date" in gen_ga4.upstream_task_ids)

    for meta in RAW_TABLES.values():
        load = dag.get_task(f"load_raw.{meta.source}.load_{meta.bq_table}")
        assert load.upstream_task_ids == {"load_raw." + meta.source + ".generate_and_upload"}

    assert dag.get_task("dbt_transform").upstream_task_ids == {"gx_validate_raw"}
    assert dag.get_task("gx_validate_marts").upstream_task_ids == {"dbt_transform"}

    summary = dag.get_task("log_run_summary")
    assert "gx_validate_marts" in summary.upstream_task_ids
    assert summary.trigger_rule.value == "all_done"


def test_partition_columns_match_load_semantics():
    """Facts partition on their business date, dimensions on _snapshot_date (PRD 6.3/D10)."""
    fact_partition_cols = {"event_date", "spend_date", "video_date"}
    for table, meta in RAW_TABLES.items():
        if table.startswith("raw_dim_"):
            assert meta.partition_col == "_snapshot_date"
        else:
            assert meta.partition_col in fact_partition_cols


def test_gcs_key_layout_matches_datagen_contract():
    for table, meta in RAW_TABLES.items():
        key = gcs_object_for_date(table, FIRST_INCREMENTAL_DATE)
        assert key == f"raw/{meta.gcs_table}/dt={FIRST_INCREMENTAL_DATE.isoformat()}/part.parquet"


def test_backfill_constant_is_initial_partition_boundary():
    assert BACKFILL_END.isoformat() == "2026-06-30"
    assert gcs_object_for_date("raw_ga4_events", BACKFILL_END) == "raw/ga4_events/dt=2026-06-30/part.parquet"