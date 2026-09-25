"""Great Expectations Core 1.x runner for the digital-marketing pipeline.

Invocation (from the GX venv, with include/ on PYTHONPATH):

    python -m gx.runner --suite raw_ga4_events --date 2026-07-01
    python -m gx.runner --suite raw_ga4_events --date 2026-07-01 --scope partition
    python -m gx.runner --all-suites --date 2026-07-01 --scope partition
    python -m gx.runner --all-suites --date 2026-06-30 --scope full

Environment:
    GCP_PROJECT_ID   BigQuery project id (default digital-marketing-509604)
    GCP_LOCATION     BigQuery location (default US)
    BQ_RAW_DATASET   raw dataset (default digital_marketing_raw)
    BQ_MARTS_DATASET marts dataset (default digital_marketing_marts)

Failure policy (PRD 9.5):
    CRITICAL expectation failures make the suite's success False and the
    process exit code 1 (the Airflow task fails and downstream tasks do not
    run). WARN expectation failures are counted and shown in Data Docs but
    never fail the task. Unknown severity defaults to CRITICAL (fail-safe).

Design notes:
    Every GX object is rebuilt per run from the version-controlled files in
    this directory: checkpoints/daily_checkpoint.yaml (the run registry) and
    suites/*.json (problem definitions). Strings containing {placeholders}
    are rendered from the run context. scope=partition renders each query
    with a WHERE <partition_column> = DATE('<ds>') clause so BigQuery prunes
    to the run's partition; scope=full renders the clause as TRUE so the
    whole table is validated (initial load). M4 (a real GX run in the GX
    venv) validates the exact 1.x API shapes recorded in the build notes.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

import great_expectations as gx
import yaml

LOGGER = logging.getLogger("gx.runner")

GX_ROOT = Path(__file__).resolve().parent
CHECKPOINT_FILE = GX_ROOT / "checkpoints" / "daily_checkpoint.yaml"
DATA_SOURCE_NAME = "bigquery"

CRITICAL = "CRITICAL"
WARN = "WARN"


@dataclass(frozen=True)
class DataSourceConfig:
    project_id: str
    location: str
    raw_dataset: str
    marts_dataset: str

    @classmethod
    def from_env(cls) -> "DataSourceConfig":
        return cls(
            project_id=os.environ.get("GCP_PROJECT_ID", "digital-marketing-509604"),
            location=os.environ.get("GCP_LOCATION", "US"),
            raw_dataset=os.environ.get("BQ_RAW_DATASET", "digital_marketing_raw"),
            marts_dataset=os.environ.get("BQ_MARTS_DATASET", "digital_marketing_marts"),
        )


@dataclass(frozen=True)
class ValidationOutcome:
    suite_name: str
    batch: str
    success: bool
    n_expectations: int
    n_failed: int
    critical_failures: int
    warn_failures: int


def _render(value: Any, context: Mapping[str, Any]) -> Any:
    if isinstance(value, str) and "{" in value:
        return value.format_map(context)
    if isinstance(value, list):
        return [_render(item, context) for item in value]
    if isinstance(value, dict):
        return {key: _render(item, context) for key, item in value.items()}
    return value


def _expectation_from_entry(entry: Mapping[str, Any]) -> Any:
    expectation_class = getattr(gx.expectations, entry["type"])
    kwargs = dict(entry.get("kwargs", {}))
    meta = dict(entry.get("meta", {}))
    return expectation_class(**kwargs, meta=meta)


def _add_or_get(store: Any, obj: Any) -> Any:
    try:
        store.add(obj)
        return obj
    except Exception as exc:
        LOGGER.warning("store.add failed for %s, falling back to get: %s", obj.name, exc)
        return store.get(name=obj.name)


def _connection_string(cfg: DataSourceConfig) -> str:
    return f"bigquery://{cfg.project_id}?location={cfg.location}"


def _get_or_add_datasource(context: gx.DataContext, cfg: DataSourceConfig) -> Any:
    try:
        return context.data_sources.add_sql(name=DATA_SOURCE_NAME, connection_string=_connection_string(cfg))
    except Exception as exc:
        LOGGER.warning("datasource add failed, falling back to get: %s", exc)
        return context.data_sources.get(name=DATA_SOURCE_NAME)


def _table_batch_sql(
    cfg: DataSourceConfig,
    dataset: str,
    table: str,
    partition_column: str | None,
    ds: date,
    scope: str,
) -> str:
    qualified = f"`{cfg.project_id}.{dataset}.{table}`"
    if scope == "partition" and partition_column:
        return f"SELECT * FROM {qualified} WHERE {partition_column} = DATE('{ds.isoformat()}')"
    return f"SELECT * FROM {qualified}"


def _render_context(cfg: DataSourceConfig, dataset: str, ds: date, scope: str) -> dict[str, Any]:
    day = ds.isoformat()
    plus_1 = (ds + timedelta(days=1)).isoformat()
    plus_2 = (ds + timedelta(days=2)).isoformat()

    def partition_clause(column: str) -> str:
        if scope == "partition":
            return f"{column} = DATE('{day}')"
        return "TRUE"

    return {
        "ds": day,
        "ds_plus_1": plus_1,
        "ds_plus_2": plus_2,
        "project": cfg.project_id,
        "location": cfg.location,
        "dataset": dataset,
        "raw_dataset": cfg.raw_dataset,
        "marts_dataset": cfg.marts_dataset,
        "scope": scope,
        "clause_event_date": partition_clause("event_date"),
        "clause_spend_date": partition_clause("spend_date"),
        "clause_video_date": partition_clause("video_date"),
        "clause_stat_date": partition_clause("stat_date"),
        "clause_session_date": partition_clause("session_date"),
        "clause_snapshot_date": partition_clause("_snapshot_date"),
    }


def _batch_sql(
    batch_spec: Mapping[str, Any],
    dataset: str,
    cfg: DataSourceConfig,
    ds: date,
    scope: str,
    render_context: Mapping[str, Any],
) -> str:
    if batch_spec["type"] == "table":
        return _table_batch_sql(
            cfg,
            dataset,
            batch_spec["table"],
            batch_spec.get("partition_column"),
            ds,
            scope,
        )
    return _render(batch_spec["sql"], render_context)


def _add_query_batch_definition(datasource: Any, suite_name: str, batch_name: str, sql: str) -> Any:
    asset_name = f"{suite_name}__{batch_name}"
    asset = datasource.add_query_asset(name=asset_name, query=sql)
    return asset.add_batch_definition_whole_query(name="whole_query")


def _build_validation_definition(
    context: gx.DataContext,
    suite_name: str,
    batch_name: str,
    batch_definition: Any,
    expectations: Sequence[Mapping[str, Any]],
) -> Any:
    suite = gx.ExpectationSuite(name=f"{suite_name}__{batch_name}")
    for entry in expectations:
        suite.add_expectation(_expectation_from_entry(entry))
    suite = _add_or_get(context.suites, suite)
    validation_definition = gx.ValidationDefinition(
        name=f"{suite_name}__{batch_name}",
        data=batch_definition,
        suite=suite,
    )
    return _add_or_get(context.validation_definitions, validation_definition)


def _outcome_from_result(suite_name: str, batch_label: str, result: Any) -> ValidationOutcome:
    run_results = list(result.run_results.values()) if result.run_results else []
    n_expectations = 0
    n_failed = 0
    critical_failures = 0
    warn_failures = 0
    for validation_result in run_results:
        expectation_results = getattr(validation_result, "results", None)
        if expectation_results is None:
            n_failed += 1
            critical_failures += 1
            continue
        for expectation_result in expectation_results:
            n_expectations += 1
            if expectation_result.success:
                continue
            n_failed += 1
            config = getattr(expectation_result, "expectation_config", None)
            meta = getattr(config, "meta", None) or {}
            if meta.get("severity", CRITICAL) == WARN:
                warn_failures += 1
            else:
                critical_failures += 1
    return ValidationOutcome(
        suite_name=suite_name,
        batch=batch_label,
        success=critical_failures == 0,
        n_expectations=n_expectations,
        n_failed=n_failed,
        critical_failures=critical_failures,
        warn_failures=warn_failures,
    )


def _load_registry() -> dict[str, Any]:
    with CHECKPOINT_FILE.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _find_suite_entry(registry: Mapping[str, Any], suite_name: str) -> dict[str, Any]:
    for entry in registry["suites"]:
        if entry["name"] == suite_name:
            return dict(entry)
    raise KeyError(f"suite {suite_name!r} not found in {CHECKPOINT_FILE.name}")


def _load_suite_file(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def run_suite(
    suite_name: str,
    date: date,
    scope: str,
    data_source_config: DataSourceConfig,
    registry: Mapping[str, Any] | None = None,
) -> ValidationOutcome:
    registry = registry if registry is not None else _load_registry()
    entry = _find_suite_entry(registry, suite_name)
    dataset = (
        data_source_config.raw_dataset
        if entry["dataset"] == "raw"
        else data_source_config.marts_dataset
    )
    suite_spec = _load_suite_file(GX_ROOT / entry["file"])
    batch_label = f"{date.isoformat()}:{scope}"

    context = gx.get_context(context_root_dir=str(GX_ROOT))
    datasource = _get_or_add_datasource(context, data_source_config)
    render_context = _render_context(data_source_config, dataset, date, scope)

    validation_definitions: list[Any] = []
    for batch_spec in suite_spec["batches"]:
        batch_name = batch_spec["name"]
        sql = _batch_sql(batch_spec, dataset, data_source_config, date, scope, render_context)
        batch_definition = _add_query_batch_definition(datasource, suite_name, batch_name, sql)
        expectations = _render(batch_spec["expectations"], render_context)
        validation_definitions.append(
            _build_validation_definition(
                context,
                suite_name,
                batch_name,
                batch_definition,
                expectations,
            )
        )

    checkpoint = gx.Checkpoint(name=f"{suite_name}_checkpoint", validation_definitions=validation_definitions)
    checkpoint = _add_or_get(context.checkpoints, checkpoint)
    result = checkpoint.run()

    outcome = _outcome_from_result(suite_name, batch_label, result)
    print(
        f"{outcome.suite_name},{outcome.batch},{outcome.success},"
        f"{outcome.n_expectations},{outcome.n_failed}"
    )
    return outcome


def _build_data_docs() -> None:
    context = gx.get_context(context_root_dir=str(GX_ROOT))
    sites = context.build_data_docs()
    LOGGER.info("data docs sites built: %s", ", ".join(sites.keys()))


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(
        description="Run Great Expectations Core 1.x suites for the digital-marketing pipeline."
    )
    suite_group = parser.add_mutually_exclusive_group(required=True)
    suite_group.add_argument(
        "--suite",
        help="run a single suite by name from checkpoints/daily_checkpoint.yaml",
    )
    suite_group.add_argument(
        "--all-suites",
        action="store_true",
        help="run every suite in the checkpoint registry",
    )
    suite_group.add_argument(
        "--dataset",
        choices=["raw", "marts"],
        help="run all suites for a dataset (raw or marts)",
    )
    parser.add_argument("--date", required=True, help="data date YYYY-MM-DD (the run's ds)")
    parser.add_argument(
        "--scope",
        choices=["partition", "full"],
        default="partition",
        help="partition = WHERE <partition_column> = ds; full = whole table (initial load)",
    )
    args = parser.parse_args(argv)

    ds = date.fromisoformat(args.date)
    cfg = DataSourceConfig.from_env()
    registry = _load_registry()
    if args.all_suites:
        suite_names = [entry["name"] for entry in registry["suites"]]
    elif args.dataset:
        suite_names = [entry["name"] for entry in registry["suites"] if entry.get("dataset") == args.dataset]
        if not suite_names:
            raise ValueError(f"no suites found for dataset {args.dataset!r}")
    else:
        suite_names = [args.suite]
    for suite_name in suite_names:
        _find_suite_entry(registry, suite_name)

    outcomes: list[ValidationOutcome] = []
    for suite_name in suite_names:
        try:
            outcomes.append(run_suite(suite_name, ds, args.scope, cfg, registry))
        except Exception as exc:
            LOGGER.error("suite %s failed to run: %s", suite_name, exc)
            failure = ValidationOutcome(
                suite_name=suite_name,
                batch=f"{ds.isoformat()}:{args.scope}",
                success=False,
                n_expectations=0,
                n_failed=1,
                critical_failures=1,
                warn_failures=0,
            )
            print(
                f"{failure.suite_name},{failure.batch},{failure.success},"
                f"{failure.n_expectations},{failure.n_failed}"
            )
            outcomes.append(failure)

    try:
        _build_data_docs()
    except Exception as exc:
        LOGGER.warning("data docs build failed: %s", exc)

    return 1 if any(not outcome.success for outcome in outcomes) else 0


if __name__ == "__main__":
    sys.exit(main())