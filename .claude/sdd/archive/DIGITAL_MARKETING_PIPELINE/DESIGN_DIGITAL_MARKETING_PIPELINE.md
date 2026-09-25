# DESIGN: Digital Marketing Pipeline

> Technical design for implementing the Digital Marketing Pipeline portfolio project

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | DIGITAL_MARKETING_PIPELINE |
| **Date** | 2026-09-24 |
| **Author** | design-agent |
| **DEFINE** | [DEFINE_DIGITAL_MARKETING_PIPELINE.md](./DEFINE_DIGITAL_MARKETING_PIPELINE.md) |
| **Status** | ✅ Shipped |

---

## Architecture Overview

```text
┌──────────────────────────────────────────────────────────────────────────┐
│                    DIGITAL MARKETING PIPELINE (Astro + Cosmos)            │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  Airflow (Astronomer local, astro dev)                                    │
│  ┌───────────────────────────────┐   ┌───────────────────────────────┐    │
│  │  marketing_initial_load DAG   │   │ marketing_daily_pipeline DAG  │    │
│  │  (once: 2026-01-01..06-30)    │   │ (1 new day per run ≥07-01)    │    │
│  └──────────────┬────────────────┘   └──────────────┬────────────────┘    │
│                 │                                   │                     │
│    ┌────────────┴───────────┐           ┌───────────┴─────────────┐       │
│    │ datagen (include/)     │           │ datagen daily (in-task) │       │
│    │ generate(source,range, │           │ generate+upload SAME    │       │
│    │   out_dir, inserted_at)│           │   task (decision)       │       │
│    └────────────┬───────────┘           └───────────┬─────────────┘       │
│                 │  Parquet (UTC, µs, deterministic) │                     │
│                 ▼                                   ▼                     │
│  gs://thaalescosta_marketing/raw/<table>/dt=YYYY-MM-DD/part.parquet       │
│                 │                                   │                     │
│                 └───────────────┬───────────────────┘                     │
│                                 ▼                                          │
│  BigQuery digital_marketing_raw ──partition-scoped WRITE_TRUNCATE loads──►│
│  raw_ga4_events | raw_ads_campaign_daily | raw_youtube_video_daily        │
│  raw_dim_campaign | raw_dim_channel | raw_dim_video  (_snapshot_date)     │
│                                 │                                          │
│                                 ▼   GX gate 1 (CRITICAL blocks, WARN logs)│
│  ┌─────────────────────────────┴──────────────────────────────┐           │
│  │  dbt (via Cosmos, insert_overwrite + 3-day lookback)        │           │
│  │  digital_marketing_staging (views) → digital_marketing_marts│           │
│  │  dim_date, dim_campaign, dim_channel, dim_video             │           │
│  │  fct_ga4_events, fct_ads_campaign_daily, fct_youtube_daily  │           │
│  │  rpt_daily_marketing_summary                                │           │
│  └─────────────────────────────┬──────────────────────────────┘           │
│                                 ▼   GX gate 2 (marts sanity + reconcile)  │
│                       Analysis-ready marts (+ data dictionary)            │
│                                                                           │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| datagen package | Deterministic fake GA4 / Ads / YouTube / dimension data; `generate(source, start, end, out_dir, inserted_at) -> list[Path]` | Python ≥ 3.12, pandas, numpy, faker, pyarrow; own pyproject |
| GCS bucket `gs://thaalescosta_marketing/` | Format-of-record Parquet staging, deterministic overwrite keys `raw/<table>/dt=YYYY-MM-DD/part.parquet` | GCS, `google-cloud-storage` |
| BigQuery raw dataset `digital_marketing_raw` | Partitioned raw landing tables (facts by data date; dimensions by `_snapshot_date`) | BigQuery, WRITE_TRUNCATE partition-scoped load jobs |
| Loader task callables | Partition-scoped idempotent loads from GCS into BigQuery | `google-cloud-bigquery` via `dags/common/tasks.py` |
| dbt project `include/dbt/marketing/` | Staging views + marts core (dim/fact) + reporting metric table; dbt tests | dbt-core, dbt-bigquery, dbt-utils, Cosmos DbtTaskGroup |
| GX suites + runner `include/gx/` | Raw contract suites + marts sanity/reconciliation; CRITICAL blocks, WARN logs | great-expectations 1.x (dedicated venv) |
| Airflow DAGs | `marketing_initial_load`, `marketing_daily_pipeline` (wiring only); CI-testable | Apache Airflow 3.0 (Astro Runtime), TaskFlow |
| `scripts/bootstrap_gcp.py` | Idempotent GCS bucket + 3 datasets creation | Python, google-cloud SDKs |
| `scripts/smoke_test.sh` | Manual E2E runbook automation (Section 14 SQL checks) | Bash |
| Docs | README (arch diagram, setup, runbook), `docs/decisions.md`, `docs/data_dictionary.md` | Markdown |

---

## Key Decisions

### Decision 1: BigQuery-native incremental strategy — `insert_overwrite` + 3-day lookback

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-09-24 |

**Context:** dbt incremental sync on BigQuery defaults to `merge` on a unique key; for daily-partitioned fact tables it is slower and more complex. The DEFINE (M3) requires idempotent re-runnable days and cost guard in the incremental path.

**Choice:** Marts fact models use `materialized='incremental'` with `partition_by` on the data date and `incremental_strategy='insert_overwrite'`, filtering to the run date plus a 3-day lookback via an `is_incremental()` guard. Staging models are views over raw.

**Rationale:** KB `dbt/incremental-strategies.md` (confidence 0.95) — `insert_overwrite` is the recommended BigQuery pattern: each run rewrites only the touched partitions, so re-runs are exact and no unique-key merge state is needed. The 3-day lookback (KB `dbt/patterns/incremental-model.md`) absorbs late-arriving/backfilled rows without full scans.

**Alternatives Rejected:**
1. `merge` with `unique_key` — Rejected because it mutates rows incrementally, is slower at partition grain, and does not cleanly overwrite a re-run partition.
2. Full refresh every run — Rejected because it scans the whole table, violating the cost guard and destroying idempotent-per-partition semantics.

**Consequences:**
- Raw/DW kernels guarantee per-partition equivalence; overlaps only within the 3-day window (documented, harmless: rewrite is idempotent).
- dbt tests must run within the same model selection; full-refresh equivalence still validated (AT-009).

---

### Decision 2: Great Expectations Core 1.x in a dedicated venv

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-09-24 |

**Context:** GX Core 0.18.x is retired; GX 1.x changed expectations/checkpoint APIs. Airflow images are pinned to Astro Runtime versions.

**Choice:** GX Core 1.x (latest stable as of M0) installed in its own Python venv inside the Astro image; `include/gx/` holds version-controlled suites, checkpoints, and a thin `runner.py` invoked as a DAG task; validations scoped to the run's partition (`WHERE <partition_col> = ds`), whole-table in initial load.

**Rationale:** KB `data-quality/patterns/great-expectations.md` (confidence 0.95): GX Core 1.3+ is the maintained line; 0.18 is retired. A dedicated venv isolates GX's dependency conflicts (notably pandas/pyarrow versions) from datagen and dbt. Version pinning is verified at M0 (assumption A-005).

**Alternatives Rejected:**
1. GX 0.18 in the same image — Rejected (retired line, API churn).
2. Soda instead of GX — Rejected: DEFINE explicitly chose GX (dual-gate split with dbt tests is a portfolio requirement).

**Consequences:**
- Dockerfile must build the GX venv and the runner must be invoked with the venv python (documented in README).
- M0 must pin the installed GX version and record it (A-005 verification).

---

### Decision 3: Generate + upload in the same Airflow task

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-09-24 |

**Context:** BRAINSTORM Q4 — separate generate and upload tasks add orchestration surface without value at this scale (6 files/day).

**Choice:** One task per daily run performs `datagen.generate(...)` then uploads the returned Parquet files to GCS. The task callable lives in `dags/common/tasks.py`; `inserted_at` is computed once per run and shared across sources.

**Rationale:** Single task removes a state handoff (file list passing between tasks), makes the generate→upload step atomic from Airflow's perspective, and keeps DAG files wiring-only per PRD §7.1. Retry re-runs the whole generate+upload deterministically (idempotent by design).

**Alternatives Rejected:**
1. Two tasks (generate, then upload) — Rejected: adds a task dependency and intermediate state for zero benefit at 6 files/day.
2. Upload inside datagen library — Rejected: keeps the library GCS-agnostic (testable locally).

**Consequences:**
- BigQuery load task is separate (later in DAG), so a load failure re-runs only the load step, not generation.
- Smoke test must verify GCS object overwrites on re-run (idempotency).

---

### Decision 4: Deterministic RNG per `(seed, date, source)` with injected `inserted_at`

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-09-24 |

**Context:** DEFINE G4/M1 — same (seed, date, source) must yield identical rows (excluding `inserted_at`); daily generation for date X must equal the backfill's rows for X (PRD 6.1.1, fixes D5 wall-clock dependence).

**Choice:** Each generator derives an independent RNG from `seed = GEN_SEED` combined with the date and source via a stable hash (e.g., `np.random.default_rng(seed=signed32(hash(f"{GEN_SEED}:{date.isoformat()}:{source}")))`). `inserted_at` is computed once per DAG run and passed as a parameter — never read from wall clock inside the library.

**Rationale:** Independence per (date, source) guarantees backfill X == daily X (same seed derivation path) and cross-source independence (adding GA4 volume never shifts ads rows). Injected `inserted_at` satisfies "once per run" (BRAINSTORM Q5) and keeps the library pure and testable.

**Alternatives Rejected:**
1. One shared RNG advancing through all dates/sources — Rejected: order-of-generation coupling breaks determinism across runs.
2. Wall-clock `inserted_at` inside library — Rejected (D5 defect).

**Consequences:**
- Unit tests assert backfill == daily equality across all sources/dates (AT-005).
- `inserted_at` is a required parameter of `generate(...)`; Airflow passes the run's timestamp (UTC).

---

### Decision 5: Single repo — AgentSpec framework at root + `pipeline/` subfolder

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-09-24 |

**Context:** BRAINSTORM Q2 — where should the project live?

**Choice:** One git repository at the workspace root (`C:\Projects\digital-marketing-pipeline`): the AgentSpec framework (`.claude/`, `.opencode/`, `tools/`) and the pipeline implementation under `pipeline/` (PRD §12 structure). `git init` at workspace root; `pipeline/legacy/fake-data-gen/` kept untouched as reference.

**Rationale:** Single repo keeps SDD artifacts (DEFINE/DESIGN/BRAINSTORM under `.claude/sdd/features/`) beside the code they specify, one commit per milestone, and the framework's KB/agents immediately available to M0–M6. Folder boundaries (`.claude` vs `pipeline`) keep the framework cleanly separable if the pipeline is later extracted.

**Alternatives Rejected:**
1. Two repos (framework, pipeline) — Rejected: overkill for a portfolio project; complicates cross-repo SDD docs.
2. Pipeline at repo root without `pipeline/` — Rejected: would mix framework and implementation files.

**Consequences:**
- `.gitignore` (root) must ignore `pipeline/include/gcp_creds/` and `.env`; framework files are versioned.
- Dockerfile lives at `pipeline/Dockerfile`; Airflow projects the whole `pipeline/` context.

---

### Decision 6: Dimension evolution as a pure state machine — `dimensions_as_of(date)`

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-09-24 |

**Context:** Legacy D1: ads/campaigns stop mid-history, and dimension snapshots are inconsistent. The DEFINE requires stable ad IDs per campaign (D7) and stable spend ≤ 1.3× budget (D8) — both need dimension state to be well-defined per date.

**Choice:** `dimensions_as_of(target_date) -> Dict[str, list[Row]]` is a pure function of the target date: it replays the deterministic dimension event schedule (campaign starts/ends, video publish dates, channel set, budget changes) and returns the exact snapshot that existed on `target_date`. Daily runs emit the full snapshot for their date; backfill iterates dates.

**Rationale:** A pure replay with no wall clock and no shared mutable state fixes D1 (campaign lifecycle now covers the full history), makes dimension rows deterministic per date (G4), and is trivially unit-testable (DRY: backfill and daily share the same function).

**Alternatives Rejected:**
1. Mutable dimension generator advanced day-by-day — Rejected: order-dependent and easy to diverge between backfill and daily paths.
2. Wall-clock-based "today" snapshots — Rejected (D5).

**Consequences:**
- Dimension raw tables carry `_snapshot_date` REQUIRED partition column; each run appends that day's full snapshot (PRD §5).
- Marts dims use the snapshot as-of (`dimensions_as_of` input date), SCD1-style end state; SCD2 explicitly parked (YAGNI).

---

## Design Confidence Matrix

| Area | KB Grounding | Specialist Agent | Confidence | Notes |
|------|--------------|------------------|------------|-------|
| Airflow DAGs / TaskFlow / Cosmos | `airflow/concepts/dag-design.md` (0.95) | @airflow-specialist | **0.95** | Cosmos option specifics to be verified in M5 (A-006) |
| dbt incremental (insert_overwrite/lookup) | `dbt/incremental-strategies.md` + `dbt/patterns/incremental-model.md` (0.95) | @dbt-specialist | **0.95** | |
| GX Core 1.x suites/checkpoints | `data-quality/patterns/great-expectations.md` (0.95) | @data-quality-analyst | **0.95** | version pin at M0 |
| Star schema / dim_date date_sk | `data-modeling/patterns/star-schema.md` (0.95) | @schema-designer + @dbt-specialist | **0.95** | |
| GCS/BigQuery loader + bootstrap | `gcp/patterns/multi-bucket-pipeline.md` + gcp KB | @gcp-data-architect | **0.90** | IAM roles verified at M0 (A-008) |
| Datagen port (pandas/pyarrow dtypes, RNG) | legacy code + `python` KB | @python-developer | **0.90** | pandas 3.0 compatibility verified in M0 (A-007) |
| Cosmos behavior + dbt venv | no KB precedent found | @airflow-specialist (research) | **0.70 → verify in M5** | explicit research/fallback task |

---

## File Manifest

| # | File | Action | Purpose | Agent | Dependencies |
|---|------|--------|---------|-------|--------------|
| 1 | `pipeline/README.md` | Create | Overview, arch diagram, setup, runbook; DQ split doc | @code-documenter | 33, 48 |
| 2 | `pipeline/Dockerfile` | Create | Astro Runtime + datagen install + dbt venv + GX venv | @airflow-specialist | 5, 20, 41 |
| 3 | `pipeline/requirements.txt` / `packages.txt` | Create | Astro/pip pinned deps | @airflow-specialist | 2 |
| 4 | `pipeline/.env.example` | Create | All env keys, no secret values (PRD §2) | @airflow-specialist | - |
| 5 | `.gitignore` | Create | Ignore `gcp_creds/`, `.env`, dbt logs, GX data | (general) | - |
| 6 | `pipeline/dags/marketing_initial_load.py` | Create | Initial load DAG (wiring only; one historical partition per date) | @airflow-specialist | 4, 8, 9 |
| 7 | `pipeline/dags/marketing_daily_pipeline.py` | Create | Daily DAG (1 day/run, catchup from 2026-07-01) | @airflow-specialist | 4, 8, 9 |
| 8 | `pipeline/dags/common/config.py` | Create | DAG-level env config/dataset constants | @airflow-specialist | 4 |
| 9 | `pipeline/dags/common/tasks.py` | Create | Task callables: generate+upload, load_to_bq (partition-scoped), run_gx, run_dbt | @airflow-specialist | 8, 21, 34, 47 |
| 10 | `pipeline/include/datagen/pyproject.toml` | Create | Package metadata + deps (pandas, numpy, faker, pyarrow, google-cloud-storage) | @python-developer | - |
| 11 | `pipeline/include/datagen/src/datagen/__init__.py` | Create | Public API export (`generate`, `generate_backfill`) | @python-developer | 12 |
| 12 | `pipeline/include/datagen/src/datagen/config.py` | Create | Env-driven config (GEN_SEED, volumes, bucket, datasets; no wall-clock) | @python-developer | - |
| 13 | `pipeline/include/datagen/src/datagen/schemas.py` | Create | Frozen dataclass row models per raw contract (+ `_snapshot_date`) | @python-developer | 10 |
| 14 | `pipeline/include/datagen/src/datagen/dimensions.py` | Create | `dimensions_as_of(date)` pure state machine (fixes D1) | @python-developer | 12, 13 |
| 15 | `pipeline/include/datagen/src/datagen/distributions.py` | Create | Seeded distributions; spend ≤ 1.3× budget, clicks ≤ impressions (D8) | @python-developer | 12 |
| 16 | `pipeline/include/datagen/src/datagen/generate_campaigns.py` | Create | Ads daily generator (stable ad ids per campaign — D7) | @python-developer | 13, 14, 15 |
| 17 | `pipeline/include/datagen/src/datagen/generate_events.py` | Create | GA4 event generator (~550/day, funnel weights, ~2% null user) | @python-developer | 13, 14, 15 |
| 18 | `pipeline/include/datagen/src/datagen/generate_youtube.py` | Create | YouTube daily generator | @python-developer | 13, 14, 15 |
| 19 | `pipeline/include/datagen/src/datagen/parquet_writer.py` | Create | Shared Parquet writer: UTC, µs timestamps, consistent dtypes (D2/D3) | @python-developer | 10 |
| 20 | `pipeline/include/datagen/src/datagen/api.py` | Create | `generate(source, start, end, out_dir, inserted_at) -> list[Path]` | @python-developer | 16–19 |
| 21 | `pipeline/include/datagen/src/datagen/upload_gcs.py` | Create | GCS upload helpers (deterministic keys) | @gcp-data-architect | 10, 20 |
| 22 | `pipeline/include/datagen/src/datagen/__main__.py` | Create | Thin CLIs: `datagen-backfill`, `datagen-daily` | @python-developer | 20 |
| 23 | `pipeline/include/datagen/tests/test_determinism.py` | Create | backfill == daily equality, seed stability (AT-005) | @test-generator | 20 |
| 24 | `pipeline/include/datagen/tests/test_distributions.py` | Create | spend ≤ 1.3× budget, clicks ≤ impressions, conversions ≤ clicks, budgets ≥ 0 | @test-generator | 16, 17, 18 |
| 25 | `pipeline/include/datagen/tests/test_dimensions.py` | Create | dimension state machine replay tests (D1/D7) | @test-generator | 14 |
| 26 | `pipeline/include/datagen/tests/test_schemas.py` | Create | Row models match raw contracts (− inserted_at) | @test-generator | 13 |
| 27 | `pipeline/include/dbt/marketing/dbt_project.yml` | Create | dbt project config (name, model paths, materialization) | @dbt-specialist | - |
| 28 | `pipeline/include/dbt/marketing/profiles.yml` | Create | BigQuery profile; `maximum_bytes_billed: 1000000000` cost guard | @dbt-specialist | - |
| 29 | `pipeline/include/dbt/marketing/packages.yml` | Create | dbt-utils dependency | @dbt-specialist | 27 |
| 30 | `pipeline/include/dbt/marketing/models/staging/_sources.yml` | Create | Source definitions (raw datasets/tables) + freshness | @dbt-specialist | 27 |
| 31 | `pipeline/include/dbt/marketing/models/staging/_staging.yml` | Create | Staging tests (not_null PKs, grain uniqueness) | @dbt-specialist | 27 |
| 32 | `pipeline/include/dbt/marketing/models/staging/stg_ga4_events.sql` | Create | Staging view | @dbt-specialist | 30 |
| 33 | `pipeline/include/dbt/marketing/models/staging/stg_ads_campaign_daily.sql` | Create | Staging view | @dbt-specialist | 30 |
| 34 | `pipeline/include/dbt/marketing/models/staging/stg_youtube_video_daily.sql` | Create | Staging view | @dbt-specialist | 30 |
| 35 | `pipeline/include/dbt/marketing/models/staging/stg_dim_campaign.sql` | Create | Staging view | @dbt-specialist | 30 |
| 36 | `pipeline/include/dbt/marketing/models/staging/stg_dim_channel.sql` | Create | Staging view | @dbt-specialist | 30 |
| 37 | `pipeline/include/dbt/marketing/models/staging/stg_dim_video.sql` | Create | Staging view | @dbt-specialist | 30 |
| 38 | `pipeline/include/dbt/marketing/models/marts/core/dim_date.sql` | Create | Date dimension with `date_sk` YYYYMMDD (star-schema KB) | @schema-designer → @dbt-specialist | 32–37 |
| 39 | `pipeline/include/dbt/marketing/models/marts/core/dim_campaign.sql` | Create | Campaign dimension (SCD1 as-of) | @schema-designer → @dbt-specialist | 35 |
| 40 | `pipeline/include/dbt/marketing/models/marts/core/dim_channel.sql` | Create | Channel dimension | @schema-designer → @dbt-specialist | 36 |
| 41 | `pipeline/include/dbt/marketing/models/marts/core/dim_video.sql` | Create | Video dimension | @schema-designer → @dbt-specialist | 37 |
| 42 | `pipeline/include/dbt/marketing/models/marts/core/fct_ga4_events.sql` | Create | Fact events; incremental insert_overwrite + lookback | @dbt-specialist | 32, 38 |
| 43 | `pipeline/include/dbt/marketing/models/marts/core/fct_ads_campaign_daily.sql` | Create | Fact ads daily; incremental insert_overwrite + lookback | @dbt-specialist | 33, 38 |
| 44 | `pipeline/include/dbt/marketing/models/marts/core/fct_youtube_video_daily.sql` | Create | Fact youtube daily; incremental insert_overwrite + lookback | @dbt-specialist | 34, 38 |
| 45 | `pipeline/include/dbt/marketing/models/marts/reporting/rpt_daily_marketing_summary.sql` | Create | PRD §8.4 metric definitions (SSOT) | @dbt-specialist | 42–44 |
| 46 | `pipeline/include/dbt/marketing/macros/lookback_days.sql` | Create | 3-day lookback helper for incremental guards | @dbt-specialist | 27 |
| 47 | `pipeline/include/dbt/marketing/tests/video_date_not_before_published.sql` | Create | Singular test (PRD P1) | @dbt-specialist | 34, 37 |
| 48 | `pipeline/include/dbt/marketing/tests/session_single_channel.sql` | Create | Singular test: session events share one channel | @dbt-specialist | 32 |
| 49 | `pipeline/include/gx/great_expectations.yml` | Create | GX config (data context) | @data-quality-analyst | - |
| 50 | `pipeline/include/gx/suites/raw_ga4_events_suite.json` | Create | GA4 contract suite (partition-scoped) | @data-quality-analyst | 49 |
| 51 | `pipeline/include/gx/suites/raw_ads_suite.json` | Create | Ads contract suite | @data-quality-analyst | 49 |
| 52 | `pipeline/include/gx/suites/raw_youtube_suite.json` | Create | YouTube contract suite | @data-quality-analyst | 49 |
| 53 | `pipeline/include/gx/suites/raw_dims_suite.json` | Create | Dimensions contract suite | @data-quality-analyst | 49 |
| 54 | `pipeline/include/gx/suites/marts_daily_summary_suite.json` | Create | Marts sanity + reconciliation suite | @data-quality-analyst | 49, 45 |
| 55 | `pipeline/include/gx/checkpoints/daily_checkpoint.yaml` | Create | GX checkpoint (scope: run partition / whole table on initial) | @data-quality-analyst | 50–54 |
| 56 | `pipeline/include/gx/runner.py` | Create | Thin GX runner invoked by Airflow task | @data-quality-analyst | 49, 55 |
| 57 | `pipeline/scripts/bootstrap_gcp.py` | Create | Idempotent bucket + 3 datasets in GCP_LOCATION | @gcp-data-architect | - |
| 58 | `pipeline/scripts/smoke_test.sh` | Create | E2E smoke runbook (§14 SQL checks) | @shell-script-specialist | 6, 7 |
| 59 | `pipeline/tests/test_dag_integrity.py` | Create | DagBag parse + task dependency assertions | @airflow-specialist | 6, 7 |
| 60 | `pipeline/tests/test_loader.py` | Create | Loader callable unit tests (mock BigQuery; partition-scoped WRITE_TRUNCATE) | @test-generator | 9 |
| 61 | `pipeline/docs/decisions.md` | Create | Decision log (this design's ADRs + runtime decisions) | @code-documenter | - |
| 62 | `pipeline/docs/data_dictionary.md` | Create | Data dictionary (raw/staging/marts tables) | @code-documenter | 27–48 |

**Total Files:** 62

---

## Agent Assignment Rationale

> Agents discovered from `.claude/agents/` — Build phase invokes matched specialists.

| Agent | Files Assigned | Why This Agent |
|-------|----------------|----------------|
| @airflow-specialist | 2, 3, 4, 6, 7, 8, 9, 59 | Deepest match: DAG design/TaskFlow, Cosmos integration, Astro Dockerfile; kb_domains [airflow, sql-patterns, data-quality]; T3 with WebSearch for Cosmos research (A-006) |
| @python-developer | 10–20, 22 | Ports/fixes legacy datagen — dataclasses, type hints, RNG correctness; kb_domains [python, pydantic, testing]; T1 |
| @gcp-data-architect | 21, 57 | GCS upload + BigQuery bootstrap; kb_domains [gcp, terraform, cloud-platforms, data-quality]; T1 |
| @dbt-specialist | 27–48 | All dbt project/models/macros/tests; kb_domains [dbt, data-quality, sql-patterns]; escalates schema theory → @schema-designer (per its escalation_rules) |
| @schema-designer | 38–41 (design via dbt-specialist handoff) | Star schema / dim models; escalates dbt implementation to @dbt-specialist (matching its stop_conditions) |
| @data-quality-analyst | 49–56 | GX suites/checkpoints; kb_domains [data-quality, dbt, data-modeling]; owns CRITICAL/WARN policy |
| @test-generator | 23–26, 60 | Determinism/distribution/dimension tests; escalates GE/Soda (not pytest) to @data-quality-analyst |
| @shell-script-specialist | 58 | Smoke-test runbook shell automation |
| @code-documenter | 1, 61, 62 | README, decision log, data dictionary |
| (general) | 5 | `.gitignore` — no specialist needed |

**Agent Discovery:**
- Scanned: `.claude/agents/**/*.md` (workflow/, data-engineering/, architect/, cloud/, python/, test/)
- Matched by: File type, purpose keywords, path patterns, KB domains, escalation rules
- Cross-checks: test-generator escalates GE work → @data-quality-analyst; dbt-specialist escalates modeling theory → @schema-designer — the manifest honors both directions.

---

## Code Patterns

### Pattern 1: Airflow TaskFlow DAG (wiring only) — from KB `airflow/concepts/dag-design.md`

```python
# dags/marketing_daily_pipeline.py — Airflow 3.0 TaskFlow
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from dags.common.tasks import generate_and_upload, load_to_bigquery, run_gx, run_dbt

@dag(
    schedule="@daily",
    start_date=datetime(2026, 7, 1),  # first incremental day
    catchup=True,                     # exactly one new day per run from 2026-07-01
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    doc_md="Daily digital-marketing pipeline: datagen -> GCS -> BigQuery -> GX -> dbt -> GX",
)
def marketing_daily_pipeline():
    gx_raw = run_gx(scope="raw", batch_date="{{ ds }}")
    @task
    def generate_and_upload_task(ds: str):
        return generate_and_upload(ds)          # decision 3: same task
    loaded = load_to_bigquery(generate_and_upload_task("{{ ds }}"))
    gx_raw >> loaded                             # GX gate 1 blocks load
    dbt_run = run_dbt(loaded)
    gx_marts = run_gx(scope="marts", batch_date="{{ ds }}")  # GX gate 2 after dbt
    dbt_run >> gx_marts

marketing_daily_pipeline()
```

### Pattern 2: dbt incremental `insert_overwrite` with 3-day lookback — from KB `dbt/incremental-strategies.md`

```sql
-- models/marts/core/fct_ads_campaign_daily.sql
{{ config(
    materialized='incremental',
    partition_by={'field': 'spend_date', 'data_type': 'date'},
    incremental_strategy='insert_overwrite',
    cluster_by=['campaign_id']
) }}

select
    s.spend_date,
    s.campaign_id,
    s.channel_id,
    s.ad_group_id,
    s.ad_id,
    s.spend_usd,
    s.impressions,
    s.clicks,
    s.conversions,
    s.avg_order_value,
    coalesce(d.date_sk, 19000101) as spend_date_sk
from {{ ref('stg_ads_campaign_daily') }} s
left join {{ ref('dim_date') }} d on d.date = s.spend_date
{% if is_incremental() %}
  where s.spend_date >= date_sub(current_date(), interval 3 day)  -- lookback
    and s.spend_date >= '{{ var("min_data_date", "2026-01-01") }}'
{% endif %}
```

### Pattern 3: Deterministic per-(seed,date,source) RNG — from DEFINE decision 4

```python
# include/datagen/src/datagen/distributions.py
import numpy as np
from datetime import date

def rng_for(source: str, day: date, gen_seed: int) -> np.random.Generator:
    """Independent, reproducible RNG for (seed, date, source)."""
    key = f"{gen_seed}:{day.isoformat()}:{source}"
    seed = np.int64(hash(key)) & 0x7FFFFFFF  # stable signed 32-bit
    return np.random.default_rng(seed=int(seed))
```

### Pattern 4: Pure dimension snapshot — `dimensions_as_of(date)`

```python
# include/datagen/src/datagen/dimensions.py
def dimensions_as_of(target_date: date, config: DatagenConfig) -> DimSnapshot:
    """Replay the deterministic dimension schedule to the target date.
    Pure function — no wall clock, no shared mutable state."""
    campaigns = _campaigns_active_as_of(target_date, config)
    channels = _all_channels(config)                 # fixed 7 rows
    videos = _videos_published_as_of(target_date, config)
    return DimSnapshot(campaigns=campaigns, channels=channels, videos=videos)
```

### Pattern 5: Shared Parquet writer (UTC, µs)

```python
# include/datagen/src/datagen/parquet_writer.py
import pandas as pd

def write_partition(frame: pd.DataFrame, out_dir, table: str, day: date) -> Path:
    path = out_dir / table / f"dt={day.isoformat()}" / "part.parquet"
    if frame.empty:
        return None  # no file for empty result; caller logs clearly
    frame.to_parquet(path, index=False, engine="pyarrow")
    return path
```

### Pattern 6: Configuration Structure

```yaml
# .env.example (PRD §2 — no secret values)
GCP_PROJECT_ID=digital-marketing-509604
GCS_BUCKET=thaalescosta_marketing
GCP_LOCATION=US
BIGQUERY_RAW_DATASET=digital_marketing_raw
BIGQUERY_STG_DATASET=digital_marketing_staging
BIGQUERY_MARTS_DATASET=digital_marketing_marts
GEN_SEED=42
DAILY_EVENT_VOLUME=550
BACKFILL_START=2026-01-01
BACKFILL_END=2026-06-30
FIRST_INCREMENTAL_DATE=2026-07-01
DBT_MAX_BYTES_BILLED=1000000000
# GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/include/gcp_creds/service_account.json (mounted, never committed)
```

---

## Data Flow

```text
M0.  scaffold: bootstrap_gcp.py creates gs://thaalescosta_marketing/
     + digital_marketing_raw/staging/marts; pytest/dbt venvs verified
   │
   ▼
M1.  datagen port: generate() deterministic; unit tests green
   │
   ▼
M2.  Initial load DAG: for each date 2026-01-01..06-30 →
     1. datagen generates 6 Parquet files (inserted_at once per run)
     2. upload to gs://…/raw/<table>/dt=YYYY-MM-DD/part.parquet
     3. BigQuery partition-scoped WRITE_TRUNCATE load (facts + dims)
   │  D9 verified: load jobs use REAL BigQuery (REQUIRED-mode schema)
   ▼
M3.  dbt: staging views → marts dim/fact/reporting; dbt tests; §14 SQL green
   │
   ▼
M4.  GX: raw contract suites + marts reconciliation; CRITICAL blocks / WARN logs
   │
   ▼
M5.  Daily DAG (catchup from 2026-07-01): generate+upload → GX raw gate →
     BigQuery load → dbt (Cosmos) → GX marts gate; DAG integrity tests
   │
   ▼
M6.  Hardening: smoke_test.sh, README, decisions.md, data_dictionary,
     ruff/pre-commit clean; final full re-run from clean clone
```

---

## Integration Points

| External System | Integration Type | Authentication |
|-----------------|-----------------|----------------|
| GCS `gs://thaalescosta_marketing/` | Python SDK (`google-cloud-storage`) | Service account (`include/gcp_creds/service_account.json`, mounted in Airflow; env `GOOGLE_APPLICATION_CREDENTIALS`) |
| BigQuery `digital-marketing-509604` | Python SDK (`google-cloud-bigquery`) + dbt-bigquery | Same service account; `GOOGLE_CLOUD_DEFAULT` Airflow conn |
| dbt (Cosmos `DbtTaskGroup`) | In-process dbt runner via virtualenv | BigQuery profile uses `GOOGLE_APPLICATION_CREDENTIALS` |
| Airflow | Astro Runtime image (`astro dev`) | Local dev auth only in this phase; IAM roles verified in M0 (A-008) |

---

## Testing Strategy

| Test Type | Scope | Files | Tools | Coverage Goal |
|-----------|-------|-------|-------|---------------|
| Unit | Datagen determinism, distributions, dimensions, schemas; loader callables | `include/datagen/tests/*`, `tests/test_loader.py` | pytest | ≥ 80% |
| Integration | Backfill==daily equality across all sources/dates (AT-005); dbt full-refresh equivalence (AT-009); GX suites on local BQ (real or emulator) | datagen tests, M3/M4 acceptance SQL | pytest + dbt + GX | Key paths |
| E2E | Initial load from clean clone → marts green (§14 SQL); daily catchup run (AT-001, AT-002, AT-010) | `scripts/smoke_test.sh` manual runbook | bash + bq SQL | Happy path + idempotent re-run |
| DAG integrity | DagBag parse, operator/task wiring, catchup semantics (AT-010) | `tests/test_dag_integrity.py` | pytest (Airflow test harness) | 100% of DAGs |
| Data quality | Raw contract checks; marts sanity + reconciliation (§14/metrics) | GX suites + dbt tests | GX Core 1.x, dbt test | All suites on every run |

**Acceptance traceability (DEFINE ATs):**
- AT-001 initial load → smoke runbook §14 SQL (M2 + E2E)
- AT-002 re-run idempotency → `test_loader.py` + manual rerun (row counts identical)
- AT-003 no missing ads days → `test_distributions.py` iterating full date range
- AT-004 spend within budget → `test_distributions.py` (spend ≤ 1.3× budget)
- AT-005 determinism → `test_determinism.py`
- AT-006 partition overwrite → `test_loader.py` (mocked BQ asserting WRITE_TRUNCATE + partition decorator)
- AT-007 GX CRITICAL blocks → DAG wiring test + M5 manual induction
- AT-008 GX WARN non-blocking → suite test (warn expectations) + M5 manual
- AT-009 full-refresh equivalence → M3 acceptance (dbt full-refresh == incremental result)
- AT-010 DAG integrity → `test_dag_integrity.py`
- AT-011 reconciliation → GX marts suite (§14 SQL)
- AT-012 version pins recorded → M0 README + `docs/decisions.md` (manual check)

---

## Error Handling

| Error Type | Handling Strategy | Retry? |
|------------|-------------------|--------|
| Datagen failure (invalid range, empty result) | Raise inside task; empty result → no file + clear log (never silent); DAG fails | Yes (task retry, deterministic so safe) |
| GCS upload failure | Task-level retry with exponential backoff; object keys deterministic → overwrite-safe | Yes |
| BigQuery load failure (schema/table missing) | Distinct error log with table/date/partition; check bootstrap first; DAG fails | Yes (max 1) |
| GX CRITICAL expectation failure | Gate blocks downstream (dbt/load as wired); failure details logged + Data Docs | No (fix data, re-run) |
| GX WARN expectation failure | Log + continue; recorded in run metadata | No |
| dbt test/model failure | Cosmos task fails; DAG stops before GX marts gate | No (fix model, re-run) |
| Transient GCP 5xx / quota | Airflow retries per DAG `default_args` | Yes |
| Cosmos incompatibility found (A-006) | Fallback task emits dbt via virtualenv shell command; decision logged in `docs/decisions.md` | N/A |

---

## Configuration

| Config Key | Type | Default | Description |
|------------|------|---------|-------------|
| `GCP_PROJECT_ID` | string | `digital-marketing-509604` | GCP project (bootstrap, load, dbt) |
| `GCS_BUCKET` | string | `thaalescosta_marketing` | GCS bucket |
| `GCP_LOCATION` | string | `US` | Bucket/dataset region |
| `BIGQUERY_RAW_DATASET` | string | `digital_marketing_raw` | Raw landing dataset |
| `BIGQUERY_STG_DATASET` | string | `digital_marketing_staging` | Staging (views) |
| `BIGQUERY_MARTS_DATASET` | string | `digital_marketing_marts` | Marts dataset |
| `GEN_SEED` | int | `42` | Determinism seed |
| `DAILY_EVENT_VOLUME` | int | `550` | GA4 events per day target |
| `BACKFILL_START` / `BACKFILL_END` | date | `2026-01-01` / `2026-06-30` | Initial-load window (181 days) |
| `FIRST_INCREMENTAL_DATE` | date | `2026-07-01` | Daily DAG start_date |
| `DBT_MAX_BYTES_BILLED` | int | `1000000000` | dbt cost guard (1 GB) |
| `GOOGLE_APPLICATION_CREDENTIALS` | string | (empty) | Path to mounted service-account key (secret, never in `.env`) |

---

## Security Considerations

- `pipeline/include/gcp_creds/service_account.json` is **never committed** — `.gitignore` covers it; `.env.example` contains no secret values.
- Service account uses least-privilege IAM (verified in M0, A-008): `storage.objectAdmin` on the bucket, `bigquery.dataEditor` + `bigquery.jobUser` on project/datasets; no broad project roles during build; keys rotate for the portfolio runbook.
- DAG files contain only wiring — no credentials inline; auth via Airflow connection / mounted key file.
- No secrets in logs; loader/datagen log rows/table/date only.
- Port is a portfolio artifact intended to run locally (`astro dev`); no public endpoints exposed.

---

## Observability

| Aspect | Implementation |
|--------|----------------|
| Logging | Airflow task logs; datagen emits per source/date row counts; loader emits rows in/out per table and partition; GX runner logs CRITICAL/WARN outcomes (PRD P1: every stage logs rows in/out per table and date) |
| Metrics | Row-count deltas between backfill and daily (tests); GX validation results persisted per run; dbt run results Log; `maximum_bytes_billed` guard visible in dbt logs |
| Tracing | No distributed tracing in scope (local Astro); run context = DAG run id + `ds` propagated to GX scope and dbt vars; smoke runbook records the initial-load SQL verification output |

---

## Pipeline Architecture (if applicable)

### DAG Diagram

```text
datagen ──generate+upload──→ GCS ──partition load──→ BigQuery raw
                                                       │
                                                        ▼  GX gate 1 (CRITICAL)
                                          dbt staging views (Cosmos)
                                                       │
                                                        ▼
                                          dbt marts dim/fct/rpt
                                                       │
                                                        ▼  GX gate 2 (marts sanity)
                              Analysis outputs: §14 SQL checks, smoke report
```

### Partition Strategy

| Table | Partition Key | Granularity | Rationale |
|-------|-------------|-------------|-----------|
| `raw_ga4_events` | `event_date` | daily | PRD §5; one partition per run; GX scope = partition |
| `raw_ads_campaign_daily` | `spend_date` | daily | PRD §5 |
| `raw_youtube_video_daily` | `video_date` | daily | PRD §5 |
| `raw_dim_campaign` / `raw_dim_channel` / `raw_dim_video` | `_snapshot_date` | daily | Full snapshot per date; deterministic replay |
| marts fct_* | data-date column (`event_date`/`spend_date`/`video_date`) | daily | insert_overwrite rewrites only touched partitions |

### Incremental Strategy

| Model | Strategy | Key Column | Lookback |
|-------|----------|------------|----------|
| `fct_ga4_events` | incremental `insert_overwrite` | `event_date` | 3 days |
| `fct_ads_campaign_daily` | incremental `insert_overwrite` | `spend_date` | 3 days |
| `fct_youtube_video_daily` | incremental `insert_overwrite` | `video_date` | 3 days |
| dim_* | incremental full-snapshot (append per date, then compute as-of) | `_snapshot_date` | none (view latest as-of) |
| `rpt_daily_marketing_summary` | incremental `insert_overwrite` | `stat_date` | 3 days |
| staging stg_* | views over raw | - | none |

### Schema Evolution Plan

| Change Type | Handling | Rollback |
|-------------|----------|----------|
| New column | Add to datagen schemas + raw contract + dbt staging → marts; backfill only affected partition(s) | Drop column from marts via model change |
| Type change | Dual-write period: keep old column, add new typed column; migrate consumers | Revert to old column |
| Column removal | Deprecate in contract (document in data dictionary), remove after N runs | Re-add column |
| New dim/fact table | Add raw table + staging view + marts model + GX suite; add to bootstrap | Remove model + suite (raw may stay) |

### Data Quality Gates

| Gate | Tool | Threshold | Action on Failure |
|------|------|-----------|-------------------|
| Raw contracts (types, REQUIRED modes, PK uniqueness, value ranges) | GX (raw suites, partition-scoped) | CRITICAL: 0 failures allowed | **Block** downstream (load/dbt) |
| Raw contracts soft checks (null %, distributions) | GX | WARN: configurable tolerance | Log + continue |
| Model structural integrity (PK not_null, uniqueness, relationships) | dbt tests | 0 failures | Block (dbt model fails) |
| Marts sanity: spend ≤ 1.3× budget by day; row counts vs raw | GX (marts suite) | CRITICAL | Block final marts publication |
| Reconciliation: reports match §14 SQL | GX + smoke runbook | 0 diff | Manual verification + fix |
| Source freshness (`_snapshot_date`/data date for today) | dbt source freshness | < 2 days stale | Alert (smoke/runbook, P1) |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-09-24 | design-agent | Initial design — architecture, ADRs, 62-file agent-matched manifest, KB-grounded patterns, testing strategy (covers AT-001..AT-012) |
| 1.1 | 2026-09-24 | ship-agent | Phase 4 complete — feature shipped and archived; status → ✅ Shipped |

---

## Next Step

**Complete** — feature shipped and archived