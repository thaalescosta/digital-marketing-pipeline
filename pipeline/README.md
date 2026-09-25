# Digital Marketing ELT Pipeline

> A deterministic, production-shaped ELT portfolio project: simulated GA4 / Google Ads / YouTube data flows from Parquet on GCS into BigQuery, through dbt staging and star-schema marts, gated by Great Expectations at both ends — all orchestrated by Airflow (Astro) with Astronomer Cosmos.

## What & Why

This is a self-directed study / portfolio project that demonstrates the architecture of a real-world digital-marketing data pipeline without needing real ad-platform credentials. Real sources would be Google Analytics 4, Google Ads, and YouTube Analytics; here the same architecture is exercised against a **deterministic fake dataset** (`datagen`), so every run is reproducible and interview-ready.

The pipeline simulates a production lifecycle: a one-time **initial load** of 181 days of history (`2026-01-01` … `2026-06-30`), followed by an incremental daily pipeline that adds **one new data day per Airflow run** starting `2026-07-01`. Each day's data is generated locally, uploaded to GCS, loaded into partitioned BigQuery raw tables, validated by Great Expectations (CRITICAL blocks the run, WARN logs), transformed by dbt (staging views → marts), and validated again before landing in analysis-ready marts. Idempotency and determinism are first-class: re-running any day never duplicates rows.

Built with Airflow 3 (Astro Runtime), Astronomer Cosmos, dbt on BigQuery, Great Expectations Core 1.x, GCS, and BigQuery — full stack pins in [Stack & versions](#stack--versions).

---

## Architecture

```text
┌────────────────────────────────────────────────────────────────────────────┐
│                DIGITAL MARKETING PIPELINE (Astro + Cosmos)                  │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  Airflow (Astronomer local, astro dev)                                     │
│  ┌───────────────────────────────┐   ┌────────────────────────────────┐    │
│  │  marketing_initial_load DAG   │   │  marketing_daily_pipeline DAG   │    │
│  │  (manual trigger,             │   │  (@daily, catchup from          │    │
│  │   SIM_START_DATE..BACKFILL_END│   │   2026-07-01, 1 day per run)    │    │
│  └──────────────┬────────────────┘   └──────────────┬─────────────────┘    │
│                 │                                   │                       │
│    ┌────────────┴───────────┐           ┌───────────┴─────────────┐         │
│    │ datagen.generate_      │           │ datagen.generate +      │         │
│    │ backfill (per source)  │           │ upload — SAME task      │         │
│    └────────────┬───────────┘           └───────────┬─────────────┘         │
│                 │  Parquet (UTC, µs, deterministic)  │                      │
│                 ▼                                   ▼                       │
│  gs://thaalescosta_marketing/raw/<table>/dt=YYYY-MM-DD/part.parquet          │
│                 │                                   │                       │
│                 └───────────────┬───────────────────┘                       │
│                                 ▼                                           │
│  BigQuery digital_marketing_raw — partition-scoped WRITE_TRUNCATE loads      │
│  raw_ga4_events | raw_ads_campaign_daily | raw_youtube_video_daily           │
│  raw_dim_campaign | raw_dim_channel | raw_dim_video  (_snapshot_date)        │
│                                 │                                           │
│                                 ▼  GX gate 1 (CRITICAL blocks, WARN logs)   │
│  dbt via Cosmos — insert_overwrite + 3-day lookback                         │
│  digital_marketing_staging (views) → digital_marketing_marts                 │
│  dim_date, dim_campaign, dim_channel, dim_video                             │
│  fct_web_events, fct_web_sessions, fct_ad_performance_daily,                 │
│  fct_youtube_video_daily + 4 rpt_* reporting tables                         │
│                                 │                                           │
│                                 ▼  GX gate 2 (marts sanity + reconcile)     │
│                      Analysis-ready marts (+ data dictionary)                │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

**Data flow per day.** `datagen` writes deterministic Parquet for the run's data date → upload to GCS (`raw/<gcs_table>/dt=<date>/part.parquet`, overwrite-safe) → partition-scoped BigQuery load (`table$YYYYMMDD` + `WRITE_TRUNCATE`) → GX raw gate → dbt (staging views, incremental marts) → GX marts gate → run summary. See `docs/decisions.md` for why each step works this way.

---

## Repository structure

```
pipeline/
├── README.md                        # this file — overview, setup, runbook
├── Dockerfile                       # Astro Runtime + datagen + dbt venv + GX venv
├── requirements.txt                 # pip deps for the Airflow image
├── packages.txt                     # apt deps (libgomp1)
├── .env.example                     # env keys with defaults — NO secret values
├── dags/
│   ├── marketing_initial_load.py    # one-time 181-day backfill + full-refresh dbt
│   ├── marketing_daily_pipeline.py  # daily run: 1 new data day per DAG run
│   └── common/
│       ├── config.py                # RAW_TABLES contract, GCS layout, Cosmos config
│       └── tasks.py                 # plain task callables (generate+upload, load, gx, dbt, bootstrap)
├── include/
│   ├── PRD.md                       # product requirements document (the pipeline spec)
│   ├── datagen/                     # deterministic fake-source package (own pyproject, CLIs)
│   │   ├── pyproject.toml           # private package: datagen-backfill / datagen-daily entry points
│   │   └── src/datagen/             # api.py, config.py, dimensions.py, distributions.py,
│   │                                #   generate_{events,campaigns,youtube}.py, schemas.py,
│   │                                #   parquet_writer.py, upload_gcs.py, __main__.py
│   ├── gcp_creds/                   # service-account key file — MOUNTED, NEVER committed (.gitignore)
│   ├── gx/                          # Great Expectations Core 1.x
│   │   ├── runner.py                # python -m gx.runner --suite <name> --date <ds> --scope <partition|full>
│   │   ├── great_expectations.yml   # minimal data context
│   │   ├── checkpoints/daily_checkpoint.yaml   # run registry: suite → dataset
│   │   └── suites/*.json            # raw_ga4_events, raw_ads, raw_youtube, raw_dims, marts_daily_summary
│   └── dbt/marketing/               # dbt project
│       ├── dbt_project.yml          # name=marketing; staging views; marts tables/facts
│       ├── profiles.yml             # BigQuery profile, env-var driven, 1 GB cost guard
│       ├── packages.yml             # dbt-labs/dbt_utils
│       ├── models/staging/          # _sources.yml, _staging.yml, 6 stg_* views
│       ├── models/marts/core/       # 4 dims + 4 fct_* incremental facts
│       ├── models/marts/reporting/  # 4 rpt_* tables
│       ├── macros/                  # marketing_lookback_boundary (run_date resolution)
│       └── tests/                   # singular tests (video_date, session channel)
├── scripts/
│   ├── bootstrap_gcp.py             # idempotent bucket + 3 datasets creation
│   └── smoke_test.sh                # E2E smoke runbook (PRD §14 SQL checks)
├── tests/
│   └── test_dag_integrity.py        # DagBag parse + task-graph assertions
├── docs/
│   ├── decisions.md                 # decision log (ADR + runtime decisions)
│   └── data_dictionary.md           # data dictionary (raw / staging / marts)
├── legacy/fake-data-gen/            # untouched reference copy of the abandoned project
└── fake-data-gen/                   # duplicate copy at the pipeline root (cleanup candidate)
```

The spec lives at `include/PRD.md` (it is kept next to the code it specifies, per the single-repo decision). `legacy/fake-data-gen/` is the pre-port reference; `fake-data-gen/` at the root is a leftover duplicate — safe to delete once `include/datagen/` is your source of truth.

---

## Stack & versions

Pins are recorded here and in `docs/decisions.md` as of **2026-09-24**. **Verify current versions before relying on this project long-term** — check the official docs for Astro Runtime, `astronomer-cosmos`, `dbt-core` / `dbt-bigquery`, and Great Expectations Core, then adjust the pins (`PRD §0-3`: don't guess versions; `M5` re-verifies compatibility).

| Layer | Component | Pinned version |
|---|---|---|
| Runtime | Astro Runtime image (`astrocrpublic.azurecr.io/runtime`) | `3.2-8` (Airflow 3.2.x era; see Dockerfile comment — 3.3.x exists, GX 1.x supports Python ≤ 3.13) |
| Orchestration | `apache-airflow` (in Runtime) | Airflow 3.x |
| | `astronomer-cosmos` | `==1.15.1` |
| Web data | `pandas` | `==3.0.2` |
| | `numpy` | `>=2.1,<3` |
| | `pyarrow` | `>=17.0,<24.0` |
| GCP SDKs | `google-cloud-storage` | `>=2.16,<3.1` |
| | `google-cloud-bigquery` | `>=3.22,<4.0` |
| dbt (dedicated `dbt_venv`) | `dbt-core` | `==1.12.3` |
| | `dbt-bigquery` | `==1.12.0` |
| | `dbt-utils` (also in `packages.yml` `>=1.3.0,<2.0.0`) | `==1.4.1` |
| GX (dedicated `gx_venv`) | `great-expectations` | `==1.23.1` |
| | `sqlalchemy-bigquery` | `==1.17.2` |
| Datagen | private package, Python `>=3.12` | `datagen 0.1.0` (deps: numpy, pandas, pyarrow, gcs, bq) |

`packages.txt` installs `libgomp1` (OpenMP runtime, needed by numpy/pyarrow in the Astro image).

---

## Prerequisites

- **Docker** (Astro Runtime runs in containers).
- **Astro CLI** — https://docs.astronomer.io/astro/cli
- **Google Cloud CLI** (`gcloud`, optional but recommended; `bq` used by the smoke runbook).
- A **GCP project** with a **service-account key** file (`service_account.json`) holding at least:
  - `BigQuery Data Editor`
  - `BigQuery Data Viewer`
  - `Storage Admin`
  These are the roles the PRD assumes for the project.
- The key file must be placed at `pipeline/include/gcp_creds/service_account.json` **on your machine** — that folder is mounted into the Airflow container by `astro dev` and is **never committed** (`.gitignore` covers it, `PRD §0-4`).

---

## Setup

### 1. Environment

Copy `pipeline/.env.example` → `pipeline/.env` and adjust only what you need to change. The file contains **no secrets** — the service-account key is mounted, not env-var'd.

| Variable | Default | Purpose |
|---|---|---|
| `GCP_PROJECT_ID` | `digital-marketing-509604` | Project for bucket, datasets, loads, dbt |
| `GCP_LOCATION` | `US` | Bucket + dataset region (must match) |
| `GCS_BUCKET` | `thaalescosta_marketing` | Raw Parquet staging bucket |
| `BQ_RAW_DATASET` | `digital_marketing_raw` | Raw landing dataset (Python loader) |
| `BQ_STAGING_DATASET` | `digital_marketing_staging` | dbt staging views |
| `BQ_MARTS_DATASET` | `digital_marketing_marts` | dbt marts |
| `SIM_START_DATE` | `2026-01-01` | Simulation start (also datagen backfill start) |
| `HISTORY_MONTHS` | `6` | Backfill window (CLI default) |
| `GEN_SEED` | `42` | Determinism seed |
| `DAILY_SESSION_VOLUME` | `550` | GA4 **sessions** per day (~3.5 events each — see PRD §2 note) |
| `GOOGLE_APPLICATION_CREDENTIALS` | `/usr/local/airflow/include/gcp_creds/service_account.json` | Mounted key inside the container |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `False` | Astro core setting |
| `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` | `False` | Astro core setting (both DAGs also self-pause) |

Additional code-supported overrides (not required): `DATAGEN_OUT_DIR` (default `/tmp/datagen`), `DBT_VENV` / `GX_VENV` (defaults under `/usr/local/airflow`).

### 2. Start Airflow

```bash
cd pipeline
astro dev start
```

First boot builds the image from the `Dockerfile` (installs datagen as an editable package, creates the `dbt_venv` and `gx_venv` virtualenvs). The webserver is at http://localhost:8080 (default login `admin` / `admin`).

### 3. Airflow GCP connection

The DAGs authenticate two ways, both from the mounted key:

- **Python SDK clients** (datagen upload, BigQuery loader, bootstrap task, GX runner) use Application Default Credentials via `GOOGLE_APPLICATION_CREDENTIALS`, which the container sets to the mounted key path.
- **Cosmos → dbt** uses the `google_cloud_default` Airflow connection via `GoogleCloudServiceAccountDictionaryProfileMapping`. Add it in the Airflow UI (Connections → add, type `google_cloud_platform`) pointing at the key file, or set `AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT`. The Google provider ships in the Astro Runtime image.

### 4. Bootstrap GCP resources (idempotent)

```bash
cd pipeline
python scripts/bootstrap_gcp.py      # run twice to prove idempotency
```

Creates `gs://thaalescosta_marketing/` plus the three BigQuery datasets **only if missing** — never deletes anything. The initial-load DAG also calls bootstrap in-process, so this step is optional before Airflow but required before standalone dbt runs.

---

## How to run

The pipeline is exercised milestone-by-milestone (`PRD §13`). The end-to-end path is: **bootstrap → initial load → daily pipeline**. A smoke runbook (`scripts/smoke_test.sh`) automates parts of it and is the M6 "clone to green" companion.

### M0 — Bootstrap

```bash
cd pipeline
python scripts/bootstrap_gcp.py
```

Expect `created` (first run) then `skipped` (second run) lines — the M0 idempotency acceptance.

### M1 — Datagen (deterministic fake data), standalone

The package is installed in the image; for local experimentation create a venv:

```bash
cd pipeline/include/datagen
python -m venv .venv && . .venv/Scripts/activate   # Windows; bin/activate on Unix
pip install -e .
```

Two CLI entry points (thin wrappers around the same importable API):

```bash
datagen-backfill --start 2026-01-01 --end 2026-06-30 --out-dir /tmp/dm_backfill
datagen-daily   --date 2026-07-01 --out-dir /tmp/dm_daily
# or: python -m datagen backfill|daily ...
```

Determinism guarantee: the same `(seed, date, source)` yields identical rows (excluding `inserted_at`), so backfill(`2026-07-01`) == daily(`2026-07-01`). The M1 pytest suite (determinism, distributions, dimensions, schemas) lands under `include/datagen/tests/`.

### M3 — dbt standalone

The dbt project runs against real BigQuery with the standalone `profiles.yml` (this is the M3 acceptance path, before Airflow):

```bash
cd pipeline/include/dbt/marketing
# create a Python 3.12+ venv with the dbt pins from the Dockerfile
# (dbt-core==1.12.3, dbt-bigquery==1.12.0, dbt-utils==1.4.1)
dbt deps
export GOOGLE_APPLICATION_CREDENTIALS=<path-to>/service_account.json
export GCP_PROJECT_ID=digital-marketing-509604
export GCP_LOCATION=US
export BQ_STAGING_DATASET=digital_marketing_staging
export BQ_MARTS_DATASET=digital_marketing_marts
dbt build --profiles-dir . --vars '{"run_date":"2026-06-30","lookback_days":3}'
```

`profiles.yml` sets `maximum_bytes_billed: 1000000000` (1 GB) as a cost guardrail. The raw tables for the tested date range must already exist (run the initial-load DAG first, or load a few days manually).

### M5 — Airflow DAGs

Both DAGs are **paused on creation** by design — unpause/trigger them explicitly.

**`marketing_initial_load`** (manual trigger, safe to re-run — full reset):

```
Trigger from the Airflow UI, or:
astro dev run dags trigger marketing_initial_load
```

Graph: `bootstrap_gcp` → per-source `generate_backfill_and_upload` → `load_<table>` (wildcard `dt=*` + `WRITE_TRUNCATE`) → `gx_validate_raw` (scope=full) → `dbt_transform` (full-refresh, `run_date=2026-06-30`) → `gx_validate_marts` (scope=full). Green means the 181-day history is in BigQuery and marts are built.

**`marketing_daily_pipeline`** (`@daily`, `start_date=2026-07-01`, `catchup=True`):

```
astro dev run dags backfill marketing_daily_pipeline -s 2026-07-01 -e 2026-07-01
# or unpause from the UI and let catchup create one run per day from 2026-07-01
```

**What `ds` means here.** With `catchup=True`, each DAG run corresponds to one data interval; the run's `ds` (data interval start, UTC) is the **data date** the run generates — not the wall-clock run date. Unpausing on 2026-09-24 therefore creates one run per day from `2026-07-01` to `2026-09-23` (~85 sequential runs) — the "one more day per run" behaviour and a catchup backfill demo. A manual run that must target a specific day passes the `target_date` param (ISO `yyyy-mm-dd`), which overrides `ds` end-to-end (datagen, load URIs, dbt `run_date` var, GX partition).

Graph: `resolve_data_date` → `assert_initial_load_done` (fails fast unless the `2026-06-30` partition exists in every raw table) → per-source `generate_and_upload` → `load_<table>` (partition decorator `table$YYYYMMDD` + `WRITE_TRUNCATE`) → `gx_validate_raw` (scope=partition) → `dbt_transform` (incremental, `insert_overwrite` + 3-day lookback) → `gx_validate_marts` (scope=partition) → `log_run_summary` (`trigger_rule=all_done`).

`inserted_at` is computed **once per DAG run** (`compute_inserted_at`) and passed to every generator task — it is run metadata, never read from the wall clock inside the library (`PRD D5`).

---

## Data quality

Two complementary layers (`PRD §9.1` — the division of responsibility is a portfolio talking point):

| Layer | Question it answers | Mechanism | Failure policy |
|---|---|---|---|
| **dbt tests** | Is the **model** structurally correct? | `unique`/`not_null` on every PK, `not_null` FKs, `relationships` FK→dim, `accepted_values` enums, `dbt_utils.accepted_range` / `expression_is_true` for non-negative measures and `clicks ≤ impressions`, `conversions ≤ clicks`; singular tests (`video_date ≥ published_at`, all session events share one `channel_id`) | 0 failures — a failing model blocks the dbt task |
| **Great Expectations** | Is the **data** healthy? | Version-controlled suites in `include/gx/` run as two gates per DAG: `gx_validate_raw(ds)` after loads, `gx_validate_marts(ds)` after dbt. Scope is the run's partition (`WHERE <partition_col> = ds`) daily, whole table on initial load. Data Docs built per run under `include/gx/uncommitted/` | **CRITICAL** expectation failures → task fails → downstream (dbt / next stage) does **not** run. **WARN** failures log + appear in Data Docs, task succeeds |

Runner invocation (GX venv python): `python -m gx.runner --suite <registered-name> --date <ds> --scope partition|full` (or `--all-suites`). Registered suites — see `include/gx/checkpoints/daily_checkpoint.yaml`:

| Suite | Target | Key checks |
|---|---|---|
| `raw_ga4_events` | raw | row-count band (WARN 1200–3000), `event_id` unique/not-null, `event_date`=ds, event_name/channel_id enums, `user_pseudo_id` null ≤5% (WARN), `event_timestamp` in `[ds, ds+2d)` |
| `raw_ads` | raw | row count >0, key uniqueness, `spend_date`=ds, clicks≤impressions, conversions≤clicks, `avg_order_value > 0` |
| `raw_youtube` | raw | (video_id, video_date) unique, `video_date`=ds, likes≤views, subscribers_gained≤views |
| `raw_dims` | raw | key-per-snapshot uniqueness, type/status enums, end≥start, `raw_dim_channel` exactly 7 rows |
| `marts_daily_summary` | marts | completeness per date, `dim_date` no gaps, raw→mart reconciliation (`SUM(spend_usd)`, `SUM(clicks)`, `COUNT(DISTINCT event_id)`, `SUM(views)`, sessions), reporting sanity (ctr ∈ [0,1], no negative measures, no null keys) |

M4 must also demonstrate a **deliberately failing** case (e.g. negative `spend_usd` on a scratch table) so the blocking behaviour is exercised.

---

## Design decisions

Full decision log: [`docs/decisions.md`](docs/decisions.md) — ADRs and runtime decisions with context / decision / why. Summary of the six architecture decisions (DESIGN ADRs 1–6):

1. **`insert_overwrite` + 3-day lookback** for incremental facts — BigQuery-native, no `unique_key` merge state, exact idempotent partition rewrites.
2. **GX Core 1.x in a dedicated venv** — the 0.18 line is retired; isolating GX's pandas/pyarrow pins from datagen and dbt.
3. **Generate + upload in the same Airflow task** — removes a state handoff; retries re-run deterministically.
4. **Deterministic RNG per `(seed, date, source)`** — SHA-256-derived (not Python's salted builtin `hash`), so backfill(X) == daily(X) and sources are independent.
5. **Single repository** — AgentSpec framework at root + `pipeline/` implementation; SDD artifacts beside the code.
6. **`dimensions_as_of(date)` pure state machine** — replays the dimension event schedule, fixing the "campaigns run out" defect and keeping dimension rows deterministic per date.

Runtime decisions (GCS flat layout, dbt `+schema` resolution, `run_date` var vs `current_date()`, dims during initial load, zero-fill policy, etc.) are logged there too, with pending-verification items marked.

---

## Verification

### Smoke runbook

```bash
cd pipeline
bash scripts/smoke_test.sh
```

Five phases: (1) preflight env + GCP backend, (2) bootstrap run twice, (3) datagen determinism commands + pytest, (4) initial-load runbook (astro dev + trigger), (5) the exact PRD §14 SQL checks against BigQuery. Options: `--stop-on-first`, `--log-file`, `--skip-{bootstrap,datagen,airflow,sql}`, `--help`.

### Definition-of-done SQL (PRD §14)

After the initial load:

```sql
-- expect: 2026-01-01 | 2026-06-30 | 181
SELECT MIN(event_date), MAX(event_date), COUNT(DISTINCT event_date)
FROM `digital-marketing-509604.digital_marketing_raw.raw_ga4_events`;

-- expect: no dates missing ads (0 rows returned)
SELECT d.date FROM `digital-marketing-509604.digital_marketing_marts.dim_date` d
LEFT JOIN (SELECT DISTINCT spend_date FROM `digital-marketing-509604.digital_marketing_raw.raw_ads_campaign_daily`) a
  ON a.spend_date = d.date
WHERE d.date BETWEEN '2026-01-01' AND '2026-06-30' AND a.spend_date IS NULL;
```

After one daily run (`ds = 2026-07-01`): 182 distinct `event_date`s. Re-run the same day: identical counts (idempotency).

### Tests

```bash
cd pipeline
python -m pytest tests -q              # DAG integrity: parse + task-graph assertions
# datagen suite (M1): python -m pytest include/datagen/tests -q   (once the suite lands)
```

---

## Roadmap & status

Milestone plan from `PRD §13`; status reflects what is in the tree today. All "pending" items are real-GCP acceptance runs or areas the code marks `Mx VERIFY`:

| Milestone | Scope | Status |
|---|---|---|
| **M0** | Scaffold, `.env.example`, `.gitignore`, Astro Dockerfile, bootstrap | Built — acceptance (bootstrap twice) pending a real GCP run |
| **M1** | `datagen` port + fixes D1–D8/D10, shared writer, `dimensions_as_of`, deterministic RNG, CLIs | Package built — pytest suite (`include/datagen/tests`) still to land |
| **M2** | GCS upload + partition-scoped loads, initial-load DAG | Loader + DAG built — D9 (REQUIRED vs NULLABLE) verification on real BigQuery pending |
| **M3** | dbt project: staging views, marts, tests, docs | Built — `dbt build` on real data + full-refresh equivalence pending |
| **M4** | GX suites + runner + Data Docs | Suites/runner built — real 1.x run, negative test, reconciliation acceptance pending |
| **M5** | Both DAGs + Cosmos, DAG integrity tests | Built (`tests/test_dag_integrity.py` green target) — end-to-end runs, ≥3 consecutive catchup days, induced-critical-failure test, Cosmos pins pending |
| **M6** | README (this), `docs/decisions.md`, `docs/data_dictionary.md`, smoke runbook | Docs, smoke script in place — final "clone → green initial load" run pending |

**Future work** (`PRD §16`): dashboards (Superset/Metabase); GitHub Actions CI (ruff, pytest, `dbt parse`, DAG import); dbt snapshots (SCD Type 2) over the per-day dimension snapshots; `INJECT_DIRTY_DATA` scenarios; `pipeline_run_log` audit table; real GA4/YouTube API extractors behind the same raw contracts; Terraform for GCP resources; Astro Cloud deploy; alerting on DAG/GX failure.

---

## Security

- `pipeline/include/gcp_creds/service_account.json` is **never committed** — `.gitignore` ignores the folder; `.env.example` contains no secret values.
- DAG files contain wiring only — no credentials inline. Auth flows through `GOOGLE_APPLICATION_CREDENTIALS` (ADC) and the `google_cloud_default` Airflow connection.
- Service account uses least-privilege roles (`BigQuery Data Editor`, `BigQuery Data Viewer`, `Storage Admin`).
- Logs carry only table/date/row counts — never data payloads.