# DEFINE: Digital Marketing Pipeline

> Build a production-like, deterministic ELT pipeline for digital-marketing data on GCP — simulated GA4/Ads/YouTube sources → GCS → BigQuery → dbt marts — orchestrated by Astronomer Airflow + Cosmos with dual data-quality gates, as an interview-ready portfolio piece.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | DIGITAL_MARKETING_PIPELINE |
| **Date** | 2026-09-24 |
| **Author** | define-agent |
| **Status** | ✅ Shipped |
| **Clarity Score** | 15/15 |

---

## Problem Statement

This is a self-directed study/portfolio project: build a production-grade ELT pipeline for digital-marketing data on GCP — incremental loading, orchestration, data-quality gates, and documentation — using deterministic fake data that simulates GA4, Google Ads, and YouTube sources (no real API connectors). The current legacy generator (`fake-data-gen`) is abandoned and broken (D1–D10: ads stop mid-history, pandas 3.0 crashes, non-idempotent loads, wall-clock dependence), so the pipeline must be rebuilt and extended from it.

---

## Target Users

| User | Role | Pain Point |
|------|------|------------|
| Project owner | Learner / portfolio author studying data engineering | Needs a portfolio project that demonstrates production-grade ELT skills (Airflow/Cosmos, dbt, BigQuery, GX, docs) — not just "a script that loads CSV" |
| Hiring manager / interviewer | Evaluates the candidate's DE skills | Needs to verify incremental ELT reasoning, idempotency choices, quality-gate split, and documentation from a repo they can clone and run |
| Future consumers of marts | Analyst / BI phase (post-portfolio) | Needs analysis-ready marts with a documented data dictionary; no dashboards in this phase, but marts must be joinable and correct |

---

## Goals

What success looks like (prioritized):

| Priority | Goal |
|----------|------|
| **MUST (G1)** | Simulate a production-like incremental pipeline: 6-month history load (`2026-01-01`…`2026-06-30`, 181 days), then exactly **one new day per Airflow run** starting `2026-07-01` (catchup from that date). |
| **MUST (G2)** | Land raw Parquet in GCS, load into BigQuery raw tables (partitioned, idempotent), transform with dbt into staging views + marts, orchestrated by Airflow via Astronomer Cosmos. |
| **MUST (G3)** | Enforce data quality at two points: Great Expectations (source contracts + published-mart sanity + reconciliation) and dbt tests (model structural integrity). Document the division of responsibility. |
| **MUST (G4)** | Idempotent, reproducible, re-runnable: re-running any day/task/DAG yields identical row counts and no duplicates in raw, staging, or marts. Deterministic `(seed, date, source)` → identical data. |
| **MUST (G5)** | Documented well enough to explain in an interview: README, architecture diagram, decision log (`docs/decisions.md`), data dictionary, smoke-test runbook. A stranger can go from clone to green initial-load run using only the README. |
| **SHOULD** | Code quality: Python ≥ 3.12, type hints, `ruff` clean, `pre-commit` (PRD P1). |
| **SHOULD** | Observability: every stage logs rows in/out per table and date (PRD P1). |
| **SHOULD** | Cost guard: `maximum_bytes_billed` on dbt profile (e.g. 1 GB) and no unpartitioned full scans of `raw_ga4_events` in incremental paths (PRD P1). |
| **SHOULD** | dbt singular tests (P1): `video_date >= published_at`, session events all share one channel. |
| **COULD** | Deferred-YAGNI items that may return after all P0/P1 green (parked with reasons; see Out of Scope): cluster-by columns, `require_partition_filter`, `pipeline_run_log`, GCS-hosted Data Docs, SCD2 snapshots, `INJECT_DIRTY_DATA`, CSV export, watermark mode, CI. |

**Priority Guide:**
- **MUST** = MVP fails without this (PRD P0; D7/D8 are P1 but are de-facto MUST because M1 acceptance criteria require stable ad IDs and spend ≤ 1.3× budget)
- **SHOULD** = Important, but a workaround exists (PRD P1)
- **COULD** = Nice-to-have, cut first if needed (PRD P2 / parked)

---

## Success Criteria

Measurable outcomes (numbers from PRD §13–§14):

- [ ] **M0**: `astro dev start` boots; `bootstrap_gcp.py` runs twice without error; GCP connection test passes; `.env.example` + `.gitignore` complete; legacy reference preserved.
- [ ] **M1**: pytest green; **ads rows > 0 for every date `2026-01-01`…`2026-09-30`**; max campaign-day `spend/budget ≤ 1.3`; ad/ad-group IDs stable within a campaign; `daily(X) == backfill(X)` ignoring `_load_*`; all Parquet timestamps are `timestamp[us]` UTC; backfill for 6 months runs without error on current pandas.
- [ ] **M2** (real GCP): initial load creates all 6 raw tables partitioned as specified; `raw_ga4_events` has exactly **181 distinct `event_date`s** (`2026-01-01`…`2026-06-30`); loading `2026-07-01` **twice leaves row counts unchanged**; partition/cluster config verified via `INFORMATION_SCHEMA`.
- [ ] **M3**: `dbt build` green; second build after adding one raw day changes **only that partition**; `dbt build --full-refresh` yields same row counts as the incremental history; every model documented; `dbt docs generate` works.
- [ ] **M4**: GX suites pass on clean data; **fails** on a deliberately corrupted scratch table (e.g. negative `spend_usd`); reconciliation checks pass; warn-level expectations don't fail the task.
- [ ] **M5**: DAGs parse with no errors; `marketing_initial_load` runs green end-to-end; `marketing_daily_pipeline` completes **≥ 3 consecutive catchup days** green; Airflow graph shows **one task per dbt model**; re-running a completed day leaves all row counts unchanged; an induced GX critical failure **stops dbt** from running.
- [ ] **M6**: README (what/why, architecture, setup, runbook, design decisions, DQ split, data dictionary link), `docs/decisions.md`, smoke-test runbook; a new reader goes from clone to a green initial-load run using only the README.
- [ ] Final verification SQL (§14): `MIN(event_date)=2026-01-01`, `MAX(event_date)=2026-06-30`, `COUNT(DISTINCT event_date)=181`; no missing ad dates; after one daily run (`ds=2026-07-01`) → 182 distinct event dates; re-run same day → identical counts.
- [ ] No secrets in repo; `.env.example` contains all variables (no values for secrets).

---

## Acceptance Tests

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-001 | Initial history load | Fresh GCP project, `marketing_initial_load` DAG available | DAG runs to completion | `raw_ga4_events` has 181 distinct `event_date`s spanning `2026-01-01`…`2026-06-30`; all 6 raw tables exist with correct partitioning; all P0/P1 dbt tests green; GX raw + marts green |
| AT-002 | Idempotent daily load | `2026-07-01` already loaded | Re-run the same day's load | Row counts in raw, staging, marts unchanged; no duplicate keys |
| AT-003 | Ads never missing | Any date `2026-01-01`…`2026-09-30` | Corrected generator runs for that date | `raw_ads_campaign_daily` has rows for every date (fixes D1); §14 left-join returns 0 rows |
| AT-004 | Spend within budget | Campaign-day after D8 fix | Check `spend / budget` across whole history | Max campaign-day ≤ 1.3×; mean near budget × weekday × lifecycle |
| AT-005 | Determinism / daily==backfill | Same `(seed=42, date, source)` | Generate via daily path vs backfill path | Identical rows ignoring `_load_date`/`inserted_at` |
| AT-006 | Incremental partition overwrite | Raw has a completed partition for `D` | Reload only `D`'s object | Only partition `D` changes; other partitions untouched |
| AT-007 | GX critical failure blocks dbt | Corrupt raw partition (negative `spend_usd`) | Run daily DAG | `gx_validate_raw` task fails; dbt downstream does not run (M4/M5) |
| AT-008 | GX warn does not block | Data within null-rate/volume warn bands | Run daily DAG | Task succeeds; warning recorded in Data Docs |
| AT-009 | Full-refresh equivalence | Incremental history built M0…M3 | `dbt build --full-refresh` | Row counts equal incremental history |
| AT-010 | DAG integrity | DagBag loads the project | Import DAGs | No import errors; `catchup=True`, `max_active_runs=1`, `start_date=2026-07-01` as specified; one task per dbt model in Cosmos group |
| AT-011 | Reconciliation raw→mart | Data for `ds` in raw and marts | GX marts suite runs scoped to `ds` | `SUM(spend_usd)`/`SUM(clicks)`/`COUNT(DISTINCT event_id)`/`SUM(views)`/session count match raw; ctr ∈ [0,1]; no negative measures; no null keys |
| AT-012 | Version pin recorded | M0 complete | Check README | Astro Runtime / astronomer-cosmos / dbt-core / dbt-bigquery / GX Core versions pinned as a mutually compatible set, verified against current docs |

---

## Out of Scope

Explicitly NOT included in this feature:

- **Dashboards / BI** — marts must be analysis-ready, no dashboard layer (future phase).
- **Real GA4 / YouTube Analytics API connectors, streaming, CDC, ML** — simulated sources only.
- **Terraform / IaC, cloud deployment** — local `astro dev`; GCP resources via idempotent `bootstrap_gcp.py` create-if-missing, never deletes.
- **PII handling** — no PII generated at all.
- **Deferred (YAGNI park, with reasons in BRAINSTORM):** CSV export flag, `INJECT_DIRTY_DATA`, `pipeline_run_log` audit table, `require_partition_filter`, GCS-hosted Data Docs, cluster-by columns (P2), watermark mode, dbt SCD2 snapshots, GitHub Actions CI.

---

## Constraints

| Type | Constraint | Impact |
|------|------------|--------|
| Technical | GCP project `digital-marketing-509604`; bucket `thaalescosta_marketing`; location `US` (bucket + all datasets same location) | Design hard-coded identifiers read from env, never hard-coded in code |
| Technical | Dataset base name **`digital_marketing`** (resolved from PRD's `digital_marketin` typo) → `digital_marketing_raw` / `_staging` / `_marts`; dbt profile `dataset: digital_marketing` with `+schema: staging/marts` using default `generate_schema_name` | Corrects PRD §2/§8/§14 inconsistencies; keep base configurable via env |
| Technical | Simulation start `2026-01-01`, history 181 days, first incremental day `2026-07-01`, UTC everywhere, `GEN_SEED=42`, `DAILY_SESSION_VOLUME=550` (~3.5 events/session) | Deterministic input contract |
| Technical | Service-account auth via Airflow `google_cloud_default` conn; key file `pipeline/include/gcp_creds/service_account.json` mounted, **never committed**; least-privilege roles already set (BigQuery Data Editor/Viewer, Storage Admin) | Secrets never in git/logs |
| Technical | No wall-clock access inside the datagen library; `inserted_at` set once per DAG run and passed in | D5 fix; reproducible runs |
| Technical | All timestamps written UTC, microsecond precision (BigQuery rejects nanosecond Parquet on some pandas) | Shared Parquet writer (D2/D3 fix) |
| Technical | BigQuery loads: partition-decorator `table$YYYYMMDD` + `WRITE_TRUNCATE` daily; wildcard `dt=*/` + `WRITE_TRUNCATE` initial; `autodetect=False` explicit schema | D4 fix; idempotent loads |
| Technical | Python ≥ 3.12; packages in dedicated virtualenvs: dbt-bigquery in one, GX Core in a second (avoids Airflow conflicts) | Dockerfile requirement |
| Technical | Versions pinned after checking current docs; Cosmos specifics (venv mode, `TestBehavior.AFTER_EACH` vs `AFTER_ALL`) verified, fallback recorded | README records mutually compatible set |
| Resource | BigQuery free-tier scale (~350k events total, ~45 ad rows/day, ~13 YouTube rows/day) | No scale-forcing; partition-scoped GX keeps cost flat |
| Process | One commit per milestone; M0–M6 acceptance criteria gate progression; PRD §0 instruction #6: if reality contradicts PRD, stop, document, propose smallest change | Milestone discipline preserved |
| Timeline | Milestones sequential; GX kill-switch (narrow to raw-only if M4 drags) documented in BRAINSTORM | Scope lever if timeline slips |

---

## Technical Context

> Essential context for Design phase - prevents misplaced files and missed infrastructure needs.

| Aspect | Value | Notes |
|--------|-------|-------|
| **Deployment Location** | `pipeline/` project root: `dags/`, `include/datagen/`, `include/dbt/marketing/`, `include/gx/`, `scripts/`, `tests/`, `docs/` (per PRD §12) | Single repo: AgentSpec framework at workspace root, `pipeline/` subfolder is the project |
| **KB Domains** | `dbt`, `airflow`, `data-quality`, `gcp`, `data-modeling`, `medallion`, `python`, `testing` | Patterns to consult in Design: dbt incremental (insert_overwrite + lookback), GX Core 1.x suites, Airflow DAG design, BigQuery partitioning, star-schema marts, medallion layering |
| **IaC Impact** | None (no Terraform) — GCP resources created by idempotent `scripts/bootstrap_gcp.py` (bucket + 3 datasets in `GCP_LOCATION`); Dockerfile with Astro Runtime + 2 virtualenvs | Bootstrap never deletes; secrets mounted not committed |

**Why This Matters:**

- **Location** → Design uses PRD §12 structure so files land correctly.
- **KB Domains** → Design pulls `insert_overwrite`, GX suite patterns, DAG conventions.
- **IaC Impact** → Bootstrap script is a Design deliverable; no Terraform needed, but bucket/dataset lifecycle must be create-if-missing only.

---

## Data Contract (if applicable)

> The full raw contracts live in PRD §5 (6 tables). This section records the contract summary the Design phase must honor. Complete schema contracts are in the PRD; Design references them rather than re-deriving.

### Source Inventory
| Source | Type | Volume | Freshness | Owner |
|--------|------|--------|-----------|-------|
| GA4 events (`raw_ga4_events`) | Simulated fake, deterministic | ~1.9k events/day (~350k for 181 days) | 1 day per DAG run (`ds`) | datagen |
| Google Ads daily (`raw_ads_campaign_daily`) | Simulated fake, deterministic | ~45 rows/day (order 10⁴ total) | 1 day per DAG run (`ds`) | datagen |
| YouTube video daily (`raw_youtube_video_daily`) | Simulated fake, deterministic | ~13 rows/day | 1 day per DAG run (`ds`) | datagen |
| Dimensions (`raw_dim_campaign/channel/video`) | Simulated fake, **evolving** (`dimensions_as_of`) | snapshot per day; 7 channels fixed | 1 day per DAG run (`ds`); initial snapshot `2026-06-30` | datagen |

### Schema Contract (summary; full columns in PRD §5)
| Table | Partition | Grain | Key constraints |
|-------|-----------|-------|-----------------|
| `raw_ga4_events` | `event_date` | 1 row/event | `event_id` unique; `event_timestamp ∈ [event_date, event_date+2d)`; `event_name` ∈ 6 values; `channel_id` 1–7 |
| `raw_ads_campaign_daily` | `spend_date` | (campaign_id, ad_group_id, ad_id, spend_date) | stable ad IDs per campaign (D7); `clicks ≤ impressions`; `conversions ≤ clicks`; `spend_usd ≥ 0` |
| `raw_youtube_video_daily` | `video_date` | (video_id, video_date) | counts ≥ 0; `likes ≤ views`; `subscribers_gained ≤ views` |
| `raw_dim_campaign` | `_snapshot_date` | `campaign_id` | `status` ∈ Active/Ended derived as-of snapshot; `end_date ≥ start_date`; ≥2 active per date |
| `raw_dim_channel` | `_snapshot_date` | `channel_id` | exactly 7 fixed rows |
| `raw_dim_video` | `_snapshot_date` | `video_id` | `published_at ≤ snapshot`; ≥8 videos exist on `2026-01-01` |

All tables: `inserted_at TIMESTAMP` REQUIRED (UTC, set by DAG run); dims gain `_snapshot_date` (D10). Modes per legacy `RAW_SCHEMAS` **pending D9 resolution** (REQUIRED vs NULLABLE — verify against real BigQuery in M2; fallback: relax to NULLABLE + enforce via GX/dbt, record decision).

### Freshness SLAs
| Layer | Target | Measurement |
|-------|--------|-------------|
| Raw / Staging / Marts | One day per DAG run; initial load covers 181-day history | `ds` = run's data date; `inserted_at` once per run |
| Daily DAG | Catchup from `2026-07-01` (≈85 sequential runs on unpause 2026-09-24) | `data_interval_start` determines the day; optional `target_date` param |

### Completeness Metrics
- Ads rows > 0 for every date `2026-01-01`…`2026-09-30` (D1 fix).
- ≥ 2 campaigns active on every date ≥ `2026-01-01`; ≥ 8 videos exist on `2026-01-01`.
- `raw_dim_channel` exactly 7 rows per snapshot.
- GX expects: row-count > 0 per day; `user_pseudo_id` null share ≤ 5% (warn); `page_location` non-null ≥ 90% except `session_start` (warn); `event_value` only on `purchase`; no duplicate keys.
- Raw → mart reconciliation per `ds`: spend/clicks/events/views/sessions match exactly.

### Lineage Requirements
- `dbt docs generate` works; every model and column described (data dictionary).
- GCS object names deterministic per PRD §4 (`raw/<source>/<table>/dt=YYYY-MM-DD/part.parquet`).
- Raw → staging views → marts tracked through dbt `ref()` DAG; GX reconciliation cross-checks layers.

---

## Assumptions

Assumptions that if wrong could invalidate the design (PRD §15 defaults + brainstorm decisions):

| ID | Assumption | If Wrong, Impact | Validated? |
|----|------------|------------------|------------|
| A-001 | Bucket/datasets may or may not already exist; bootstrap creates only if missing, never deletes (PRD Q3) | Resources in unexpected location/state — bootstrap is idempotent so re-run is safe | [ ] (verify in M0) |
| A-002 | "One day per run" = the run's `ds` decides the day (catchup from `2026-07-01`); watermark mode is P2 (PRD Q4) | Different semantics would change DAG scheduling logic | [x] (Q4 answered by PRD default) |
| A-003 | Parquet is the format of record; CSV export is not needed (PRD Q5) | Would add a writer + load path | [x] (confirmed in brainstorm YAGNI) |
| A-004 | GX integrated as GX Core in its own venv invoked from Airflow tasks (PRD Q6) | Alternative integration would change Dockerfile + task architecture | [x] (design decision in brainstorm) |
| A-005 | Existing versions of Astro Runtime / astronomer-cosmos / dbt-core / dbt-bigquery / GX Core can be pinned into a mutually compatible set (PRD §0.3) | If incompatible, fallback recorded (plain Cosmos `dbt build` task) and versions adjusted in README | [ ] (verify in M0) |
| A-006 | BigQuery accepts `REQUIRED`-mode schema from pandas-writable Parquet (PRD D9) | If rejected, relax to `NULLABLE` + enforce via GX/dbt; decision recorded | [ ] (verify in M2) |
| A-007 | Current audience of SDKs supports Python ≥ 3.12 and pandas 3.x | Downgrade/upgrade pins; recorded in README | [x] (legacy verified on pandas 3.0.2) |
| A-008 | Service account already has least-privilege roles (BigQuery Data Editor/Viewer, Storage Admin) per PRD §2 | Bootstrap or manual IAM step required | [ ] (verify in M0) |
| A-009 | Deterministic seed 42 + default volume stays within BigQuery free tier | If over, reduce `DAILY_SESSION_VOLUME` or history window | [x] (PRD §3.3 volume estimate) |
| A-010 | GA4 purchases and Ads conversions are independent and will NOT reconcile (expected behavior, PRD §3.3) | Reconciliation checks must NOT assert cross-source equality | [x] (PRD data note) |
| A-011 | Sessions can cross midnight; all events of a session share `event_date`; incremental-by-`event_date` is safe (PRD §3.3) | Never assert `date(event_timestamp) = event_date`; allow `[event_date, +2d)` | [x] (PRD data note) |

**Note:** Validate critical assumptions before DESIGN phase. A-005, A-006, A-008 are blocking verifications for M0/M2; others are design inputs already confirmed.

---

## Clarity Score Breakdown

| Element | Score (0-3) | Notes |
|---------|-------------|-------|
| Problem | 3 | Crystal clear: portfolio ELT pipeline on GCP, stack fixed (Airflow+Cosmos+dbt+GX), legacy generator with verified defects, interview audience |
| Users | 3 | Three personas with concrete pains (owner, interviewer, future mart consumer) |
| Goals | 3 | G1–G5 measurable (181 days, 1 day/run, 6 tables, 2 quality gates, idempotent); MoSCoW priorities mapped to PRD P0/P1/P2 |
| Success | 3 | M0–M6 acceptance criteria with explicit numbers + §14 verification SQL + DoD checklist |
| Scope | 3 | Exhaustive non-goals from PRD §1.3 + 13 YAGNI-parked items with recorded reasons; exact dataset/table/field contracts |
| **Total** | **15/15** | Ideal pre-validated input: PRD + brainstorm answered all discovery questions |

**Scoring Guide:**
- 0 = Missing entirely
- 1 = Vague or incomplete
- 2 = Clear but missing details
- 3 = Crystal clear, actionable

**Minimum to proceed: 12/15** — gate passed at 15/15.

---

## Open Questions

None blocking — the PRD's §15 questions (Q1–Q6) were resolved as follows:

- Q1 (raw schema layout) → three datasets from base `digital_marketing`.
- Q2 (location) → `US` multi-region (default).
- Q3 (bucket/datasets exist?) → unknown; bootstrap creates only if missing (A-001).
- Q4 (one day per run) → `ds` decides the day; watermark mode parked (A-002).
- Q5 (Parquet vs CSV) → Parquet only (A-003).
- Q6 (GX mechanism) → GX Core in own venv, Airflow tasks (A-004).

Open verification items (design inputs, not blockers): A-005 (version compat), A-006 (D9 REQUIRED-mode), A-008 (IAM roles) — all verify during M0/M2 per the milestone plan.

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-09-24 | define-agent | Initial version from `BRAINSTORM_DIGITAL_MARKETING_PIPELINE.md` + `pipeline/include/PRD.md`; dataset base resolved to `digital_marketing`; YAGNI park recorded; clarity 15/15 |
| 1.1 | 2026-09-24 | define-agent | Reframed owner/user language as self-directed study/portfolio project (no specific company, employer, or "new role" implication); PRD §1.1 realigned to match |
| 1.2 | 2026-09-24 | design-agent | Phase 2 complete — `DESIGN_DIGITAL_MARKETING_PIPELINE.md` created (Status: Ready for Build); status → ✅ Complete (Designed) |
| 1.3 | 2026-09-24 | ship-agent | Phase 4 complete — feature shipped and archived; status → ✅ Shipped |

---

## Next Step

**Complete** — feature shipped and archived