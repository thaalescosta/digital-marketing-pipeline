# Decision Log

> Short entries: context, decision, why. Per PRD §0-5. Pending-verification items marked with `[PENDING M#]`.

---

## 1. Dataset base name: `digital_marketing` (not `digital_marketin`)

**Context:** PRD §2 fixed identifiers listed `digital_marketin` (likely a typo for `digital_marketing`).

**Decision:** Use `digital_marketing` as the base for all three datasets: `digital_marketing_raw`, `digital_marketing_staging`, `digital_marketing_marts`.

**Why:** Corrects the typo; consistent with DEFINE Phase 1 resolution (Q1).

---

## 2. Single repo layout: framework + `pipeline/`

**Context:** BRAINSTORM Q2 — where should the project live relative to the AgentSpec framework?

**Decision:** One git repository at workspace root; AgentSpec framework (`.claude/`, `.opencode/`, `tools/`) at root + `pipeline/` subfolder with the implementation (PRD §12 structure). `git init` at root.

**Why:** Keeps SDD artifacts (DEFINE/DESIGN/BRAINSTORM) beside the code they specify; one commit per milestone; framework cleanly separable if the pipeline is later extracted.

---

## 3. GX Core 1.x in a dedicated virtualenv

**Context:** GX Core 0.18.x is retired; GX 1.x changed expectations/checkpoint APIs. Airflow images are pinned to Astro Runtime versions.

**Decision:** GX Core 1.x installed in its own Python venv inside the Astro image; `include/gx/` holds version-controlled suites, checkpoints, and `runner.py` invoked as a DAG task.

**Why:** KB `data-quality/patterns/great-expectations.md` (confidence 0.95): GX Core 1.3+ is the maintained line; 0.18 is retired. A dedicated venv isolates GX's dependency conflicts from datagen and dbt. Version pinning verified at M0 (A-005). `[PENDING M4: verify exact 1.x API shape]`

---

## 4. dbt incremental: `insert_overwrite` + 3-day lookback

**Context:** dbt incremental on BigQuery defaults to `merge` on a unique key; for daily-partitioned fact tables it is slower and more complex.

**Decision:** Marts fact models use `materialized='incremental'` with `partition_by` on the data date and `incremental_strategy='insert_overwrite'`, filtering to the run date plus a 3-day lookback via an `is_incremental()` guard. Staging models are views over raw. No `unique_key` on incremental facts.

**Why:** KB `dbt/incremental-strategies.md` (0.95) — `insert_overwrite` is the recommended BigQuery pattern: each run rewrites only the touched partitions, so re-runs are exact and no merge state is needed. The 3-day lookback (KB `dbt/patterns/incremental-model.md`) absorbs late-arriving/backfilled rows without full scans.

---

## 5. Generate + upload in the same Airflow task

**Context:** BRAINSTORM Q4 — separate generate and upload tasks add orchestration surface without value at this scale (6 files/day).

**Decision:** One task per daily run performs `datagen.generate(...)` then uploads the returned Parquet files to GCS. The task callable lives in `dags/common/tasks.py`; `inserted_at` is computed once per run and shared across sources.

**Why:** Single task removes a state handoff, makes the generate→upload step atomic from Airflow's perspective, and keeps DAG files wiring-only per PRD §7.1. Retry re-runs the whole generate+upload deterministically (idempotent by design).

---

## 6. Deterministic RNG per `(seed, date, source)` with injected `inserted_at`

**Context:** DEFINE G4/M1 — same (seed, date, source) must yield identical rows (excluding `inserted_at`); daily generation for date X must equal the backfill's rows for X (PRD 6.1.1, fixes D5 wall-clock dependence).

**Decision:** Each generator derives an independent RNG from `seed = GEN_SEED` combined with the date and source via a stable hash: `np.random.default_rng(seed=int(sha256(f"{GEN_SEED}:{date.isoformat()}:{source}").hexdigest(), 16) & 0x7FFFFFFF)`. `inserted_at` is computed once per DAG run and passed as a parameter — never read from wall clock inside the library.

**Why:** Independence per (date, source) guarantees backfill X == daily X (same seed derivation path) and cross-source independence (adding GA4 volume never shifts ads rows). Injected `inserted_at` satisfies "once per run" (BRAINSTORM Q5) and keeps the library pure and testable.

---

## 7. GCS layout: flat `raw/<gcs_table>/dt=YYYY-MM-DD/part.parquet`

**Context:** PRD §4 shows nested `raw/<source>/<table>/` layout; DESIGN diagram showed flat `raw/<table>/`. Airflow-specialist implemented flat with `gcs_table` names (`ga4_events`, `ads_campaign_daily`, `youtube_video_daily`, `dim_campaign`, `dim_channel`, `dim_video`) as both local output directories and GCS key segments.

**Decision:** Flat layout; the loader's `TABLE_BY_GCS_DIR` maps `gcs_table` → BigQuery table name (`raw_ga4_events` etc.). Reversing to nested is a one-line change in `gcs_prefix` per table.

**Why:** Keeps load URIs derivable from the table contract alone; simpler wildcard for initial load. Documented in `dags/common/config.py` lines 7–14.

---

## 8. D9: REQUIRED-mode raw schemas vs nullable Parquet — deferred

**Context:** Legacy `RAW_SCHEMAS` uses `REQUIRED` mode, but pandas writes nullable Parquet columns. BigQuery may reject the load.

**Decision:** Keep `REQUIRED` in `RAW_SCHEMAS` for now; verify against real BigQuery in M2. If rejected: write Parquet with explicit PyArrow schema (non-nullable) OR relax to `NULLABLE` and enforce non-null via GX + dbt.

**Why:** Per PRD §3.2 D9 — "Record the decision." `[PENDING M2: real BigQuery load test]`

---

## 9. Cosmos: `ExecutionMode.LOCAL` + `dbt_executable_path` with VIRTUALENV fallback

**Context:** Cosmos supports multiple execution modes; Astro Runtime has constraints; dbt and GX each need isolated venvs.

**Decision:** Primary mode: `ExecutionMode.LOCAL` with `dbt_executable_path=/usr/local/airflow/dbt_venv/bin/dbt` (simplest on Astro where only dbt is venv'd). Fallback: `ExecutionMode.VIRTUALENV` pointing at the same venv, or the shell `run_dbt` helper in tasks.py (M3 standalone + last resort). `TestBehavior.AFTER_ALL`, `LoadMode.DBT_LS` (→ `DBT_MANIFEST` if parse time degrades).

**Why:** Avoids virtualenv overhead per model task while keeping dbt dependency-isolated. `[PENDING M5: verify profile_args keys vs cosmos 1.15.1's BigQuery mapping; TriggerRule.value StrEnum; GA4 object pre-partitioning]`

---

## 10. dbt schema resolution: explicit dataset env vars via `+schema`

**Context:** PRD §2 says "set the profile `dataset: digital_marketin` and use `+schema: staging` / `+schema: marts` — default `generate_schema_name` then yields exactly the names above." With the typo fixed, `dataset: digital_marketing` + `+schema: staging` should yield `digital_marketing_staging` via the default macro.

**Decision:** In `dbt_project.yml`, set `+schema: "{{ env_var('BQ_STAGING_DATASET', 'digital_marketing_staging') }}"` and `+schema: "{{ env_var('BQ_MARTS_DATASET', 'digital_marketing_marts') }}"` (full dataset names, env-driven). The default macro returns the `+schema` value verbatim, producing the correct dataset names.

**Why:** The default `generate_schema_name` macro does NOT produce `<base>_<schema>` — it returns `+schema` verbatim. Explicit env vars ensure correctness and standalone runnability (PRD §7.4/§8.1). `[PENDING M3: standalone dbt run verification]`

---

## 11. No wall-clock in dbt models: `run_date` var only

**Context:** PRD §2/§5/D5 forbid wall-clock dependence for this simulated-dates dataset; `run_date` is the project's "today" passed by the DAG.

**Decision:** Incremental models use `run_date` var (default from DAG `{{ ds }}`); when not provided, fall back to `coalesce((select max(partition_col) from {{ this }}), dim_date_start)` so a first incremental run on an empty table still backfills. Never use `current_date()`.

**Why:** Keeps the simulation deterministic and decoupled from real time.

---

## 12. Initial-load dims: full 181-day wildcard + WRITE_TRUNCATE

**Context:** PRD §6.3 says "Initial load writes one dimension snapshot as of `2026-06-30`" but the initial load DAG processes all 181 days.

**Decision:** The initial-load DAG uploads all 181 dimension snapshots and loads with wildcard + `WRITE_TRUNCATE` on the whole table (full reset). The dimension GX suites use composite `(key, _snapshot_date)` uniqueness so both partition and full scopes pass. The "one snapshot" phrasing refers to the fact that the daily DAG from 2026-07-01 onward adds one snapshot per day.

**Why:** Matches the actual DAG implementation; GX dims suites cover both scopes via the same expectation definitions. `[PENDING M4: GX dims suite verification in both scopes]`

---

## 13. `rpt_campaign_performance_daily` no zero-fill; `rpt_channel_performance_daily` zero-fills

**Context:** Reporting models grain: date × campaign vs date × channel.

**Decision:** Channel report zero-fills (left join `dim_date` × `dim_channel`, coalesce metrics to 0). Campaign report keeps only active-spend rows (no scaffold) to avoid fabricating rows for campaigns outside their start/end window.

**Why:** Campaign lifecycle means some dates have zero campaigns; zero-filling would create misleading rows. Channel dimension is stable (7 fixed rows) so zero-fill is safe and expected.

---

## 14. `channel_group` in `dim_channel` (not `group`)

**Context:** `group` is a BigQuery reserved word.

**Decision:** Expose `channel_group` (mapping to PRD's "group"); document in column description.

**Why:** Avoids SQL quoting/escaping issues; consistent with BigQuery best practice.

---

## 15. Funnel rates: % of sessions, NOT step-to-step

**Context:** PRD §3.3 data note: funnel events are generated independently (no enforced order), so a session can have `begin_checkout` without `add_to_cart`.

**Decision:** Funnel metrics expressed as "% of sessions with event X" (denominator = sessions), never as step-to-step rates.

**Why:** Matches the data generation semantics; prevents misinterpretation.