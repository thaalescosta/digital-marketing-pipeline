# BRAINSTORM: Digital Marketing Pipeline

> Exploratory session to clarify intent and approach before requirements capture

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | DIGITAL_MARKETING_PIPELINE |
| **Date** | 2026-09-24 |
| **Author** | brainstorm-agent |
| **Status** | ✅ Complete (Defined) |

---

## Initial Idea

**Raw Input:** Portfolio project: incremental ELT pipeline on GCP (Astronomer/Cosmos + dbt BigQuery + Great Expectations + GCS/BigQuery), driven by a deterministic fake digital-marketing dataset. The piece is a self-directed study/portfolio project and must be explainable in a job interview. Source of truth is `pipeline/include/PRD.md` (528 lines, marked "Ready for implementation").

**Context Gathered:**
- Working directory is the AgentSpec framework repo; the portfolio project lives in the `pipeline/` subfolder (contains `fake-data-gen/` legacy generator + `include/PRD.md` + `include/gcp_creds/service_account.json`).
- Legacy generator verified against the PRD's claims: `_coerce_timestamps_to_us`, `RAW_SCHEMAS`, `DAILY_EVENT_VOLUME` all present; it is the abandoned project the PRD ports from.
- No SDD phase docs exist yet (`.claude/sdd/features/` empty). Git is not initialized.
- PRD is unusually mature: data contracts, DAG shapes, milestone acceptance criteria (M0–M6), Definition of Done, decision-log requirement, open questions Q1–Q6 with defaults already given.

**Technical Context Observed (for Define):**

| Aspect | Observation | Implication |
|--------|-------------|-------------|
| Likely Location | `pipeline/` subfolder (project root per PRD §12); datagen → `pipeline/include/datagen/`, dbt → `pipeline/include/dbt/marketing/`, DAGs → `pipeline/dags/` | Code lives inside `pipeline/`; AgentSpec framework stays at workspace root as tooling |
| Relevant KB Domains | `dbt`, `airflow`, `data-quality`, `gcp`, `data-modeling`, `medallion`, `python`, `testing` | Patterns to consult in Define/Design |
| IaC Patterns | None — no Terraform (explicit non-goal). GCP resources created by `scripts/bootstrap_gcp.py` | Bootstrap script is idempotent create-if-missing, never deletes |

---

## Discovery Questions & Answers

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | The PRD is internally inconsistent about the BigQuery dataset base name (`digital_marketin` typo vs `digital_marketing_raw`). What is the canonical base name? | **(a) `digital_marketing`** | All dataset names resolve from base `digital_marketing` → `digital_marketing_raw` / `_staging` / `_marts`. PRD Sections 2, 8, 14 corrected. Single env-driven base. Cleaner interview story. |
| 2 | Where does the final repository live relative to the AgentSpec framework files? | **(b) Everything in one repo** — keep framework + `pipeline/` in a single repo; pipeline is a subfolder | Git initialized at workspace root (per PRD §12, project "lives in the thalesscosta/digital-marketing-pipeline repository"); `pipeline/` is the project root inside it. |
| 3 | What sample data grounds the build besides generated fake data? | **(a) Legacy code is the ground truth** — its schemas, distributions, and tests are the sample corpus | No real GA4/YouTube exports. Port contracts/tests from `fake-data-gen`, not from real API samples. |
| 4 | Which execution model? | **(a) SDD milestone-driven** — full BRAINSTORM → DEFINE → DESIGN → BUILD(M0–M6) → SHIP | PRD stays anchor reference; DEFINE clarity gate catches remaining inconsistencies; M0–M6 map 1:1 onto Build tasks; phase docs serve interview goal G5. |

**Minimum Questions:** 3 ✅ (4 asked)

---

## Sample Data Inventory

> Samples improve LLM accuracy through in-context learning and few-shot prompting.

| Type | Location | Count | Notes |
|------|----------|-------|-------|
| Input files | `pipeline/fake-data-gen/src/datagen/schemas.py` | 6 raw contracts | Dataclasses = the star of the raw-table contracts (port, then update for D10) |
| Input files | `pipeline/fake-data-gen/src/datagen/config.py` | 1 | Env-driven config, channel map, event names (reuse with changes) |
| Input files | `pipeline/fake-data-gen/src/datagen/distributions.py` | 1 | weekday seasonality, campaign lifecycle, ad metrics, YouTube decay (reuse as-is) |
| Related code | `pipeline/fake-data-gen/src/datagen/*.py` | 10 modules | Backfill/daily/upload flows — port & fix D1–D10 |
| Ground truth | `pipeline/fake-data-gen/tests/` | 3 test files | Determinism, distribution, upload tests — keep and update |
| Ground truth | `pipeline/include/PRD.md` | 1 spec | Source of truth for contracts, DAG shapes, milestones, DoD |
| Output examples | N/A | — | No real GA4/YouTube sample exports; generated output defines itself |

**How samples will be used:**
- `schemas.py` dataclasses → verbatim base for `RAW_SCHEMAS` and dbt `_sources.yml` column definitions.
- `distributions.py` behaviors → preserved under new `(seed, date, source)` RNG streams so distributions stay realistic.
- Legacy tests → extended with dimension-evolution invariants, daily==backfill equivalence, timestamp precision, no-wall-clock checks.
- PRD SQL examples (§14) → final verification queries in the smoke test runbook.

---

## Approaches Explored

### Approach A: SDD milestone-driven ⭐ Recommended

**Description:** Feed the PRD through the full 5-phase SDD flow. BRAINSTORM (this doc) → DEFINE (PRD extracted into validated requirements with clarity score) → DESIGN (architecture diagram, component breakdown, agent-matched file manifest, ADRs) → BUILD (implements M0→M6 one milestone at a time, one commit each) → SHIP (archive + lessons learned).

**Pros:**
- M0–M6 acceptance criteria map 1:1 onto Build phase tasks; preserves the one-commit-per-milestone discipline.
- DEFINE's clarity gate caught the `digital_marketin`/`digital_marketing` contradiction during this session — the workflow earns its keep.
- Phase documents + `docs/decisions.md` directly serve interview goal G5.
- Native fit with this repo's component model and workflow contracts.

**Cons:**
- Front-end ceremony (DEFINE/DESIGN docs before M0). Marginal in practice since the PRD is already 90% of DEFINE.

**Why Recommended:** This is the repository's native workflow, grounded in KB patterns (`dbt`, `airflow`, `data-quality` all confidence 0.90–0.95). It converts the PRD instead of competing with it, and the milestone gates are exactly the phase discipline the workflow enforces.

---

### Approach B: PRD-as-contract, build immediately

**Description:** Skip formal DEFINE/DESIGN documents; treat the PRD as the specification contract and implement milestones directly.

**Pros:** Fastest to first commit.
**Cons:** The naming inconsistency, version-pinning risks, and D9 unknowns surface during build instead of before — the most expensive point. Loses traceability for the interview story.

---

### Approach C: Spike-first portfolio

**Description:** Build a throwaway end-to-end vertical slice (1 day of data), prove the integration, then expand into the full 181-day design.

**Pros:** Early de-risking of Cosmos/dbt/GX integration; short feedback loops.
**Cons:** Duplicate work (spike is throwaway). PRD's M0→M5 ordering already sequences risk (M2 resolves D9 against real BigQuery before DAGs exist).

---

## Data Engineering Context

### Source Systems
| Source | Type | Volume Estimate | Current Freshness |
|--------|------|-----------------|-------------------|
| GA4 events | Simulated (deterministic fake) | ~1.9k events/day (~350k for 181 days) | 1 day per DAG run |
| Google Ads (campaign daily) | Simulated | ~45 rows/day (order 10⁴ total) | 1 day per DAG run |
| YouTube (video daily) | Simulated | ~13 rows/day | 1 day per DAG run |
| Dimensions (campaign/channel/video) | Simulated, evolving | snapshot per day | 1 day per DAG run |

### Data Flow Sketch
```text
[datagen (deterministic fake, pure fn of seed/date/source)]
  → [GCS raw/.../dt=YYYY-MM-DD/part.parquet]
  → [BigQuery digital_marketing_raw (partition-scoped WRITE_TRUNCATE)]
  → [GX raw validation]
  → [dbt staging views → marts (insert_overwrite + lookback)]
  → [GX marts validation (incl. raw→mart reconciliation)]
  Orchestrated: Astronomer Airflow + Cosmos
```

### Key Data Questions Explored
| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | What's the expected data volume? | ~350k events / ~10⁴ ad rows / ~2.4k YouTube rows in history; ~1.9k events/day incremental | Fits BigQuery free tier; no scale-forcing; partition-scoped GX keeps cost flat |
| 2 | What freshness SLA is needed? | One new day per DAG run (catchup from 2026-07-01) | Batch ELT, not streaming |
| 3 | Who consumes the output? | Interviewer / hiring manager; marts ready for future dashboards | Research-grade marts with documented data dictionary; no dashboards in scope |

---

## Selected Approach

| Attribute | Value |
|-----------|-------|
| **Chosen** | Approach A — SDD milestone-driven |
| **User Confirmation** | 2026-09-24 (session) |
| **Reasoning** | Native workflow fit; M0–M6 map onto Build; DEFINE gate already proved valuable; phase docs serve the interview goal |

---

## Key Decisions Made

| # | Decision | Rationale | Alternative Rejected |
|---|----------|-----------|----------------------|
| 1 | Canonical dataset base name = `digital_marketing` (not `digital_marketin`) | Single config-driven base; fixes PRD's internal contradiction; cleaner interview story | Bug-for-bug `digital_marketin` |
| 2 | Single repo: AgentSpec framework + `pipeline/` subfolder | User choice; portfolio is a subfolder of the framework repo; git init at workspace root | `pipeline/` as its own repo root |
| 3 | Legacy `fake-data-gen` is the sample ground truth; port contracts/tests, no real API samples | Self-consistent, verified corpus; avoids second source of truth | Search for real GA4/YouTube exports |
| 4 | Full SDD execution (Approach A) | DEFINE clarity gate + phase docs = interview traceability | PRD-as-contract direct build; spike-first |
| 5 | GX Core 1.x in its own venv (external python task); dbt tests own structural integrity | Divides responsibilities per PRD §9.1; KB-confirmed GX 1.x current | GX only; dbt-tests-only; Soda |
| 6 | dbt incremental = `insert_overwrite` + 3-day lookback for facts | BigQuery-correct partition rebuild pattern (KB `dbt/incremental-strategies` 0.95) | merge/append |
| 7 | Generate+upload in same Airflow task; load from GCS only | Works on any executor; no file handoff between tasks | Generate → upload → load as separate tasks |
| 8 | `inserted_at` set once per DAG run, passed in (never at import/wall clock) | Fixes D5; deterministic runs | Module-import-time timestamp |
| 9 | Independent RNG stream per (seed, date, source); `dimensions_as_of(date)` pure state machine | Fixes D6 and D1; any day re-runnable in any order | Shared global RNG |
| 10 | YAGNI park (see below) | Keep M0–M6 tight; defer interview-nice-but-not-load-bearing items | Build all P2s now |

---

## Features Removed (YAGNI)

| Feature Suggested | Reason Removed | Can Add Later? |
|-------------------|----------------|----------------|
| Dashboards / BI | Non-goal §1.3; marts must simply be ready to be used | Yes (later phase) |
| Real GA4/YouTube API connectors | Non-goal §1.3; fake data demonstrates architecture | Yes (future work §16) |
| Terraform / cloud deployment | Non-goal §1.3; local `astro dev` is the boundary | Yes (future work §16) |
| CSV export flag (P2 §4) | Parquet is format of record; added surface, zero interview value | Yes |
| `INJECT_DIRTY_DATA` (P2 §6.1.4) | M4's negative test with corrupted scratch table gives same DQ demo | Yes (future work §16) |
| `pipeline_run_log` audit table (P2 §10) | Per-stage row logging (P1) satisfies observability | Yes (future work §16) |
| `require_partition_filter` (P2 §6.3) | Scoped GX queries + `maximum_bytes_billed` already control cost | Yes |
| GCS-hosted Data Docs (P2 §9.2) | Local Data Docs prove the mechanism | Yes |
| Cluster-by columns (P2 §5) | No performance need at 350k rows | Yes |
| Watermark mode (P2 Q4) | Catchup already demonstrates "one day per run" | Yes |
| `weekday_seasonality` scaling (P2 §6.1.4) | Distributions already include weekday seasonality per §3.1/6.1.3 | Yes |
| dbt snapshots / SCD2 on `dim_campaign` (§16) | Raw `_snapshot_date` + latest-snapshot staging already cover the demo | Yes |
| GitHub Actions CI (§16) | pre-commit + local tests suffice for portfolio; CI adds repo-ops overhead | Yes |

---

## Incremental Validations

| Section | Presented | User Feedback | Adjusted? |
|---------|-----------|---------------|-----------|
| Architecture (sources, ingestion, transformation, quality, orchestration, repo) | ✅ | "yes" — matches | No |
| Execution plan (SDD × milestones, key decisions) | ✅ | "looks right" | No |

**Minimum Validations:** 2 ✅

---

## Suggested Requirements for /define

Based on this brainstorm session, the following should be captured in the DEFINE phase:

### Problem Statement (Draft)
Build a production-like, deterministic ELT pipeline for digital marketing data on GCP — simulated GA4/Ads/YouTube sources → GCS → BigQuery → dbt marts — orchestrated by Astronomer Airflow + Cosmos, with dual data-quality gates (dbt tests + Great Expectations), as a portfolio piece an interviewer can follow end-to-end.

### Target Users (Draft)
| User | Pain Point |
|------|------------|
| Project owner (portfolio author) | Needs a portfolio that demonstrates incremental ELT, quality gates, and docs discipline; must survive interview scrutiny |
| Hiring manager / interviewer | Needs to verify DE skills: orchestration, dbt, BigQuery partitioning, idempotency, testing, documentation |
| Future consumer of marts | Analyst wanting ready-to-use marts (before dashboards phase) |

### Success Criteria (Draft)
- [ ] Milestones M0–M6 pass per PRD §13 acceptance criteria (one commit per milestone).
- [ ] Definition of Done (§14): 6 raw tables, 6 staging views, 4 dims + 4 facts + ≥3 reporting marts; `dbt build` green; GX raw + marts validated; negative test demonstrated.
- [ ] Daily DAG green for ≥3 consecutive catchup days; reruns idempotent (identical counts).
- [ ] No secrets in repo; `.env.example` complete; README lets a stranger reproduce from clone to green initial load.
- [ ] Data dictionary + `docs/decisions.md` written; ready to explain in an interview.

### Constraints Identified
- GCP: project `digital-marketing-509604`, bucket `thaalescosta_marketing`, location `US`, service-account least-privilege auth (key mounted, never committed).
- Dataset base name `digital_marketing` (+ `_raw` / `_staging` / `_marts`); UTC timestamps; `GEN_SEED=42`; simulation start `2026-01-01`, history 181 days, first incremental day `2026-07-01`.
- No wall-clock in the library; no secrets in git/logs; deterministic `(seed, date, source)`.
- Versions pinned after checking current docs (Astro Runtime, astronomer-cosmos, dbt-core/bigquery, GX Core). Cosmos specifics are the underdetermined corner (no KB precedent; verify + record + keep fallback).

### Out of Scope (Confirmed)
- Dashboards/BI, real GA4/YouTube connectors, streaming, CDC, ML, Terraform/IaC, cloud deployment beyond `astro dev`, PII.
- Deferred (YAGNI park): CSV export, `INJECT_DIRTY_DATA`, `pipeline_run_log`, `require_partition_filter`, GCS Data Docs, clustering, watermark mode, dbt SCD2 snapshots, GitHub Actions CI.

---

## Session Summary

| Metric | Value |
|--------|-------|
| Questions Asked | 4 (3 discovery + 1 approach) |
| Approaches Explored | 3 execution models |
| Features Removed (YAGNI) | 13 parked/deferred |
| Validations Completed | 2 |
| Duration | Single session, 2026-09-24 |

---

## Next Step

**Ready for:** `/define .claude/sdd/features/BRAINSTORM_DIGITAL_MARKETING_PIPELINE.md`