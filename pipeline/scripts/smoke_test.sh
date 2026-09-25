#!/usr/bin/env bash
# =============================================================================
# smoke_test.sh - end-to-end smoke test / runbook for the GCP digital-marketing
# ELT pipeline (Astro + Cosmos, dbt, Great Expectations, GCS, BigQuery).
#
# PURPOSE
#   Automates the final-verification SQL checks from PRD Section 14 (Definition
#   of Done) and the M0/M1 acceptance gates, so a new reader can go from a
#   clone to a green initial-load run using only the README + this runbook
#   (PRD Section 11, Section 13 M6, AT-001).
#
# PHASES
#   1. Preflight    Required env vars + GCP backend availability. Two supported
#                   backends are documented: A) Google Cloud CLI (gcloud + bq),
#                   B) python3 with google-cloud-storage/google-cloud-bigquery.
#   2. Bootstrap    Runs `python scripts/bootstrap_gcp.py` TWICE - PRD M0
#                   requires idempotency (the second run must not error).
#   3. Datagen      Prints the M1 determinism check commands (the pytest suite
#                   is authoritative; backfill-twice + content hash is the
#                   manual spot-check) and runs `pytest include/datagen/tests -q`
#                   when pytest is importable in this shell.
#   4. Initial load Prints the astro dev + Airflow trigger runbook for the
#                   marketing_initial_load DAG and checks local tooling.
#   5. SQL checks   Runs the EXACT PRD Section 14 queries via `bq query`
#                   (python google-cloud-bigquery fallback; prints the SQL for
#                   manual execution if neither exists). Expects:
#                     - MIN/MAX/COUNT(DISTINCT event_date) = 2026-01-01 |
#                       2026-06-30 | 181 after the initial load
#                     - 0 dates missing ads in the initial window
#                     - 182 distinct event_dates after one daily run, and the
#                       same 182 after re-running that day (idempotency).
#
# HOW TO RUN (path-independent; resolves relative to this repo root)
#   bash scripts/smoke_test.sh                     # full smoke (usage shown first)
#   bash scripts/smoke_test.sh --stop-on-first     # exit right at the first FAIL
#   bash scripts/smoke_test.sh --log-file /tmp/smoke.log
#   bash scripts/smoke_test.sh --skip-sql          # only preflight/bootstrap/datagen/airflow
#   bash scripts/smoke_test.sh --help              # full usage
#
# PREREQUISITES
#   Env vars (source pipeline/.env first):
#     GCP_PROJECT_ID, GCS_BUCKET, GCP_LOCATION,
#     BQ_RAW_DATASET | BIGQUERY_RAW_DATASET,
#     BQ_STAGING_DATASET | BIGQUERY_STG_DATASET,
#     BQ_MARTS_DATASET | BIGQUERY_MARTS_DATASET
#   Backend A: Google Cloud CLI (gcloud + bq), authenticated via
#     `gcloud auth application-default login` or GOOGLE_APPLICATION_CREDENTIALS.
#   Backend B: python3 + google-cloud-storage + google-cloud-bigquery (same ADC).
#   Phase 4: Astro CLI + Docker with this project (`astro dev start`).
#   Phase 3: python3 + pytest + datagen deps (cd include/datagen && pip install -e .)
#
# WINDOWS NOTE
#   Run via Git Bash / WSL: `bash scripts/smoke_test.sh`. The shebang
#   `#!/usr/bin/env bash` targets macOS/Ubuntu/Astro containers; on Windows the
#   interpreter is chosen explicitly when you invoke `bash scripts/...`.
#
# OUTPUT
#   Timestamped PASS/FAIL/SKIP lines on stdout (colors via tput when stdout is
#   a TTY; plain-text fallback). Optional --log-file mirrors plain text. Exit 0
#   = all checks green; exit 1 = >=1 FAIL; SKIP lines are documented manual
#   steps that still need completing.
# =============================================================================

set -euo pipefail

# -----------------------------------------------------------------------------
# Options (settled during arg parsing; defaults keep set -u happy)
# -----------------------------------------------------------------------------
STOP_ON_FIRST=0
LOG_FILE=""
SKIP_BOOTSTRAP=0
SKIP_DATAGEN=0
SKIP_AIRFLOW=0
SKIP_SQL=0
HAD_ARGS=0

# Result counters
PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

# -----------------------------------------------------------------------------
# Colors - only when stdout is a TTY and tput works; plain fallback otherwise
# -----------------------------------------------------------------------------
USE_COLOR=0
if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
  if command -v tput >/dev/null 2>&1 && tput setaf 1 >/dev/null 2>&1; then
    USE_COLOR=1
  fi
fi

GREEN="" RED="" YELLOW="" CYAN="" BOLD="" DIM="" RESET=""
if [[ "$USE_COLOR" == "1" ]]; then
  GREEN="$(tput setaf 2 2>/dev/null || true)"
  RED="$(tput setaf 1 2>/dev/null || true)"
  YELLOW="$(tput setaf 3 2>/dev/null || true)"
  CYAN="$(tput setaf 6 2>/dev/null || true)"
  BOLD="$(tput bold 2>/dev/null || true)"
  DIM="$(tput dim 2>/dev/null || true)"
  RESET="$(tput sgr0 2>/dev/null || true)"
fi

# -----------------------------------------------------------------------------
# Logging - timestamped stdout; plain mirror to LOG_FILE when set
# -----------------------------------------------------------------------------
say_plain() {
  local tag="$1" color_code="$2"
  shift 2
  local stamp plain
  stamp="$(date '+%Y-%m-%d %H:%M:%S')"
  plain="[${stamp}] ${tag} $*"
  printf '%s\n' "${color_code}${plain}${RESET}"
  if [[ -n "$LOG_FILE" ]]; then
    printf '%s\n' "$plain" >> "$LOG_FILE" || true
  fi
}

info_msg()   { say_plain "INFO" "$CYAN" "$*"; }
step_msg()   { say_plain "STEP" "$DIM" "$*"; }
warn_msg()   { say_plain "WARN" "$YELLOW" "$*"; }
pass_check() { PASS_COUNT=$((PASS_COUNT + 1)); say_plain "PASS" "$GREEN" "$*"; }
skip_check() { SKIP_COUNT=$((SKIP_COUNT + 1)); say_plain "SKIP" "$YELLOW" "$*"; }
fail_check() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  say_plain "FAIL" "$RED" "$*"
  if [[ "$STOP_ON_FIRST" == "1" ]]; then
    print_summary
  fi
}

# Useful breadcrumb if an unguarded command aborts the script (set -e).
trap 'say_plain "FATAL" "$RED" "Unexpected error at approximately line $LINENO. Fix the cause and re-run."' ERR

# -----------------------------------------------------------------------------
# Repo-relative paths - safe from any CWD
# -----------------------------------------------------------------------------
SCRIPT_SOURCE="${BASH_SOURCE[0]}"
SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_SOURCE")" && pwd)"
PIPELINE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# -----------------------------------------------------------------------------
# Env resolution - accept both PRD (Section 2) and DESIGN (.env.example)
# spellings for the dataset names. Values are validated in phase 1.
# -----------------------------------------------------------------------------
GCP_PROJECT_ID="${GCP_PROJECT_ID:-}"
GCS_BUCKET="${GCS_BUCKET:-}"
GCP_LOCATION="${GCP_LOCATION:-}"
BQ_RAW_DATASET="${BQ_RAW_DATASET:-${BIGQUERY_RAW_DATASET:-}}"
BQ_STAGING_DATASET="${BQ_STAGING_DATASET:-${BIGQUERY_STG_DATASET:-}}"
BQ_MARTS_DATASET="${BQ_MARTS_DATASET:-${BIGQUERY_MARTS_DATASET:-}}"

# Optional simulation-window knobs with the PRD/DESIGN defaults
BACKFILL_START="${BACKFILL_START:-${SIM_START_DATE:-2026-01-01}}"
BACKFILL_END="${BACKFILL_END:-2026-06-30}"
FIRST_INCREMENTAL_DATE="${FIRST_INCREMENTAL_DATE:-2026-07-01}"
EXPECTED_INITIAL_DAYS="${EXPECTED_INITIAL_DAYS:-181}"
EXPECTED_AFTER_DAILY="${EXPECTED_AFTER_DAILY:-182}"

# -----------------------------------------------------------------------------
# Backend detection - phase 1 documents both paths and their status
# -----------------------------------------------------------------------------
PYTHON_BIN=""
if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
fi

HAVE_GCLOUD=0
HAVE_BQ=0
HAVE_PY_GC=0
if command -v gcloud >/dev/null 2>&1; then HAVE_GCLOUD=1; fi
if command -v bq >/dev/null 2>&1; then HAVE_BQ=1; fi
if [[ -n "$PYTHON_BIN" ]]; then
  if "$PYTHON_BIN" -c 'import google.cloud.bigquery, google.cloud.storage' >/dev/null 2>&1; then
    HAVE_PY_GC=1
  fi
fi

# -----------------------------------------------------------------------------
# Usage / runbook help
# -----------------------------------------------------------------------------
print_usage() {
  cat <<'EOF'
smoke_test.sh - end-to-end smoke test / runbook for the GCP digital-marketing ELT pipeline

USAGE
    bash scripts/smoke_test.sh            # full smoke test (short usage shown first)
    bash scripts/smoke_test.sh [OPTIONS]

OPTIONS
    -h, --help            Show this help and exit.
    --stop-on-first       Exit immediately after the first FAIL (default: run all).
    --log-file FILE       Also write the timestamped plain-text log to FILE.
    --skip-bootstrap      Skip phase 2 (bootstrap_gcp.py idempotency).
    --skip-datagen        Skip phase 3 (datagen determinism / pytest).
    --skip-airflow        Skip phase 4 (astro dev / initial-load runbook).
    --skip-sql            Skip phase 5 (PRD Section 14 SQL verification).

PHASES (5)
    1. Preflight    Required env vars + GCP backend availability.
    2. Bootstrap    python scripts/bootstrap_gcp.py run twice (idempotent).
    3. Datagen      Print M1 determinism commands; run pytest include/datagen/tests -q if available.
    4. Initial load Print the astro dev + Airflow trigger runbook for marketing_initial_load.
    5. SQL checks   EXACT PRD Section 14 queries via bq (python fallback), expects
                    2026-01-01 | 2026-06-30 | 181, 0 missing-ads days, then 182
                    after one daily run; re-run identical counts.

REQUIRED ENV (source pipeline/.env first)
    GCP_PROJECT_ID, GCS_BUCKET, GCP_LOCATION,
    BQ_RAW_DATASET | BIGQUERY_RAW_DATASET, BQ_STAGING_DATASET | BIGQUERY_STG_DATASET,
    BQ_MARTS_DATASET | BIGQUERY_MARTS_DATASET

BACKENDS (either is enough)
    A) Google Cloud CLI: gcloud + bq, authenticated (ADC or gcloud auth).
    B) python3 with google-cloud-storage + google-cloud-bigquery (same ADC).

EXAMPLES
    bash scripts/smoke_test.sh --stop-on-first --log-file /tmp/smoke.log
    bash scripts/smoke_test.sh --skip-sql        # preflight + bootstrap + datagen only
    bash scripts/smoke_test.sh --skip-bootstrap --skip-sql --skip-airflow --skip-datagen
EOF
}

# -----------------------------------------------------------------------------
# Small helpers
# -----------------------------------------------------------------------------
print_clipped() {
  # Print text indented, clipped to max_lines, without SIGPIPE hazards.
  local text="$1" max_lines="${2:-8}"
  if [[ -z "$text" ]]; then
    printf '%s\n' "    (no output)"
    return 0
  fi
  printf '%s\n' "$text" | awk -v n="$max_lines" 'NR <= n { print "    " $0 } END { if (NR > n) print "    ... (" (NR - n) " more line(s))" }'
}

env_check() {
  local var="$1"
  if [[ -z "${!var:-}" ]]; then
    fail_check "Required env var ${var} is not set. Source pipeline/.env (or export it) before running."
  else
    pass_check "env ${var} is set"
  fi
}

# run_sql <sql> - run a BigQuery query via bq CLI or the python fallback.
# Populates SQL_MODE ("bq" | "python" | "manual") and SQL_OUTPUT.
# Returns 0 on success, 1 on query error, 2 when no backend exists.
run_sql() {
  local sql="$1"
  SQL_MODE="manual"
  SQL_OUTPUT=""
  if [[ "$HAVE_BQ" == "1" ]]; then
    local out=""
    if ! out="$(bq --headless query --use_legacy_sql=false --format=csv --max_rows=10000 --quiet --project_id="$GCP_PROJECT_ID" "$sql" 2>&1)"; then
      SQL_MODE="bq"
      SQL_OUTPUT="$out"
      return 1
    fi
    SQL_MODE="bq"
  elif [[ "$HAVE_PY_GC" == "1" ]]; then
    # Python fallback: identical CSV-shaped output to the bq path.
    local out=""
    if ! out="$(SMOKE_SQL="$sql" "$PYTHON_BIN" - "$GCP_PROJECT_ID" 2>&1 <<'PY'
import os
import sys
from google.cloud import bigquery
client = bigquery.Client(project=sys.argv[1])
rows = client.query(os.environ["SMOKE_SQL"]).result()
fields = [field.name for field in rows.schema]
print(",".join(fields))
for row in rows:
    print(",".join(str(row[f]) if row[f] is not None else "" for f in fields))
PY
)"; then
      SQL_MODE="python"
      SQL_OUTPUT="$out"
      return 1
    fi
    SQL_MODE="python"
  else
    SQL_OUTPUT="$sql"
    return 2
  fi
  SQL_OUTPUT="${out//$'\r'/}"
  return 0
}

# -----------------------------------------------------------------------------
# Phase 5 - PRD Section 14 SQL verification sub-checks
# -----------------------------------------------------------------------------
verify_initial_window() {
  local sql row got_min got_max got_cnt
  sql="SELECT MIN(event_date), MAX(event_date), COUNT(DISTINCT event_date) \
FROM \`${GCP_PROJECT_ID}.${BQ_RAW_DATASET}.raw_ga4_events\`"
  step_msg "PRD Section 14 check 1 - MIN/MAX/COUNT(DISTINCT event_date) on raw_ga4_events"
  if ! run_sql "$sql"; then
    fail_check "SQL check 1 query failed (backend: ${SQL_MODE}):"
    print_clipped "$SQL_OUTPUT" 10
    return 0
  fi
  row="$(printf '%s\n' "$SQL_OUTPUT" | sed -n '2p')"
  got_min=""
  got_max=""
  got_cnt=""
  if [[ -n "$row" ]]; then
    IFS=',' read -r got_min got_max got_cnt <<< "$row" || true
  fi
  got_min="${got_min//[[:space:]]/}"
  got_max="${got_max//[[:space:]]/}"
  got_cnt="${got_cnt//[[:space:]]/}"
  if [[ "$got_min" == "$BACKFILL_START" && "$got_max" == "$BACKFILL_END" && "$got_cnt" == "$EXPECTED_INITIAL_DAYS" ]]; then
    pass_check "Initial window green: MIN=${got_min} MAX=${got_max} COUNT=${got_cnt} (expected ${BACKFILL_START}|${BACKFILL_END}|${EXPECTED_INITIAL_DAYS})"
  else
    fail_check "Initial window mismatch: got ${got_min}|${got_max}|${got_cnt}, expected ${BACKFILL_START}|${BACKFILL_END}|${EXPECTED_INITIAL_DAYS}"
  fi
}

verify_no_missing_ads() {
  local sql missing
  sql="SELECT d.date FROM \`${GCP_PROJECT_ID}.${BQ_MARTS_DATASET}.dim_date\` d \
LEFT JOIN (SELECT DISTINCT spend_date FROM \`${GCP_PROJECT_ID}.${BQ_RAW_DATASET}.raw_ads_campaign_daily\`) a \
ON a.spend_date = d.date \
WHERE d.date BETWEEN '${BACKFILL_START}' AND '${BACKFILL_END}' AND a.spend_date IS NULL"
  step_msg "PRD Section 14 check 2 - dates missing ads in the initial window (expect 0 rows)"
  if ! run_sql "$sql"; then
    fail_check "SQL check 2 query failed (backend: ${SQL_MODE}):"
    print_clipped "$SQL_OUTPUT" 10
    return 0
  fi
  # Count only CSV data rows (skip the header); date rows never match a stderr
  # noise line, so this also tolerates a stray bq status line.
  missing="$(printf '%s\n' "$SQL_OUTPUT" | awk 'NR>1 && $0 ~ /^[0-9]{4}-[0-9]{2}-[0-9]{2}/ {n++} END {print n+0}')"
  if [[ "$missing" == "0" ]]; then
    pass_check "No dates missing ads - 0 rows returned."
  else
    fail_check "${missing} date(s) in dim_date without ads between ${BACKFILL_START} and ${BACKFILL_END}:"
    print_clipped "$SQL_OUTPUT" 10
  fi
}

verify_after_daily_run() {
  local sql got_cnt
  sql="SELECT COUNT(DISTINCT event_date) \
FROM \`${GCP_PROJECT_ID}.${BQ_RAW_DATASET}.raw_ga4_events\`"
  step_msg "PRD Section 14 check 3 - distinct event_date count after one daily run (expect ${EXPECTED_AFTER_DAILY})"
  if ! run_sql "$sql"; then
    fail_check "SQL check 3 query failed (backend: ${SQL_MODE}):"
    print_clipped "$SQL_OUTPUT" 10
    return 0
  fi
  got_cnt="$(printf '%s\n' "$SQL_OUTPUT" | sed -n '2p' | tr -d '[:space:]')"
  got_cnt="${got_cnt:-}"
  if [[ "$got_cnt" == "$EXPECTED_AFTER_DAILY" ]]; then
    pass_check "After daily run: ${got_cnt} distinct event_dates (expected ${EXPECTED_AFTER_DAILY}). \
Re-run the daily DAG for the same ds and confirm the count stays identical (idempotency, PRD Section 14)."
  elif [[ "$got_cnt" == "$EXPECTED_INITIAL_DAYS" ]]; then
    skip_check "Still ${got_cnt} dates - the daily run has not been performed yet. Trigger marketing_daily_pipeline for ${FIRST_INCREMENTAL_DATE}:"
    printf '%s\n' \
      "    astro dev run dags backfill marketing_daily_pipeline -s ${FIRST_INCREMENTAL_DATE} -e ${FIRST_INCREMENTAL_DATE}" \
      "    (or trigger marketing_daily_pipeline from the Airflow UI), wait for green," \
      "    then re-run this script - the count must become ${EXPECTED_AFTER_DAILY}."
  else
    fail_check "Distinct event_dates = ${got_cnt}, expected ${EXPECTED_AFTER_DAILY} after one daily run. \
If you have run more daily days intentionally, set EXPECTED_AFTER_DAILY to the current \
max(event_date) and re-run."
  fi
}

# -----------------------------------------------------------------------------
# Phases
# -----------------------------------------------------------------------------
phase_preflight() {
  # Environment
  env_check "GCP_PROJECT_ID"
  env_check "GCS_BUCKET"
  env_check "GCP_LOCATION"
  env_check "BQ_RAW_DATASET"
  env_check "BQ_STAGING_DATASET"
  env_check "BQ_MARTS_DATASET"

  if [[ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" && ! -f "${GOOGLE_APPLICATION_CREDENTIALS}" ]]; then
    fail_check "GOOGLE_APPLICATION_CREDENTIALS points to a missing file: ${GOOGLE_APPLICATION_CREDENTIALS}"
  fi

  # Sanity-check the project id shape (a typo could target the wrong project).
  case "${GCP_PROJECT_ID:-}" in
    *[!a-zA-Z0-9_-]*)
      fail_check "GCP_PROJECT_ID '${GCP_PROJECT_ID}' looks invalid (allowed: letters, digits, - and _)."
      ;;
  esac

  # GCP backend paths - documented here, used by phases 2 and 5.
  info_msg "Backend path A: Google Cloud CLI (gcloud + bq). Backend path B: python google-cloud libs (google-cloud-storage, google-cloud-bigquery)."
  if [[ "$HAVE_BQ" == "1" ]]; then
    pass_check "bq CLI available (path A)"
  else
    info_msg "bq CLI not found - path A unavailable (install the Google Cloud SDK or use path B)."
  fi
  if [[ "$HAVE_PY_GC" == "1" ]]; then
    pass_check "python google-cloud libs available (path B, ${PYTHON_BIN})"
  elif [[ -n "$PYTHON_BIN" ]]; then
    info_msg "python present (${PYTHON_BIN}) but google.cloud.bigquery/storage import failed - install with: ${PYTHON_BIN} -m pip install google-cloud-bigquery google-cloud-storage"
  else
    info_msg "python3/python not found - path B unavailable."
  fi

  if [[ "$HAVE_BQ" != "1" && "$HAVE_PY_GC" != "1" ]]; then
    fail_check "No GCP backend available (need bq CLI or python + google-cloud libs) - install one (see --help) or run all backend steps manually."
  fi

  # gcloud auth is informational: with ADC (GOOGLE_APPLICATION_CREDENTIALS)
  # there may be no active gcloud user and everything still works.
  if [[ "$HAVE_GCLOUD" == "1" ]]; then
    local gcloud_account=""
    gcloud_account="$(gcloud auth list --filter=status:ACTIVE --format='value(account)' 2>/dev/null | sed -n '1p' | tr -d '[:space:]' || true)"
    if [[ -n "$gcloud_account" ]]; then
      info_msg "gcloud active account: ${gcloud_account}"
    else
      warn_msg "gcloud present but no ACTIVE account - run 'gcloud auth login' / 'gcloud auth application-default login' or set GOOGLE_APPLICATION_CREDENTIALS."
    fi
  fi
}

phase_bootstrap() {
  if [[ "$SKIP_BOOTSTRAP" == "1" ]]; then
    skip_check "Bootstrap skipped via --skip-bootstrap."
    return 0
  fi
  info_msg "PRD M0 acceptance: bootstrap_gcp.py must run twice without error."
  if [[ -z "$PYTHON_BIN" ]]; then
    fail_check "No python3/python found - cannot run scripts/bootstrap_gcp.py. Manual gcloud equivalent (NOT idempotent - only bootstrap_gcp.py is the safe route):"
    printf '%s\n' \
      "    gcloud storage buckets create gs://${GCS_BUCKET:-<bucket>} --location=${GCP_LOCATION:-US} --project=${GCP_PROJECT_ID:-<project>}" \
      "    bq --location=${GCP_LOCATION:-US} mk --dataset ${GCP_PROJECT_ID:-<project>}:${BQ_RAW_DATASET:-digital_marketing_raw}" \
      "    bq --location=${GCP_LOCATION:-US} mk --dataset ${GCP_PROJECT_ID:-<project>}:${BQ_STAGING_DATASET:-digital_marketing_staging}" \
      "    bq --location=${GCP_LOCATION:-US} mk --dataset ${GCP_PROJECT_ID:-<project>}:${BQ_MARTS_DATASET:-digital_marketing_marts}"
    return 0
  fi
  if [[ "$HAVE_PY_GC" != "1" ]]; then
    fail_check "google-cloud libraries missing for ${PYTHON_BIN}. Install them: ${PYTHON_BIN} -m pip install google-cloud-storage google-cloud-bigquery"
    return 0
  fi

  step_msg "Run 1/2: ${PYTHON_BIN} scripts/bootstrap_gcp.py"
  local out=""
  if ! out="$("$PYTHON_BIN" scripts/bootstrap_gcp.py 2>&1)"; then
    fail_check "bootstrap run 1 failed:"
    print_clipped "$out" 15
    return 0
  fi

  step_msg "Run 2/2 (idempotency check): ${PYTHON_BIN} scripts/bootstrap_gcp.py"
  local out2=""
  if ! out2="$("$PYTHON_BIN" scripts/bootstrap_gcp.py 2>&1)"; then
    fail_check "bootstrap run 2 failed (idempotency broken - a re-run must exit 0):"
    print_clipped "$out2" 15
    return 0
  fi
  pass_check "bootstrap_gcp.py ran twice with exit 0 - bucket + 3 datasets present (idempotent)."
}

phase_datagen() {
  if [[ "$SKIP_DATAGEN" == "1" ]]; then
    skip_check "Datagen determinism phase skipped via --skip-datagen."
    return 0
  fi
  info_msg "M1 determinism check - two methods (PRD Section 6.1.1, AT-005):"
  cat <<PYBLOCK

    Method 1 (authoritative - the AT-005 determinism suite):
      ${PYTHON_BIN:-python3} -m pip install -e include/datagen
      ${PYTHON_BIN:-python3} -m pytest include/datagen/tests -q

    Method 2 (manual spot-check): run the backfill twice and compare CONTENT.
    Raw 'cmp parquet' is unreliable: parquet bytes can differ (metadata, the
    injected inserted_at column) even when the rows are identical. Hash the
    decoded frames instead (same seed, date, source -> identical rows):

      datagen-backfill --start ${BACKFILL_START} --end ${BACKFILL_END} --out /tmp/dm_backfill_1
      datagen-backfill --start ${BACKFILL_START} --end ${BACKFILL_END} --out /tmp/dm_backfill_2
      ${PYTHON_BIN:-python3} - <<'PY'
import hashlib
import pathlib
import pandas as pd

def frame_hash(root, ignore={"inserted_at"}):
    h = hashlib.sha256()
    for p in sorted(pathlib.Path(root).rglob("*.parquet")):
        df = pd.read_parquet(p)
        cols = sorted(c for c in df.columns if c not in ignore)
        h.update((str(p) + ":" + df[cols].to_csv(index=False)).encode())
    return h.hexdigest()

h1 = frame_hash("/tmp/dm_backfill_1")
h2 = frame_hash("/tmp/dm_backfill_2")
print("run1:", h1)
print("run2:", h2)
assert h1 == h2, "backfill determinism FAILED"
print("backfill determinism OK: run1 == run2")
PY

    (CLI flags above follow the DESIGN contract generate(source, start, end,
     out_dir, inserted_at); adjust to 'datagen-backfill --help' after M1 lands.)
PYBLOCK

  step_msg "Attempting the pytest determinism suite (only if pytest is importable here)"
  if [[ -n "$PYTHON_BIN" ]] && "$PYTHON_BIN" -m pytest --version >/dev/null 2>&1; then
    local out=""
    if out="$("$PYTHON_BIN" -m pytest include/datagen/tests -q 2>&1)"; then
      pass_check "pytest include/datagen/tests -q - green (determinism, distributions, dimensions, schemas)."
    else
      fail_check "pytest include/datagen/tests -q - FAILED:"
      print_clipped "$out" 20
    fi
  else
    skip_check "pytest not available in this shell - run it in the datagen environment: ${PYTHON_BIN:-python3} -m pip install -e include/datagen && ${PYTHON_BIN:-python3} -m pytest include/datagen/tests -q"
  fi
  return 0
}

phase_airflow() {
  if [[ "$SKIP_AIRFLOW" == "1" ]]; then
    skip_check "Initial-load runbook phase skipped via --skip-airflow."
    return 0
  fi
  info_msg "Manual runbook for marketing_initial_load (PRD Section 11, Section 13 M6, AT-001):"
  cat <<RUNBOOK
    From pipeline/ (Astro CLI + Docker required; GCP auth via mounted service
    account key or GOOGLE_APPLICATION_CREDENTIALS):

      1) astro dev start
      2) Open http://localhost:8080  (default login admin/admin)
      3) DAGs -> marketing_initial_load -> [Trigger DAG]
         (CLI alternative: astro dev run dags trigger marketing_initial_load)
      4) Wait for the run to finish green: datagen -> GCS -> BigQuery raw
         (partition-scoped WRITE_TRUNCATE) -> GX raw gate -> dbt build
         --full-refresh -> GX marts gate.
      5) Trigger one daily run (PRD Section 7.3 / Section 14): the run's ds
         is its data date. For exactly ${FIRST_INCREMENTAL_DATE}:
           astro dev run dags backfill marketing_daily_pipeline -s ${FIRST_INCREMENTAL_DATE} -e ${FIRST_INCREMENTAL_DATE}
         (or trigger marketing_daily_pipeline from the UI and let catchup pick
         it up; the first run emits the ${FIRST_INCREMENTAL_DATE} day).
      6) Re-run this script to execute the phase-5 SQL checks: the distinct
         event_date count must move ${EXPECTED_INITIAL_DAYS} -> ${EXPECTED_AFTER_DAILY}
         and stay identical if the same day is re-run (idempotency).
RUNBOOK

  local have_astro=0 have_docker=0
  if command -v astro >/dev/null 2>&1; then have_astro=1; fi
  if command -v docker >/dev/null 2>&1; then have_docker=1; fi

  if [[ "$have_astro" == "1" && "$have_docker" == "1" ]]; then
    pass_check "Astro CLI and Docker found - you can run the runbook above from pipeline/."
  elif [[ "$have_astro" == "1" ]]; then
    skip_check "Astro CLI found but docker is not on PATH - start Docker Desktop and re-check."
  else
    skip_check "Astro CLI not on PATH - install it and run from pipeline/ (Docker also required). See https://docs.astronomer.io/astro/cli"
  fi

  # Soft reachability probe: the webserver on the default 8080 port.
  if command -v curl >/dev/null 2>&1; then
    local code=""
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "http://localhost:8080/home" 2>/dev/null || true)"
    if [[ "$code" != "" && "$code" != "000" ]]; then
      info_msg "Airflow webserver reachable at http://localhost:8080 (HTTP ${code})."
    else
      info_msg "Airflow webserver not reachable at http://localhost:8080 - expected until 'astro dev start' has finished."
    fi
  fi
  return 0
}

phase_sql() {
  if [[ "$SKIP_SQL" == "1" ]]; then
    skip_check "SQL verification skipped via --skip-sql."
    return 0
  fi
  if [[ "$HAVE_BQ" != "1" && "$HAVE_PY_GC" != "1" ]]; then
    fail_check "No SQL backend (bq CLI or python google-cloud-bigquery). Cannot auto-run PRD Section 14 checks - run manually (expected results in comments):"
    printf '%s\n' \
      "-- check 1: expect 2026-01-01 | 2026-06-30 | 181" \
      "SELECT MIN(event_date), MAX(event_date), COUNT(DISTINCT event_date)" \
      "FROM \`${GCP_PROJECT_ID}.${BQ_RAW_DATASET}.raw_ga4_events\`;" \
      "" \
      "-- check 2: expect 0 rows (no dates missing ads)" \
      "SELECT d.date FROM \`${GCP_PROJECT_ID}.${BQ_MARTS_DATASET}.dim_date\` d" \
      "LEFT JOIN (SELECT DISTINCT spend_date FROM \`${GCP_PROJECT_ID}.${BQ_RAW_DATASET}.raw_ads_campaign_daily\`) a" \
      "  ON a.spend_date = d.date" \
      "WHERE d.date BETWEEN '${BACKFILL_START}' AND '${BACKFILL_END}' AND a.spend_date IS NULL;" \
      "" \
      "-- check 3 (after one daily run): expect 182" \
      "SELECT COUNT(DISTINCT event_date) FROM \`${GCP_PROJECT_ID}.${BQ_RAW_DATASET}.raw_ga4_events\`;"
    return 0
  fi
  local backend="bq CLI"
  if [[ "$HAVE_BQ" != "1" ]]; then backend="python google-cloud-bigquery (fallback)"; fi
  info_msg "SQL backend: ${backend}"
  verify_initial_window
  verify_no_missing_ads
  verify_after_daily_run
}

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
print_summary() {
  printf '\n--------------------------------------------------------------\n'
  say_plain "SUMMARY" "$BOLD" "PASS: ${PASS_COUNT} | FAIL: ${FAIL_COUNT} | SKIP: ${SKIP_COUNT}"
  if [[ "$FAIL_COUNT" -gt 0 ]]; then
    say_plain "RESULT" "$RED" "SMOKE TEST FAILED - ${FAIL_COUNT} check(s) failed. See the FAIL lines above."
    exit 1
  fi
  if [[ "$SKIP_COUNT" -gt 0 ]]; then
    say_plain "RESULT" "$YELLOW" "SMOKE TEST PASSED with ${SKIP_COUNT} skip(s) - complete the manual steps printed above to finish the runbook, then re-run for a fully green signal."
    exit 0
  fi
  say_plain "RESULT" "$GREEN" "SMOKE TEST PASSED - all checks green."
  exit 0
}

# -----------------------------------------------------------------------------
# Argument parsing
# -----------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  HAD_ARGS=1
  case "$1" in
    --help|-h)
      print_usage
      exit 0
      ;;
    --stop-on-first)
      STOP_ON_FIRST=1
      shift
      ;;
    --log-file)
      if [[ $# -lt 2 ]]; then
        printf '%s\n' "ERROR: --log-file requires a file path" >&2
        exit 2
      fi
      LOG_FILE="$2"
      shift 2
      ;;
    --skip-bootstrap) SKIP_BOOTSTRAP=1; shift ;;
    --skip-datagen)   SKIP_DATAGEN=1;   shift ;;
    --skip-airflow)   SKIP_AIRFLOW=1;   shift ;;
    --skip-sql)       SKIP_SQL=1;       shift ;;
    *)
      printf '%s\n' "ERROR: unknown option: $1" >&2
      printf '%s\n' "Run with --help for usage." >&2
      exit 2
      ;;
  esac
done

if [[ "$HAD_ARGS" == "0" ]]; then
  print_usage
  printf '\n%s\n' "Proceeding with the default full smoke test (all 5 phases). Use --help for options."
fi

# Resolve LOG_FILE against the original CWD before we cd into the repo.
# A Windows drive path (C:/... or C:\...) already contains ":" and is absolute,
# so it must NOT get the CWD prefix stuffed onto it.
if [[ -n "$LOG_FILE" && "$LOG_FILE" != /* && "$LOG_FILE" != *:* ]]; then
  LOG_FILE="$(pwd)/${LOG_FILE}"
fi

cd "$PIPELINE_DIR"

if [[ -n "$LOG_FILE" ]]; then
  if ! ( : > "$LOG_FILE" ) 2>/dev/null; then
    printf '%s\n' "ERROR: cannot create log file: ${LOG_FILE}" >&2
    exit 2
  fi
  printf '%s\n' "# smoke_test.sh - started $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
fi

printf '%s\n' "${BOLD}smoke_test.sh - GCP digital-marketing ELT pipeline smoke test${RESET}"
info_msg "pipeline dir: ${PIPELINE_DIR}"

if [[ "${SKIP_BOOTSTRAP}${SKIP_DATAGEN}${SKIP_AIRFLOW}${SKIP_SQL}" == "1111" ]]; then
  warn_msg "All optional phases skipped (--skip-* on everything); only phase 1 preflight will run."
fi

# -----------------------------------------------------------------------------
# Main flow - 5 phases
# -----------------------------------------------------------------------------
phase_header() {
  local n="$1" title="$2"
  printf '\n--------------------------------------------------------------\n'
  say_plain "PHASE" "$BOLD" "${n}/5  ${title}"
}

phase_header 1 "Preflight - env vars + GCP backend availability"
phase_preflight

phase_header 2 "Bootstrap - scripts/bootstrap_gcp.py (idempotent, run twice)"
phase_bootstrap

phase_header 3 "Datagen determinism (M1) - commands + pytest suite"
phase_datagen

phase_header 4 "Initial-load smoke runbook (astro dev / Airflow)"
phase_airflow

phase_header 5 "Final-verification SQL (PRD Section 14, Definition of Done)"
phase_sql

print_summary