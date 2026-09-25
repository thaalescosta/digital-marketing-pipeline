# AgentSpec — Universal Agent Guide

> Spec-Driven Development framework for Data Engineering (OpenCode & Multi-Agent Standard)

---

## Project Overview

**AgentSpec** is an open Spec-Driven Development (SDD) framework specialized for Data Engineering. It equips AI coding assistants with:
- **58+ specialized agents** across 8 categories (Architecture, Cloud, Data Engineering, Developer Tools, Microsoft Fabric, Python, Testing, SDD Workflow).
- **31 custom workflow commands** (`/brainstorm`, `/define`, `/design`, `/build`, `/ship`, `/pipeline`, `/schema`, `/data-quality`, etc.).
- **20 modular skills** (SDD phase engines, agent router, GitHub workflows, visual documentation).
- **24 Knowledge Base (KB) domains** providing grounded best practices for dbt, Spark, Airflow, Streaming, Data Modeling, Cloud Platforms, and more.

### Supported Environments
- **OpenCode** (native support via `opencode.json`, `.opencode/agents/`, `.opencode/commands/`, and `AGENTS.md`)
- **Claude Code** (native support via `.claude/` and distributable `plugin/`)

---

## Component Model Architecture

Canonical reference: `.claude/kb/shared/component-model.md`

| Layer | Responsibility | OpenCode Implementation | Claude Code Implementation |
|---|---|---|---|
| **Agents** | **EXECUTION** — Identity, scope, boundaries, escalation | `.opencode/agents/<name>.md` (`@agent`) | `.claude/agents/<cat>/<name>.md` |
| **Commands** | **ENTRYPOINT** — Slash command surface, agent binding | `.opencode/commands/<cmd>.md` (`/cmd`) | `.claude/commands/<cat>/<cmd>.md` |
| **Skills** | **CAPABILITY** — The HOW: methodology & execution | `.claude/skills/<skill>/SKILL.md` | `.claude/skills/<skill>/SKILL.md` |
| **KBs** | **SOURCE OF TRUTH** — Deep-dive technology patterns | `.claude/kb/<domain>/` | `.claude/kb/<domain>/` |

---

## SDD Workflow (5 Phases)

AgentSpec builds systems through 5 disciplined phases with clear quality gates:

```
[Phase 0: Brainstorm] ──> [Phase 1: Define] ──> [Phase 2: Design] ──> [Phase 3: Build] ──> [Phase 4: Ship]
   /brainstorm               /define               /design              /build             /ship
 (brainstorm-agent)       (define-agent)       (design-agent)       (build-agent)       (ship-agent)
```

1. **Phase 0: Brainstorm (`/brainstorm`)**: Clarify raw ideas, compare approaches, determine scope without premature code.
2. **Phase 1: Define (`/define`)**: Extract requirements, user stories, data contracts, and success criteria into a `DEFINE_<FEATURE>.md` spec.
3. **Phase 2: Design (`/design`)**: Architecture, data models (star schema, Data Vault, OBT), pipeline DAGs, and technology trade-offs.
4. **Phase 3: Build (`/build`)**: Test-first implementation against spec contracts, adhering to partition strategies and SCD patterns.
5. **Phase 4: Ship (`/ship`)**: Quality gate verification, automated docs, PR creation, and release readiness.
6. **Iterate (`/iterate`)**: Targeted bug fixes or enhancements preserving spec contracts.

---

## OpenCode Quick Start

### 1. Using Slash Commands
In OpenCode, execute any command with `/`:
```bash
# Workflow Phases
/brainstorm "Daily orders pipeline from Postgres to Snowflake star schema"
/define ORDERS_PIPELINE
/design ORDERS_PIPELINE
/build ORDERS_PIPELINE
/ship ORDERS_PIPELINE

# Direct Data Engineering Tasks
/pipeline "Daily orders ETL with Airflow"
/schema "Star schema for e-commerce analytics"
/data-quality models/staging/stg_orders.sql
/sql-review models/marts/
/data-contract "Contract between orders team and analytics"
/migration "Migrate legacy Airflow DAGs to Dagster"
```

### 2. Invoking Specialized Agents
In OpenCode, invoke any agent directly with `@`:
```text
@pipeline-architect Review our Airflow DAG partitioning strategy
@schema-designer Help design a slowly changing dimension (SCD Type 2) in dbt
@data-quality-analyst Generate Great Expectations suites for the staging layer
@sql-optimizer Identify bottleneck joins and optimize this query
```

### 3. Using OpenCode with Free Model Limits
AgentSpec agents in `.opencode/agents/` are configured as subagents without forcing hardcoded proprietary models. OpenCode will use your active session model (e.g. Gemini 2.5 Flash, Groq, Ollama, or default provider), allowing full usage of free tier quotas.

To conserve tokens with free or smaller models:
- Leverage **Just-In-Time (JIT) KB loading**: Read specific KB domain files on-demand (e.g., `.claude/kb/airflow/concepts/` or `.claude/kb/dbt/patterns/`) rather than entire directories.
- Trust the spec contracts in `.claude/sdd/architecture/WORKFLOW_CONTRACTS.yaml`.

---

## Directory Conventions

- `.opencode/agents/`: OpenCode subagents (flat files: `<agent-name>.md`)
- `.opencode/commands/`: OpenCode slash commands (flat files: `<command-name>.md`)
- `.claude/kb/`: 24 Knowledge Base domains (read-only reference)
- `.claude/skills/`: Methodology skills (`SKILL.md`)
- `.claude/sdd/templates/`: Document templates for SDD phases
- `.claude/sdd/features/`: Active feature specifications

---

## Developer Tooling

- `make help`: List all developer targets
- `make build`: Full plugin build and test check
- `make opencode`: Regenerate `.opencode/` agents, commands, and configs from `.claude/` source
- `make check`: Verify zero drift between source definitions and generated artifacts
- `python scripts/generate-opencode.py`: Sync OpenCode assets
