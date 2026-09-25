---
name: data-engineering-guide
description: |
  Data engineering expertise for pipelines, schemas, data quality, SQL, lakehouse, and streaming.
  Use PROACTIVELY when the user discusses data pipelines, ETL/ELT, schema design, dimensional modeling,
  data quality checks, SQL optimization, dbt models, Spark jobs, Airflow DAGs, streaming pipelines,
  lakehouse architecture, or data contracts.
---

# Data Engineering Guide

You have access to 23 specialized knowledge base domains and 15+ data engineering agents. Route the user to the right tool based on their task.

## Quick Routing

| User Task | Command | Agent |
|-----------|---------|-------|
| Design a data pipeline / DAG | `/agentspec:pipeline` | pipeline-architect |
| Design a schema / star schema / data model | `/agentspec:schema` | schema-designer |
| Add data quality checks | `/agentspec:data-quality` | data-quality-analyst |
| Review SQL performance | `/agentspec:sql-review` | sql-optimizer |
| Choose table format (Iceberg/Delta) | `/agentspec:lakehouse` | lakehouse-architect |
| Build RAG / embedding pipeline | `/agentspec:ai-pipeline` | ai-data-engineer |
| Create a data contract | `/agentspec:data-contract` | data-contracts-engineer |
| Migrate legacy ETL | `/agentspec:migrate` | dbt-specialist + spark-engineer |

## Knowledge Domains Available

| Category | Domains |
|----------|---------|
| Core DE | dbt, spark, airflow, streaming, sql-patterns |
| Data Design | data-modeling, data-quality, medallion |
| Infrastructure | lakehouse, cloud-platforms, aws, gcp, microsoft-fabric, lakeflow, terraform |
| AI & Modern | ai-data-engineering, genai, prompt-engineering, modern-stack |
| Foundations | pydantic, python, testing |

## How Agents Use Knowledge

1. Agent reads KB index at `${CLAUDE_PLUGIN_ROOT}/kb/{domain}/index.md`
2. Loads specific pattern/concept file matching the task
3. Falls back to MCP if KB insufficient (max 3 MCP calls)
4. Calculates confidence from evidence matrix

## When to Suggest Commands

- User mentions "dbt model" or "staging model" → `/agentspec:schema` or delegate to dbt-specialist
- User mentions "pipeline" or "DAG" or "orchestration" → `/agentspec:pipeline`
- User mentions "data quality" or "expectations" or "tests" → `/agentspec:data-quality`
- User mentions "slow query" or "optimize SQL" → `/agentspec:sql-review`
- User mentions "Iceberg" or "Delta Lake" or "table format" → `/agentspec:lakehouse`
- User mentions "RAG" or "embeddings" or "vector" → `/agentspec:ai-pipeline`
- User mentions "contract" or "SLA" or "schema governance" → `/agentspec:data-contract`
- User mentions "migrate" or "legacy" or "SSIS" or "Informatica" → `/agentspec:migrate`
