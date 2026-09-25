{#
    marketing_lookback_boundary(partition_col)

    Returns a scalar SQL expression for the incremental lower-bound "run date" used by the
    insert_overwrite lookback window (DESIGN Code Pattern 2, PRD §8.3):

        where <partition_col> >= date_sub(<this expression>, interval lookback_days day)

    Resolution order (PRD §8.1/§8.3 — no wall clock, data dates are simulated):
      1.  run_date var (the DAG's ds / data_interval_start) when provided;
      2.  else the latest partition already loaded in the target table
          ((select max(partition_col) from {{ this }})), coalesced to dim_date_start so a
          first incremental run on an empty table processes the whole history.

    partition_col must be the UNQUALIFIED partition column of the target ({{ this }}),
    e.g. 'event_date' / 'session_date' / 'spend_date' / 'video_date'.
#}
{% macro marketing_lookback_boundary(partition_col) -%}
    {%- if var('run_date', none) -%}
        cast('{{ var('run_date') }}' as date)
    {%- else -%}
        coalesce(
            (select max({{ partition_col }}) from {{ this }}),
            cast('{{ var('dim_date_start', '2026-01-01') }}' as date)
        )
    {%- endif -%}
{%- endmacro %}