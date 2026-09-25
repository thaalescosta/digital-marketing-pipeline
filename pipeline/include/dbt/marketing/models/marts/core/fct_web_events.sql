-- Web event fact (PRD §8.3). Grain: event_id. One row per GA4 event.
-- Incremental insert_overwrite on event_date with a lookback window (DESIGN Decision 1,
-- Code Pattern 2): each run rebuilds only the partitions >= run_date - lookback_days
-- (run_date from the DAG, falling back to max(event_date) in this table when absent).
-- Full-refresh reproduces the same result as the incremental history.

{{ config(
    materialized='incremental',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    incremental_strategy='insert_overwrite',
    cluster_by=['channel_id']
) }}

with source as (
    select
        event_id,
        event_date,
        event_timestamp,
        event_name,
        user_pseudo_id,
        session_id,
        channel_id,
        page_path,
        event_value,
        event_hour
    from {{ ref('stg_ga4__events') }}
    {% if is_incremental() %}
    where event_date >= date_sub(
        {{ marketing_lookback_boundary('event_date') }},
        interval {{ var('lookback_days', 3) }} day
    )
    {% endif %}
)

select
    s.event_id,
    s.event_date,
    d.date_sk,
    s.event_timestamp,
    s.event_name,
    s.user_pseudo_id,
    s.session_id,
    s.channel_id,
    s.page_path,
    s.event_value,
    s.event_hour
from source as s
left join {{ ref('dim_date') }} as d
    on d.date = s.event_date