-- Web session fact (PRD §8.3). Grain: session_id. One row per GA4 session, aggregated
-- from stg_ga4__events.
--
-- session_date = min(event_date) of the session's events. Data note (PRD §3.3): all events
-- of one session share event_date, so incremental-by-event_date is safe and no session is
-- split across the lookback boundary.
--
-- channel_id = the channel of the first event (session_start) — the first event by
-- event_timestamp (tie-break event_id), per "channel is per-event; use the session_start
-- event's channel".
--
-- Incremental insert_overwrite on session_date with a 3-day lookback (DESIGN Decision 1);
-- the boundary macro reads run_date, or max(session_date) already loaded in this table.

{{ config(
    materialized='incremental',
    partition_by={'field': 'session_date', 'data_type': 'date'},
    incremental_strategy='insert_overwrite',
    cluster_by=['channel_id']
) }}

with events as (
    select
        event_id,
        event_date,
        event_timestamp,
        event_name,
        user_pseudo_id,
        session_id,
        channel_id,
        event_value
    from {{ ref('stg_ga4__events') }}
    {% if is_incremental() %}
    where event_date >= date_sub(
        {{ marketing_lookback_boundary('session_date') }},
        interval {{ var('lookback_days', 3) }} day
    )
    {% endif %}
),

session_agg as (
    select
        session_id,
        min(event_date) as session_date,
        min(event_timestamp) as session_start_ts,
        max(event_timestamp) as session_end_ts,
        timestamp_diff(max(event_timestamp), min(event_timestamp), minute) as session_duration_min,
        max(user_pseudo_id) as user_pseudo_id,
        array_agg(channel_id order by event_timestamp, event_id limit 1)[offset(0)] as channel_id,
        count(*) as event_count,
        countif(event_name = 'page_view') as page_view_count,
        countif(event_name = 'add_to_cart') > 0 as has_add_to_cart,
        countif(event_name = 'begin_checkout') > 0 as has_begin_checkout,
        countif(event_name = 'purchase') > 0 as has_purchase,
        sum(case when event_name = 'purchase' then coalesce(event_value, 0) else 0 end) as purchase_value
    from events
    group by session_id
)

select
    sa.session_id,
    sa.session_date,
    d.date_sk,
    sa.session_start_ts,
    sa.session_end_ts,
    sa.session_duration_min,
    sa.user_pseudo_id,
    sa.channel_id,
    sa.event_count,
    sa.page_view_count,
    sa.has_add_to_cart,
    sa.has_begin_checkout,
    sa.has_purchase,
    sa.purchase_value
from session_agg as sa
left join {{ ref('dim_date') }} as d
    on d.date = sa.session_date