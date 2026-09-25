-- Staging view over raw_ga4_events (PRD §8.2).
-- Grain: event_id. Dedupes by event_id (latest inserted_at) and adds light derived fields:
--   page_path         - path component of page_location
--   is_anonymous_user - user_pseudo_id is null
--   event_hour        - hour of day (UTC) from event_timestamp
-- No business logic, no joins across sources.

{{ config(materialized='view') }}

with source as (
    select
        event_id,
        event_date,
        event_timestamp,
        event_name,
        user_pseudo_id,
        session_id,
        channel_id,
        page_location,
        event_value,
        inserted_at
    from {{ source('raw', 'raw_ga4_events') }}
),

deduplicated as (
    select
        event_id,
        event_date,
        event_timestamp,
        event_name,
        user_pseudo_id,
        session_id,
        channel_id,
        page_location,
        event_value,
        inserted_at,
        row_number() over (
            partition by event_id
            order by inserted_at desc
        ) as dedupe_row_num
    from source
)

select
    event_id,
    event_date,
    event_timestamp,
    event_name,
    user_pseudo_id,
    session_id,
    channel_id,
    page_location,
    regexp_extract(page_location, r'//[^/]+(/[^?#]*)') as page_path,
    event_value,
    user_pseudo_id is null as is_anonymous_user,
    extract(hour from event_timestamp) as event_hour,
    inserted_at
from deduplicated
where dedupe_row_num = 1