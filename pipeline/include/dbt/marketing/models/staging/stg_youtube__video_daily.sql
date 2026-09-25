-- Staging view over raw_youtube_video_daily (PRD §8.2).
-- Grain: (video_id, video_date) with a surrogate key for convenience.
-- Dedupes on the grain (latest inserted_at). No business logic, no joins.

{{ config(materialized='view') }}

with source as (
    select
        video_id,
        video_date,
        views,
        watch_time_min,
        likes,
        comments,
        shares,
        subscribers_gained,
        inserted_at,
        _load_date
    from {{ source('raw', 'raw_youtube_video_daily') }}
),

deduplicated as (
    select
        video_id,
        video_date,
        views,
        watch_time_min,
        likes,
        comments,
        shares,
        subscribers_gained,
        inserted_at,
        _load_date,
        row_number() over (
            partition by video_id, video_date
            order by inserted_at desc
        ) as dedupe_row_num
    from source
)

select
    {{ dbt_utils.generate_surrogate_key(['video_id', 'video_date']) }} as video_daily_key,
    video_id,
    video_date,
    views,
    watch_time_min,
    likes,
    comments,
    shares,
    subscribers_gained,
    inserted_at,
    _load_date
from deduplicated
where dedupe_row_num = 1