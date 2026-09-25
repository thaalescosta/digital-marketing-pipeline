-- YouTube video daily fact (PRD §8.3). Grain: (video_id, video_date). One row per video
-- per day. Incremental insert_overwrite on video_date with a 3-day lookback (DESIGN
-- Decision 1); the boundary macro reads run_date, or max(video_date) loaded so far.
-- days_since_published is computed against dim_video.published_at. Full-refresh
-- reproduces the same result as the incremental history.

{{ config(
    materialized='incremental',
    partition_by={'field': 'video_date', 'data_type': 'date'},
    incremental_strategy='insert_overwrite',
    cluster_by=['video_id']
) }}

with source as (
    select
        video_daily_key,
        video_id,
        video_date,
        views,
        watch_time_min,
        likes,
        comments,
        shares,
        subscribers_gained
    from {{ ref('stg_youtube__video_daily') }}
    {% if is_incremental() %}
    where video_date >= date_sub(
        {{ marketing_lookback_boundary('video_date') }},
        interval {{ var('lookback_days', 3) }} day
    )
    {% endif %}
)

select
    s.video_daily_key,
    s.video_id,
    s.video_date,
    d.date_sk,
    s.views,
    s.watch_time_min,
    s.likes,
    s.comments,
    s.shares,
    s.subscribers_gained,
    date_diff(s.video_date, v.published_at, day) as days_since_published
from source as s
left join {{ ref('dim_date') }} as d
    on d.date = s.video_date
left join {{ ref('dim_video') }} as v
    on v.video_id = s.video_id