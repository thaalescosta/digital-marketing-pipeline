-- Reporting: YouTube video performance, daily (PRD §8.3, §8.4). Grain: date x video.
-- Daily metrics from fct_youtube_video_daily joined to dim_video; cumulative views /
-- watch time by video over date (window functions). avg_view_duration_min and
-- engagement_rate are safe_divide of the daily numerators/denominators (never averaged).

{{ config(materialized='table') }}

with video_daily as (
    select
        y.video_date as date,
        y.video_id,
        v.video_title,
        v.published_at,
        y.days_since_published,
        y.views,
        y.watch_time_min,
        y.likes,
        y.comments,
        y.shares,
        y.subscribers_gained
    from {{ ref('fct_youtube_video_daily') }} as y
    left join {{ ref('dim_video') }} as v
        on v.video_id = y.video_id
)

select
    date,
    video_id,
    video_title,
    published_at,
    days_since_published,
    views,
    watch_time_min,
    likes,
    comments,
    shares,
    subscribers_gained,
    coalesce({{ dbt_utils.safe_divide('watch_time_min', 'views') }}, 0) as avg_view_duration_min,
    coalesce({{ dbt_utils.safe_divide('likes + comments + shares', 'views') }}, 0) as engagement_rate,
    sum(views) over (
        partition by video_id
        order by date
        rows between unbounded preceding and current row
    ) as cumulative_views,
    sum(watch_time_min) over (
        partition by video_id
        order by date
        rows between unbounded preceding and current row
    ) as cumulative_watch_time_min
from video_daily