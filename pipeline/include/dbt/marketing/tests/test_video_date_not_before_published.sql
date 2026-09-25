-- Singular test (PRD §8.5 P1): a YouTube daily fact row must never have a video_date
-- before the video was published. Fails if any row is returned.
select
    y.video_date,
    y.video_id,
    v.published_at
from {{ ref('fct_youtube_video_daily') }} as y
left join {{ ref('dim_video') }} as v
    on v.video_id = y.video_id
where y.video_date < v.published_at