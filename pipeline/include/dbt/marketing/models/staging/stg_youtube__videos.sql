-- Staging view over raw_dim_video (PRD §8.2).
-- Grain: video_id. Latest snapshot per key (max _snapshot_date, tie-break inserted_at) —
-- SCD1-style end state for dim_video. No joins.

{{ config(materialized='view') }}

with source as (
    select
        video_id,
        video_title,
        published_at,
        video_duration_min,
        _snapshot_date,
        inserted_at
    from {{ source('raw', 'raw_dim_video') }}
),

ranked as (
    select
        video_id,
        video_title,
        published_at,
        video_duration_min,
        _snapshot_date,
        inserted_at,
        row_number() over (
            partition by video_id
            order by _snapshot_date desc, inserted_at desc
        ) as snapshot_row_num
    from source
)

select
    video_id,
    video_title,
    published_at,
    video_duration_min,
    _snapshot_date,
    inserted_at
from ranked
where snapshot_row_num = 1