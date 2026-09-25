-- Video dimension (PRD §8.3, SCD1 end state). Grain: video_id.
-- Attributes from the latest video snapshot.

{{ config(materialized='table') }}

select
    video_id,
    video_title,
    published_at,
    video_duration_min
from {{ ref('stg_youtube__videos') }}