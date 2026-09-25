-- Staging view over raw_dim_channel (PRD §8.2).
-- Grain: channel_id. Latest snapshot per key (max _snapshot_date, tie-break inserted_at) —
-- the channel master (7 fixed rows). No joins.

{{ config(materialized='view') }}

with source as (
    select
        channel_id,
        channel_name,
        channel_group,
        _snapshot_date,
        inserted_at
    from {{ source('raw', 'raw_dim_channel') }}
),

ranked as (
    select
        channel_id,
        channel_name,
        channel_group,
        _snapshot_date,
        inserted_at,
        row_number() over (
            partition by channel_id
            order by _snapshot_date desc, inserted_at desc
        ) as snapshot_row_num
    from source
)

select
    channel_id,
    channel_name,
    channel_group,
    _snapshot_date,
    inserted_at
from ranked
where snapshot_row_num = 1