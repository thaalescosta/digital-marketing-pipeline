-- Channel dimension (PRD §8.3). Grain: channel_id. The 7 fixed channels (paid_search,
-- organic_search, direct, social, email, referral, organic_video) as the latest snapshot.

{{ config(materialized='table') }}

select
    channel_id,
    channel_name,
    channel_group
from {{ ref('stg_reference__channels') }}