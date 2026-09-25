-- Singular test (PRD §8.5 P1): all GA4 events of one session must share a single
-- channel_id. Fails if any session maps to more than one channel.
select
    session_id,
    count(distinct channel_id) as distinct_channels
from {{ ref('stg_ga4__events') }}
group by session_id
having count(distinct channel_id) > 1