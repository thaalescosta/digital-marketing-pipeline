-- Ad performance fact (PRD §8.3). Grain: ad_performance_key (surrogate of campaign_id,
-- ad_group_id, ad_id, spend_date). One row per ad per day.
-- Incremental insert_overwrite on spend_date with a 3-day lookback (DESIGN Decision 1,
-- Code Pattern 2); the boundary macro reads run_date, or max(spend_date) loaded so far.
-- Full-refresh reproduces the same result as the incremental history.

{{ config(
    materialized='incremental',
    partition_by={'field': 'spend_date', 'data_type': 'date'},
    incremental_strategy='insert_overwrite',
    cluster_by=['campaign_id']
) }}

with source as (
    select
        ad_performance_key,
        campaign_id,
        channel_id,
        ad_group_id,
        ad_id,
        spend_date,
        spend_usd,
        impressions,
        clicks,
        conversions,
        avg_order_value,
        estimated_revenue_usd
    from {{ ref('stg_google_ads__campaign_daily') }}
    {% if is_incremental() %}
    where spend_date >= date_sub(
        {{ marketing_lookback_boundary('spend_date') }},
        interval {{ var('lookback_days', 3) }} day
    )
    {% endif %}
)

select
    s.ad_performance_key,
    s.spend_date,
    d.date_sk,
    s.campaign_id,
    s.channel_id,
    s.ad_group_id,
    s.ad_id,
    s.spend_usd,
    s.impressions,
    s.clicks,
    s.conversions,
    s.avg_order_value,
    s.estimated_revenue_usd
from source as s
left join {{ ref('dim_date') }} as d
    on d.date = s.spend_date