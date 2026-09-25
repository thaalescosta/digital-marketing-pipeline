-- Staging view over raw_ads_campaign_daily (PRD §8.2).
-- Grain: ad_performance_key = surrogate of (campaign_id, ad_group_id, ad_id, spend_date).
-- Dedupes on the key (latest inserted_at) and adds estimated_revenue_usd =
-- conversions * avg_order_value (PRD §8.4). No business logic, no joins across sources.

{{ config(materialized='view') }}

with source as (
    select
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
        inserted_at,
        _load_date
    from {{ source('raw', 'raw_ads_campaign_daily') }}
),

deduplicated as (
    select
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
        inserted_at,
        _load_date,
        row_number() over (
            partition by campaign_id, ad_group_id, ad_id, spend_date
            order by inserted_at desc
        ) as dedupe_row_num
    from source
)

select
    {{ dbt_utils.generate_surrogate_key(['campaign_id', 'ad_group_id', 'ad_id', 'spend_date']) }} as ad_performance_key,
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
    conversions * avg_order_value as estimated_revenue_usd,
    inserted_at,
    _load_date
from deduplicated
where dedupe_row_num = 1