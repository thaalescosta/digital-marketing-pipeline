-- Reporting: campaign performance, daily (PRD §8.3, §8.4). Grain: date x campaign.
-- Ads metrics are summed from the ad-level fact to the campaign x date grain; ratios are
-- safe_divide of the SUMMED numerators/denominators (never averaged) per PRD §8.4.
-- budget_utilization = spend / daily_budget may exceed 1 (spend can reach ~1.3x budget, D8).
-- Rows exist only for (date, campaign) combinations with ads activity — no zero-fill here
-- (unlike rpt_channel_performance_daily) to avoid phantom rows for campaigns outside their
-- start/end window.

{{ config(materialized='table') }}

with campaign_daily as (
    select
        a.spend_date as date,
        a.campaign_id,
        c.campaign_name,
        c.campaign_type,
        c.status,
        sum(a.spend_usd) as spend_usd,
        sum(a.impressions) as impressions,
        sum(a.clicks) as clicks,
        sum(a.conversions) as conversions,
        sum(a.estimated_revenue_usd) as estimated_revenue_usd,
        max(c.daily_budget_usd) as daily_budget_usd
    from {{ ref('fct_ad_performance_daily') }} as a
    left join {{ ref('dim_campaign') }} as c
        on c.campaign_id = a.campaign_id
    group by 1, 2, 3, 4, 5
)

select
    date,
    campaign_id,
    campaign_name,
    campaign_type,
    status,
    spend_usd,
    daily_budget_usd,
    coalesce({{ dbt_utils.safe_divide('spend_usd', 'daily_budget_usd') }}, 0) as budget_utilization,
    impressions,
    clicks,
    conversions,
    estimated_revenue_usd,
    coalesce({{ dbt_utils.safe_divide('clicks', 'impressions') }}, 0) as ctr,
    coalesce({{ dbt_utils.safe_divide('spend_usd', 'clicks') }}, 0) as cpc,
    coalesce({{ dbt_utils.safe_divide('conversions', 'clicks') }}, 0) as cvr,
    coalesce({{ dbt_utils.safe_divide('spend_usd', 'conversions') }}, 0) as cpa,
    coalesce({{ dbt_utils.safe_divide('estimated_revenue_usd', 'spend_usd') }}, 0) as roas
from campaign_daily