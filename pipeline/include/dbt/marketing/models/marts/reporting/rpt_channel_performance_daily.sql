-- Reporting: channel performance, daily (PRD §8.3, §8.4). Grain: date x channel.
-- Joins GA4 session metrics and Google Ads metrics on the conformed channel_id from
-- dim_channel; zero-fills missing metrics via a date x channel scaffold (left join
-- dim_date x dim_channel + coalesce). All ratios use safe_divide on the SUM at this grain
-- (never averaged). roas_estimated is labelled ESTIMATED (PRD §8.4).

{{ config(materialized='table') }}

with date_channel as (
    select
        d.date,
        c.channel_id,
        c.channel_name,
        c.channel_group
    from {{ ref('dim_date') }} as d
    cross join {{ ref('dim_channel') }} as c
),

ga4_sessions as (
    select
        session_date as date,
        channel_id,
        count(*) as sessions,
        count(distinct user_pseudo_id) as users,
        countif(has_purchase) as purchases,
        sum(purchase_value) as revenue_usd
    from {{ ref('fct_web_sessions') }}
    group by 1, 2
),

ads_performance as (
    select
        spend_date as date,
        channel_id,
        sum(spend_usd) as ad_spend_usd,
        sum(impressions) as ad_impressions,
        sum(clicks) as ad_clicks,
        sum(conversions) as ad_conversions,
        sum(estimated_revenue_usd) as ad_estimated_revenue_usd
    from {{ ref('fct_ad_performance_daily') }}
    group by 1, 2
)

select
    dc.date,
    dc.channel_id,
    dc.channel_name,
    dc.channel_group,
    coalesce(g.sessions, 0) as sessions,
    coalesce(g.users, 0) as users,
    coalesce(g.purchases, 0) as purchases,
    coalesce(g.revenue_usd, 0) as revenue_usd,
    coalesce({{ dbt_utils.safe_divide('g.purchases', 'g.sessions') }}, 0) as session_to_purchase_rate,
    coalesce(a.ad_spend_usd, 0) as ad_spend_usd,
    coalesce(a.ad_impressions, 0) as ad_impressions,
    coalesce(a.ad_clicks, 0) as ad_clicks,
    coalesce(a.ad_conversions, 0) as ad_conversions,
    coalesce(a.ad_estimated_revenue_usd, 0) as ad_estimated_revenue_usd,
    coalesce({{ dbt_utils.safe_divide('a.ad_clicks', 'a.ad_impressions') }}, 0) as ctr,
    coalesce({{ dbt_utils.safe_divide('a.ad_spend_usd', 'a.ad_clicks') }}, 0) as cpc,
    coalesce({{ dbt_utils.safe_divide('a.ad_estimated_revenue_usd', 'a.ad_spend_usd') }}, 0) as roas_estimated
from date_channel as dc
left join ga4_sessions as g
    on g.date = dc.date
    and g.channel_id = dc.channel_id
left join ads_performance as a
    on a.date = dc.date
    and a.channel_id = dc.channel_id