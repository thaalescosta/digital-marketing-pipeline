-- Reporting: conversion funnel, daily (PRD §8.3 P1, §3.3). Grain: date x channel.
-- Each stage is expressed as a percentage of SESSIONS (not step-to-step rates): funnel
-- events are generated independently with no enforced order, so a session can have
-- begin_checkout without add_to_cart (PRD §3.3). Rates are safe_divide of stage sessions
-- over total sessions (never averaged).

{{ config(materialized='table') }}

with sessions as (
    select
        session_date as date,
        channel_id,
        count(*) as sessions,
        countif(page_view_count > 0) as sessions_with_page_view,
        countif(has_add_to_cart) as sessions_with_add_to_cart,
        countif(has_begin_checkout) as sessions_with_begin_checkout,
        countif(has_purchase) as sessions_with_purchase
    from {{ ref('fct_web_sessions') }}
    group by 1, 2
)

select
    s.date,
    s.channel_id,
    c.channel_name,
    c.channel_group,
    s.sessions,
    s.sessions_with_page_view,
    coalesce({{ dbt_utils.safe_divide('s.sessions_with_page_view', 's.sessions') }}, 0) as page_view_rate,
    s.sessions_with_add_to_cart,
    coalesce({{ dbt_utils.safe_divide('s.sessions_with_add_to_cart', 's.sessions') }}, 0) as add_to_cart_rate,
    s.sessions_with_begin_checkout,
    coalesce({{ dbt_utils.safe_divide('s.sessions_with_begin_checkout', 's.sessions') }}, 0) as begin_checkout_rate,
    s.sessions_with_purchase,
    coalesce({{ dbt_utils.safe_divide('s.sessions_with_purchase', 's.sessions') }}, 0) as purchase_rate
from sessions as s
left join {{ ref('dim_channel') }} as c
    on c.channel_id = s.channel_id