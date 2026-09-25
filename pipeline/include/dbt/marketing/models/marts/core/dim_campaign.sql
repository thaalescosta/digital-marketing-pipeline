-- Campaign dimension (PRD §8.3, SCD1 end state). Grain: campaign_id.
-- Attributes from the latest campaign snapshot; campaign_duration_days = end_date - start_date.

{{ config(materialized='table') }}

select
    campaign_id,
    campaign_name,
    campaign_type,
    status,
    start_date,
    end_date,
    daily_budget_usd,
    date_diff(end_date, start_date, day) as campaign_duration_days
from {{ ref('stg_google_ads__campaigns') }}