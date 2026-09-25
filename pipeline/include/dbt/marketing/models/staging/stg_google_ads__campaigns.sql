-- Staging view over raw_dim_campaign (PRD §8.2).
-- Grain: campaign_id. Resolves the daily dimension snapshots to the LATEST snapshot per
-- key (max _snapshot_date, tie-break inserted_at) — SCD1-style end state for dim_campaign.
-- No joins.

{{ config(materialized='view') }}

with source as (
    select
        campaign_id,
        campaign_name,
        campaign_type,
        status,
        start_date,
        end_date,
        daily_budget_usd,
        _snapshot_date,
        inserted_at
    from {{ source('raw', 'raw_dim_campaign') }}
),

ranked as (
    select
        campaign_id,
        campaign_name,
        campaign_type,
        status,
        start_date,
        end_date,
        daily_budget_usd,
        _snapshot_date,
        inserted_at,
        row_number() over (
            partition by campaign_id
            order by _snapshot_date desc, inserted_at desc
        ) as snapshot_row_num
    from source
)

select
    campaign_id,
    campaign_name,
    campaign_type,
    status,
    start_date,
    end_date,
    daily_budget_usd,
    _snapshot_date,
    inserted_at
from ranked
where snapshot_row_num = 1