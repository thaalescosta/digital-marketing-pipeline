-- Date dimension (PRD §8.3, star-schema KB).
-- One row per date from dim_date_start to dim_date_end; date_sk is an INT in YYYYMMDD
-- format (e.g. 2026-01-01 -> 20260101) per the conformed date-dimension pattern.

{{ config(materialized='table') }}

with date_spine as (
    select
        date
    from unnest(
        generate_date_array(
            cast('{{ var('dim_date_start', '2026-01-01') }}' as date),
            cast('{{ var('dim_date_end', '2026-12-31') }}' as date),
            interval 1 day
        )
    ) as date
)

select
    date,
    cast(format_date('%Y%m%d', date) as int64) as date_sk,
    extract(year from date) as year,
    extract(quarter from date) as quarter,
    extract(month from date) as month,
    format_date('%B', date) as month_name,
    date_trunc(date, week(monday)) as week_start,
    extract(dayofweek from date) as day_of_week,
    extract(dayofweek from date) in (1, 7) as is_weekend
from date_spine