{{ 
    config (
        materialized="table", 
        unique_key="date_id"
    ) 
}}

with event_dates as (
    select 
        min(date(created_at)) as start_date, 
        max(date(created_at)) as end_date
    from {{ ref("events") }}
),

date_spine as (
    select 
        calendar_date
    from event_dates,
    unnest(generate_date_array(start_date, end_date)) as calendar_date
),

calendar as (
    select
        calendar_date,
        date_trunc(calendar_date, month) as start_of_month,
        date_trunc(calendar_date, year) as start_of_year,
        date_trunc(calendar_date, week) as start_of_week,
        farm_fingerprint(cast(calendar_date as string)) as date_id
    from date_spine
)

select * from calendar

{% if is_incremental() %}
    -- This filter only runs on incremental builds.
    -- It ensures we only insert new dates not already in the target table.
    where date_id > (select max(date_id) from {{ this }})
{% endif %}
