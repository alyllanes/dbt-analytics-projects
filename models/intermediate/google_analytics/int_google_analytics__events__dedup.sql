{{
    config(
        materialized='ephemeral'
    )
}}

with import_stage as (
    select * from {{ref('stg_google_analytics__events')}}
),

qualified as (
    select * from import_stage
    qualify 1=row_number() over (partition by customer_event_id order by event_timestamp desc)
)

select * from qualified