with abandoned_checkouts as (
    select
        created_month,
        traffic_source,
        count(session_id) as monthly_abandoned_checkouts
    from {{ref("abandoned_checkouts")}}
    group by 1, 2
),

total_monthly_sessions as (
    select
        date(date_trunc(created_at, month)) as created_month,
        traffic_source,
        count(distinct session_id) as monthly_sessions
    from {{ref("fct_events")}}
    group by 1, 2
),

calendar as (
    select 
        start_of_month 
    from {{ref("dim_calendar")}}
),

abandoned_checkout_rate as (
    select
        start_of_month as created_month,
        c.traffic_source,
        monthly_abandoned_checkouts,
        monthly_sessions,
        safe_divide(monthly_abandoned_checkouts,monthly_sessions) as monthly_abandoned_checkout_rate
    from calendar a
    left join abandoned_checkouts b on a.start_of_month = b.created_month
    left join total_monthly_sessions c on a.start_of_month = c.created_month and b.traffic_source = c.traffic_source
)

select * from abandoned_checkout_rate