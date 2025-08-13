with import_source as (
    select * from {{source('google_analytics', 'events_*')}}
),

cleanup as (
    select
        parse_date('%Y%m%d', event_date) as event_date,
        timestamp_micros(event_timestamp) as event_timestamp,
        event_name,
        event_params,
        event_previous_timestamp,
        event_value_in_usd, 
        event_bundle_sequence_id,
        event_server_timestamp_offset,
        user_id,
        user_pseudo_id,
        privacy_info,
        user_properties,
        timestamp_micros(user_first_touch_timestamp) as user_first_touch_timestamp,
        user_ltv,
        device,
        geo,
        app_info,
        traffic_source,
        stream_id,
        platform,
        event_dimensions,
        ecommerce,
        items,
        -- creates a unique key using the user_pseudo_id, event_bundle_sequence_id, and event_timestamp
        farm_fingerprint(concat(cast(user_pseudo_id as string), cast(event_bundle_sequence_id as string), cast(event_timestamp as string))) as customer_event_id
    from import_source
)

select * from cleanup