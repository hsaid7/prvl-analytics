-- models/staging/stg_fct_venue_register.sql


{{ config(materialized='table') }}

with source_data as (
    select
        _id,
        user_id,
        venue_id,
        created_at as created_at_utc,       -- raw
        event_timestamp as event_timestamp_utc,
        event_name,
        event_triggered_by,
        event_type,

        4 as venue_timezone_offset_hours  -- Hardcode 4 for Dubai for now
    from {{ source('raw','fct_venue_register') }}
),

converted as (
    select
        _id,
        user_id,
        venue_id,
        event_name,
        event_triggered_by,
        event_type,

        created_at_utc,
        event_timestamp_utc,
        
        -- Convert from UTC to local
        event_timestamp_utc + (venue_timezone_offset_hours * interval '1' hour) as event_timestamp_local,
        cast(event_timestamp_utc as date) as event_date_utc,
        cast(event_timestamp_utc + (venue_timezone_offset_hours * interval '1' hour) as date) as event_date_local

    from source_data
)

select * from converted
