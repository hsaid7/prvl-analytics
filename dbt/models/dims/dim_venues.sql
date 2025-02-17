-- models/dims/dim_venues.sql

{{ config(materialized='table') }}

with distinct_venues as (
    -- Gather all distinct venue_ids
    select venue_id
    from {{ ref('stg_fct_venue_register') }}
    union distinct
    select venue_id
    from {{ ref('stg_fct_venue_daily_policies') }}
    union distinct
    select venue_id
    from {{ ref('stg_fct_venue_daily_capacity') }}
),

ranked as (
    select
        venue_id,
        row_number() over (order by venue_id) as rn
    from distinct_venues
),

assigned_names as (
    select
        venue_id,
        rn,
        'Venue ' || rn as venue_dummy_name
    from ranked
)

select
    v.venue_id,
    v.venue_dummy_name,
    4 as timezone_offset_hours,  -- Hardcoded for now

    -- Example: last policy date, if you want
    (
      select max(policy_date_utc)
      from {{ ref('stg_fct_venue_daily_policies') }} pol
      where pol.venue_id = v.venue_id
    ) as last_policy_date_utc,

    (
      select max(policy_date_utc) + (4 * interval '1' hour)
      from {{ ref('stg_fct_venue_daily_policies') }} pol
      where pol.venue_id = v.venue_id
    ) as last_policy_date_local

from assigned_names v
