-- models/staging/stg_fct_venue_daily_policies.sql


{{ config(materialized='table') }}

with raw as (
    select
        cast(date as date) as policy_date_utc,  -- The CSV indicates a date like 2024-10-14
        partner_id,
        venue_id,
        access_distribution,
        venue_type,
        cast(is_revolving as boolean) as is_revolving
    from {{ source('raw','fct_venue_daily_policies') }}
)

select distinct * from raw
