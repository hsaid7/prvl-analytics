-- models/staging/stg_fct_venue_daily_capacity.sql


{{ config(materialized='table') }}

with raw as (
    select
        cast(date as date) as capacity_date_utc,
        venue_id,
        cast(capacity_adults as integer) as capacity_adults
    from {{ source('raw','fct_venue_daily_capacity') }}
)

select distinct * from raw
