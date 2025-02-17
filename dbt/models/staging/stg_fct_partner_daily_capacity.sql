-- models/staging/stg_fct_partner_daily_capacity.sql

{{ config(materialized='table') }}

with raw as (
    select
        cast(date as date) as capacity_date_utc,
        partner_id,
        cast(capacity_adults as integer) as capacity_adults
    from {{ source('raw','fct_partner_daily_capacity') }}
)

select distinct * from raw
