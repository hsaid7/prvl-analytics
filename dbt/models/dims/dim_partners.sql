{{ config(materialized='table') }}

WITH unique_partners AS (
    SELECT DISTINCT partner_id FROM {{ ref('stg_fct_venue_daily_policies') }}
), numbered_partners AS (
    SELECT
        partner_id,
        'Partner ' || ROW_NUMBER() OVER (ORDER BY partner_id) AS partner_dummy_name
    FROM unique_partners
)

SELECT * FROM numbered_partners