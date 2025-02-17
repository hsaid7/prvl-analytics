-- tests/custom_tests.sql

{% test no_negative_venue_capacity(relation_name) %}
  -- Already shown previously, ensures venue-level capacity >= 0
  with invalid_rows as (
    select *
    from {{ relation_name }}
    where capacity_adults < 0
  )
  select count(*) as num_errors
  from invalid_rows
  having count(*) = 0
{% endtest %}


{% test no_negative_partner_capacity(relation_name) %}
  -- Partner-level capacity must also be non-negative
  with invalid_rows as (
    select *
    from {{ relation_name }}
    where capacity_adults < 0
  )
  select count(*) as num_errors
  from invalid_rows
  having count(*) = 0
{% endtest %}


{% test no_negative_usage(relation_name) %}
  -- Check that usage metrics (unique_checkins, total_checkins, peak_usage) are never negative
  with invalid_rows as (
    select *
    from {{ relation_name }}
    where (unique_checkins < 0 or total_checkins < 0 or peak_usage < 0)
  )
  select count(*) as num_errors
  from invalid_rows
  having count(*) = 0
{% endtest %}


{% test no_missing_policy_for_revolving_usage(relation_name) %}
  /*
    Ensures that if we have revolve usage for a venue_id/day, there's a corresponding policy row
    stating that the venue is revolving that day. 
    (We only check if there's usage in fact_daily_usage for revolve = true.)
  */
  with revolve_usage as (
    select distinct local_date, venue_id
    from {{ relation_name }}
    where is_revolving = true
      and venue_id is not null    -- excludes partner-level rows
  ),
  revolve_policy as (
    select distinct
        cast(policy_date_utc as date) as policy_date,
        venue_id
    from {{ ref('stg_fct_venue_daily_policies') }}
    where cast(is_revolving as boolean) = true
  ),
  missing as (
    select 
      u.local_date,
      u.venue_id
    from revolve_usage u
    left join revolve_policy p
      on u.local_date = p.policy_date
     and u.venue_id  = p.venue_id
    where p.venue_id is null
  )
  select count(*) as num_missing
  from missing
  having count(*) = 0
{% endtest %}


{% test no_missing_capacity_for_revolve_allocated(relation_name) %}
  /*
    If fact_daily_usage says this is revolve + allocated for (local_date, venue_id),
    we expect an entry in stg_fct_venue_daily_capacity for that date & venue 
    (unless usage is 0, in which case it might be okay, but typically we want a capacity row).
  */
  with revolve_allocated_usage as (
    select local_date, venue_id
    from {{ relation_name }}
    where is_revolving = true
      and access_distribution = 'per-venue'
      and total_checkins > 0  -- usage actually happened
  ),
  capacity_rows as (
    select 
      capacity_date_utc as capacity_date, 
      venue_id
    from {{ ref('stg_fct_venue_daily_capacity') }}
  ),
  missing as (
    select u.local_date, u.venue_id
    from revolve_allocated_usage u
    left join capacity_rows c
      on u.local_date = c.capacity_date
     and u.venue_id  = c.venue_id
    where c.venue_id is null
  )
  select count(*) as num_missing
  from missing
  having count(*) = 0
{% endtest %}


{% test no_revolving_concurrency_for_nonrevolve(relation_name) %}
  /*
    If a row is marked is_revolving = false, then peak_usage should always be NULL 
    or zero. We don't expect concurrency for non-revolve. 
  */
  with invalid_rows as (
    select *
    from {{ relation_name }}
    where is_revolving = false
      and peak_usage is not null
      and peak_usage > 0
  )
  select count(*) as num_errors
  from invalid_rows
  having count(*) = 0
{% endtest %}


{% test no_negative_excess_usage(relation_name) %}
  -- Excess usage should never be negative. 
  with invalid_rows as (
    select *
    from {{ relation_name }}
    where excess_usage < 0
  )
  select count(*) as num_errors
  from invalid_rows
  having count(*) = 0
{% endtest %}


{% test capacity_utilization_range(relation_name) %}
  -- capacity_utilization should be either null or >= 0
  -- (some might say it can't exceed 1.0, but we allow > 1.0 if usage > capacity => overage)
  with invalid_rows as (
    select *
    from {{ relation_name }}
    where capacity_utilization < 0
  )
  select count(*) as num_errors
  from invalid_rows
  having count(*) = 0
{% endtest %}
