-- models/marts/fact_daily_usage.sql

{{ config(materialized='table') }}

with

/* ###################################################################
   1) REVOLVING, ALLOCATED (per-venue)
###################################################################### */
revolve_allocated_events as (
    select
        r.event_date_local as local_date,
        p.partner_id,
        r.venue_id,
        p.is_revolving,
        p.access_distribution,
        case 
          when r.event_name = 'enter' then 1 
          when r.event_name = 'leave' then -1 
          else 0
        end as delta,
        r.event_timestamp_local as evt_time
    from {{ ref('stg_fct_venue_register') }} r
    join {{ ref('stg_fct_venue_daily_policies') }} p
      on  r.event_date_utc = p.policy_date_utc
      and r.venue_id       = p.venue_id
    where p.is_revolving = true
      and p.access_distribution = 'per-venue'
),

/* STEP 1a: Compute running occupancy via window function */
revolve_allocated_running as (
    select
        local_date,
        partner_id,
        venue_id,
        evt_time,
        -- This is a window function, which we do not nest in an aggregate
        sum(delta) over (
          partition by local_date, partner_id, venue_id
          order by evt_time
          rows between unbounded preceding and current row
        ) as running_occupancy,
        case when delta = 1 then 1 else 0 end as enter_flag
    from revolve_allocated_events
),

/* STEP 1b: Group to find peak usage and total enters */
revolve_allocated_occupancy as (
    select
        local_date,
        partner_id,
        venue_id,
        max(running_occupancy) as peak_usage,
        sum(enter_flag) as total_enters
    from revolve_allocated_running
    group by 1,2,3
),

revolve_allocated_joined as (
    select
        occ.local_date,
        occ.partner_id,
        occ.venue_id,
        true  as is_revolving,
        'per-venue' as access_distribution,
        null::bigint as unique_checkins,  -- revolve uses concurrency
        occ.total_enters as total_checkins,
        occ.peak_usage,
        cap.capacity_adults as capacity
    from revolve_allocated_occupancy occ
    left join {{ ref('stg_fct_venue_daily_capacity') }} cap
           on occ.local_date = cap.capacity_date_utc
          and occ.venue_id   = cap.venue_id
),

revolve_allocated_final as (
    select
        local_date,
        partner_id,
        venue_id,
        is_revolving,
        access_distribution,
        unique_checkins,
        total_checkins,
        peak_usage,
        capacity,
        case 
            when capacity is null or capacity = 0 then null
            else peak_usage * 1.0 / capacity
        end as capacity_utilization,
        case 
            when peak_usage > capacity then peak_usage - capacity
            else 0
        end as excess_usage
    from revolve_allocated_joined
),

/* ###################################################################
   2) REVOLVING, SHARED (across-locations, partner-level concurrency)
###################################################################### */
revolve_shared_events as (
    select
        r.event_date_local as local_date,
        p.partner_id,
        null as venue_id,
        p.is_revolving,
        p.access_distribution,
        case 
          when r.event_name = 'enter' then 1 
          when r.event_name = 'leave' then -1 
          else 0
        end as delta,
        r.event_timestamp_local as evt_time
    from {{ ref('stg_fct_venue_register') }} r
    join {{ ref('stg_fct_venue_daily_policies') }} p
      on  r.event_date_utc = p.policy_date_utc
      and r.venue_id       = p.venue_id
    where p.is_revolving = true
      and p.access_distribution = 'across-locations'
),

/* STEP 2a: Compute running occupancy for revolve+shared at partner level */
revolve_shared_running as (
    select
        local_date,
        partner_id,
        evt_time,
        sum(delta) over (
          partition by local_date, partner_id
          order by evt_time
          rows between unbounded preceding and current row
        ) as running_occupancy,
        case when delta = 1 then 1 else 0 end as enter_flag
    from revolve_shared_events
),

/* STEP 2b: Group to find peak usage at partner level */
revolve_shared_occupancy as (
    select
        local_date,
        partner_id,
        max(running_occupancy) as peak_usage,
        sum(enter_flag) as total_enters
    from revolve_shared_running
    group by 1,2
),

revolve_shared_joined as (
    select
        occ.local_date,
        occ.partner_id,
        null as venue_id,
        true  as is_revolving,
        'across-locations' as access_distribution,
        null::bigint as unique_checkins,
        occ.total_enters as total_checkins,
        occ.peak_usage,
        cap.capacity_adults as capacity
    from revolve_shared_occupancy occ
    left join {{ ref('stg_fct_partner_daily_capacity') }} cap
           on occ.local_date = cap.capacity_date_utc
          and occ.partner_id = cap.partner_id
),

revolve_shared_final as (
    select
        local_date,
        partner_id,
        venue_id,
        is_revolving,
        access_distribution,
        unique_checkins,
        total_checkins,
        peak_usage,
        capacity,
        case 
            when capacity is null or capacity = 0 then null
            else peak_usage * 1.0 / capacity
        end as capacity_utilization,
        case 
            when peak_usage > capacity then peak_usage - capacity
            else 0
        end as excess_usage
    from revolve_shared_joined
),

/* ###################################################################
   3) NON-REVOLVING, ALLOCATED (unique daily visitors per venue)
###################################################################### */
non_revolve_allocated_users as (
    select
        r.event_date_local as local_date,
        p.partner_id,
        r.venue_id,
        p.is_revolving,
        p.access_distribution,
        r.user_id
    from {{ ref('stg_fct_venue_register') }} r
    join {{ ref('stg_fct_venue_daily_policies') }} p
      on  r.event_date_utc = p.policy_date_utc
      and r.venue_id       = p.venue_id
    where p.is_revolving = false
      and p.access_distribution = 'per-venue'
      and r.event_name = 'enter'
),
non_revolve_allocated_agg as (
    select
        local_date,
        partner_id,
        venue_id,
        false as is_revolving,
        'per-venue' as access_distribution,
        count(distinct user_id) as unique_checkins,
        count(*) as total_checkins
    from non_revolve_allocated_users
    group by 1,2,3
),
non_revolve_allocated_joined as (
    select
        agg.local_date,
        agg.partner_id,
        agg.venue_id,
        agg.is_revolving,
        agg.access_distribution,
        agg.unique_checkins,
        agg.total_checkins,
        null::bigint as peak_usage,
        cap.capacity_adults as capacity
    from non_revolve_allocated_agg agg
    left join {{ ref('stg_fct_venue_daily_capacity') }} cap
           on agg.local_date = cap.capacity_date_utc
          and agg.venue_id   = cap.venue_id
),
non_revolve_allocated_final as (
    select
        local_date,
        partner_id,
        venue_id,
        is_revolving,
        access_distribution,
        unique_checkins,
        total_checkins,
        peak_usage,
        capacity,
        case 
            when capacity is null or capacity = 0 then null
            else unique_checkins * 1.0 / capacity
        end as capacity_utilization,
        case 
            when unique_checkins > capacity then unique_checkins - capacity
            else 0
        end as excess_usage
    from non_revolve_allocated_joined
),

/* ###################################################################
   4) NON-REVOLVING, SHARED (partner-level distinct users)
###################################################################### */
non_revolve_shared_users as (
    select
        r.event_date_local as local_date,
        p.partner_id,
        null as venue_id,
        p.is_revolving,
        p.access_distribution,
        r.user_id
    from {{ ref('stg_fct_venue_register') }} r
    join {{ ref('stg_fct_venue_daily_policies') }} p
      on  r.event_date_utc = p.policy_date_utc
      and r.venue_id       = p.venue_id
    where p.is_revolving = false
      and p.access_distribution = 'across-locations'
      and r.event_name = 'enter'
),
non_revolve_shared_agg as (
    select
        local_date,
        partner_id,
        null as venue_id,
        false as is_revolving,
        'across-locations' as access_distribution,
        count(distinct user_id) as unique_checkins,
        count(*) as total_checkins
    from non_revolve_shared_users
    group by 1,2,3
),
non_revolve_shared_joined as (
    select
        agg.local_date,
        agg.partner_id,
        agg.venue_id,
        agg.is_revolving,
        agg.access_distribution,
        agg.unique_checkins,
        agg.total_checkins,
        null::bigint as peak_usage,
        cap.capacity_adults as capacity
    from non_revolve_shared_agg agg
    left join {{ ref('stg_fct_partner_daily_capacity') }} cap
           on agg.local_date = cap.capacity_date_utc
          and agg.partner_id = cap.partner_id
),
non_revolve_shared_final as (
    select
        local_date,
        partner_id,
        venue_id,
        is_revolving,
        access_distribution,
        unique_checkins,
        total_checkins,
        peak_usage,
        capacity,
        case
            when capacity is null or capacity = 0 then null
            else unique_checkins * 1.0 / capacity
        end as capacity_utilization,
        case 
            when unique_checkins > capacity then unique_checkins - capacity
            else 0
        end as excess_usage
    from non_revolve_shared_joined
),

/* ###################################################################
   UNION ALL
###################################################################### */
unioned as (
    select * from revolve_allocated_final
    union all
    select * from revolve_shared_final
    union all
    select * from non_revolve_allocated_final
    union all
    select * from non_revolve_shared_final
)

select
    local_date,
    u.partner_id,
    p.partner_dummy_name,
    u.venue_id,
    v.venue_dummy_name,
    is_revolving,
    access_distribution,
    coalesce(unique_checkins, 0)::bigint as unique_checkins,
    coalesce(total_checkins, 0)::bigint as total_checkins,
    peak_usage::bigint as peak_usage,
    capacity::bigint as capacity,
    capacity_utilization,
    excess_usage::bigint as excess_usage
from unioned u
left join  {{ ref('dim_venues') }} v
on v.venue_id = u.venue_id
left join  {{ ref('dim_partners') }} p
on u.partner_id = p.partner_id