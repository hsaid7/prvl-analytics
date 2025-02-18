# Privilee Analytics - Data Engineer Challenge

## Overview

This repository solves the “Data Engineer Challenge” by providing:
- A daily usage analytics pipeline for revolve vs. non-revolve capacity.
- Allocated vs. shared distribution logic.
- Timezone conversions from UTC to Dubai (UTC+4).
- An Airflow pipeline to orchestrate ingestion + dbt tasks.
- Metabase dashboards for final insights.
---

## Table of Contents
1. [Overview](#overview)  
2. [Project Structure](#project-structure)  
3. [Build & Run Instructions](#build--run-instructions)  
4. [Data Flow & Business Logic](#data-flow--business-logic)  
5. [Metabase Dashboards](#metabase-dashboards)  
6. [Data Checks & Challenge Requirements](#data-checks--challenge-requirements)

---


---

## Project Structure

├── docker-compose.yml # Services: Postgres, Airflow (optional), Metabase  
├── data/  
│   ├── fct_partner_daily_capacity.csv  
│   ├── fct_venue_daily_capacity.csv  
│   ├── fct_venue_daily_policies.csv  
│   └── fct_venue_register.csv # All raw CSV data  
├── airflow/  
│   ├── dags/  
│   │   └── venue_usage_dag.py # Example DAG orchestrating dbt  
│   ├── Dockerfile  
│   └── requirements.txt  
├── dbt/  
│   ├── dbt_project.yml  
│   ├── profiles.yml # DB credentials for dev  
│   ├── macros/  
│   │   └── custom_tests.sql # Additional data checks  
│   ├── models/  
│   │   ├── staging/ # stg_fct_*.sql staging transformations  
│   │   ├── dims/ # dim_venues.sql, dim_partners.sql  
│   │   └── marts/  
│   │       └── fact_daily_usage.sql # Final aggregated usage  
│   ├── tests/  
│   │   └── schema_tests.yml # dbt tests referencing custom macros  
│   └── packages.yml # If using external dbt packages  
├── postgres/  
│   ├── data/ # Docker volume for Postgres data  
│   └── init.sql # Potentially loads CSV into raw tables  
├── scripts/  
│   └── data_ingestion.py # Example ingestion script  
└── README.md

---

## Build & Run Instructions

### Step 1: Clone this Repo
```bash
git clone https://github.com/hsaid7/prvl-analytics.git
cd prvl-analytics
```
### Step 2: Launch Docker (Postgres, Airflow, Metabase)
```bash
docker-compose up -d
# Postgres on port 5432
# Airflow on port 8080
# Metabase on port 3000
```


### Step 3: Ingest Data

- Visit Airflow at http://localhost:8080 (login as configured in docker-compose.yml).

- Enable the venue_usage_pipeline DAG to run ingestion + dbt tasks.


### Step 4: Metabase at http://localhost:3000:
- Connect to dbt_db on host postgres with credentials from docker-compose.
- Explore final usage in fact_daily_usage.

### Step 5: Check the Final Table in Postgres

Verify that the `fact_daily_usage` table is created in Postgres, including columns for:
- `unique_checkins` (for non-revolving)
- `total_checkins` (all enter events)
- `capacity_utilization`
- `peak_usage` (for revolving)
- `excess_usage`

### Step 6: Set up Metabase
1. Open [http://localhost:3000](http://localhost:3000) in your browser.
2. Set up a new Postgres connection with the following details:
   - **Host**: `postgres`
   - **Database**: `dbt_db`
   - **User**: `db_user`
   - **Password**: `db_password`
3. Create dashboards to visualize daily usage metrics.

## Data Ingestion & Airflow Pipeline
 

- **scripts/data_ingestion.py**: 
  - Waits for Postgres readiness,
  - Creates DB and raw tables,
  - Loads CSV files into `raw` schema.

- **Airflow DAG** (`venue_usage_pipeline`):
  1) extract_data (runs ingestion script),
  2) dbt_deps (install packages),
  3) dbt_run (build models),
  4) dbt_test (run data checks).

## DBT Transformations & Final Query

- We build **staging** models converting UTC → UTC+4, standardizing data.
- We unify revolve vs. non-revolve, allocated vs. shared in a final model called `fact_daily_usage`.
- This final model merges four branches:
  1. Revolving, allocated
  2. Revolving, shared
  3. Non-revolving, allocated
  4. Non-revolving, shared

- The result has daily usage stats:
  - unique_checkins, total_checkins, peak_usage, capacity_utilization, excess_usage, etc.
  
For the specific SQL logic, see the Notion page (final query snippet).


## Data Checks in Sources & Custom Tests

- **Source-level tests** (in `sources.yml`): 
  - Check `not_null`, `accepted_values` for columns in fct_venue_register, policies, capacities.
- **Custom dbt tests** (macros/custom_tests.sql + schema_tests.yml): 
  - E.g., no_negative_venue_capacity, no_missing_policy_for_revolving_usage, etc.
- These ensure we catch invalid capacity, missing revolve policies, negative usage, etc.


## Meeting the Requirements

1. **Time Zone**: event_timestamp → UTC+4 in staging queries.
2. **Daily Usage**: final table aggregates data by (local_date, partner_id, venue_id).
3. **Unique Checkins**: non-revolve logic uses distinct user counts.
4. **Peak Usage**: revolve logic increments concurrency with +1/-1 per enter/leave.
5. **Capacity Utilization** & **Excess Usage**: usage vs. capacity for allocated or shared distributions.
6. **Airflow**: Orchestrates entire pipeline from CSV ingestion → dbt builds + tests.
7. **Metabase**: Delivers daily usage stats, capacity usage, concurrency, etc.

This fully addresses the daily usage statistics, revolve vs. non-revolve metrics, and data quality needs of the challenge.

