#!/bin/bash

# Exit script on any error
set -e

# Function to wait for PostgreSQL
wait_for_postgres() {
  until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres; do
    >&2 echo "Postgres is unavailable - sleeping"
    sleep 1
  done
  >&2 echo "Postgres is up - continuing"
}

# Set default values if not set
DB_HOST=${DB_HOST:-postgres}
DB_PORT=${DB_PORT:-5432}
DB_USER=${DB_USER:-db_user}

# Wait for PostgreSQL to be ready
wait_for_postgres

# Initialize the Airflow database
airflow db init

# Create an admin user if it doesn't exist
if airflow users list | grep -w "airflow"; then
    echo "Admin user 'airflow' already exists. Skipping creation."
else
    airflow users create \
        --username airflow \
        --firstname Airflow \
        --lastname Admin \
        --role Admin \
        --email admin@example.com \
        --password airflow
fi

# Run the scheduler in the background
airflow scheduler &

# Start the webserver
exec airflow webserver