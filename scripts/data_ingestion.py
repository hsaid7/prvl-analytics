import os
import psycopg2
import pandas as pd
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time



DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'dbt_db')
DB_USER = os.getenv('DB_USER', 'db_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'db_password')


def wait_for_postgres():
    retries = 10
    while retries > 0:
        try:
            conn = psycopg2.connect(
                dbname='postgres',
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
            )
            conn.close()
            print('PostgreSQL is up and running!')
            return True
        except psycopg2.OperationalError:
            print('PostgreSQL is unavailable, waiting 5 seconds...')
            time.sleep(5)
            retries -= 1
    raise Exception('Could not connect to PostgreSQL after several attempts.')

def create_database(dbname):
    try:
        conn = psycopg2.connect(
            dbname='postgres',
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        # Check if database exists
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{dbname}'")
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(f"CREATE DATABASE {dbname} OWNER {DB_USER}")
            print(f"Database '{dbname}' created successfully.")
        else:
            print(f"Database '{dbname}' already exists.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error creating database '{dbname}': {e}")
        raise

def create_tables():
    # Connect to the dbt_db database
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    
    cursor = conn.cursor()

    # Create schemas
    cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS analytics;
    """)

    # Create tables if they do not exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw.fct_venue_register (
        _id TEXT PRIMARY KEY,
        user_id TEXT,
        venue_id TEXT,
        created_at TIMESTAMP,
        event_timestamp TIMESTAMP,
        event_name TEXT,
        event_triggered_by TEXT,
        event_type TEXT
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw.fct_venue_daily_policies (
        date DATE,
        partner_id TEXT,
        venue_id TEXT,
        access_distribution TEXT,
        venue_type TEXT,
        is_revolving BOOLEAN
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw.fct_venue_daily_capacity (
        date DATE,
        venue_id TEXT,
        capacity_adults INTEGER
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw.fct_partner_daily_capacity (
        date DATE,
        partner_id TEXT,
        capacity_adults INTEGER
    );
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print('Tables created successfully.')

if wait_for_postgres():
    create_database(DB_NAME)
    create_tables()
else:
    print('PostgreSQL is not available. Exiting.')
    exit(1)


conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)
cursor = conn.cursor()

data_files = {
    'fct_venue_register.csv': 'raw.fct_venue_register',
    'fct_venue_daily_policies.csv': 'raw.fct_venue_daily_policies',
    'fct_venue_daily_capacity.csv': 'raw.fct_venue_daily_capacity',
    'fct_partner_daily_capacity.csv': 'raw.fct_partner_daily_capacity',
}

for file_name, table_name in data_files.items():
    df = pd.read_csv(f'/data/{file_name}')

    if 'is_revolving' in df.columns:
        df['is_revolving'] = df['is_revolving'].astype(bool)

    # Clean and validate data here if necessary

    # Prepare data for insertion
    tuples = [tuple(x) for x in df.to_numpy()]
    columns = ','.join(df.columns)
    placeholders = ','.join(['%s'] * len(df.columns))

    cursor.execute(f"DELETE FROM {table_name}")

    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

    cursor.executemany(insert_query, tuples)

conn.commit()
conn.close()