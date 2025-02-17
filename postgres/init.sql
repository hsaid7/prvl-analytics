-- Create user if not exists
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = 'db_user') THEN
      CREATE USER db_user WITH PASSWORD 'db_password';
   END IF;
END
$do$;

-- Create metabase_user if not exists
DO $$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = 'metabase_user') THEN
      CREATE USER metabase_user WITH PASSWORD 'metabase_password';
   END IF;
END
$$;


-- Create airflow_db if it doesn't exist
SELECT
  'CREATE DATABASE airflow_db OWNER db_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')
\gexec

-- Create metabase DB if it doesn't exist
SELECT
  'CREATE DATABASE metabase OWNER metabase_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')
\gexec