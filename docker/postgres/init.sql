-- PostgreSQL initialization script
-- Runs once when the container is first created.
-- Flyway manages all schema changes AFTER this initial setup.

-- Enable extensions used by the platform
CREATE EXTENSION IF NOT EXISTS "pgcrypto";    -- gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";  -- query performance stats

-- Grant privileges (container creates platform user and platform_db via env vars)
GRANT ALL PRIVILEGES ON DATABASE platform_db TO platform;

-- Set timezone
ALTER DATABASE platform_db SET timezone TO 'UTC';
