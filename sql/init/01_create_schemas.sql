-- ==============================================================================
-- E-COMMERCE ANALYTICS DATABASE - SCHEMA SETUP
-- Creates logical schemas for organizing different data domains
-- ==============================================================================

-- Raw data from Kafka topics
CREATE SCHEMA IF NOT EXISTS raw_data;

-- Processed/normalized business entities
CREATE SCHEMA IF NOT EXISTS analytics;

-- Future dbt staging models
CREATE SCHEMA IF NOT EXISTS staging;

-- Final data marts for reporting/dashboards
CREATE SCHEMA IF NOT EXISTS marts;

-- Monitoring and operational tables
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Create the database if it does not exist
DO $$
BEGIN
	IF NOT EXISTS (
		SELECT FROM pg_database WHERE datname = 'ecommerce_analytics'
	) THEN
		CREATE DATABASE ecommerce_analytics;
	END IF;
END
$$;

-- Set search path for convenience
ALTER DATABASE ecommerce_analytics SET search_path TO analytics, raw_data, staging, marts, public;

-- Grant permissions
GRANT USAGE ON SCHEMA raw_data TO admin;
GRANT USAGE ON SCHEMA analytics TO admin;
GRANT USAGE ON SCHEMA staging TO admin;
GRANT USAGE ON SCHEMA marts TO admin;
GRANT USAGE ON SCHEMA monitoring TO admin;

-- Create extensions for UUID and time functions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
