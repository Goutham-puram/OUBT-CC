-- ============================================================================
-- Create External Schema for Redshift Spectrum
-- ============================================================================
-- This script creates an external schema that allows Redshift to query data
-- directly from S3 using the AWS Glue Data Catalog without loading the data.
--
-- Prerequisites:
-- 1. Redshift Serverless namespace and workgroup created
-- 2. IAM role with permissions for Glue Data Catalog and S3 access
-- 3. AWS Glue database 'nyc_taxi_catalog' exists with tables
-- 4. S3 bucket with NYC taxi data in Parquet format
--
-- Usage:
--   Replace <SPECTRUM_ROLE_ARN> with your actual IAM role ARN
--   Run this script using Redshift query editor or psql
-- ============================================================================

-- Drop existing schema if needed (be careful in production!)
-- DROP SCHEMA IF EXISTS spectrum_schema CASCADE;

-- Create external schema pointing to Glue Data Catalog
CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_schema
FROM DATA CATALOG
DATABASE 'nyc_taxi_catalog'
IAM_ROLE '<SPECTRUM_ROLE_ARN>'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Grant usage to appropriate users/groups
-- Adjust these grants based on your security requirements
GRANT USAGE ON SCHEMA spectrum_schema TO PUBLIC;

-- ============================================================================
-- Verify External Schema Creation
-- ============================================================================

-- List all external schemas
SELECT
    schemaname,
    schemacreator,
    schemaowner
FROM
    svv_external_schemas
WHERE
    schemaname = 'spectrum_schema';

-- List all external tables in the schema
SELECT
    schemaname,
    tablename,
    location
FROM
    svv_external_tables
WHERE
    schemaname = 'spectrum_schema'
ORDER BY
    tablename;

-- Show table properties for external tables
SELECT
    schemaname,
    tablename,
    values AS table_properties
FROM
    svv_external_columns
WHERE
    schemaname = 'spectrum_schema'
LIMIT 10;

-- ============================================================================
-- Sample Queries to Test External Schema
-- ============================================================================

-- Count rows in the external table (adjust table name as needed)
-- SELECT COUNT(*) FROM spectrum_schema.taxi_trips_curated;

-- Preview data from external table
-- SELECT * FROM spectrum_schema.taxi_trips_curated LIMIT 10;

-- Check data types and columns
-- SELECT
--     columnname,
--     external_type,
--     columnnum
-- FROM
--     svv_external_columns
-- WHERE
--     schemaname = 'spectrum_schema'
--     AND tablename = 'taxi_trips_curated'
-- ORDER BY
--     columnnum;

-- ============================================================================
-- Performance Tips for Spectrum Queries
-- ============================================================================
-- 1. Use columnar formats (Parquet, ORC) for better performance
-- 2. Partition data in S3 for partition pruning
-- 3. Use Parquet statistics for predicate pushdown
-- 4. Consider using COPY to load frequently accessed data into Redshift
-- 5. Use WHERE clauses to minimize data scanned in S3
--
-- Example: Query with partition pruning
-- SELECT
--     COUNT(*),
--     AVG(fare_amount)
-- FROM
--     spectrum_schema.taxi_trips_curated
-- WHERE
--     year = 2024
--     AND month = 1;
-- ============================================================================

-- ============================================================================
-- Troubleshooting Commands
-- ============================================================================

-- Check Spectrum query history and performance
-- SELECT
--     query,
--     segment,
--     s3_scanned_rows,
--     s3_scanned_bytes,
--     s3query_returned_rows,
--     s3query_returned_bytes,
--     files,
--     avg_request_parallelism
-- FROM
--     svl_s3query_summary
-- WHERE
--     query = pg_last_query_id()
-- ORDER BY
--     query, segment;

-- Check for any errors accessing external tables
-- SELECT
--     query,
--     line_number,
--     colname,
--     filename,
--     line,
--     err_reason
-- FROM
--     stl_load_errors
-- ORDER BY
--     query DESC
-- LIMIT 10;

-- View IAM role associated with Redshift cluster
-- SELECT
--     role_name,
--     role_arn,
--     role_type
-- FROM
--     svv_iam_roles;

-- ============================================================================
-- Cost Optimization Notes
-- ============================================================================
-- Redshift Spectrum pricing:
-- - $5 per TB of data scanned from S3
-- - Use Parquet/ORC compression to reduce data scanned
-- - Partition pruning can significantly reduce costs
-- - Consider loading frequently queried data into Redshift tables
--
-- Example cost calculation:
-- - 100 GB scanned per day = $0.50/day = $15/month
-- - 1 TB scanned per day = $5/day = $150/month
-- ============================================================================

-- ============================================================================
-- Security Best Practices
-- ============================================================================
-- 1. Use least privilege IAM roles
-- 2. Enable encryption at rest and in transit
-- 3. Use VPC endpoints for S3 and Glue access
-- 4. Audit external schema access using AWS CloudTrail
-- 5. Implement row-level security if needed
-- 6. Use separate IAM roles for different data access levels
-- ============================================================================

COMMENT ON SCHEMA spectrum_schema IS 'External schema for querying NYC taxi data from S3 via Glue Data Catalog';
