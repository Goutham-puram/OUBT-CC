-- ============================================================================
-- Create Analytics Views for NYC Taxi Data
-- ============================================================================
-- This script creates materialized and regular views for common analytics
-- queries on the NYC taxi data stored in the external Spectrum schema.
--
-- Prerequisites:
-- 1. External schema 'spectrum_schema' created (001_create_external_schema.sql)
-- 2. Glue catalog table exists (e.g., taxi_trips_curated)
--
-- Views Created:
-- 1. v_daily_revenue      - Daily revenue aggregations
-- 2. v_hourly_trips       - Hourly trip patterns
-- 3. v_payment_analysis   - Payment type analysis with tips
-- 4. v_location_performance - Location-based performance metrics
-- ============================================================================

-- ============================================================================
-- View 1: Daily Revenue Analysis
-- ============================================================================
-- Aggregates revenue metrics by date for trend analysis
-- Use case: Daily revenue tracking, monthly comparisons, growth analysis

CREATE OR REPLACE VIEW v_daily_revenue AS
SELECT
    DATE_TRUNC('day', pickup_datetime) AS trip_date,
    COUNT(*) AS total_trips,
    COUNT(DISTINCT vendor_id) AS unique_vendors,
    SUM(fare_amount) AS total_fare,
    SUM(tip_amount) AS total_tips,
    SUM(tolls_amount) AS total_tolls,
    SUM(total_amount) AS total_revenue,
    AVG(fare_amount) AS avg_fare,
    AVG(tip_amount) AS avg_tip,
    AVG(total_amount) AS avg_total,
    AVG(trip_distance) AS avg_distance,
    AVG(CASE
        WHEN trip_distance > 0 THEN fare_amount / trip_distance
        ELSE NULL
    END) AS avg_fare_per_mile,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_amount) AS median_fare,
    SUM(passenger_count) AS total_passengers,
    AVG(passenger_count) AS avg_passengers_per_trip
FROM
    spectrum_schema.taxi_trips_curated
WHERE
    pickup_datetime IS NOT NULL
    AND total_amount > 0
    AND total_amount < 1000  -- Filter outliers
    AND fare_amount > 0
GROUP BY
    DATE_TRUNC('day', pickup_datetime)
ORDER BY
    trip_date DESC;

COMMENT ON VIEW v_daily_revenue IS 'Daily revenue and trip metrics aggregated from taxi data';

-- ============================================================================
-- View 2: Hourly Trip Patterns
-- ============================================================================
-- Analyzes trip volume and characteristics by hour of day
-- Use case: Demand forecasting, driver scheduling, surge pricing insights

CREATE OR REPLACE VIEW v_hourly_trips AS
SELECT
    EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
    EXTRACT(DOW FROM pickup_datetime) AS day_of_week,  -- 0=Sunday, 6=Saturday
    CASE
        WHEN EXTRACT(DOW FROM pickup_datetime) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    CASE
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 6 AND 9 THEN 'Morning Rush'
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 10 AND 15 THEN 'Midday'
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 16 AND 19 THEN 'Evening Rush'
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 20 AND 23 THEN 'Evening'
        ELSE 'Late Night/Early Morning'
    END AS time_period,
    COUNT(*) AS trip_count,
    AVG(trip_distance) AS avg_distance,
    AVG(CASE
        WHEN trip_distance > 0
            AND EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60 > 0
        THEN (trip_distance / (EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60)) * 60
        ELSE NULL
    END) AS avg_speed_mph,
    AVG(fare_amount) AS avg_fare,
    AVG(tip_amount) AS avg_tip,
    AVG(CASE
        WHEN fare_amount > 0 THEN (tip_amount / fare_amount) * 100
        ELSE 0
    END) AS avg_tip_percentage,
    AVG(EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60) AS avg_duration_minutes,
    AVG(passenger_count) AS avg_passengers
FROM
    spectrum_schema.taxi_trips_curated
WHERE
    pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND dropoff_datetime > pickup_datetime
    AND total_amount > 0
    AND trip_distance > 0
    AND trip_distance < 100  -- Filter unrealistic trips
GROUP BY
    EXTRACT(HOUR FROM pickup_datetime),
    EXTRACT(DOW FROM pickup_datetime),
    CASE
        WHEN EXTRACT(DOW FROM pickup_datetime) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END,
    CASE
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 6 AND 9 THEN 'Morning Rush'
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 10 AND 15 THEN 'Midday'
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 16 AND 19 THEN 'Evening Rush'
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 20 AND 23 THEN 'Evening'
        ELSE 'Late Night/Early Morning'
    END
ORDER BY
    day_of_week,
    pickup_hour;

COMMENT ON VIEW v_hourly_trips IS 'Hourly trip patterns and metrics by day of week and time period';

-- ============================================================================
-- View 3: Payment Analysis
-- ============================================================================
-- Analyzes payment methods and tipping behavior
-- Use case: Payment processing optimization, tipping pattern analysis

CREATE OR REPLACE VIEW v_payment_analysis AS
SELECT
    payment_type,
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided Trip'
        ELSE 'Other'
    END AS payment_type_name,
    COUNT(*) AS total_transactions,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage_of_transactions,
    SUM(fare_amount) AS total_fare_amount,
    SUM(tip_amount) AS total_tip_amount,
    SUM(total_amount) AS total_transaction_amount,
    AVG(fare_amount) AS avg_fare,
    AVG(tip_amount) AS avg_tip,
    AVG(CASE
        WHEN fare_amount > 0 THEN (tip_amount / fare_amount) * 100
        ELSE 0
    END) AS avg_tip_percentage,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tip_amount) AS median_tip,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY tip_amount) AS p75_tip,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY tip_amount) AS p90_tip,
    MAX(tip_amount) AS max_tip,
    SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) AS trips_with_tip,
    ROUND(SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS tip_frequency_percentage,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(passenger_count) AS avg_passengers
FROM
    spectrum_schema.taxi_trips_curated
WHERE
    total_amount > 0
    AND fare_amount > 0
    AND payment_type IS NOT NULL
GROUP BY
    payment_type
ORDER BY
    total_transactions DESC;

COMMENT ON VIEW v_payment_analysis IS 'Payment type distribution and tipping behavior analysis';

-- ============================================================================
-- View 4: Location Performance Analysis
-- ============================================================================
-- Analyzes trip patterns by pickup and dropoff locations
-- Use case: Location-based demand analysis, route optimization

CREATE OR REPLACE VIEW v_location_performance AS
SELECT
    pickup_location_id,
    dropoff_location_id,
    COUNT(*) AS trip_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 4) AS percentage_of_total,
    AVG(trip_distance) AS avg_distance,
    AVG(fare_amount) AS avg_fare,
    AVG(tip_amount) AS avg_tip,
    AVG(total_amount) AS avg_total_amount,
    AVG(EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60) AS avg_duration_minutes,
    AVG(CASE
        WHEN trip_distance > 0
            AND EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60 > 0
        THEN (trip_distance / (EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60)) * 60
        ELSE NULL
    END) AS avg_speed_mph,
    AVG(CASE
        WHEN fare_amount > 0 THEN (tip_amount / fare_amount) * 100
        ELSE 0
    END) AS avg_tip_percentage,
    SUM(fare_amount) AS total_revenue,
    AVG(passenger_count) AS avg_passengers,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fare_amount) AS median_fare,
    -- Identify popular routes
    CASE
        WHEN COUNT(*) >= (SELECT PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY cnt)
                         FROM (SELECT COUNT(*) AS cnt
                               FROM spectrum_schema.taxi_trips_curated
                               WHERE pickup_location_id IS NOT NULL
                                 AND dropoff_location_id IS NOT NULL
                               GROUP BY pickup_location_id, dropoff_location_id) t)
        THEN 'High Volume'
        WHEN COUNT(*) >= (SELECT PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY cnt)
                         FROM (SELECT COUNT(*) AS cnt
                               FROM spectrum_schema.taxi_trips_curated
                               WHERE pickup_location_id IS NOT NULL
                                 AND dropoff_location_id IS NOT NULL
                               GROUP BY pickup_location_id, dropoff_location_id) t)
        THEN 'Medium Volume'
        ELSE 'Low Volume'
    END AS volume_category
FROM
    spectrum_schema.taxi_trips_curated
WHERE
    pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND dropoff_datetime > pickup_datetime
    AND pickup_location_id IS NOT NULL
    AND dropoff_location_id IS NOT NULL
    AND trip_distance > 0
    AND trip_distance < 100
    AND total_amount > 0
    AND total_amount < 1000
GROUP BY
    pickup_location_id,
    dropoff_location_id
HAVING
    COUNT(*) >= 10  -- Filter routes with at least 10 trips
ORDER BY
    trip_count DESC;

COMMENT ON VIEW v_location_performance IS 'Performance metrics by pickup and dropoff location pairs';

-- ============================================================================
-- Grant Permissions on Views
-- ============================================================================
-- Adjust these grants based on your security requirements

GRANT SELECT ON v_daily_revenue TO PUBLIC;
GRANT SELECT ON v_hourly_trips TO PUBLIC;
GRANT SELECT ON v_payment_analysis TO PUBLIC;
GRANT SELECT ON v_location_performance TO PUBLIC;

-- ============================================================================
-- Verify View Creation
-- ============================================================================

-- List all views created
SELECT
    schemaname,
    viewname,
    viewowner
FROM
    pg_views
WHERE
    schemaname = CURRENT_SCHEMA()
    AND viewname IN ('v_daily_revenue', 'v_hourly_trips', 'v_payment_analysis', 'v_location_performance')
ORDER BY
    viewname;

-- ============================================================================
-- Sample Queries to Test Views
-- ============================================================================

-- Test v_daily_revenue
-- SELECT * FROM v_daily_revenue LIMIT 10;

-- Test v_hourly_trips for weekday morning rush
-- SELECT * FROM v_hourly_trips
-- WHERE day_type = 'Weekday' AND time_period = 'Morning Rush'
-- ORDER BY pickup_hour;

-- Test v_payment_analysis
-- SELECT * FROM v_payment_analysis ORDER BY total_transactions DESC;

-- Test v_location_performance for top routes
-- SELECT * FROM v_location_performance
-- WHERE volume_category = 'High Volume'
-- LIMIT 20;

-- ============================================================================
-- Performance Optimization Notes
-- ============================================================================
-- 1. Views query the external Spectrum schema directly, which scans S3 data
-- 2. For frequently accessed data, consider creating materialized views or tables
-- 3. Use WHERE clauses in queries to leverage partition pruning
-- 4. Consider loading aggregated results into Redshift tables for faster access
--
-- Example: Create materialized view for daily revenue (requires loading data)
-- CREATE MATERIALIZED VIEW mv_daily_revenue AS
-- SELECT * FROM v_daily_revenue;
--
-- Refresh materialized view periodically:
-- REFRESH MATERIALIZED VIEW mv_daily_revenue;
-- ============================================================================

-- ============================================================================
-- View Dependencies and Maintenance
-- ============================================================================
-- These views depend on:
-- - spectrum_schema.taxi_trips_curated table
-- - Column names: pickup_datetime, dropoff_datetime, fare_amount, tip_amount,
--   total_amount, trip_distance, passenger_count, payment_type, vendor_id,
--   pickup_location_id, dropoff_location_id
--
-- If the underlying schema changes, views may need to be recreated or modified
-- ============================================================================
