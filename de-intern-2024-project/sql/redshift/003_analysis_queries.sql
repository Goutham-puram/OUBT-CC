-- ============================================================================
-- NYC Taxi Data Analysis Queries
-- ============================================================================
-- This script contains analytical queries for NYC taxi data using the views
-- and external schema created in previous scripts.
--
-- Prerequisites:
-- 1. External schema created (001_create_external_schema.sql)
-- 2. Analytics views created (002_create_analytics_views.sql)
--
-- Query Categories:
-- 1. Revenue Analysis by Month
-- 2. Popular Routes Analysis
-- 3. Peak Hours Analysis
-- 4. Tip Patterns by Payment Type
-- ============================================================================

-- ============================================================================
-- SECTION 1: Revenue Analysis by Month
-- ============================================================================

-- Query 1.1: Monthly Revenue Trends
-- Shows total revenue, trips, and average metrics by month
SELECT
    DATE_TRUNC('month', trip_date) AS month,
    TO_CHAR(DATE_TRUNC('month', trip_date), 'YYYY-MM') AS month_label,
    SUM(total_trips) AS total_trips,
    SUM(total_revenue) AS total_revenue,
    SUM(total_fare) AS total_fare,
    SUM(total_tips) AS total_tips,
    SUM(total_tolls) AS total_tolls,
    AVG(avg_fare) AS avg_fare_per_trip,
    AVG(avg_tip) AS avg_tip_per_trip,
    AVG(avg_distance) AS avg_distance_miles,
    SUM(total_passengers) AS total_passengers,
    -- Growth calculations
    LAG(SUM(total_revenue)) OVER (ORDER BY DATE_TRUNC('month', trip_date)) AS prev_month_revenue,
    ROUND(
        (SUM(total_revenue) - LAG(SUM(total_revenue)) OVER (ORDER BY DATE_TRUNC('month', trip_date)))
        / NULLIF(LAG(SUM(total_revenue)) OVER (ORDER BY DATE_TRUNC('month', trip_date)), 0) * 100,
        2
    ) AS revenue_growth_pct
FROM
    v_daily_revenue
GROUP BY
    DATE_TRUNC('month', trip_date)
ORDER BY
    month DESC
LIMIT 24;  -- Last 24 months

-- Query 1.2: Year-over-Year Revenue Comparison
-- Compares revenue metrics across years for the same month
SELECT
    EXTRACT(MONTH FROM trip_date) AS month_number,
    TO_CHAR(trip_date, 'Month') AS month_name,
    EXTRACT(YEAR FROM trip_date) AS year,
    SUM(total_revenue) AS total_revenue,
    SUM(total_trips) AS total_trips,
    AVG(avg_fare) AS avg_fare,
    AVG(avg_tip) AS avg_tip
FROM
    v_daily_revenue
GROUP BY
    EXTRACT(MONTH FROM trip_date),
    TO_CHAR(trip_date, 'Month'),
    EXTRACT(YEAR FROM trip_date)
ORDER BY
    month_number,
    year DESC;

-- Query 1.3: Revenue by Day of Week
-- Identifies which days generate most revenue
SELECT
    EXTRACT(DOW FROM trip_date) AS day_of_week_num,
    CASE EXTRACT(DOW FROM trip_date)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_of_week,
    CASE
        WHEN EXTRACT(DOW FROM trip_date) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    SUM(total_trips) AS total_trips,
    SUM(total_revenue) AS total_revenue,
    AVG(avg_fare) AS avg_fare,
    AVG(avg_tip) AS avg_tip,
    AVG(total_trips) AS avg_trips_per_day,
    AVG(total_revenue) AS avg_revenue_per_day
FROM
    v_daily_revenue
WHERE
    trip_date >= CURRENT_DATE - INTERVAL '90 days'  -- Last 90 days
GROUP BY
    EXTRACT(DOW FROM trip_date),
    CASE EXTRACT(DOW FROM trip_date)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END,
    CASE
        WHEN EXTRACT(DOW FROM trip_date) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END
ORDER BY
    day_of_week_num;

-- Query 1.4: Top Revenue Days (Outliers and Special Events)
-- Identifies days with exceptional revenue
SELECT
    trip_date,
    TO_CHAR(trip_date, 'Day, DD Mon YYYY') AS formatted_date,
    EXTRACT(DOW FROM trip_date) AS day_of_week,
    total_trips,
    total_revenue,
    avg_fare,
    avg_tip,
    -- Calculate z-score for revenue (shows how many std devs from mean)
    ROUND(
        (total_revenue - AVG(total_revenue) OVER ()) / NULLIF(STDDEV(total_revenue) OVER (), 0),
        2
    ) AS revenue_zscore
FROM
    v_daily_revenue
WHERE
    trip_date >= CURRENT_DATE - INTERVAL '365 days'
ORDER BY
    total_revenue DESC
LIMIT 20;

-- ============================================================================
-- SECTION 2: Popular Routes Analysis
-- ============================================================================

-- Query 2.1: Top 20 Most Popular Routes
-- Identifies the busiest pickup-dropoff location pairs
SELECT
    pickup_location_id,
    dropoff_location_id,
    trip_count,
    percentage_of_total,
    avg_distance,
    avg_fare,
    avg_tip,
    avg_duration_minutes,
    avg_speed_mph,
    total_revenue,
    volume_category,
    -- Calculate efficiency score (revenue per minute)
    ROUND(avg_fare / NULLIF(avg_duration_minutes, 0), 2) AS revenue_per_minute
FROM
    v_location_performance
WHERE
    volume_category = 'High Volume'
ORDER BY
    trip_count DESC
LIMIT 20;

-- Query 2.2: Most Profitable Routes (by average fare)
-- Shows routes with highest average fares
SELECT
    pickup_location_id,
    dropoff_location_id,
    trip_count,
    avg_fare,
    avg_tip,
    avg_distance,
    avg_duration_minutes,
    total_revenue,
    ROUND(avg_fare / NULLIF(avg_distance, 0), 2) AS fare_per_mile,
    ROUND(avg_tip / NULLIF(avg_fare, 0) * 100, 2) AS tip_percentage
FROM
    v_location_performance
WHERE
    trip_count >= 50  -- Ensure statistical significance
    AND avg_distance > 0
ORDER BY
    avg_fare DESC
LIMIT 20;

-- Query 2.3: Shortest but Most Frequent Routes
-- Identifies short-distance high-volume routes
SELECT
    pickup_location_id,
    dropoff_location_id,
    trip_count,
    avg_distance,
    avg_fare,
    avg_duration_minutes,
    ROUND(trip_count::NUMERIC / NULLIF(avg_distance, 0), 2) AS trips_per_mile_efficiency
FROM
    v_location_performance
WHERE
    avg_distance > 0
    AND avg_distance <= 5  -- Short trips only (5 miles or less)
    AND trip_count >= 100
ORDER BY
    trip_count DESC
LIMIT 20;

-- Query 2.4: Route Performance by Time of Day
-- Analyzes how route performance varies throughout the day
SELECT
    pickup_hour,
    time_period,
    day_type,
    trip_count,
    avg_distance,
    avg_fare,
    avg_tip,
    avg_speed_mph,
    avg_duration_minutes,
    avg_tip_percentage,
    -- Rank routes within each time period
    RANK() OVER (PARTITION BY time_period ORDER BY trip_count DESC) AS route_rank_in_period
FROM
    v_hourly_trips
WHERE
    day_type = 'Weekday'
ORDER BY
    pickup_hour,
    trip_count DESC;

-- ============================================================================
-- SECTION 3: Peak Hours Analysis
-- ============================================================================

-- Query 3.1: Hourly Demand Patterns
-- Shows trip volume and characteristics by hour
SELECT
    pickup_hour,
    time_period,
    SUM(CASE WHEN day_type = 'Weekday' THEN trip_count ELSE 0 END) AS weekday_trips,
    SUM(CASE WHEN day_type = 'Weekend' THEN trip_count ELSE 0 END) AS weekend_trips,
    AVG(CASE WHEN day_type = 'Weekday' THEN avg_fare END) AS weekday_avg_fare,
    AVG(CASE WHEN day_type = 'Weekend' THEN avg_fare END) AS weekend_avg_fare,
    AVG(CASE WHEN day_type = 'Weekday' THEN avg_speed_mph END) AS weekday_avg_speed,
    AVG(CASE WHEN day_type = 'Weekend' THEN avg_speed_mph END) AS weekend_avg_speed,
    AVG(avg_tip_percentage) AS avg_tip_pct,
    AVG(avg_duration_minutes) AS avg_duration
FROM
    v_hourly_trips
GROUP BY
    pickup_hour,
    time_period
ORDER BY
    pickup_hour;

-- Query 3.2: Rush Hour Performance Comparison
-- Compares morning vs evening rush hours
SELECT
    time_period,
    day_type,
    SUM(trip_count) AS total_trips,
    AVG(avg_fare) AS avg_fare,
    AVG(avg_tip) AS avg_tip,
    AVG(avg_speed_mph) AS avg_speed,
    AVG(avg_duration_minutes) AS avg_duration,
    AVG(avg_distance) AS avg_distance,
    -- Calculate congestion indicator
    ROUND(AVG(avg_distance) / NULLIF(AVG(avg_speed_mph), 0) * 60, 2) AS expected_duration_if_no_traffic,
    ROUND(AVG(avg_duration_minutes) - (AVG(avg_distance) / NULLIF(AVG(avg_speed_mph), 0) * 60), 2) AS traffic_delay_minutes
FROM
    v_hourly_trips
WHERE
    time_period IN ('Morning Rush', 'Evening Rush')
GROUP BY
    time_period,
    day_type
ORDER BY
    time_period,
    day_type;

-- Query 3.3: Peak vs Off-Peak Comparison
-- Analyzes differences between peak and off-peak hours
SELECT
    CASE
        WHEN time_period IN ('Morning Rush', 'Evening Rush') THEN 'Peak Hours'
        ELSE 'Off-Peak Hours'
    END AS hour_category,
    day_type,
    SUM(trip_count) AS total_trips,
    AVG(avg_fare) AS avg_fare,
    AVG(avg_tip) AS avg_tip,
    AVG(avg_tip_percentage) AS avg_tip_pct,
    AVG(avg_speed_mph) AS avg_speed,
    AVG(avg_duration_minutes) AS avg_duration,
    AVG(avg_passengers) AS avg_passengers
FROM
    v_hourly_trips
GROUP BY
    CASE
        WHEN time_period IN ('Morning Rush', 'Evening Rush') THEN 'Peak Hours'
        ELSE 'Off-Peak Hours'
    END,
    day_type
ORDER BY
    hour_category,
    day_type;

-- Query 3.4: Late Night Analysis
-- Focuses on late night/early morning patterns
SELECT
    pickup_hour,
    day_of_week,
    CASE day_of_week
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_name,
    trip_count,
    avg_fare,
    avg_tip,
    avg_tip_percentage,
    avg_distance,
    avg_duration_minutes
FROM
    v_hourly_trips
WHERE
    time_period = 'Late Night/Early Morning'
    AND (pickup_hour >= 22 OR pickup_hour <= 5)
ORDER BY
    day_of_week,
    pickup_hour;

-- ============================================================================
-- SECTION 4: Tip Patterns by Payment Type
-- ============================================================================

-- Query 4.1: Tipping Behavior by Payment Method
-- Comprehensive analysis of tipping across payment types
SELECT
    payment_type_name,
    total_transactions,
    percentage_of_transactions,
    total_tip_amount,
    avg_tip,
    avg_tip_percentage,
    median_tip,
    p75_tip,
    p90_tip,
    max_tip,
    trips_with_tip,
    tip_frequency_percentage,
    avg_fare,
    avg_trip_distance,
    -- Calculate tip efficiency
    ROUND(total_tip_amount / NULLIF(total_transactions, 0), 2) AS tip_per_transaction,
    ROUND(total_tip_amount / NULLIF(total_fare_amount, 0) * 100, 2) AS overall_tip_rate
FROM
    v_payment_analysis
ORDER BY
    total_transactions DESC;

-- Query 4.2: Tip Amount Distribution by Fare Buckets
-- Shows how tipping varies with fare amount
SELECT
    CASE
        WHEN fare_amount <= 10 THEN '$0-10'
        WHEN fare_amount <= 20 THEN '$10-20'
        WHEN fare_amount <= 30 THEN '$20-30'
        WHEN fare_amount <= 50 THEN '$30-50'
        WHEN fare_amount <= 100 THEN '$50-100'
        ELSE '$100+'
    END AS fare_bucket,
    payment_type,
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        ELSE 'Other'
    END AS payment_type_name,
    COUNT(*) AS trip_count,
    AVG(fare_amount) AS avg_fare,
    AVG(tip_amount) AS avg_tip,
    AVG(CASE WHEN fare_amount > 0 THEN (tip_amount / fare_amount) * 100 ELSE 0 END) AS avg_tip_percentage,
    SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) AS trips_with_tip,
    ROUND(SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS tip_frequency_pct
FROM
    spectrum_schema.taxi_trips_curated
WHERE
    fare_amount > 0
    AND total_amount > 0
    AND payment_type IN (1, 2)  -- Credit and Cash only
GROUP BY
    CASE
        WHEN fare_amount <= 10 THEN '$0-10'
        WHEN fare_amount <= 20 THEN '$10-20'
        WHEN fare_amount <= 30 THEN '$20-30'
        WHEN fare_amount <= 50 THEN '$30-50'
        WHEN fare_amount <= 100 THEN '$50-100'
        ELSE '$100+'
    END,
    payment_type,
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        ELSE 'Other'
    END
ORDER BY
    fare_bucket,
    payment_type;

-- Query 4.3: Generous Tippers Analysis
-- Identifies patterns in high tip percentages
SELECT
    CASE
        WHEN tip_pct <= 10 THEN '0-10%'
        WHEN tip_pct <= 15 THEN '10-15%'
        WHEN tip_pct <= 20 THEN '15-20%'
        WHEN tip_pct <= 25 THEN '20-25%'
        WHEN tip_pct <= 30 THEN '25-30%'
        ELSE '30%+'
    END AS tip_percentage_bucket,
    COUNT(*) AS trip_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage_of_trips,
    AVG(fare_amount) AS avg_fare,
    AVG(tip_amount) AS avg_tip,
    AVG(trip_distance) AS avg_distance,
    AVG(passenger_count) AS avg_passengers
FROM (
    SELECT
        fare_amount,
        tip_amount,
        trip_distance,
        passenger_count,
        CASE WHEN fare_amount > 0 THEN (tip_amount / fare_amount) * 100 ELSE 0 END AS tip_pct
    FROM
        spectrum_schema.taxi_trips_curated
    WHERE
        payment_type = 1  -- Credit card only (tips are recorded)
        AND fare_amount > 0
        AND tip_amount >= 0
        AND total_amount > 0
) t
GROUP BY
    CASE
        WHEN tip_pct <= 10 THEN '0-10%'
        WHEN tip_pct <= 15 THEN '10-15%'
        WHEN tip_pct <= 20 THEN '15-20%'
        WHEN tip_pct <= 25 THEN '20-25%'
        WHEN tip_pct <= 30 THEN '25-30%'
        ELSE '30%+'
    END
ORDER BY
    trip_count DESC;

-- Query 4.4: Tip Patterns by Day and Time
-- Shows how tipping varies throughout the week
SELECT
    EXTRACT(DOW FROM pickup_datetime) AS day_of_week,
    CASE EXTRACT(DOW FROM pickup_datetime)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_name,
    EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
    CASE
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 6 AND 9 THEN 'Morning Rush'
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 10 AND 15 THEN 'Midday'
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 16 AND 19 THEN 'Evening Rush'
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 20 AND 23 THEN 'Evening'
        ELSE 'Late Night'
    END AS time_period,
    COUNT(*) AS trip_count,
    AVG(tip_amount) AS avg_tip,
    AVG(CASE WHEN fare_amount > 0 THEN (tip_amount / fare_amount) * 100 ELSE 0 END) AS avg_tip_pct,
    SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) AS trips_with_tip,
    ROUND(SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS tip_frequency_pct
FROM
    spectrum_schema.taxi_trips_curated
WHERE
    payment_type = 1  -- Credit card
    AND fare_amount > 0
    AND pickup_datetime IS NOT NULL
GROUP BY
    EXTRACT(DOW FROM pickup_datetime),
    CASE EXTRACT(DOW FROM pickup_datetime)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END,
    EXTRACT(HOUR FROM pickup_datetime),
    CASE
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 6 AND 9 THEN 'Morning Rush'
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 10 AND 15 THEN 'Midday'
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 16 AND 19 THEN 'Evening Rush'
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 20 AND 23 THEN 'Evening'
        ELSE 'Late Night'
    END
ORDER BY
    day_of_week,
    pickup_hour;

-- ============================================================================
-- BONUS QUERIES: Advanced Analytics
-- ============================================================================

-- Bonus Query 1: Weather Impact Analysis Placeholder
-- Note: Requires integration with weather data
-- This is a template showing how to join with weather data if available
/*
SELECT
    DATE_TRUNC('day', t.pickup_datetime) AS trip_date,
    w.weather_condition,
    w.temperature_f,
    w.precipitation_inches,
    COUNT(*) AS trip_count,
    AVG(t.fare_amount) AS avg_fare,
    AVG(t.trip_distance) AS avg_distance,
    AVG(t.tip_amount) AS avg_tip
FROM
    spectrum_schema.taxi_trips_curated t
LEFT JOIN
    weather_data w ON DATE_TRUNC('day', t.pickup_datetime) = w.date
GROUP BY
    DATE_TRUNC('day', t.pickup_datetime),
    w.weather_condition,
    w.temperature_f,
    w.precipitation_inches
ORDER BY
    trip_date DESC;
*/

-- Bonus Query 2: Passenger Count Impact on Tips
-- Analyzes if passenger count affects tipping
SELECT
    passenger_count,
    COUNT(*) AS trip_count,
    AVG(fare_amount) AS avg_fare,
    AVG(tip_amount) AS avg_tip,
    AVG(CASE WHEN fare_amount > 0 THEN (tip_amount / fare_amount) * 100 ELSE 0 END) AS avg_tip_pct,
    AVG(trip_distance) AS avg_distance
FROM
    spectrum_schema.taxi_trips_curated
WHERE
    payment_type = 1
    AND passenger_count > 0
    AND passenger_count <= 6
    AND fare_amount > 0
GROUP BY
    passenger_count
ORDER BY
    passenger_count;

-- Bonus Query 3: Distance-based Performance Buckets
-- Shows metrics across different distance ranges
SELECT
    CASE
        WHEN trip_distance <= 1 THEN '0-1 miles'
        WHEN trip_distance <= 2 THEN '1-2 miles'
        WHEN trip_distance <= 5 THEN '2-5 miles'
        WHEN trip_distance <= 10 THEN '5-10 miles'
        WHEN trip_distance <= 20 THEN '10-20 miles'
        ELSE '20+ miles'
    END AS distance_bucket,
    COUNT(*) AS trip_count,
    AVG(fare_amount) AS avg_fare,
    AVG(tip_amount) AS avg_tip,
    AVG(CASE WHEN fare_amount > 0 THEN (tip_amount / fare_amount) * 100 ELSE 0 END) AS avg_tip_pct,
    AVG(EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60) AS avg_duration_minutes,
    AVG(CASE
        WHEN trip_distance > 0
            AND EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60 > 0
        THEN (trip_distance / (EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60)) * 60
        ELSE NULL
    END) AS avg_speed_mph
FROM
    spectrum_schema.taxi_trips_curated
WHERE
    trip_distance > 0
    AND trip_distance < 100
    AND pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND dropoff_datetime > pickup_datetime
GROUP BY
    CASE
        WHEN trip_distance <= 1 THEN '0-1 miles'
        WHEN trip_distance <= 2 THEN '1-2 miles'
        WHEN trip_distance <= 5 THEN '2-5 miles'
        WHEN trip_distance <= 10 THEN '5-10 miles'
        WHEN trip_distance <= 20 THEN '10-20 miles'
        ELSE '20+ miles'
    END
ORDER BY
    MIN(trip_distance);

-- ============================================================================
-- Query Performance Tips
-- ============================================================================
-- 1. Use EXPLAIN to understand query execution plans
-- 2. Add WHERE clauses for date ranges to leverage partitioning
-- 3. Consider creating materialized views for frequently run queries
-- 4. Monitor query costs using SVL_S3QUERY_SUMMARY system view
-- 5. Use ANALYZE TABLE to update statistics for better query plans
--
-- Example: Check query cost
-- SELECT query, s3_scanned_bytes / 1024.0 / 1024.0 / 1024.0 AS gb_scanned
-- FROM svl_s3query_summary
-- WHERE query = pg_last_query_id();
-- ============================================================================
