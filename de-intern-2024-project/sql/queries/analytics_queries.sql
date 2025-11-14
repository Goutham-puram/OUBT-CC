-- Analytics Queries for NYC Taxi Data
-- Week 4: Sample queries for business insights

-- Query 1: Daily revenue and trip statistics
SELECT
    d.full_date,
    d.day_name,
    COUNT(*) as total_trips,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_fare,
    AVG(f.trip_distance) as avg_distance,
    AVG(f.trip_duration_minutes) as avg_duration,
    SUM(f.passenger_count) as total_passengers
FROM fact_taxi_trips f
JOIN dim_date d ON f.pickup_date_key = d.date_key
WHERE d.year = 2023 AND d.month = 1
GROUP BY d.full_date, d.day_name
ORDER BY d.full_date;

-- Query 2: Peak hours analysis
SELECT
    t.hour,
    t.time_of_day,
    COUNT(*) as total_trips,
    AVG(f.total_amount) as avg_fare,
    AVG(f.trip_distance) as avg_distance,
    AVG(f.tip_amount) as avg_tip
FROM fact_taxi_trips f
JOIN dim_time t ON f.pickup_time_key = t.time_key
GROUP BY t.hour, t.time_of_day
ORDER BY t.hour;

-- Query 3: Top pickup locations by revenue
SELECT
    l.zone,
    l.borough,
    COUNT(*) as total_pickups,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_fare,
    AVG(f.trip_distance) as avg_distance
FROM fact_taxi_trips f
JOIN dim_location l ON f.pickup_location_key = l.location_key
GROUP BY l.zone, l.borough
ORDER BY total_revenue DESC
LIMIT 20;

-- Query 4: Payment type analysis
SELECT
    p.payment_type_name,
    COUNT(*) as total_trips,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_fare,
    AVG(f.tip_amount) as avg_tip,
    AVG(f.tip_amount / NULLIF(f.fare_amount, 0)) * 100 as avg_tip_percentage
FROM fact_taxi_trips f
JOIN dim_payment_type p ON f.payment_type_key = p.payment_type_key
GROUP BY p.payment_type_name
ORDER BY total_trips DESC;

-- Query 5: Weekend vs Weekday comparison
SELECT
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
    COUNT(*) as total_trips,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_fare,
    AVG(f.trip_distance) as avg_distance,
    AVG(f.trip_duration_minutes) as avg_duration
FROM fact_taxi_trips f
JOIN dim_date d ON f.pickup_date_key = d.date_key
GROUP BY day_type;

-- Query 6: Popular routes (top 20 pickup-dropoff pairs)
SELECT
    pl.zone as pickup_zone,
    dl.zone as dropoff_zone,
    COUNT(*) as trip_count,
    AVG(f.trip_distance) as avg_distance,
    AVG(f.trip_duration_minutes) as avg_duration,
    AVG(f.total_amount) as avg_fare
FROM fact_taxi_trips f
JOIN dim_location pl ON f.pickup_location_key = pl.location_key
JOIN dim_location dl ON f.dropoff_location_key = dl.location_key
GROUP BY pl.zone, dl.zone
ORDER BY trip_count DESC
LIMIT 20;

-- Query 7: Rush hour revenue analysis
SELECT
    d.full_date,
    t.is_rush_hour,
    COUNT(*) as total_trips,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_fare
FROM fact_taxi_trips f
JOIN dim_date d ON f.pickup_date_key = d.date_key
JOIN dim_time t ON f.pickup_time_key = t.time_key
GROUP BY d.full_date, t.is_rush_hour
ORDER BY d.full_date;

-- Query 8: Monthly trends
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(*) as total_trips,
    SUM(f.total_amount) as total_revenue,
    AVG(f.trip_distance) as avg_distance,
    AVG(f.passenger_count) as avg_passengers
FROM fact_taxi_trips f
JOIN dim_date d ON f.pickup_date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- Query 9: Data quality analysis
SELECT
    quality_flag,
    COUNT(*) as trip_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
FROM fact_taxi_trips
GROUP BY quality_flag
ORDER BY trip_count DESC;

-- Query 10: Average speed by time of day
SELECT
    t.time_of_day,
    AVG(f.avg_speed_mph) as avg_speed,
    AVG(f.trip_distance) as avg_distance,
    AVG(f.trip_duration_minutes) as avg_duration
FROM fact_taxi_trips f
JOIN dim_time t ON f.pickup_time_key = t.time_key
WHERE f.avg_speed_mph > 0 AND f.avg_speed_mph < 100
GROUP BY t.time_of_day
ORDER BY
    CASE t.time_of_day
        WHEN 'Morning' THEN 1
        WHEN 'Afternoon' THEN 2
        WHEN 'Evening' THEN 3
        WHEN 'Night' THEN 4
    END;
