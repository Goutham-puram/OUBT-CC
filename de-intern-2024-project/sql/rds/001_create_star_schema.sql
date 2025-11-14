-- ============================================================================
-- NYC Taxi Data - Star Schema Design
-- ============================================================================
-- This schema implements a star schema for NYC Yellow Taxi trip data
-- optimized for analytical queries and reporting.
--
-- Schema Structure:
--   - dim_location: Location dimension (pickup/dropoff zones)
--   - dim_time: Time dimension (trip timestamps)
--   - dim_rate: Rate code dimension (fare types)
--   - fact_trips: Fact table (trip transactions)
--
-- Author: Data Engineering Team
-- Database: oubt_ptg
-- ============================================================================

-- Start transaction
BEGIN;

-- ============================================================================
-- Drop existing tables (if any) in reverse dependency order
-- ============================================================================
DROP TABLE IF EXISTS fact_trips CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;
DROP TABLE IF EXISTS dim_rate CASCADE;

-- ============================================================================
-- Dimension Tables
-- ============================================================================

-- ----------------------------------------------------------------------------
-- dim_location: Location dimension for pickup and dropoff zones
-- ----------------------------------------------------------------------------
CREATE TABLE dim_location (
    location_id INTEGER PRIMARY KEY,
    borough VARCHAR(50),
    zone VARCHAR(100) NOT NULL,
    service_zone VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add indexes for common queries
CREATE INDEX idx_location_borough ON dim_location(borough);
CREATE INDEX idx_location_zone ON dim_location(zone);
CREATE INDEX idx_location_service_zone ON dim_location(service_zone);

-- Add comments
COMMENT ON TABLE dim_location IS 'Location dimension for NYC taxi zones';
COMMENT ON COLUMN dim_location.location_id IS 'Unique identifier for location (matches TLC LocationID)';
COMMENT ON COLUMN dim_location.borough IS 'NYC borough name';
COMMENT ON COLUMN dim_location.zone IS 'Taxi zone name';
COMMENT ON COLUMN dim_location.service_zone IS 'Service zone classification';

-- ----------------------------------------------------------------------------
-- dim_time: Time dimension for temporal analysis
-- ----------------------------------------------------------------------------
CREATE TABLE dim_time (
    time_id BIGSERIAL PRIMARY KEY,
    pickup_datetime TIMESTAMP NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    weekday INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    quarter INTEGER NOT NULL,
    day_of_year INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add indexes for temporal queries
CREATE INDEX idx_time_pickup_datetime ON dim_time(pickup_datetime);
CREATE INDEX idx_time_year_month ON dim_time(year, month);
CREATE INDEX idx_time_hour ON dim_time(hour);
CREATE INDEX idx_time_weekday ON dim_time(weekday);
CREATE INDEX idx_time_is_weekend ON dim_time(is_weekend);

-- Add unique constraint on pickup_datetime to prevent duplicates
CREATE UNIQUE INDEX idx_time_unique_datetime ON dim_time(pickup_datetime);

-- Add comments
COMMENT ON TABLE dim_time IS 'Time dimension for temporal analysis of trips';
COMMENT ON COLUMN dim_time.time_id IS 'Surrogate key for time dimension';
COMMENT ON COLUMN dim_time.pickup_datetime IS 'Original pickup timestamp';
COMMENT ON COLUMN dim_time.weekday IS 'Day of week (0=Monday, 6=Sunday)';
COMMENT ON COLUMN dim_time.is_weekend IS 'True if Saturday or Sunday';

-- ----------------------------------------------------------------------------
-- dim_rate: Rate code dimension
-- ----------------------------------------------------------------------------
CREATE TABLE dim_rate (
    rate_code_id INTEGER PRIMARY KEY,
    rate_code_name VARCHAR(50) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add comments
COMMENT ON TABLE dim_rate IS 'Rate code dimension for fare types';
COMMENT ON COLUMN dim_rate.rate_code_id IS 'Rate code identifier';
COMMENT ON COLUMN dim_rate.rate_code_name IS 'Rate code name (e.g., Standard, JFK, Newark)';

-- ============================================================================
-- Fact Table
-- ============================================================================

-- ----------------------------------------------------------------------------
-- fact_trips: Main fact table for trip transactions
-- ----------------------------------------------------------------------------
CREATE TABLE fact_trips (
    trip_id BIGSERIAL PRIMARY KEY,
    time_id BIGINT NOT NULL,
    pickup_location_id INTEGER NOT NULL,
    dropoff_location_id INTEGER NOT NULL,
    rate_code_id INTEGER NOT NULL,
    passenger_count INTEGER,
    trip_distance NUMERIC(10, 2),
    trip_duration NUMERIC(10, 2),
    fare_amount NUMERIC(10, 2),
    extra NUMERIC(10, 2),
    mta_tax NUMERIC(10, 2),
    tip_amount NUMERIC(10, 2),
    tolls_amount NUMERIC(10, 2),
    improvement_surcharge NUMERIC(10, 2),
    congestion_surcharge NUMERIC(10, 2),
    total_amount NUMERIC(10, 2) NOT NULL,
    payment_type INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Foreign key constraints
    CONSTRAINT fk_time FOREIGN KEY (time_id) REFERENCES dim_time(time_id) ON DELETE CASCADE,
    CONSTRAINT fk_pickup_location FOREIGN KEY (pickup_location_id) REFERENCES dim_location(location_id) ON DELETE CASCADE,
    CONSTRAINT fk_dropoff_location FOREIGN KEY (dropoff_location_id) REFERENCES dim_location(location_id) ON DELETE CASCADE,
    CONSTRAINT fk_rate_code FOREIGN KEY (rate_code_id) REFERENCES dim_rate(rate_code_id) ON DELETE CASCADE,

    -- Check constraints for data quality
    CONSTRAINT chk_passenger_count CHECK (passenger_count >= 0),
    CONSTRAINT chk_trip_distance CHECK (trip_distance >= 0),
    CONSTRAINT chk_trip_duration CHECK (trip_duration >= 0),
    CONSTRAINT chk_fare_amount CHECK (fare_amount >= 0),
    CONSTRAINT chk_total_amount CHECK (total_amount >= 0)
);

-- Add indexes for fact table queries
CREATE INDEX idx_fact_time_id ON fact_trips(time_id);
CREATE INDEX idx_fact_pickup_location ON fact_trips(pickup_location_id);
CREATE INDEX idx_fact_dropoff_location ON fact_trips(dropoff_location_id);
CREATE INDEX idx_fact_rate_code ON fact_trips(rate_code_id);
CREATE INDEX idx_fact_total_amount ON fact_trips(total_amount);
CREATE INDEX idx_fact_passenger_count ON fact_trips(passenger_count);

-- Composite indexes for common query patterns
CREATE INDEX idx_fact_location_time ON fact_trips(pickup_location_id, time_id);
CREATE INDEX idx_fact_rate_time ON fact_trips(rate_code_id, time_id);

-- Add comments
COMMENT ON TABLE fact_trips IS 'Fact table containing NYC taxi trip transactions';
COMMENT ON COLUMN fact_trips.trip_id IS 'Surrogate key for trip';
COMMENT ON COLUMN fact_trips.time_id IS 'Foreign key to dim_time';
COMMENT ON COLUMN fact_trips.pickup_location_id IS 'Foreign key to dim_location (pickup)';
COMMENT ON COLUMN fact_trips.dropoff_location_id IS 'Foreign key to dim_location (dropoff)';
COMMENT ON COLUMN fact_trips.trip_duration IS 'Trip duration in minutes';
COMMENT ON COLUMN fact_trips.payment_type IS '1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided trip';

-- ============================================================================
-- Seed Reference Data
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Populate dim_rate with standard NYC taxi rate codes
-- ----------------------------------------------------------------------------
INSERT INTO dim_rate (rate_code_id, rate_code_name, description) VALUES
(1, 'Standard rate', 'Standard rate for trips within NYC'),
(2, 'JFK', 'Flat rate to/from JFK Airport'),
(3, 'Newark', 'Flat rate to/from Newark Airport'),
(4, 'Nassau or Westchester', 'Negotiated fare to Nassau or Westchester'),
(5, 'Negotiated fare', 'Negotiated fare for other destinations'),
(6, 'Group ride', 'Group ride rate');

-- ----------------------------------------------------------------------------
-- Populate dim_location with sample/placeholder data
-- Note: Full location data should be loaded from NYC TLC taxi zone lookup
-- ----------------------------------------------------------------------------
INSERT INTO dim_location (location_id, borough, zone, service_zone) VALUES
(1, 'Manhattan', 'Newark Airport', 'EWR'),
(2, 'Queens', 'Jamaica Bay', 'Boro Zone'),
(4, 'Manhattan', 'Alphabet City', 'Yellow Zone'),
(12, 'Manhattan', 'Battery Park', 'Yellow Zone'),
(13, 'Manhattan', 'Battery Park City', 'Yellow Zone'),
(24, 'Manhattan', 'Bloomingdale', 'Yellow Zone'),
(41, 'Manhattan', 'Central Harlem', 'Boro Zone'),
(42, 'Manhattan', 'Central Harlem North', 'Boro Zone'),
(43, 'Manhattan', 'Central Park', 'Yellow Zone'),
(45, 'Manhattan', 'Chinatown', 'Yellow Zone'),
(48, 'Manhattan', 'Clinton East', 'Yellow Zone'),
(50, 'Manhattan', 'Clinton West', 'Yellow Zone'),
(68, 'Manhattan', 'East Chelsea', 'Yellow Zone'),
(79, 'Manhattan', 'East Village', 'Yellow Zone'),
(87, 'Manhattan', 'Financial District North', 'Yellow Zone'),
(88, 'Manhattan', 'Financial District South', 'Yellow Zone'),
(90, 'Manhattan', 'Flatiron', 'Yellow Zone'),
(100, 'Manhattan', 'Garment District', 'Yellow Zone'),
(103, 'Queens', 'JFK Airport', 'Airports'),
(107, 'Manhattan', 'Gramercy', 'Yellow Zone'),
(113, 'Manhattan', 'Greenwich Village North', 'Yellow Zone'),
(114, 'Manhattan', 'Greenwich Village South', 'Yellow Zone'),
(116, 'Manhattan', 'Hamilton Heights', 'Boro Zone'),
(125, 'Manhattan', 'Harlem-Central', 'Boro Zone'),
(127, 'Manhattan', 'Hell''s Kitchen North', 'Yellow Zone'),
(128, 'Manhattan', 'Hell''s Kitchen South', 'Yellow Zone'),
(137, 'Queens', 'Kips Bay', 'Yellow Zone'),
(140, 'Queens', 'Lenox Hill East', 'Yellow Zone'),
(141, 'Manhattan', 'Lenox Hill West', 'Yellow Zone'),
(142, 'Manhattan', 'Lincoln Square East', 'Yellow Zone'),
(143, 'Manhattan', 'Lincoln Square West', 'Yellow Zone'),
(144, 'Manhattan', 'Little Italy/NoLiTa', 'Yellow Zone'),
(148, 'Manhattan', 'Lower East Side', 'Yellow Zone'),
(151, 'Manhattan', 'Manhattan Valley', 'Yellow Zone'),
(152, 'Manhattan', 'Manhattanville', 'Boro Zone'),
(153, 'Manhattan', 'Marble Hill', 'Boro Zone'),
(158, 'Manhattan', 'Meatpacking/West Village West', 'Yellow Zone'),
(161, 'Manhattan', 'Midtown Center', 'Yellow Zone'),
(162, 'Manhattan', 'Midtown East', 'Yellow Zone'),
(163, 'Manhattan', 'Midtown North', 'Yellow Zone'),
(164, 'Manhattan', 'Midtown South', 'Yellow Zone'),
(166, 'Manhattan', 'Morningside Heights', 'Boro Zone'),
(170, 'Manhattan', 'Murray Hill', 'Yellow Zone'),
(186, 'Manhattan', 'Penn Station/Madison Sq West', 'Yellow Zone'),
(194, 'Manhattan', 'Randalls Island', 'Yellow Zone'),
(202, 'Manhattan', 'Seaport', 'Yellow Zone'),
(209, 'Manhattan', 'SoHo', 'Yellow Zone'),
(211, 'Manhattan', 'Stuy Town/Peter Cooper Village', 'Yellow Zone'),
(224, 'Manhattan', 'Sutton Place/Turtle Bay North', 'Yellow Zone'),
(229, 'Manhattan', 'Times Sq/Theatre District', 'Yellow Zone'),
(230, 'Manhattan', 'TriBeCa/Civic Center', 'Yellow Zone'),
(231, 'Manhattan', 'Two Bridges/Seward Park', 'Yellow Zone'),
(232, 'Manhattan', 'UN/Turtle Bay South', 'Yellow Zone'),
(233, 'Manhattan', 'Union Sq', 'Yellow Zone'),
(234, 'Manhattan', 'Upper East Side North', 'Yellow Zone'),
(236, 'Manhattan', 'Upper East Side South', 'Yellow Zone'),
(237, 'Manhattan', 'Upper West Side North', 'Yellow Zone'),
(238, 'Manhattan', 'Upper West Side South', 'Yellow Zone'),
(239, 'Manhattan', 'Washington Heights North', 'Boro Zone'),
(240, 'Manhattan', 'Washington Heights South', 'Boro Zone'),
(243, 'Manhattan', 'West Chelsea/Hudson Yards', 'Yellow Zone'),
(244, 'Manhattan', 'West Village', 'Yellow Zone'),
(246, 'Manhattan', 'World Trade Center', 'Yellow Zone'),
(249, 'Manhattan', 'Yorkville East', 'Yellow Zone'),
(261, 'Manhattan', 'Yorkville West', 'Yellow Zone'),
(262, 'Queens', 'Astoria', 'Boro Zone'),
(263, 'Queens', 'Long Island City/Queens Plaza', 'Boro Zone');

-- ============================================================================
-- Views for Common Queries
-- ============================================================================

-- ----------------------------------------------------------------------------
-- View: trip_summary
-- Provides denormalized view of trips with dimension details
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW trip_summary AS
SELECT
    ft.trip_id,
    dt.pickup_datetime,
    dt.year,
    dt.month,
    dt.day,
    dt.hour,
    dt.weekday,
    dt.is_weekend,
    pl.borough AS pickup_borough,
    pl.zone AS pickup_zone,
    dl.borough AS dropoff_borough,
    dl.zone AS dropoff_zone,
    dr.rate_code_name,
    ft.passenger_count,
    ft.trip_distance,
    ft.trip_duration,
    ft.fare_amount,
    ft.tip_amount,
    ft.total_amount,
    ft.payment_type
FROM fact_trips ft
JOIN dim_time dt ON ft.time_id = dt.time_id
JOIN dim_location pl ON ft.pickup_location_id = pl.location_id
JOIN dim_location dl ON ft.dropoff_location_id = dl.location_id
JOIN dim_rate dr ON ft.rate_code_id = dr.rate_code_id;

COMMENT ON VIEW trip_summary IS 'Denormalized view of trips with all dimension details';

-- ============================================================================
-- Utility Functions
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Function: insert_or_get_time_id
-- Returns time_id for a given timestamp, creating record if needed
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION insert_or_get_time_id(p_datetime TIMESTAMP)
RETURNS BIGINT AS $$
DECLARE
    v_time_id BIGINT;
BEGIN
    -- Try to find existing record
    SELECT time_id INTO v_time_id
    FROM dim_time
    WHERE pickup_datetime = p_datetime;

    -- If not found, insert new record
    IF v_time_id IS NULL THEN
        INSERT INTO dim_time (
            pickup_datetime,
            year,
            month,
            day,
            hour,
            weekday,
            is_weekend,
            quarter,
            day_of_year,
            week_of_year
        ) VALUES (
            p_datetime,
            EXTRACT(YEAR FROM p_datetime)::INTEGER,
            EXTRACT(MONTH FROM p_datetime)::INTEGER,
            EXTRACT(DAY FROM p_datetime)::INTEGER,
            EXTRACT(HOUR FROM p_datetime)::INTEGER,
            EXTRACT(DOW FROM p_datetime)::INTEGER,
            EXTRACT(DOW FROM p_datetime)::INTEGER IN (0, 6),
            EXTRACT(QUARTER FROM p_datetime)::INTEGER,
            EXTRACT(DOY FROM p_datetime)::INTEGER,
            EXTRACT(WEEK FROM p_datetime)::INTEGER
        )
        RETURNING time_id INTO v_time_id;
    END IF;

    RETURN v_time_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION insert_or_get_time_id IS 'Insert or get time dimension record for a timestamp';

-- ============================================================================
-- Grant Permissions
-- ============================================================================

-- Grant permissions to public (adjust as needed for your security requirements)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO PUBLIC;
GRANT SELECT ON trip_summary TO PUBLIC;

-- ============================================================================
-- Schema Creation Complete
-- ============================================================================

COMMIT;

-- Display success message
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Star Schema Created Successfully!';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Tables created:';
    RAISE NOTICE '  - dim_location (% rows)', (SELECT COUNT(*) FROM dim_location);
    RAISE NOTICE '  - dim_time (% rows)', (SELECT COUNT(*) FROM dim_time);
    RAISE NOTICE '  - dim_rate (% rows)', (SELECT COUNT(*) FROM dim_rate);
    RAISE NOTICE '  - fact_trips (% rows)', (SELECT COUNT(*) FROM fact_trips);
    RAISE NOTICE '========================================';
END $$;
