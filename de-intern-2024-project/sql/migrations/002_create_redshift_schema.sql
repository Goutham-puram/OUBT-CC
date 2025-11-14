-- Migration: Create Redshift data warehouse schema
-- Week 4: Dimensional model (star schema)

-- Dimension Table: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20),
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
)
DISTSTYLE ALL;

-- Dimension Table: Time
CREATE TABLE IF NOT EXISTS dim_time (
    time_key INTEGER PRIMARY KEY,
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    time_of_day VARCHAR(20), -- Morning, Afternoon, Evening, Night
    is_rush_hour BOOLEAN
)
DISTSTYLE ALL;

-- Dimension Table: Location
CREATE TABLE IF NOT EXISTS dim_location (
    location_key INTEGER PRIMARY KEY,
    location_id INTEGER NOT NULL,
    borough VARCHAR(50),
    zone VARCHAR(100),
    service_zone VARCHAR(50)
)
DISTSTYLE ALL;

-- Dimension Table: Payment Type
CREATE TABLE IF NOT EXISTS dim_payment_type (
    payment_type_key INTEGER PRIMARY KEY,
    payment_type_id INTEGER NOT NULL,
    payment_type_name VARCHAR(50)
)
DISTSTYLE ALL;

-- Dimension Table: Rate Code
CREATE TABLE IF NOT EXISTS dim_rate_code (
    rate_code_key INTEGER PRIMARY KEY,
    rate_code_id INTEGER NOT NULL,
    rate_code_name VARCHAR(50),
    rate_code_description VARCHAR(200)
)
DISTSTYLE ALL;

-- Fact Table: Taxi Trips
CREATE TABLE IF NOT EXISTS fact_taxi_trips (
    trip_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    pickup_date_key INTEGER NOT NULL,
    pickup_time_key INTEGER NOT NULL,
    dropoff_date_key INTEGER NOT NULL,
    dropoff_time_key INTEGER NOT NULL,
    pickup_location_key INTEGER,
    dropoff_location_key INTEGER,
    payment_type_key INTEGER,
    rate_code_key INTEGER,
    vendor_id INTEGER,
    passenger_count INTEGER,
    trip_distance NUMERIC(10, 2),
    trip_duration_minutes NUMERIC(10, 2),
    fare_amount NUMERIC(10, 2),
    extra NUMERIC(10, 2),
    mta_tax NUMERIC(10, 2),
    tip_amount NUMERIC(10, 2),
    tolls_amount NUMERIC(10, 2),
    improvement_surcharge NUMERIC(10, 2),
    total_amount NUMERIC(10, 2),
    congestion_surcharge NUMERIC(10, 2),
    airport_fee NUMERIC(10, 2),
    avg_speed_mph NUMERIC(10, 2),
    quality_flag VARCHAR(20),
    created_at TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(pickup_location_key)
SORTKEY(pickup_date_key, pickup_time_key);

-- Foreign Key Constraints (enforced at application level in Redshift)
-- These are informational only
ALTER TABLE fact_taxi_trips ADD CONSTRAINT fk_pickup_date FOREIGN KEY (pickup_date_key) REFERENCES dim_date(date_key);
ALTER TABLE fact_taxi_trips ADD CONSTRAINT fk_dropoff_date FOREIGN KEY (dropoff_date_key) REFERENCES dim_date(date_key);
ALTER TABLE fact_taxi_trips ADD CONSTRAINT fk_pickup_time FOREIGN KEY (pickup_time_key) REFERENCES dim_time(time_key);
ALTER TABLE fact_taxi_trips ADD CONSTRAINT fk_dropoff_time FOREIGN KEY (dropoff_time_key) REFERENCES dim_time(time_key);
ALTER TABLE fact_taxi_trips ADD CONSTRAINT fk_pickup_location FOREIGN KEY (pickup_location_key) REFERENCES dim_location(location_key);
ALTER TABLE fact_taxi_trips ADD CONSTRAINT fk_dropoff_location FOREIGN KEY (dropoff_location_key) REFERENCES dim_location(location_key);
ALTER TABLE fact_taxi_trips ADD CONSTRAINT fk_payment_type FOREIGN KEY (payment_type_key) REFERENCES dim_payment_type(payment_type_key);
ALTER TABLE fact_taxi_trips ADD CONSTRAINT fk_rate_code FOREIGN KEY (rate_code_key) REFERENCES dim_rate_code(rate_code_key);
