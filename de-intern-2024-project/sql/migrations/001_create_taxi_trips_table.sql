-- Migration: Create taxi_trips table in PostgreSQL
-- Week 1: Initial table for raw taxi data

CREATE TABLE IF NOT EXISTS taxi_trips (
    trip_id SERIAL PRIMARY KEY,
    VendorID INTEGER,
    tpep_pickup_datetime TIMESTAMP NOT NULL,
    tpep_dropoff_datetime TIMESTAMP NOT NULL,
    passenger_count INTEGER,
    trip_distance NUMERIC(10, 2),
    RatecodeID INTEGER,
    store_and_fwd_flag VARCHAR(1),
    PULocationID INTEGER,
    DOLocationID INTEGER,
    payment_type INTEGER,
    fare_amount NUMERIC(10, 2),
    extra NUMERIC(10, 2),
    mta_tax NUMERIC(10, 2),
    tip_amount NUMERIC(10, 2),
    tolls_amount NUMERIC(10, 2),
    improvement_surcharge NUMERIC(10, 2),
    total_amount NUMERIC(10, 2),
    congestion_surcharge NUMERIC(10, 2),
    airport_fee NUMERIC(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_pickup_datetime ON taxi_trips(tpep_pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_dropoff_datetime ON taxi_trips(tpep_dropoff_datetime);
CREATE INDEX IF NOT EXISTS idx_pu_location ON taxi_trips(PULocationID);
CREATE INDEX IF NOT EXISTS idx_do_location ON taxi_trips(DOLocationID);
CREATE INDEX IF NOT EXISTS idx_payment_type ON taxi_trips(payment_type);

-- Add comments
COMMENT ON TABLE taxi_trips IS 'NYC Yellow Taxi trip records';
COMMENT ON COLUMN taxi_trips.tpep_pickup_datetime IS 'Pickup date and time';
COMMENT ON COLUMN taxi_trips.tpep_dropoff_datetime IS 'Dropoff date and time';
COMMENT ON COLUMN taxi_trips.trip_distance IS 'Trip distance in miles';
COMMENT ON COLUMN taxi_trips.fare_amount IS 'Base fare amount';
COMMENT ON COLUMN taxi_trips.total_amount IS 'Total amount charged to passengers';
