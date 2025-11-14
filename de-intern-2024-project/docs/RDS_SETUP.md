# RDS PostgreSQL Setup for NYC Taxi Data

This guide provides step-by-step instructions for setting up an RDS PostgreSQL instance with a star schema for NYC Yellow Taxi trip data analysis.

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Architecture](#architecture)
4. [Setup Instructions](#setup-instructions)
5. [Usage Examples](#usage-examples)
6. [Troubleshooting](#troubleshooting)

## Overview

This setup creates a production-ready RDS PostgreSQL database with:
- **Instance**: db.t4g.micro (cost-effective, ARM-based)
- **Database**: oubt_ptg
- **Port**: 5432 (PostgreSQL default)
- **Schema**: Star schema optimized for analytical queries
- **Sample Data**: 10,000 NYC taxi trip records

### Star Schema Design

```
Dimension Tables:
├── dim_location (taxi zones, boroughs)
├── dim_time (temporal hierarchy)
└── dim_rate (fare rate codes)

Fact Table:
└── fact_trips (trip transactions with metrics)
```

## Prerequisites

### Software Requirements

```bash
# Python packages
pip install boto3 pandas psycopg2-binary

# AWS CLI configured with credentials
aws configure
```

### AWS Requirements

- AWS account with RDS permissions
- VPC with default settings or custom VPC configured
- Security group access (will be created automatically)
- IAM permissions for RDS operations

### Data Requirements

Sample taxi data must be available in one of these locations:
- `data/processed/yellow_tripdata_2024-01_sample.parquet`
- `data/processed/yellow_tripdata_2024-01_sample_rds.csv`
- `data/processed/yellow_tripdata_2024-01_cleaned.parquet`

Run the data preparation scripts first:
```bash
python src/data_processing/download_taxi_data.py
python src/data_processing/clean_taxi_data.py
python src/data_processing/sample_taxi_data.py
```

## Architecture

### Database Schema

#### Dimension Tables

**dim_location**
```sql
- location_id (PK)
- borough
- zone
- service_zone
```

**dim_time**
```sql
- time_id (PK)
- pickup_datetime
- year, month, day, hour
- weekday, is_weekend
- quarter, week_of_year
```

**dim_rate**
```sql
- rate_code_id (PK)
- rate_code_name
- description
```

#### Fact Table

**fact_trips**
```sql
- trip_id (PK)
- time_id (FK -> dim_time)
- pickup_location_id (FK -> dim_location)
- dropoff_location_id (FK -> dim_location)
- rate_code_id (FK -> dim_rate)
- passenger_count
- trip_distance
- trip_duration
- fare_amount
- tip_amount
- total_amount
- payment_type
```

### Views

**trip_summary**
- Denormalized view joining all dimensions with fact table
- Optimized for reporting and analytics

## Setup Instructions

### Step 1: Create RDS Instance

```bash
cd de-intern-2024-project

# Create RDS instance (interactive - will prompt for password)
python infrastructure/rds/create_rds_instance.py

# Or with parameters
python infrastructure/rds/create_rds_instance.py \
  --instance-id oubt-taxi-db \
  --database oubt_ptg \
  --username oubt_admin \
  --password "YourSecurePassword123!" \
  --instance-class db.t4g.micro \
  --region us-east-1
```

**Expected Output:**
```
Creating RDS instance: oubt-taxi-db
  Database: oubt_ptg
  Instance class: db.t4g.micro
  Storage: 20 GB
  Port: 5432

Waiting for instance to become available (this may take 10-15 minutes)...
Instance is now available (took 720 seconds)
Connection test successful!
```

The script will:
1. Create RDS instance with specified configuration
2. Create security group with PostgreSQL access
3. Wait for instance to become available
4. Test database connection
5. Save connection info to `config/rds_connection.json`

**Connection info is saved to:** `config/rds_connection.json`

### Step 2: Create Star Schema

Once the RDS instance is running, create the database schema:

```bash
# Option 1: Using psql directly
psql -h <RDS_ENDPOINT> -U oubt_admin -d oubt_ptg -f sql/rds/001_create_star_schema.sql

# Option 2: Using Python
python -c "
import psycopg2
import sys

conn = psycopg2.connect(
    host='<RDS_ENDPOINT>',
    port=5432,
    database='oubt_ptg',
    user='oubt_admin',
    password='<PASSWORD>'
)

with open('sql/rds/001_create_star_schema.sql', 'r') as f:
    sql = f.read()

cur = conn.cursor()
cur.execute(sql)
conn.commit()
print('Schema created successfully!')
"
```

**Expected Output:**
```sql
NOTICE:  ========================================
NOTICE:  Star Schema Created Successfully!
NOTICE:  ========================================
NOTICE:  Tables created:
NOTICE:    - dim_location (60 rows)
NOTICE:    - dim_time (0 rows)
NOTICE:    - dim_rate (6 rows)
NOTICE:    - fact_trips (0 rows)
NOTICE:  ========================================
```

### Step 3: Load Sample Data

Load 10,000 sample taxi trip records:

```bash
# Option 1: Using saved connection config
python src/database/load_rds_data.py --config config/rds_connection.json

# Option 2: Specify connection details
python src/database/load_rds_data.py \
  --host <RDS_ENDPOINT> \
  --database oubt_ptg \
  --user oubt_admin \
  --password "YourPassword" \
  --records 10000

# Option 3: Custom data file
python src/database/load_rds_data.py \
  --config config/rds_connection.json \
  --data-file data/processed/my_sample.parquet \
  --records 5000
```

**Expected Output:**
```
Loading data from data/processed/yellow_tripdata_2024-01_sample.parquet
Loaded 10,000 records
Populating dim_location...
  Inserted 50 location records
Populating dim_time...
  Inserted 10,000 time records
Populating fact_trips...
  Inserted 10,000 trip records
Transaction committed successfully!

DATA LOAD SUMMARY
================================================================================
Source records:     10,000
dim_location:       50 records inserted
dim_time:           10,000 records inserted
fact_trips:         10,000 records inserted
================================================================================
```

The script will:
1. Connect to RDS database
2. Verify schema exists
3. Load sample data from file
4. Populate dimension tables
5. Insert fact records with foreign keys
6. **Rollback on any error** (transaction safety)
7. Validate data load
8. Run sample analytical queries

## Usage Examples

### Connecting to RDS

**psql:**
```bash
psql -h <RDS_ENDPOINT> -p 5432 -U oubt_admin -d oubt_ptg
```

**Python (psycopg2):**
```python
import psycopg2

conn = psycopg2.connect(
    host='<RDS_ENDPOINT>',
    port=5432,
    database='oubt_ptg',
    user='oubt_admin',
    password='<PASSWORD>'
)

cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM fact_trips")
print(f"Total trips: {cur.fetchone()[0]:,}")
```

### Sample Analytical Queries

**Top pickup locations:**
```sql
SELECT
    pl.zone,
    pl.borough,
    COUNT(*) as trip_count,
    ROUND(AVG(ft.total_amount), 2) as avg_fare
FROM fact_trips ft
JOIN dim_location pl ON ft.pickup_location_id = pl.location_id
GROUP BY pl.zone, pl.borough
ORDER BY trip_count DESC
LIMIT 10;
```

**Hourly trip patterns:**
```sql
SELECT
    dt.hour,
    COUNT(*) as trip_count,
    ROUND(AVG(ft.total_amount), 2) as avg_fare,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance
FROM fact_trips ft
JOIN dim_time dt ON ft.time_id = dt.time_id
GROUP BY dt.hour
ORDER BY dt.hour;
```

**Weekend vs Weekday comparison:**
```sql
SELECT
    CASE WHEN dt.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
    COUNT(*) as trip_count,
    ROUND(AVG(ft.total_amount), 2) as avg_fare,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance,
    ROUND(AVG(ft.tip_amount), 2) as avg_tip
FROM fact_trips ft
JOIN dim_time dt ON ft.time_id = dt.time_id
GROUP BY dt.is_weekend;
```

**Using the denormalized view:**
```sql
SELECT
    pickup_zone,
    dropoff_zone,
    AVG(total_amount) as avg_fare
FROM trip_summary
WHERE is_weekend = true
GROUP BY pickup_zone, dropoff_zone
ORDER BY avg_fare DESC
LIMIT 10;
```

### Testing Connection

```bash
# Test connection to existing instance
python infrastructure/rds/create_rds_instance.py --test-only

# Check tables exist
psql -h <RDS_ENDPOINT> -U oubt_admin -d oubt_ptg -c "\dt"
```

## Maintenance Operations

### Deleting the RDS Instance

```bash
# Delete RDS instance (will prompt for confirmation)
python infrastructure/rds/create_rds_instance.py --delete

# Delete with custom instance ID
python infrastructure/rds/create_rds_instance.py --delete --instance-id oubt-taxi-db
```

⚠️ **Warning:** This will permanently delete the RDS instance and all data.

### Reloading Data

To reload data (e.g., after schema changes):

```sql
-- Connect to database
psql -h <RDS_ENDPOINT> -U oubt_admin -d oubt_ptg

-- Truncate tables in correct order
TRUNCATE TABLE fact_trips CASCADE;
TRUNCATE TABLE dim_time CASCADE;
-- dim_location and dim_rate can be preserved

-- Then rerun the load script
python src/database/load_rds_data.py --config config/rds_connection.json
```

## Troubleshooting

### Connection Issues

**Problem:** Cannot connect to RDS instance

**Solutions:**
1. Check security group rules:
   ```bash
   aws ec2 describe-security-groups --group-ids <SG_ID>
   ```

2. Verify instance is available:
   ```bash
   aws rds describe-db-instances --db-instance-identifier oubt-taxi-db
   ```

3. Test network connectivity:
   ```bash
   telnet <RDS_ENDPOINT> 5432
   ```

### Schema Creation Errors

**Problem:** Schema creation fails

**Solutions:**
1. Ensure database exists:
   ```sql
   psql -h <RDS_ENDPOINT> -U oubt_admin -l
   ```

2. Check permissions:
   ```sql
   SELECT * FROM pg_roles WHERE rolname = 'oubt_admin';
   ```

3. Drop and recreate schema:
   ```sql
   DROP SCHEMA public CASCADE;
   CREATE SCHEMA public;
   -- Then rerun schema script
   ```

### Data Load Issues

**Problem:** Data load fails or is incomplete

**Solutions:**
1. Check source data exists:
   ```bash
   ls -lh data/processed/
   ```

2. Verify schema tables exist:
   ```sql
   SELECT table_name FROM information_schema.tables
   WHERE table_schema = 'public';
   ```

3. Check for constraint violations:
   ```sql
   -- Review foreign key constraints
   SELECT conname, contype, confupdtype, confdeltype
   FROM pg_constraint
   WHERE contype = 'f';
   ```

4. View recent errors:
   ```bash
   # Check CloudWatch logs in AWS console
   # Or use AWS CLI
   aws logs tail /aws/rds/instance/oubt-taxi-db/postgresql
   ```

### Performance Issues

**Problem:** Queries are slow

**Solutions:**
1. Analyze query performance:
   ```sql
   EXPLAIN ANALYZE SELECT ...;
   ```

2. Rebuild indexes:
   ```sql
   REINDEX TABLE fact_trips;
   ```

3. Update statistics:
   ```sql
   ANALYZE fact_trips;
   ```

4. Check for missing indexes:
   ```sql
   -- Review existing indexes
   SELECT tablename, indexname, indexdef
   FROM pg_indexes
   WHERE schemaname = 'public';
   ```

## Cost Estimation

### RDS Instance Costs

- **db.t4g.micro**: ~$0.016/hour = ~$12/month
- **20 GB storage**: ~$2.30/month
- **Total**: ~$15/month (may vary by region)

### Cost Optimization

- Use **Reserved Instances** for 30-75% savings on long-term usage
- Stop instance when not in use (development only)
- Use **Automated Backups** sparingly (costs for storage)
- Monitor with **CloudWatch** to optimize instance size

## Next Steps

1. **Load Full Dataset**: Increase `--records` parameter to load more data
2. **Add Business Logic**: Create views and stored procedures for common queries
3. **Set Up Monitoring**: Configure CloudWatch alarms for database metrics
4. **Implement ETL**: Automate data ingestion from S3 or other sources
5. **Connect BI Tools**: Link Tableau, PowerBI, or Metabase for visualization
6. **Add More Dimensions**: Expand schema with weather, events, or other data

## References

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [AWS RDS PostgreSQL](https://aws.amazon.com/rds/postgresql/)
- [Star Schema Design](https://en.wikipedia.org/wiki/Star_schema)

## Support

For issues or questions:
1. Check the [Troubleshooting](#troubleshooting) section
2. Review AWS RDS logs in CloudWatch
3. Consult the team documentation or data engineering team
