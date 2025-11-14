# AWS Glue Data Catalog Implementation

This document describes the AWS Glue Data Catalog implementation for schema discovery and metadata management of the NYC taxi data lake.

## Overview

The AWS Glue Data Catalog provides a centralized metadata repository for the NYC taxi data lake. It automatically discovers schemas, tracks partitions, and maintains metadata for all data assets in S3.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    AWS Glue Data Catalog                    │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │           Database: nyc_taxi_catalog                  │  │
│  │                                                       │  │
│  │  ┌────────────────────────────────────────────────┐  │  │
│  │  │  Tables (auto-discovered)                      │  │  │
│  │  │  - taxi: 19 columns + 2 partition keys        │  │  │
│  │  │  - Partitions: year, month                     │  │  │
│  │  └────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │         Crawler: crawler-taxi-raw                    │  │
│  │         - Target: s3://{bucket}/raw/taxi/            │  │
│  │         - Schedule: On-demand                        │  │
│  │         - Schema discovery: Automatic                │  │
│  │         - Partition detection: Enabled               │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                  S3 Data Lake                               │
│  s3://{account-id}-oubt-datalake/                          │
│    └── raw/                                                 │
│        └── taxi/                                            │
│            ├── year=2024/                                   │
│            │   ├── month=01/                                │
│            │   ├── month=02/                                │
│            │   └── ...                                      │
│            └── ...                                          │
└─────────────────────────────────────────────────────────────┘
```

## Components

### 1. IAM Role (`infrastructure/iam/create_glue_role.py`)

Creates the IAM role required for AWS Glue to access S3 resources.

**Role Name:** `AWSGlueServiceRole-intern`

**Trust Policy:**
- Service: `glue.amazonaws.com`

**Permissions:**
- AWS Managed Policy: `AWSGlueServiceRole`
- Custom S3 Policy: Read/write access to data lake buckets

**Usage:**
```bash
# Create the role
python infrastructure/iam/create_glue_role.py

# Check role information
python infrastructure/iam/create_glue_role.py --info-only

# Custom role name
python infrastructure/iam/create_glue_role.py --role-name MyCustomGlueRole
```

### 2. Glue Database (`src/glue/create_glue_database.py`)

Creates the Glue catalog database to store metadata.

**Database Name:** `nyc_taxi_catalog`

**Description:** NYC taxi data lake metadata repository

**Features:**
- Centralized metadata storage
- Table and partition tracking
- Schema versioning
- Data classification tags

**Usage:**
```bash
# Create the database
python src/glue/create_glue_database.py

# Check database information
python src/glue/create_glue_database.py --info-only

# Delete database (use with caution)
python src/glue/create_glue_database.py --delete
```

### 3. Crawler Configuration (`src/glue/configure_crawler.py`)

Configures the Glue crawler for automatic schema discovery.

**Crawler Name:** `crawler-taxi-raw`

**Target:** `s3://{account-id}-oubt-datalake/raw/taxi/`

**Schedule:** On-demand (manual trigger)

**Configuration:**
- **Schema Change Policy:**
  - Update Behavior: `UPDATE_IN_DATABASE` - Updates table schemas automatically
  - Delete Behavior: `LOG` - Logs deletions without removing tables

- **Recrawl Policy:** `CRAWL_EVERYTHING` - Recrawls all data on each run

- **Partition Detection:** Automatic - Discovers year/month partitions

- **Table Grouping:** `CombineCompatibleSchemas` - Combines similar schemas

**Usage:**
```bash
# Create the crawler
python src/glue/configure_crawler.py

# Check crawler information
python src/glue/configure_crawler.py --info-only

# Update existing crawler
python src/glue/configure_crawler.py --update

# Delete crawler
python src/glue/configure_crawler.py --delete
```

### 4. Crawler Execution & Verification (`src/glue/run_crawler_verification.py`)

Runs the crawler and verifies schema discovery results.

**Verification Checks:**
- ✓ Crawler execution status
- ✓ Schema discovery (19 data columns expected)
- ✓ Partition key detection (2 keys: year, month)
- ✓ Table creation/update
- ✓ Data types and column names
- ✓ Partition values

**Usage:**
```bash
# Run crawler and wait for completion (with verification)
python src/glue/run_crawler_verification.py

# Run crawler without waiting
python src/glue/run_crawler_verification.py --no-wait

# Verify existing tables only (don't run crawler)
python src/glue/run_crawler_verification.py --verify-only

# Custom timeout (default: 600 seconds)
python src/glue/run_crawler_verification.py --timeout 900
```

## Setup Instructions

### Prerequisites

1. AWS credentials configured
2. S3 data lake created with NYC taxi data
3. Data uploaded to `s3://{account-id}-oubt-datalake/raw/taxi/`

### Step-by-Step Setup

```bash
# Step 1: Create IAM role for Glue
cd /home/user/OUBT-CC/de-intern-2024-project
python infrastructure/iam/create_glue_role.py

# Step 2: Create Glue catalog database
python src/glue/create_glue_database.py

# Step 3: Configure crawler
python src/glue/configure_crawler.py

# Step 4: Run crawler and verify
python src/glue/run_crawler_verification.py
```

### Expected Output

After successful execution, you should see:

```
================================================================================
SUCCESS: Crawler execution and verification completed!
================================================================================

Summary:
  Crawler: crawler-taxi-raw
  Database: nyc_taxi_catalog
  Tables Discovered: 1
  Status: All verifications passed

--- Verifying Table: taxi ---

Table: taxi
  Location: s3://123456789012-oubt-datalake/raw/taxi/
  Input Format: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
  Output Format: org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat

  Data Columns (19):
    - vendorid: bigint
    - tpep_pickup_datetime: timestamp
    - tpep_dropoff_datetime: timestamp
    - passenger_count: double
    - trip_distance: double
    - ratecodeid: double
    - store_and_fwd_flag: string
    - pulocationid: bigint
    - dolocationid: bigint
    - payment_type: bigint
    - fare_amount: double
    - extra: double
    - mta_tax: double
    - tip_amount: double
    - tolls_amount: double
    - improvement_surcharge: double
    - total_amount: double
    - congestion_surcharge: double
    - airport_fee: double

  Partition Keys (2):
    - year: string
    - month: string

✓ Column count matches: 19 columns
✓ Partition key count matches: 2 keys
```

## NYC Taxi Data Schema

### Data Columns (19)

| Column Name | Data Type | Description |
|------------|-----------|-------------|
| vendorid | bigint | Taxi vendor ID |
| tpep_pickup_datetime | timestamp | Pickup date and time |
| tpep_dropoff_datetime | timestamp | Drop-off date and time |
| passenger_count | double | Number of passengers |
| trip_distance | double | Trip distance in miles |
| ratecodeid | double | Rate code ID |
| store_and_fwd_flag | string | Store and forward flag |
| pulocationid | bigint | Pickup location ID |
| dolocationid | bigint | Drop-off location ID |
| payment_type | bigint | Payment type |
| fare_amount | double | Fare amount |
| extra | double | Extra charges |
| mta_tax | double | MTA tax |
| tip_amount | double | Tip amount |
| tolls_amount | double | Tolls amount |
| improvement_surcharge | double | Improvement surcharge |
| total_amount | double | Total amount |
| congestion_surcharge | double | Congestion surcharge |
| airport_fee | double | Airport fee |

### Partition Keys (2)

| Key | Data Type | Description |
|-----|-----------|-------------|
| year | string | Year of pickup (YYYY) |
| month | string | Month of pickup (MM) |

## Error Handling

All scripts include comprehensive error handling:

### Common Errors

1. **Role Already Exists**
   ```
   WARNING: Role AWSGlueServiceRole-intern already exists
   ```
   - Solution: Use `--info-only` to check existing role or specify a different name

2. **Database Already Exists**
   ```
   WARNING: Database nyc_taxi_catalog already exists
   ```
   - Solution: This is okay, the script will continue

3. **Crawler Already Running**
   ```
   WARNING: Crawler crawler-taxi-raw is already running
   ```
   - Solution: Wait for current run to complete or check status with `--info-only`

4. **No Data Found**
   ```
   ERROR: No tables were discovered
   ```
   - Solution: Ensure S3 bucket has data at the specified path

5. **Permission Denied**
   ```
   ERROR: Failed to create role: Access Denied
   ```
   - Solution: Ensure AWS credentials have IAM permissions

### Crawler Failures

The crawler may fail for several reasons:

- **S3 Path Not Found:** Verify S3 bucket and path exist
- **Insufficient Permissions:** Check IAM role has S3 read access
- **Invalid Data Format:** Ensure data is in supported format (Parquet)
- **Empty Directory:** Crawler requires at least one file to scan

## Monitoring and Maintenance

### Check Crawler Status

```bash
# Get crawler information
python src/glue/configure_crawler.py --info-only

# Via AWS CLI
aws glue get-crawler --name crawler-taxi-raw
```

### View Crawl History

```bash
# Get crawler metrics
aws glue get-crawler-metrics --crawler-name-list crawler-taxi-raw
```

### Update Schema

When data schema changes:

```bash
# Re-run crawler to update schema
python src/glue/run_crawler_verification.py
```

### View Tables and Partitions

```bash
# Get table information
aws glue get-table --database-name nyc_taxi_catalog --name taxi

# Get partitions
aws glue get-partitions --database-name nyc_taxi_catalog --table-name taxi
```

## Integration with Other Services

### Athena

Query data using the Glue catalog:

```sql
-- Query NYC taxi data
SELECT
    year,
    month,
    COUNT(*) as trip_count,
    AVG(trip_distance) as avg_distance,
    AVG(fare_amount) as avg_fare
FROM nyc_taxi_catalog.taxi
WHERE year = '2024' AND month = '01'
GROUP BY year, month;
```

### Glue ETL Jobs

Reference catalog tables in Glue jobs:

```python
# Read from Glue catalog
datasource = glueContext.create_dynamic_frame.from_catalog(
    database="nyc_taxi_catalog",
    table_name="taxi"
)
```

### Spark/PySpark

Access catalog tables:

```python
# Read from Glue catalog
df = spark.read.table("nyc_taxi_catalog.taxi")
```

## Best Practices

1. **Run Crawler After Data Updates**
   - Schedule crawler runs after new data arrives
   - Use EventBridge to trigger on S3 events

2. **Monitor Crawler Metrics**
   - Track tables created/updated/deleted
   - Monitor crawl duration and failures

3. **Partition Strategy**
   - Current partitioning: year/month
   - Consider day-level partitioning for large datasets

4. **Schema Evolution**
   - Update behavior is set to `UPDATE_IN_DATABASE`
   - Schema changes are automatically reflected

5. **Cost Optimization**
   - Run crawler only when needed (on-demand)
   - Avoid frequent crawls of unchanged data

## Troubleshooting

### Debug Mode

Enable detailed logging:

```bash
export LOG_LEVEL=DEBUG
python src/glue/run_crawler_verification.py
```

### Verify S3 Access

```bash
# Check if data exists
aws s3 ls s3://{account-id}-oubt-datalake/raw/taxi/ --recursive

# Check IAM role
python infrastructure/iam/create_glue_role.py --info-only
```

### Test Crawler Manually

```bash
# Start crawler via AWS CLI
aws glue start-crawler --name crawler-taxi-raw

# Check status
aws glue get-crawler --name crawler-taxi-raw
```

## Resources

- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Glue Data Catalog Best Practices](https://docs.aws.amazon.com/prescriptive-guidance/latest/serverless-etl-aws-glue/aws-glue-data-catalog.html)
- [Crawler Configuration](https://docs.aws.amazon.com/glue/latest/dg/add-crawler.html)
- [Partition Indexing](https://docs.aws.amazon.com/glue/latest/dg/partition-indexes.html)

## Summary

This implementation provides:

✅ Automated schema discovery
✅ Partition detection and tracking
✅ Centralized metadata management
✅ Integration with Athena, Spark, and Glue ETL
✅ Comprehensive error handling
✅ Detailed verification and monitoring

The Glue Data Catalog now serves as the central metadata repository for the NYC taxi data lake, enabling efficient data discovery and query optimization.
