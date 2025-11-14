# AWS Glue ETL Job for Taxi Data Transformation

This directory contains the AWS Glue ETL job for processing taxi data from the raw zone to the processed zone with comprehensive data quality checks.

## Files

- **`glue_etl_job.py`**: Main ETL script that runs in AWS Glue
- **`data_quality.py`**: Data quality checking module with validation functions
- **`__init__.py`**: Package initialization

## ETL Pipeline Overview

The ETL job performs the following steps:

1. **Read** raw taxi data from `s3://{bucket}/raw/taxi/`
2. **Transform** data by adding derived columns:
   - `trip_duration_minutes`: Duration of trip in minutes
   - `tip_percentage`: Tip amount as percentage of fare
   - `quality_check_timestamp`: Timestamp when quality checks were performed
   - `year`, `month`: Partition columns
3. **Validate** data quality:
   - Check for null values in required fields
   - Validate numeric ranges (passenger count, trip distance, fare amount, etc.)
   - Validate datetime logic (dropoff after pickup)
   - Validate trip duration (1-300 minutes)
   - Validate tip percentage (0-100%)
4. **Write** valid records to `s3://{bucket}/processed/taxi/` partitioned by year/month
5. **Archive** failed records to dead-letter queue at `s3://{bucket}/dead-letter-queue/taxi/`

## Job Configuration

The job is configured with the following specifications:

- **Job Name**: `job-process-taxi-data`
- **Type**: Spark ETL (`glueetl`)
- **Glue Version**: 4.0
- **Worker Type**: G.1X (4 vCPU, 16 GB memory, 64 GB disk)
- **Number of Workers**: 2
- **Job Bookmarks**: Enabled (for incremental processing)

## Data Quality Checks

### Required Fields (No Nulls)
- `tpep_pickup_datetime`
- `tpep_dropoff_datetime`
- `passenger_count`
- `trip_distance`
- `fare_amount`
- `total_amount`

### Numeric Range Validations

| Field | Minimum | Maximum |
|-------|---------|---------|
| passenger_count | 1 | 6 |
| trip_distance | 0.1 | 100.0 |
| fare_amount | 0.01 | 500.0 |
| total_amount | 0.01 | 1000.0 |
| tip_amount | 0.0 | 200.0 |
| tolls_amount | 0.0 | 100.0 |
| extra | 0.0 | 10.0 |
| mta_tax | 0.0 | 1.0 |

### Additional Validations
- Dropoff datetime must be after pickup datetime
- Trip duration: 1-300 minutes
- Tip percentage: 0-100%

## Setting Up the Job

### Prerequisites

1. IAM role for Glue must exist:
   ```bash
   python infrastructure/iam/create_glue_role.py --role-name AWSGlueServiceRole-intern
   ```

2. S3 bucket must exist with proper structure:
   ```
   {account-id}-oubt-datalake/
   ├── raw/taxi/           # Input data
   ├── processed/taxi/     # Output data
   ├── dead-letter-queue/  # Failed records
   ├── scripts/glue/       # ETL scripts
   └── temp/glue/          # Temporary data
   ```

### Creating the Job

```bash
# Create the Glue job
python src/glue/create_etl_job.py \
  --job-name job-process-taxi-data \
  --role-name AWSGlueServiceRole-intern \
  --script-path src/transform/glue_etl_job.py

# Create and start the job
python src/glue/create_etl_job.py \
  --job-name job-process-taxi-data \
  --script-path src/transform/glue_etl_job.py \
  --start
```

### Updating the Job

```bash
# Update existing job with new script
python src/glue/create_etl_job.py \
  --job-name job-process-taxi-data \
  --script-path src/transform/glue_etl_job.py \
  --update
```

### Getting Job Info

```bash
# Display job configuration
python src/glue/create_etl_job.py \
  --job-name job-process-taxi-data \
  --info-only
```

## Running Tests

Unit tests are provided using pytest with PySpark:

```bash
# Run all ETL tests
pytest tests/test_glue_etl_job.py -v

# Run data quality tests
pytest tests/test_data_quality.py -v

# Run all tests with coverage
pytest tests/test_glue_etl_job.py tests/test_data_quality.py --cov=src/transform --cov-report=html
```

## Monitoring

The job provides the following monitoring capabilities:

1. **CloudWatch Logs**: Enabled with continuous logging
2. **Spark UI**: Enabled with logs stored in `s3://{bucket}/logs/spark-ui/`
3. **Metrics**: CloudWatch metrics for job runs, duration, and DPU usage
4. **Job Bookmarks**: Track processed data for incremental runs

## Job Parameters

The job accepts the following parameters:

- `--bucket_name`: S3 bucket name (default: set by create_etl_job.py)
- `--source_prefix`: Source data prefix (default: `raw/taxi/`)
- `--target_prefix`: Target data prefix (default: `processed/taxi/`)

## Output Data

### Processed Data

Valid records are written to:
```
s3://{bucket}/processed/taxi/year={YYYY}/month={MM}/
```

Format: Parquet (compressed, partitioned by year and month)

Columns include all original columns plus:
- `trip_duration_minutes`
- `tip_percentage`
- `quality_check_timestamp`
- `processed_at`
- `etl_job_name`
- `year`
- `month`

### Failed Records (Dead Letter Queue)

Invalid records are written to:
```
s3://{bucket}/dead-letter-queue/taxi/year={YYYY}/month={MM}/run_id={run_id}/
```

Format: Parquet

Additional columns:
- `failure_reason`: Reason for rejection (e.g., 'null_in_required_field', 'numeric_range_violation')
- `failed_at`: Timestamp when record failed validation
- `run_id`: Unique identifier for the job run

## Incremental Processing

The job uses AWS Glue Job Bookmarks to track processed files and enable incremental processing:

1. **First Run**: Processes all data in the source location
2. **Subsequent Runs**: Only processes new files added since the last successful run
3. **Reset Bookmark**: Use AWS Glue Console or AWS CLI to reset bookmark if full reprocessing is needed

```bash
# Reset job bookmark (AWS CLI)
aws glue reset-job-bookmark --job-name job-process-taxi-data
```

## Troubleshooting

### Common Issues

1. **Role Not Found Error**
   - Ensure IAM role exists: `python infrastructure/iam/create_glue_role.py`

2. **S3 Access Denied**
   - Verify IAM role has S3 read/write permissions
   - Check bucket policy and encryption settings

3. **No Data Processed**
   - Check that source data exists in `s3://{bucket}/raw/taxi/`
   - Verify job bookmark hasn't already processed all data

4. **High Rejection Rate**
   - Review DLQ records to understand failure patterns
   - Adjust validation rules if necessary
   - Check source data quality

## Performance Considerations

- **Worker Scaling**: Increase `NumberOfWorkers` for larger datasets
- **Worker Type**: Use G.2X for more memory-intensive operations
- **Partitioning**: Data is partitioned by year/month for efficient querying
- **Format**: Parquet format with compression for optimal storage and query performance

## Best Practices

1. **Monitor DLQ**: Regularly review failed records in the dead-letter queue
2. **Job Bookmarks**: Enable for incremental processing to avoid reprocessing data
3. **Testing**: Run unit tests before deploying script changes
4. **Validation**: Adjust validation rules based on business requirements
5. **Logging**: Review CloudWatch logs for errors and performance metrics

## Related Documentation

- [AWS Glue Developer Guide](https://docs.aws.amazon.com/glue/)
- [PySpark API Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Project Main README](../../README.md)
