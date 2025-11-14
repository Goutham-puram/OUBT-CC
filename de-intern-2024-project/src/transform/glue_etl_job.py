"""
AWS Glue ETL Job: Taxi Data Transformation Pipeline

This job:
1. Reads raw taxi data from S3 (raw/taxi/)
2. Performs data transformations:
   - Adds trip_duration_minutes column
   - Adds tip_percentage column
   - Filters invalid records (trip_distance > 0, fare > 0)
   - Adds data quality timestamp
3. Runs comprehensive data quality checks
4. Writes processed data to S3 (processed/taxi/) partitioned by year/month
5. Writes failed records to dead-letter queue
6. Uses job bookmarks for incremental processing
"""

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Import data quality checker
# Note: In Glue, we need to package this or include it in the script
# For now, we'll inline the necessary functionality or use --extra-py-files


def add_derived_columns(df):
    """
    Add derived columns to the taxi data.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with derived columns added.
    """
    print("Adding derived columns...")

    # 1. Add trip_duration_minutes
    df = df.withColumn(
        'trip_duration_minutes',
        (F.unix_timestamp('tpep_dropoff_datetime') -
         F.unix_timestamp('tpep_pickup_datetime')) / 60.0
    )

    # 2. Add tip_percentage (tip_amount / fare_amount * 100)
    df = df.withColumn(
        'tip_percentage',
        F.when(F.col('fare_amount') > 0,
               (F.col('tip_amount') / F.col('fare_amount')) * 100.0)
        .otherwise(0.0)
    )

    # 3. Add data quality timestamp
    df = df.withColumn('quality_check_timestamp', F.current_timestamp())

    # 4. Add year and month for partitioning
    df = df.withColumn('year', F.year('tpep_pickup_datetime'))
    df = df.withColumn('month', F.month('tpep_pickup_datetime'))

    # 5. Add processing metadata
    df = df.withColumn('processed_at', F.current_timestamp())
    df = df.withColumn('etl_job_name', F.lit('job-process-taxi-data'))

    print("Derived columns added successfully")
    return df


def filter_invalid_records(df):
    """
    Filter out invalid records based on business rules.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with invalid records filtered out.
    """
    print("Filtering invalid records...")

    initial_count = df.count()

    # Filter conditions
    df = df.filter(
        (F.col('trip_distance') > 0) &
        (F.col('fare_amount') > 0) &
        (F.col('passenger_count') > 0) &
        (F.col('tpep_dropoff_datetime') > F.col('tpep_pickup_datetime'))
    )

    final_count = df.count()
    removed_count = initial_count - final_count

    print(f"Records before filtering: {initial_count}")
    print(f"Records after filtering: {final_count}")
    print(f"Records removed: {removed_count} ({removed_count/initial_count*100:.2f}%)")

    return df


def run_data_quality_checks(df, spark, bucket_name, run_id):
    """
    Run comprehensive data quality checks.

    Args:
        df: Input DataFrame.
        spark: SparkSession.
        bucket_name: S3 bucket name.
        run_id: Run identifier.

    Returns:
        Tuple of (valid DataFrame, list of invalid DataFrames).
    """
    print("\n" + "=" * 80)
    print("Running Data Quality Checks")
    print("=" * 80)

    initial_count = df.count()
    invalid_dfs = []

    # Check 1: Null values in required fields
    print("\n[CHECK 1] Checking for null values in required fields...")
    required_fields = [
        'tpep_pickup_datetime', 'tpep_dropoff_datetime',
        'passenger_count', 'trip_distance', 'fare_amount'
    ]

    null_condition = None
    for field in required_fields:
        if field in df.columns:
            field_condition = F.col(field).isNull()
            null_condition = field_condition if null_condition is None else null_condition | field_condition

    if null_condition is not None:
        invalid_nulls = df.filter(null_condition).withColumn(
            'failure_reason', F.lit('null_in_required_field')
        ).withColumn('failed_at', F.current_timestamp())

        null_count = invalid_nulls.count()
        print(f"  Found {null_count} records with null values")

        if null_count > 0:
            invalid_dfs.append(invalid_nulls)

        df = df.filter(~null_condition)

    # Check 2: Numeric range validations
    print("\n[CHECK 2] Validating numeric ranges...")
    range_condition = (
        (F.col('passenger_count') >= 1) & (F.col('passenger_count') <= 6) &
        (F.col('trip_distance') >= 0.1) & (F.col('trip_distance') <= 100.0) &
        (F.col('fare_amount') >= 0.01) & (F.col('fare_amount') <= 500.0)
    )

    invalid_ranges = df.filter(~range_condition).withColumn(
        'failure_reason', F.lit('numeric_range_violation')
    ).withColumn('failed_at', F.current_timestamp())

    range_count = invalid_ranges.count()
    print(f"  Found {range_count} records with range violations")

    if range_count > 0:
        invalid_dfs.append(invalid_ranges)

    df = df.filter(range_condition)

    # Check 3: Trip duration validation
    print("\n[CHECK 3] Validating trip duration...")
    if 'trip_duration_minutes' in df.columns:
        duration_condition = (
            (F.col('trip_duration_minutes') >= 1.0) &
            (F.col('trip_duration_minutes') <= 300.0)
        )

        invalid_duration = df.filter(~duration_condition).withColumn(
            'failure_reason', F.lit('invalid_trip_duration')
        ).withColumn('failed_at', F.current_timestamp())

        duration_count = invalid_duration.count()
        print(f"  Found {duration_count} records with invalid trip duration")

        if duration_count > 0:
            invalid_dfs.append(invalid_duration)

        df = df.filter(duration_condition)

    # Check 4: Tip percentage validation
    print("\n[CHECK 4] Validating tip percentage...")
    if 'tip_percentage' in df.columns:
        tip_condition = (
            (F.col('tip_percentage') >= 0) &
            (F.col('tip_percentage') <= 100.0)
        )

        invalid_tip = df.filter(~tip_condition).withColumn(
            'failure_reason', F.lit('invalid_tip_percentage')
        ).withColumn('failed_at', F.current_timestamp())

        tip_count = invalid_tip.count()
        print(f"  Found {tip_count} records with invalid tip percentage")

        if tip_count > 0:
            invalid_dfs.append(invalid_tip)

        df = df.filter(tip_condition)

    final_count = df.count()
    rejected_count = initial_count - final_count

    print("\n" + "=" * 80)
    print("Data Quality Summary")
    print("=" * 80)
    print(f"Records processed: {initial_count}")
    print(f"Records passed: {final_count}")
    print(f"Records rejected: {rejected_count} ({rejected_count/initial_count*100:.2f}%)")
    print("=" * 80 + "\n")

    return df, invalid_dfs


def write_to_dead_letter_queue(invalid_dfs, bucket_name, run_id, spark):
    """
    Write failed records to dead-letter queue.

    Args:
        invalid_dfs: List of invalid DataFrames.
        bucket_name: S3 bucket name.
        run_id: Run identifier.
        spark: SparkSession.
    """
    if not invalid_dfs or all(df.count() == 0 for df in invalid_dfs):
        print("No failed records to write to DLQ")
        return

    print("\nWriting failed records to dead-letter queue...")

    # Combine all invalid records
    combined_invalid = None
    for invalid_df in invalid_dfs:
        if invalid_df.count() > 0:
            if combined_invalid is None:
                combined_invalid = invalid_df
            else:
                # Union with missing columns handling
                combined_invalid = combined_invalid.unionByName(invalid_df, allowMissingColumns=True)

    if combined_invalid is None or combined_invalid.count() == 0:
        print("No failed records to write")
        return

    # Add run metadata
    combined_invalid = combined_invalid.withColumn('run_id', F.lit(run_id))

    # Write to DLQ
    now = datetime.now()
    dlq_path = f"s3://{bucket_name}/dead-letter-queue/taxi/year={now.year}/month={now.month:02d}/run_id={run_id}/"

    print(f"Writing {combined_invalid.count()} failed records to: {dlq_path}")

    try:
        combined_invalid.write \
            .mode('overwrite') \
            .parquet(dlq_path)
        print("Failed records written successfully to DLQ")
    except Exception as e:
        print(f"Error writing to DLQ: {e}")
        # Don't fail the job if DLQ write fails, just log it
        print("Continuing job execution despite DLQ write failure")


def main():
    """Main ETL job execution."""
    # Get job parameters
    args = getResolvedOptions(
        sys.argv,
        ['JOB_NAME', 'bucket_name', 'source_prefix', 'target_prefix']
    )

    # Initialize Glue context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)

    # Initialize job with bookmark support for incremental processing
    job.init(args['JOB_NAME'], args)

    # Generate run ID
    run_id = datetime.now().strftime('%Y%m%d_%H%M%S')

    print("=" * 80)
    print(f"Starting ETL Job: {args['JOB_NAME']}")
    print("=" * 80)
    print(f"Run ID: {run_id}")
    print(f"Bucket: {args['bucket_name']}")
    print(f"Source prefix: {args['source_prefix']}")
    print(f"Target prefix: {args['target_prefix']}")
    print("=" * 80 + "\n")

    # Step 1: Read raw data from S3
    source_path = f"s3://{args['bucket_name']}/{args['source_prefix']}"
    print(f"[STEP 1] Reading data from: {source_path}")

    try:
        # Create DynamicFrame for bookmark support
        raw_dyf = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [source_path],
                "recurse": True
            },
            format="parquet",
            transformation_ctx="raw_data_source"
        )

        # Convert to DataFrame for transformations
        df = raw_dyf.toDF()
        initial_count = df.count()
        print(f"Successfully read {initial_count} records\n")

    except Exception as e:
        print(f"Error reading source data: {e}")
        job.commit()
        raise

    # Step 2: Add derived columns
    print("[STEP 2] Adding derived columns...")
    df = add_derived_columns(df)

    # Step 3: Filter invalid records (basic filtering)
    print("\n[STEP 3] Filtering invalid records...")
    df = filter_invalid_records(df)

    # Step 4: Run comprehensive data quality checks
    print("\n[STEP 4] Running data quality checks...")
    df, invalid_dfs = run_data_quality_checks(df, spark, args['bucket_name'], run_id)

    # Step 5: Write failed records to DLQ
    print("\n[STEP 5] Writing failed records to dead-letter queue...")
    write_to_dead_letter_queue(invalid_dfs, args['bucket_name'], run_id, spark)

    # Step 6: Write processed data to target
    target_path = f"s3://{args['bucket_name']}/{args['target_prefix']}"
    print(f"\n[STEP 6] Writing {df.count()} processed records to: {target_path}")

    try:
        # Convert back to DynamicFrame for Glue catalog integration
        processed_dyf = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "path": target_path,
                "partitionKeys": ["year", "month"]
            },
            format="parquet",
            transformation_ctx="processed_data_sink"
        )

        # Write using standard DataFrame API with partitioning
        df.write \
            .mode('append') \
            .partitionBy('year', 'month') \
            .parquet(target_path)

        print("Data written successfully!")

    except Exception as e:
        print(f"Error writing processed data: {e}")
        job.commit()
        raise

    # Step 7: Print summary statistics
    print("\n" + "=" * 80)
    print("ETL Job Summary Statistics")
    print("=" * 80)

    summary_stats = df.agg(
        F.count('*').alias('total_records'),
        F.avg('trip_distance').alias('avg_trip_distance'),
        F.avg('fare_amount').alias('avg_fare_amount'),
        F.avg('trip_duration_minutes').alias('avg_trip_duration_min'),
        F.avg('tip_percentage').alias('avg_tip_percentage'),
        F.min('tpep_pickup_datetime').alias('min_pickup_date'),
        F.max('tpep_pickup_datetime').alias('max_pickup_date')
    )

    summary_stats.show(truncate=False)

    # Distribution by year and month
    print("\nRecords by Year/Month:")
    df.groupBy('year', 'month') \
        .agg(F.count('*').alias('record_count')) \
        .orderBy('year', 'month') \
        .show()

    print("=" * 80)
    print(f"ETL Job Completed Successfully!")
    print(f"Run ID: {run_id}")
    print("=" * 80)

    # Commit job (this updates the bookmark)
    job.commit()


if __name__ == '__main__':
    main()
