"""
Data Quality Checks for Taxi Data ETL Pipeline.

This module provides comprehensive data quality validation functions for
checking data integrity, validating numeric ranges, and handling failed records.
"""

from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType


class DataQualityChecker:
    """
    Handles data quality checks and validation for taxi data ETL pipeline.
    """

    # Required fields that cannot be null
    REQUIRED_FIELDS = [
        'tpep_pickup_datetime',
        'tpep_dropoff_datetime',
        'passenger_count',
        'trip_distance',
        'fare_amount',
        'total_amount'
    ]

    # Numeric range validations (field_name: (min_value, max_value))
    NUMERIC_RANGES = {
        'passenger_count': (1, 6),
        'trip_distance': (0.1, 100.0),
        'fare_amount': (0.01, 500.0),
        'total_amount': (0.01, 1000.0),
        'tip_amount': (0.0, 200.0),
        'tolls_amount': (0.0, 100.0),
        'extra': (0.0, 10.0),
        'mta_tax': (0.0, 1.0),
    }

    def __init__(self, spark: SparkSession, bucket_name: str):
        """
        Initialize DataQualityChecker.

        Args:
            spark: SparkSession instance.
            bucket_name: S3 bucket name for storing failed records.
        """
        self.spark = spark
        self.bucket_name = bucket_name
        self.quality_metrics = {}

    def check_null_values(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Check for null values in required fields.

        Args:
            df: Input DataFrame.

        Returns:
            Tuple of (valid_records DataFrame, invalid_records DataFrame).
        """
        print("Checking for null values in required fields...")

        # Build condition for all required fields
        null_condition = None
        for field in self.REQUIRED_FIELDS:
            if field in df.columns:
                field_condition = F.col(field).isNull()
                null_condition = field_condition if null_condition is None else null_condition | field_condition

        if null_condition is None:
            return df, self.spark.createDataFrame([], df.schema)

        # Add failure reason column to invalid records
        invalid_df = df.filter(null_condition).withColumn(
            'failure_reason',
            F.lit('null_in_required_field')
        ).withColumn(
            'failed_at',
            F.current_timestamp()
        )

        # Get valid records (no nulls in required fields)
        valid_df = df.filter(~null_condition)

        invalid_count = invalid_df.count()
        self.quality_metrics['null_violations'] = invalid_count
        print(f"  Found {invalid_count} records with null values in required fields")

        return valid_df, invalid_df

    def validate_numeric_ranges(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Validate numeric fields are within expected ranges.

        Args:
            df: Input DataFrame.

        Returns:
            Tuple of (valid_records DataFrame, invalid_records DataFrame).
        """
        print("Validating numeric ranges...")

        # Build condition for all numeric ranges
        range_condition = F.lit(True)
        for field, (min_val, max_val) in self.NUMERIC_RANGES.items():
            if field in df.columns:
                range_condition = range_condition & (
                    (F.col(field) >= min_val) & (F.col(field) <= max_val)
                )

        # Get invalid records (outside ranges)
        invalid_df = df.filter(~range_condition).withColumn(
            'failure_reason',
            F.lit('numeric_range_violation')
        ).withColumn(
            'failed_at',
            F.current_timestamp()
        )

        # Get valid records (within ranges)
        valid_df = df.filter(range_condition)

        invalid_count = invalid_df.count()
        self.quality_metrics['range_violations'] = invalid_count
        print(f"  Found {invalid_count} records with numeric range violations")

        return valid_df, invalid_df

    def validate_datetime_logic(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Validate datetime logic (dropoff must be after pickup).

        Args:
            df: Input DataFrame.

        Returns:
            Tuple of (valid_records DataFrame, invalid_records DataFrame).
        """
        print("Validating datetime logic...")

        if 'tpep_pickup_datetime' not in df.columns or 'tpep_dropoff_datetime' not in df.columns:
            return df, self.spark.createDataFrame([], df.schema)

        # Check dropoff is after pickup
        datetime_condition = F.col('tpep_dropoff_datetime') > F.col('tpep_pickup_datetime')

        # Get invalid records
        invalid_df = df.filter(~datetime_condition).withColumn(
            'failure_reason',
            F.lit('invalid_datetime_sequence')
        ).withColumn(
            'failed_at',
            F.current_timestamp()
        )

        # Get valid records
        valid_df = df.filter(datetime_condition)

        invalid_count = invalid_df.count()
        self.quality_metrics['datetime_violations'] = invalid_count
        print(f"  Found {invalid_count} records with datetime logic violations")

        return valid_df, invalid_df

    def validate_trip_duration(self, df: DataFrame,
                               min_minutes: float = 1.0,
                               max_minutes: float = 300.0) -> Tuple[DataFrame, DataFrame]:
        """
        Validate trip duration is within reasonable bounds.

        Args:
            df: Input DataFrame (must have trip_duration_minutes column).
            min_minutes: Minimum acceptable trip duration.
            max_minutes: Maximum acceptable trip duration.

        Returns:
            Tuple of (valid_records DataFrame, invalid_records DataFrame).
        """
        print(f"Validating trip duration ({min_minutes}-{max_minutes} minutes)...")

        if 'trip_duration_minutes' not in df.columns:
            print("  Warning: trip_duration_minutes column not found, skipping validation")
            return df, self.spark.createDataFrame([], df.schema)

        # Check duration is within bounds
        duration_condition = (
            (F.col('trip_duration_minutes') >= min_minutes) &
            (F.col('trip_duration_minutes') <= max_minutes)
        )

        # Get invalid records
        invalid_df = df.filter(~duration_condition).withColumn(
            'failure_reason',
            F.lit('invalid_trip_duration')
        ).withColumn(
            'failed_at',
            F.current_timestamp()
        )

        # Get valid records
        valid_df = df.filter(duration_condition)

        invalid_count = invalid_df.count()
        self.quality_metrics['duration_violations'] = invalid_count
        print(f"  Found {invalid_count} records with trip duration violations")

        return valid_df, invalid_df

    def validate_tip_percentage(self, df: DataFrame,
                                max_percentage: float = 100.0) -> Tuple[DataFrame, DataFrame]:
        """
        Validate tip percentage is reasonable.

        Args:
            df: Input DataFrame (must have tip_percentage column).
            max_percentage: Maximum acceptable tip percentage.

        Returns:
            Tuple of (valid_records DataFrame, invalid_records DataFrame).
        """
        print(f"Validating tip percentage (max {max_percentage}%)...")

        if 'tip_percentage' not in df.columns:
            print("  Warning: tip_percentage column not found, skipping validation")
            return df, self.spark.createDataFrame([], df.schema)

        # Check tip percentage is reasonable
        tip_condition = (
            (F.col('tip_percentage') >= 0) &
            (F.col('tip_percentage') <= max_percentage)
        )

        # Get invalid records
        invalid_df = df.filter(~tip_condition).withColumn(
            'failure_reason',
            F.lit('invalid_tip_percentage')
        ).withColumn(
            'failed_at',
            F.current_timestamp()
        )

        # Get valid records
        valid_df = df.filter(tip_condition)

        invalid_count = invalid_df.count()
        self.quality_metrics['tip_percentage_violations'] = invalid_count
        print(f"  Found {invalid_count} records with tip percentage violations")

        return valid_df, invalid_df

    def run_all_checks(self, df: DataFrame) -> Tuple[DataFrame, List[DataFrame]]:
        """
        Run all data quality checks in sequence.

        Args:
            df: Input DataFrame.

        Returns:
            Tuple of (valid_records DataFrame, list of invalid_records DataFrames).
        """
        print("\n" + "=" * 80)
        print("Running Data Quality Checks")
        print("=" * 80)

        initial_count = df.count()
        print(f"Initial record count: {initial_count}")

        invalid_dfs = []

        # Run each check in sequence
        df, invalid = self.check_null_values(df)
        if invalid.count() > 0:
            invalid_dfs.append(invalid)

        df, invalid = self.validate_numeric_ranges(df)
        if invalid.count() > 0:
            invalid_dfs.append(invalid)

        df, invalid = self.validate_datetime_logic(df)
        if invalid.count() > 0:
            invalid_dfs.append(invalid)

        # These checks depend on derived columns being present
        if 'trip_duration_minutes' in df.columns:
            df, invalid = self.validate_trip_duration(df)
            if invalid.count() > 0:
                invalid_dfs.append(invalid)

        if 'tip_percentage' in df.columns:
            df, invalid = self.validate_tip_percentage(df)
            if invalid.count() > 0:
                invalid_dfs.append(invalid)

        final_count = df.count()
        rejected_count = initial_count - final_count

        print("\n" + "=" * 80)
        print("Data Quality Summary")
        print("=" * 80)
        print(f"Records processed: {initial_count}")
        print(f"Records passed: {final_count}")
        print(f"Records rejected: {rejected_count} ({rejected_count/initial_count*100:.2f}%)")
        print("\nRejection breakdown:")
        for key, value in self.quality_metrics.items():
            if value > 0:
                print(f"  {key}: {value}")
        print("=" * 80 + "\n")

        return df, invalid_dfs

    def write_to_dead_letter_queue(self, invalid_dfs: List[DataFrame],
                                   run_id: Optional[str] = None) -> None:
        """
        Write failed records to dead-letter queue (DLQ) in S3.

        Args:
            invalid_dfs: List of DataFrames with invalid records.
            run_id: Optional run identifier for organizing failed records.
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
                    combined_invalid = combined_invalid.union(invalid_df)

        if combined_invalid is None or combined_invalid.count() == 0:
            print("No failed records to write")
            return

        # Add run metadata
        if run_id is None:
            run_id = datetime.now().strftime('%Y%m%d_%H%M%S')

        combined_invalid = combined_invalid.withColumn('run_id', F.lit(run_id))

        # Write to DLQ in S3
        dlq_path = f"s3://{self.bucket_name}/dead-letter-queue/taxi/year={datetime.now().year}/month={datetime.now().month:02d}/run_id={run_id}/"

        print(f"Writing {combined_invalid.count()} failed records to: {dlq_path}")

        try:
            combined_invalid.write \
                .mode('overwrite') \
                .parquet(dlq_path)
            print("Failed records written successfully to DLQ")
        except Exception as e:
            print(f"Error writing to DLQ: {e}")
            raise

    def get_quality_metrics(self) -> Dict[str, int]:
        """
        Get data quality metrics from the last run.

        Returns:
            Dictionary of quality metric names and counts.
        """
        return self.quality_metrics.copy()


def create_quality_report(df: DataFrame) -> DataFrame:
    """
    Create a data quality report with statistics.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with quality statistics.
    """
    print("\nGenerating data quality report...")

    # Calculate statistics
    stats = df.agg(
        F.count('*').alias('total_records'),
        F.avg('trip_distance').alias('avg_trip_distance'),
        F.avg('fare_amount').alias('avg_fare_amount'),
        F.avg('trip_duration_minutes').alias('avg_trip_duration'),
        F.avg('tip_percentage').alias('avg_tip_percentage'),
        F.min('tpep_pickup_datetime').alias('min_pickup_date'),
        F.max('tpep_pickup_datetime').alias('max_pickup_date')
    )

    stats.show(truncate=False)

    return stats
