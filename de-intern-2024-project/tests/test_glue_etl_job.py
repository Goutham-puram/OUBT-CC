"""
Unit tests for Glue ETL Job and Data Quality Checks.

Tests cover:
- Data transformations (trip_duration_minutes, tip_percentage)
- Data quality checks (nulls, numeric ranges, datetime logic)
- Record filtering
- Dead-letter queue functionality
"""

import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("test_glue_etl") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture
def sample_taxi_schema():
    """Define schema for taxi data."""
    return StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_type", IntegerType(), True),
    ])


@pytest.fixture
def valid_taxi_data(spark, sample_taxi_schema):
    """Create valid taxi trip data for testing."""
    base_time = datetime(2023, 1, 1, 12, 0, 0)

    data = [
        (1, base_time, base_time + timedelta(minutes=15), 1, 2.5, 12.5, 2.0, 15.5, 1),
        (1, base_time + timedelta(hours=1), base_time + timedelta(hours=1, minutes=20), 2, 5.0, 25.0, 5.0, 31.0, 1),
        (2, base_time + timedelta(hours=2), base_time + timedelta(hours=2, minutes=10), 1, 1.5, 8.5, 1.5, 10.5, 2),
        (2, base_time + timedelta(hours=3), base_time + timedelta(hours=3, minutes=30), 4, 8.0, 35.0, 7.0, 43.0, 1),
        (1, base_time + timedelta(hours=4), base_time + timedelta(hours=4, minutes=25), 2, 3.5, 15.0, 3.0, 19.0, 1),
    ]

    return spark.createDataFrame(data, sample_taxi_schema)


@pytest.fixture
def invalid_taxi_data(spark, sample_taxi_schema):
    """Create invalid taxi trip data for testing."""
    base_time = datetime(2023, 1, 1, 12, 0, 0)

    data = [
        # Invalid: trip_distance = 0
        (1, base_time, base_time + timedelta(minutes=15), 1, 0.0, 12.5, 2.0, 15.5, 1),
        # Invalid: fare_amount = 0
        (1, base_time + timedelta(hours=1), base_time + timedelta(hours=1, minutes=20), 2, 5.0, 0.0, 0.0, 0.0, 1),
        # Invalid: passenger_count = 0
        (2, base_time + timedelta(hours=2), base_time + timedelta(hours=2, minutes=10), 0, 1.5, 8.5, 1.5, 10.5, 2),
        # Invalid: dropoff before pickup
        (2, base_time + timedelta(hours=3), base_time + timedelta(hours=2, minutes=30), 4, 8.0, 35.0, 7.0, 43.0, 1),
        # Invalid: passenger_count > 6
        (1, base_time + timedelta(hours=4), base_time + timedelta(hours=4, minutes=25), 10, 3.5, 15.0, 3.0, 19.0, 1),
    ]

    return spark.createDataFrame(data, sample_taxi_schema)


@pytest.fixture
def mixed_taxi_data(spark, sample_taxi_schema):
    """Create mixed valid and invalid taxi trip data."""
    base_time = datetime(2023, 1, 1, 12, 0, 0)

    data = [
        # Valid
        (1, base_time, base_time + timedelta(minutes=15), 1, 2.5, 12.5, 2.0, 15.5, 1),
        # Invalid: trip_distance = 0
        (1, base_time + timedelta(hours=1), base_time + timedelta(hours=1, minutes=20), 2, 0.0, 25.0, 5.0, 31.0, 1),
        # Valid
        (2, base_time + timedelta(hours=2), base_time + timedelta(hours=2, minutes=10), 1, 1.5, 8.5, 1.5, 10.5, 2),
        # Invalid: null passenger_count
        (2, base_time + timedelta(hours=3), base_time + timedelta(hours=3, minutes=30), None, 8.0, 35.0, 7.0, 43.0, 1),
        # Valid
        (1, base_time + timedelta(hours=4), base_time + timedelta(hours=4, minutes=25), 2, 3.5, 15.0, 3.0, 19.0, 1),
    ]

    return spark.createDataFrame(data, sample_taxi_schema)


class TestETLTransformations:
    """Test ETL transformation functions."""

    def test_add_trip_duration_minutes(self, spark, valid_taxi_data):
        """Test calculation of trip_duration_minutes."""
        # Add trip duration
        df = valid_taxi_data.withColumn(
            'trip_duration_minutes',
            (F.unix_timestamp('tpep_dropoff_datetime') -
             F.unix_timestamp('tpep_pickup_datetime')) / 60.0
        )

        # Verify column exists
        assert 'trip_duration_minutes' in df.columns

        # Verify calculations
        results = df.select('trip_duration_minutes').collect()
        assert results[0][0] == 15.0
        assert results[1][0] == 20.0
        assert results[2][0] == 10.0
        assert results[3][0] == 30.0
        assert results[4][0] == 25.0

    def test_add_tip_percentage(self, spark, valid_taxi_data):
        """Test calculation of tip_percentage."""
        # Add tip percentage
        df = valid_taxi_data.withColumn(
            'tip_percentage',
            F.when(F.col('fare_amount') > 0,
                   (F.col('tip_amount') / F.col('fare_amount')) * 100.0)
            .otherwise(0.0)
        )

        # Verify column exists
        assert 'tip_percentage' in df.columns

        # Verify calculations
        results = df.select('tip_percentage').collect()
        assert abs(results[0][0] - 16.0) < 0.01  # 2.0/12.5 * 100 = 16%
        assert abs(results[1][0] - 20.0) < 0.01  # 5.0/25.0 * 100 = 20%
        assert abs(results[2][0] - 17.65) < 0.01  # 1.5/8.5 * 100 = 17.65%

    def test_add_year_month_partitions(self, spark, valid_taxi_data):
        """Test addition of year and month partition columns."""
        # Add partitioning columns
        df = valid_taxi_data.withColumn('year', F.year('tpep_pickup_datetime'))
        df = df.withColumn('month', F.month('tpep_pickup_datetime'))

        # Verify columns exist
        assert 'year' in df.columns
        assert 'month' in df.columns

        # Verify values
        results = df.select('year', 'month').distinct().collect()
        assert results[0][0] == 2023
        assert results[0][1] == 1

    def test_add_quality_timestamp(self, spark, valid_taxi_data):
        """Test addition of quality check timestamp."""
        df = valid_taxi_data.withColumn('quality_check_timestamp', F.current_timestamp())

        # Verify column exists
        assert 'quality_check_timestamp' in df.columns

        # Verify timestamp is not null
        timestamp_count = df.select('quality_check_timestamp').filter(
            F.col('quality_check_timestamp').isNotNull()
        ).count()
        assert timestamp_count == 5


class TestDataQualityChecks:
    """Test data quality validation functions."""

    def test_filter_invalid_trip_distance(self, spark, invalid_taxi_data):
        """Test filtering records with invalid trip_distance."""
        # Filter invalid trip_distance
        df = invalid_taxi_data.filter(F.col('trip_distance') > 0)

        # Should filter out the first record (trip_distance = 0)
        assert df.count() == 4

    def test_filter_invalid_fare_amount(self, spark, invalid_taxi_data):
        """Test filtering records with invalid fare_amount."""
        # Filter invalid fare_amount
        df = invalid_taxi_data.filter(F.col('fare_amount') > 0)

        # Should filter out the second record (fare_amount = 0)
        assert df.count() == 4

    def test_filter_invalid_passenger_count(self, spark, invalid_taxi_data):
        """Test filtering records with invalid passenger_count."""
        # Filter invalid passenger_count
        df = invalid_taxi_data.filter(
            (F.col('passenger_count') > 0) & (F.col('passenger_count') <= 6)
        )

        # Should filter out records 3 and 5 (passenger_count = 0 and 10)
        assert df.count() == 3

    def test_filter_invalid_datetime_sequence(self, spark, invalid_taxi_data):
        """Test filtering records with invalid datetime sequence."""
        # Filter invalid datetime sequence
        df = invalid_taxi_data.filter(
            F.col('tpep_dropoff_datetime') > F.col('tpep_pickup_datetime')
        )

        # Should filter out the fourth record (dropoff before pickup)
        assert df.count() == 4

    def test_combined_filters(self, spark, invalid_taxi_data):
        """Test all filters combined."""
        # Apply all filters
        df = invalid_taxi_data.filter(
            (F.col('trip_distance') > 0) &
            (F.col('fare_amount') > 0) &
            (F.col('passenger_count') > 0) &
            (F.col('passenger_count') <= 6) &
            (F.col('tpep_dropoff_datetime') > F.col('tpep_pickup_datetime'))
        )

        # All records should be filtered out
        assert df.count() == 0

    def test_null_detection(self, spark, mixed_taxi_data):
        """Test detection of null values in required fields."""
        # Check for nulls
        required_fields = ['tpep_pickup_datetime', 'tpep_dropoff_datetime',
                          'passenger_count', 'trip_distance', 'fare_amount']

        null_condition = None
        for field in required_fields:
            field_condition = F.col(field).isNull()
            null_condition = field_condition if null_condition is None else null_condition | field_condition

        invalid_df = mixed_taxi_data.filter(null_condition)

        # Should detect the one record with null passenger_count
        assert invalid_df.count() == 1

    def test_trip_duration_range(self, spark, valid_taxi_data):
        """Test trip duration is within valid range."""
        # Add trip duration
        df = valid_taxi_data.withColumn(
            'trip_duration_minutes',
            (F.unix_timestamp('tpep_dropoff_datetime') -
             F.unix_timestamp('tpep_pickup_datetime')) / 60.0
        )

        # Filter by duration range (1-300 minutes)
        valid_df = df.filter(
            (F.col('trip_duration_minutes') >= 1.0) &
            (F.col('trip_duration_minutes') <= 300.0)
        )

        # All valid records should pass
        assert valid_df.count() == 5

    def test_tip_percentage_range(self, spark, valid_taxi_data):
        """Test tip percentage is within valid range."""
        # Add tip percentage
        df = valid_taxi_data.withColumn(
            'tip_percentage',
            F.when(F.col('fare_amount') > 0,
                   (F.col('tip_amount') / F.col('fare_amount')) * 100.0)
            .otherwise(0.0)
        )

        # Filter by tip percentage range (0-100%)
        valid_df = df.filter(
            (F.col('tip_percentage') >= 0) &
            (F.col('tip_percentage') <= 100.0)
        )

        # All valid records should pass
        assert valid_df.count() == 5


class TestDataQualityModule:
    """Test the DataQualityChecker class."""

    def test_check_null_values(self, spark, mixed_taxi_data):
        """Test null value checking."""
        from src.transform.data_quality import DataQualityChecker

        checker = DataQualityChecker(spark, "test-bucket")
        valid_df, invalid_df = checker.check_null_values(mixed_taxi_data)

        # Should have 4 valid records and 1 invalid
        assert valid_df.count() == 4
        assert invalid_df.count() == 1

        # Invalid record should have failure_reason
        assert 'failure_reason' in invalid_df.columns

    def test_validate_numeric_ranges(self, spark, invalid_taxi_data):
        """Test numeric range validation."""
        from src.transform.data_quality import DataQualityChecker

        checker = DataQualityChecker(spark, "test-bucket")
        valid_df, invalid_df = checker.validate_numeric_ranges(invalid_taxi_data)

        # All records should fail some range check
        assert valid_df.count() < invalid_taxi_data.count()

    def test_validate_datetime_logic(self, spark, invalid_taxi_data):
        """Test datetime logic validation."""
        from src.transform.data_quality import DataQualityChecker

        checker = DataQualityChecker(spark, "test-bucket")
        valid_df, invalid_df = checker.validate_datetime_logic(invalid_taxi_data)

        # Should detect the record with invalid datetime sequence
        assert invalid_df.count() >= 1

    def test_run_all_checks(self, spark, valid_taxi_data):
        """Test running all quality checks."""
        from src.transform.data_quality import DataQualityChecker

        checker = DataQualityChecker(spark, "test-bucket")

        # Add required derived columns
        df = valid_taxi_data.withColumn(
            'trip_duration_minutes',
            (F.unix_timestamp('tpep_dropoff_datetime') -
             F.unix_timestamp('tpep_pickup_datetime')) / 60.0
        )
        df = df.withColumn(
            'tip_percentage',
            F.when(F.col('fare_amount') > 0,
                   (F.col('tip_amount') / F.col('fare_amount')) * 100.0)
            .otherwise(0.0)
        )

        valid_df, invalid_dfs = checker.run_all_checks(df)

        # All valid records should pass
        assert valid_df.count() == 5

        # Get quality metrics
        metrics = checker.get_quality_metrics()
        assert isinstance(metrics, dict)


class TestEndToEndETL:
    """Test end-to-end ETL pipeline."""

    def test_full_etl_pipeline(self, spark, mixed_taxi_data):
        """Test complete ETL pipeline."""
        # Step 1: Add derived columns
        df = mixed_taxi_data.withColumn(
            'trip_duration_minutes',
            (F.unix_timestamp('tpep_dropoff_datetime') -
             F.unix_timestamp('tpep_pickup_datetime')) / 60.0
        )
        df = df.withColumn(
            'tip_percentage',
            F.when(F.col('fare_amount') > 0,
                   (F.col('tip_amount') / F.col('fare_amount')) * 100.0)
            .otherwise(0.0)
        )
        df = df.withColumn('quality_check_timestamp', F.current_timestamp())
        df = df.withColumn('year', F.year('tpep_pickup_datetime'))
        df = df.withColumn('month', F.month('tpep_pickup_datetime'))

        initial_count = df.count()

        # Step 2: Filter invalid records
        df = df.filter(
            (F.col('trip_distance') > 0) &
            (F.col('fare_amount') > 0) &
            (F.col('passenger_count').isNotNull()) &
            (F.col('tpep_dropoff_datetime') > F.col('tpep_pickup_datetime'))
        )

        # Step 3: Apply range validations
        df = df.filter(
            (F.col('passenger_count') >= 1) & (F.col('passenger_count') <= 6) &
            (F.col('trip_distance') >= 0.1) & (F.col('trip_distance') <= 100.0) &
            (F.col('fare_amount') >= 0.01) & (F.col('fare_amount') <= 500.0) &
            (F.col('trip_duration_minutes') >= 1.0) &
            (F.col('trip_duration_minutes') <= 300.0) &
            (F.col('tip_percentage') >= 0) &
            (F.col('tip_percentage') <= 100.0)
        )

        final_count = df.count()

        # Verify processing
        assert final_count < initial_count  # Some records should be filtered
        assert final_count == 3  # 3 valid records from mixed data

        # Verify all required columns exist
        required_columns = ['trip_duration_minutes', 'tip_percentage',
                           'quality_check_timestamp', 'year', 'month']
        for col in required_columns:
            assert col in df.columns

    def test_etl_preserves_valid_data(self, spark, valid_taxi_data):
        """Test that ETL pipeline preserves all valid data."""
        # Apply full pipeline
        df = valid_taxi_data.withColumn(
            'trip_duration_minutes',
            (F.unix_timestamp('tpep_dropoff_datetime') -
             F.unix_timestamp('tpep_pickup_datetime')) / 60.0
        )
        df = df.withColumn(
            'tip_percentage',
            F.when(F.col('fare_amount') > 0,
                   (F.col('tip_amount') / F.col('fare_amount')) * 100.0)
            .otherwise(0.0)
        )

        df = df.filter(
            (F.col('trip_distance') > 0) &
            (F.col('fare_amount') > 0) &
            (F.col('passenger_count') > 0) &
            (F.col('passenger_count') <= 6) &
            (F.col('trip_duration_minutes') >= 1.0) &
            (F.col('trip_duration_minutes') <= 300.0)
        )

        # All valid records should be preserved
        assert df.count() == valid_taxi_data.count()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
