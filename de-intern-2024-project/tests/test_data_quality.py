"""
Unit tests for Data Quality Checker module.

Tests cover all data quality check functions and DLQ functionality.
"""

import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType
from src.transform.data_quality import DataQualityChecker, create_quality_report


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("test_data_quality") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture
def sample_schema():
    """Define schema for test data."""
    return StructType([
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
    ])


@pytest.fixture
def clean_data(spark, sample_schema):
    """Create clean test data."""
    base_time = datetime(2023, 1, 1, 12, 0, 0)

    data = [
        (base_time, base_time + timedelta(minutes=15), 2, 2.5, 12.5, 2.0, 15.5, 0.5, 0.5, 0.5),
        (base_time + timedelta(hours=1), base_time + timedelta(hours=1, minutes=20), 1, 5.0, 25.0, 5.0, 31.0, 1.0, 0.5, 0.5),
        (base_time + timedelta(hours=2), base_time + timedelta(hours=2, minutes=10), 3, 1.5, 8.5, 1.5, 10.5, 0.0, 0.5, 0.5),
    ]

    return spark.createDataFrame(data, sample_schema)


@pytest.fixture
def data_with_nulls(spark, sample_schema):
    """Create test data with null values."""
    base_time = datetime(2023, 1, 1, 12, 0, 0)

    data = [
        (base_time, base_time + timedelta(minutes=15), 2, 2.5, 12.5, 2.0, 15.5, 0.5, 0.5, 0.5),
        (None, base_time + timedelta(hours=1, minutes=20), 1, 5.0, 25.0, 5.0, 31.0, 1.0, 0.5, 0.5),  # Null pickup
        (base_time + timedelta(hours=2), base_time + timedelta(hours=2, minutes=10), None, 1.5, 8.5, 1.5, 10.5, 0.0, 0.5, 0.5),  # Null passenger
        (base_time + timedelta(hours=3), base_time + timedelta(hours=3, minutes=30), 4, None, 35.0, 7.0, 43.0, 2.0, 0.5, 0.5),  # Null distance
    ]

    return spark.createDataFrame(data, sample_schema)


@pytest.fixture
def data_with_range_violations(spark, sample_schema):
    """Create test data with range violations."""
    base_time = datetime(2023, 1, 1, 12, 0, 0)

    data = [
        (base_time, base_time + timedelta(minutes=15), 2, 2.5, 12.5, 2.0, 15.5, 0.5, 0.5, 0.5),  # Valid
        (base_time + timedelta(hours=1), base_time + timedelta(hours=1, minutes=20), 10, 5.0, 25.0, 5.0, 31.0, 1.0, 0.5, 0.5),  # passenger_count > 6
        (base_time + timedelta(hours=2), base_time + timedelta(hours=2, minutes=10), 1, 150.0, 8.5, 1.5, 10.5, 0.0, 0.5, 0.5),  # trip_distance > 100
        (base_time + timedelta(hours=3), base_time + timedelta(hours=3, minutes=30), 4, 8.0, 600.0, 7.0, 650.0, 2.0, 0.5, 0.5),  # fare_amount > 500
    ]

    return spark.createDataFrame(data, sample_schema)


@pytest.fixture
def data_with_datetime_issues(spark, sample_schema):
    """Create test data with datetime logic violations."""
    base_time = datetime(2023, 1, 1, 12, 0, 0)

    data = [
        (base_time, base_time + timedelta(minutes=15), 2, 2.5, 12.5, 2.0, 15.5, 0.5, 0.5, 0.5),  # Valid
        (base_time + timedelta(hours=1), base_time + timedelta(minutes=30), 1, 5.0, 25.0, 5.0, 31.0, 1.0, 0.5, 0.5),  # Dropoff before pickup
        (base_time + timedelta(hours=2), base_time + timedelta(hours=2), 3, 1.5, 8.5, 1.5, 10.5, 0.0, 0.5, 0.5),  # Same time
    ]

    return spark.createDataFrame(data, sample_schema)


class TestDataQualityCheckerInit:
    """Test DataQualityChecker initialization."""

    def test_init(self, spark):
        """Test initialization of DataQualityChecker."""
        checker = DataQualityChecker(spark, "test-bucket")

        assert checker.spark is spark
        assert checker.bucket_name == "test-bucket"
        assert isinstance(checker.quality_metrics, dict)
        assert len(checker.quality_metrics) == 0

    def test_required_fields(self):
        """Test that required fields are defined."""
        assert len(DataQualityChecker.REQUIRED_FIELDS) > 0
        assert 'tpep_pickup_datetime' in DataQualityChecker.REQUIRED_FIELDS
        assert 'tpep_dropoff_datetime' in DataQualityChecker.REQUIRED_FIELDS

    def test_numeric_ranges(self):
        """Test that numeric ranges are defined."""
        assert len(DataQualityChecker.NUMERIC_RANGES) > 0
        assert 'passenger_count' in DataQualityChecker.NUMERIC_RANGES
        assert 'trip_distance' in DataQualityChecker.NUMERIC_RANGES


class TestNullValueChecks:
    """Test null value checking functionality."""

    def test_check_null_values_clean_data(self, spark, clean_data):
        """Test null checking with clean data."""
        checker = DataQualityChecker(spark, "test-bucket")
        valid_df, invalid_df = checker.check_null_values(clean_data)

        assert valid_df.count() == 3
        assert invalid_df.count() == 0

    def test_check_null_values_with_nulls(self, spark, data_with_nulls):
        """Test null checking with null values."""
        checker = DataQualityChecker(spark, "test-bucket")
        valid_df, invalid_df = checker.check_null_values(data_with_nulls)

        assert valid_df.count() == 1  # Only first record is valid
        assert invalid_df.count() == 3  # Three records have nulls

        # Check failure reason is set
        assert 'failure_reason' in invalid_df.columns
        assert 'failed_at' in invalid_df.columns

    def test_null_metrics(self, spark, data_with_nulls):
        """Test that null violations are tracked in metrics."""
        checker = DataQualityChecker(spark, "test-bucket")
        valid_df, invalid_df = checker.check_null_values(data_with_nulls)

        metrics = checker.get_quality_metrics()
        assert 'null_violations' in metrics
        assert metrics['null_violations'] == 3


class TestNumericRangeValidation:
    """Test numeric range validation functionality."""

    def test_validate_numeric_ranges_clean_data(self, spark, clean_data):
        """Test range validation with clean data."""
        checker = DataQualityChecker(spark, "test-bucket")
        valid_df, invalid_df = checker.validate_numeric_ranges(clean_data)

        assert valid_df.count() == 3
        assert invalid_df.count() == 0

    def test_validate_numeric_ranges_with_violations(self, spark, data_with_range_violations):
        """Test range validation with violations."""
        checker = DataQualityChecker(spark, "test-bucket")
        valid_df, invalid_df = checker.validate_numeric_ranges(data_with_range_violations)

        assert valid_df.count() == 1  # Only first record is valid
        assert invalid_df.count() == 3  # Three records have range violations

        # Check failure reason is set
        assert 'failure_reason' in invalid_df.columns
        failure_reasons = invalid_df.select('failure_reason').distinct().collect()
        assert failure_reasons[0][0] == 'numeric_range_violation'

    def test_range_metrics(self, spark, data_with_range_violations):
        """Test that range violations are tracked in metrics."""
        checker = DataQualityChecker(spark, "test-bucket")
        valid_df, invalid_df = checker.validate_numeric_ranges(data_with_range_violations)

        metrics = checker.get_quality_metrics()
        assert 'range_violations' in metrics
        assert metrics['range_violations'] == 3


class TestDateTimeValidation:
    """Test datetime logic validation functionality."""

    def test_validate_datetime_clean_data(self, spark, clean_data):
        """Test datetime validation with clean data."""
        checker = DataQualityChecker(spark, "test-bucket")
        valid_df, invalid_df = checker.validate_datetime_logic(clean_data)

        assert valid_df.count() == 3
        assert invalid_df.count() == 0

    def test_validate_datetime_with_violations(self, spark, data_with_datetime_issues):
        """Test datetime validation with violations."""
        checker = DataQualityChecker(spark, "test-bucket")
        valid_df, invalid_df = checker.validate_datetime_logic(data_with_datetime_issues)

        assert valid_df.count() == 1  # Only first record is valid
        assert invalid_df.count() == 2  # Two records have datetime issues

        # Check failure reason is set
        failure_reasons = invalid_df.select('failure_reason').distinct().collect()
        assert failure_reasons[0][0] == 'invalid_datetime_sequence'

    def test_datetime_metrics(self, spark, data_with_datetime_issues):
        """Test that datetime violations are tracked in metrics."""
        checker = DataQualityChecker(spark, "test-bucket")
        valid_df, invalid_df = checker.validate_datetime_logic(data_with_datetime_issues)

        metrics = checker.get_quality_metrics()
        assert 'datetime_violations' in metrics
        assert metrics['datetime_violations'] == 2


class TestDerivedColumnValidation:
    """Test validation of derived columns (trip_duration, tip_percentage)."""

    def test_validate_trip_duration(self, spark, clean_data):
        """Test trip duration validation."""
        # Add trip duration column
        df = clean_data.withColumn(
            'trip_duration_minutes',
            (F.unix_timestamp('tpep_dropoff_datetime') -
             F.unix_timestamp('tpep_pickup_datetime')) / 60.0
        )

        checker = DataQualityChecker(spark, "test-bucket")
        valid_df, invalid_df = checker.validate_trip_duration(df, min_minutes=1.0, max_minutes=300.0)

        # All clean data should be valid
        assert valid_df.count() == 3
        assert invalid_df.count() == 0

    def test_validate_trip_duration_violations(self, spark, sample_schema):
        """Test trip duration validation with violations."""
        base_time = datetime(2023, 1, 1, 12, 0, 0)

        # Create data with invalid trip durations
        data = [
            (base_time, base_time + timedelta(seconds=30), 2, 2.5, 12.5, 2.0, 15.5, 0.5, 0.5, 0.5),  # 0.5 min (too short)
            (base_time + timedelta(hours=1), base_time + timedelta(hours=7), 1, 5.0, 25.0, 5.0, 31.0, 1.0, 0.5, 0.5),  # 360 min (too long)
            (base_time + timedelta(hours=8), base_time + timedelta(hours=8, minutes=15), 3, 1.5, 8.5, 1.5, 10.5, 0.0, 0.5, 0.5),  # Valid
        ]

        df = spark.createDataFrame(data, sample_schema)

        # Add trip duration column
        df = df.withColumn(
            'trip_duration_minutes',
            (F.unix_timestamp('tpep_dropoff_datetime') -
             F.unix_timestamp('tpep_pickup_datetime')) / 60.0
        )

        checker = DataQualityChecker(spark, "test-bucket")
        valid_df, invalid_df = checker.validate_trip_duration(df, min_minutes=1.0, max_minutes=300.0)

        assert valid_df.count() == 1  # Only last record is valid
        assert invalid_df.count() == 2  # Two records have invalid duration

    def test_validate_tip_percentage(self, spark, clean_data):
        """Test tip percentage validation."""
        # Add tip percentage column
        df = clean_data.withColumn(
            'tip_percentage',
            F.when(F.col('fare_amount') > 0,
                   (F.col('tip_amount') / F.col('fare_amount')) * 100.0)
            .otherwise(0.0)
        )

        checker = DataQualityChecker(spark, "test-bucket")
        valid_df, invalid_df = checker.validate_tip_percentage(df, max_percentage=100.0)

        # All clean data should be valid
        assert valid_df.count() == 3
        assert invalid_df.count() == 0

    def test_validate_tip_percentage_violations(self, spark, sample_schema):
        """Test tip percentage validation with violations."""
        base_time = datetime(2023, 1, 1, 12, 0, 0)

        # Create data with invalid tip percentages
        data = [
            (base_time, base_time + timedelta(minutes=15), 2, 2.5, 10.0, 15.0, 26.5, 0.5, 0.5, 0.5),  # 150% tip (too high)
            (base_time + timedelta(hours=1), base_time + timedelta(hours=1, minutes=20), 1, 5.0, 25.0, 5.0, 31.0, 1.0, 0.5, 0.5),  # Valid
        ]

        df = spark.createDataFrame(data, sample_schema)

        # Add tip percentage column
        df = df.withColumn(
            'tip_percentage',
            F.when(F.col('fare_amount') > 0,
                   (F.col('tip_amount') / F.col('fare_amount')) * 100.0)
            .otherwise(0.0)
        )

        checker = DataQualityChecker(spark, "test-bucket")
        valid_df, invalid_df = checker.validate_tip_percentage(df, max_percentage=100.0)

        assert valid_df.count() == 1  # Only second record is valid
        assert invalid_df.count() == 1  # First record has invalid tip percentage


class TestRunAllChecks:
    """Test running all checks together."""

    def test_run_all_checks_clean_data(self, spark, clean_data):
        """Test running all checks on clean data."""
        # Add derived columns
        df = clean_data.withColumn(
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

        checker = DataQualityChecker(spark, "test-bucket")
        valid_df, invalid_dfs = checker.run_all_checks(df)

        # All clean data should pass
        assert valid_df.count() == 3
        assert len([df for df in invalid_dfs if df.count() > 0]) == 0

    def test_run_all_checks_mixed_data(self, spark, data_with_nulls):
        """Test running all checks on mixed data."""
        checker = DataQualityChecker(spark, "test-bucket")
        valid_df, invalid_dfs = checker.run_all_checks(data_with_nulls)

        # Should have some valid and some invalid
        assert valid_df.count() < data_with_nulls.count()
        assert len(invalid_dfs) > 0

    def test_metrics_populated(self, spark, data_with_nulls):
        """Test that metrics are populated after running checks."""
        checker = DataQualityChecker(spark, "test-bucket")
        valid_df, invalid_dfs = checker.run_all_checks(data_with_nulls)

        metrics = checker.get_quality_metrics()
        assert len(metrics) > 0
        assert 'null_violations' in metrics


class TestQualityReport:
    """Test quality report generation."""

    def test_create_quality_report(self, spark, clean_data):
        """Test quality report creation."""
        # Add required derived columns
        df = clean_data.withColumn(
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

        report = create_quality_report(df)

        # Report should have statistics
        assert report.count() == 1

        # Check required columns
        columns = report.columns
        assert 'total_records' in columns
        assert 'avg_trip_distance' in columns
        assert 'avg_fare_amount' in columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
