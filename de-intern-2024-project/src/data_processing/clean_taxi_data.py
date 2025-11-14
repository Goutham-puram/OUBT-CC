#!/usr/bin/env python3
"""
Clean NYC Yellow Taxi Trip Data
Handle null values, add calculated columns, and prepare data for analysis.
"""

import logging
import pandas as pd
import numpy as np
import boto3
from pathlib import Path
from typing import Optional
from io import BytesIO
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TaxiDataCleaner:
    """Clean and transform NYC taxi trip data."""

    def __init__(self):
        """Initialize the cleaner."""
        self.df: Optional[pd.DataFrame] = None
        self.df_clean: Optional[pd.DataFrame] = None
        self.s3_client = None
        self.cleaning_stats = {}

    def load_data(self, filepath: str) -> pd.DataFrame:
        """
        Load parquet file from local filesystem.

        Args:
            filepath: Path to parquet file

        Returns:
            Loaded DataFrame

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        filepath = Path(filepath)

        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")

        logger.info(f"Loading data from {filepath}...")

        try:
            self.df = pd.read_parquet(filepath)
            logger.info(f"Successfully loaded {len(self.df):,} records")
            return self.df

        except Exception as e:
            logger.error(f"Failed to load parquet file: {e}")
            raise

    def handle_missing_values(self):
        """Handle missing values in the dataset."""
        if self.df is None:
            raise ValueError("No data loaded. Please load data first.")

        logger.info("Handling missing values...")

        initial_rows = len(self.df)
        missing_before = self.df.isna().sum().sum()

        # Make a copy for cleaning
        self.df_clean = self.df.copy()

        # Check missing values per column
        missing_by_column = self.df_clean.isna().sum()
        logger.info(f"Missing values before cleaning: {missing_before:,}")

        for col in missing_by_column[missing_by_column > 0].index:
            logger.info(f"  {col}: {missing_by_column[col]:,} "
                       f"({missing_by_column[col]/len(self.df_clean)*100:.2f}%)")

        # Strategy for handling missing values:
        # 1. For critical columns (pickup/dropoff times), remove rows
        critical_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
        for col in critical_cols:
            if col in self.df_clean.columns:
                rows_before = len(self.df_clean)
                self.df_clean = self.df_clean.dropna(subset=[col])
                rows_removed = rows_before - len(self.df_clean)
                if rows_removed > 0:
                    logger.info(f"Removed {rows_removed:,} rows with missing {col}")

        # 2. For numeric columns, fill with median or 0 depending on context
        numeric_fill_zero = ['passenger_count', 'extra', 'mta_tax', 'tip_amount',
                            'tolls_amount', 'improvement_surcharge', 'congestion_surcharge']

        for col in numeric_fill_zero:
            if col in self.df_clean.columns and self.df_clean[col].isna().any():
                self.df_clean[col].fillna(0, inplace=True)
                logger.info(f"Filled missing {col} with 0")

        # 3. For location IDs, remove rows with missing values (important for analysis)
        location_cols = ['PULocationID', 'DOLocationID']
        for col in location_cols:
            if col in self.df_clean.columns:
                rows_before = len(self.df_clean)
                self.df_clean = self.df_clean.dropna(subset=[col])
                rows_removed = rows_before - len(self.df_clean)
                if rows_removed > 0:
                    logger.info(f"Removed {rows_removed:,} rows with missing {col}")

        # Log cleaning results
        rows_removed = initial_rows - len(self.df_clean)
        missing_after = self.df_clean.isna().sum().sum()

        self.cleaning_stats['initial_rows'] = initial_rows
        self.cleaning_stats['rows_removed'] = rows_removed
        self.cleaning_stats['final_rows'] = len(self.df_clean)
        self.cleaning_stats['missing_before'] = missing_before
        self.cleaning_stats['missing_after'] = missing_after

        logger.info(f"Removed {rows_removed:,} rows ({rows_removed/initial_rows*100:.2f}%)")
        logger.info(f"Remaining rows: {len(self.df_clean):,}")
        logger.info(f"Missing values after cleaning: {missing_after:,}")

    def add_calculated_columns(self):
        """Add calculated columns for analysis."""
        if self.df_clean is None:
            raise ValueError("No cleaned data available. Please run handle_missing_values() first.")

        logger.info("Adding calculated columns...")

        # 1. Trip Duration (in minutes)
        if 'tpep_pickup_datetime' in self.df_clean.columns and \
           'tpep_dropoff_datetime' in self.df_clean.columns:

            self.df_clean['trip_duration'] = (
                self.df_clean['tpep_dropoff_datetime'] - self.df_clean['tpep_pickup_datetime']
            ).dt.total_seconds() / 60.0  # Convert to minutes

            logger.info(f"Added trip_duration column (mean: {self.df_clean['trip_duration'].mean():.2f} minutes)")

            # Remove invalid durations (negative or extremely long)
            invalid_duration = (self.df_clean['trip_duration'] <= 0) | \
                             (self.df_clean['trip_duration'] > 1440)  # > 24 hours
            invalid_count = invalid_duration.sum()

            if invalid_count > 0:
                logger.warning(f"Removing {invalid_count:,} rows with invalid trip duration")
                self.df_clean = self.df_clean[~invalid_duration]

        # 2. Tip Percentage
        if 'tip_amount' in self.df_clean.columns and 'fare_amount' in self.df_clean.columns:

            # Calculate tip percentage (handle division by zero)
            self.df_clean['tip_percentage'] = np.where(
                self.df_clean['fare_amount'] > 0,
                (self.df_clean['tip_amount'] / self.df_clean['fare_amount']) * 100,
                0
            )

            logger.info(f"Added tip_percentage column (mean: {self.df_clean['tip_percentage'].mean():.2f}%)")

            # Cap tip percentage at reasonable values (e.g., 100%)
            extreme_tips = self.df_clean['tip_percentage'] > 100
            if extreme_tips.any():
                logger.warning(f"Capping {extreme_tips.sum():,} extreme tip percentages at 100%")
                self.df_clean.loc[extreme_tips, 'tip_percentage'] = 100

        # 3. Average Speed (mph)
        if 'trip_distance' in self.df_clean.columns and 'trip_duration' in self.df_clean.columns:

            # Calculate speed (miles per hour)
            self.df_clean['avg_speed_mph'] = np.where(
                self.df_clean['trip_duration'] > 0,
                (self.df_clean['trip_distance'] / self.df_clean['trip_duration']) * 60,
                0
            )

            logger.info(f"Added avg_speed_mph column (mean: {self.df_clean['avg_speed_mph'].mean():.2f} mph)")

            # Remove unrealistic speeds (e.g., > 100 mph in NYC)
            invalid_speed = self.df_clean['avg_speed_mph'] > 100
            if invalid_speed.any():
                logger.warning(f"Removing {invalid_speed.sum():,} rows with unrealistic speed (>100 mph)")
                self.df_clean = self.df_clean[~invalid_speed]

        # 4. Hour of day and day of week (for time-based analysis)
        if 'tpep_pickup_datetime' in self.df_clean.columns:
            self.df_clean['pickup_hour'] = self.df_clean['tpep_pickup_datetime'].dt.hour
            self.df_clean['pickup_day_of_week'] = self.df_clean['tpep_pickup_datetime'].dt.dayofweek
            self.df_clean['pickup_date'] = self.df_clean['tpep_pickup_datetime'].dt.date

            logger.info("Added time-based columns: pickup_hour, pickup_day_of_week, pickup_date")

        # 5. Cost per mile
        if 'total_amount' in self.df_clean.columns and 'trip_distance' in self.df_clean.columns:
            self.df_clean['cost_per_mile'] = np.where(
                self.df_clean['trip_distance'] > 0,
                self.df_clean['total_amount'] / self.df_clean['trip_distance'],
                0
            )

            logger.info(f"Added cost_per_mile column (mean: ${self.df_clean['cost_per_mile'].mean():.2f}/mile)")

        logger.info(f"Final dataset shape: {self.df_clean.shape}")

    def validate_data(self):
        """Validate cleaned data for quality issues."""
        if self.df_clean is None:
            raise ValueError("No cleaned data available.")

        logger.info("Validating cleaned data...")

        issues = []

        # Check for negative values where they shouldn't exist
        negative_checks = {
            'trip_distance': 'trip distance',
            'fare_amount': 'fare amount',
            'total_amount': 'total amount',
            'passenger_count': 'passenger count'
        }

        for col, description in negative_checks.items():
            if col in self.df_clean.columns:
                negative_count = (self.df_clean[col] < 0).sum()
                if negative_count > 0:
                    issues.append(f"{negative_count:,} negative {description} values")

        # Check for zero passenger count
        if 'passenger_count' in self.df_clean.columns:
            zero_passengers = (self.df_clean['passenger_count'] == 0).sum()
            if zero_passengers > 0:
                issues.append(f"{zero_passengers:,} trips with 0 passengers")

        # Check for unrealistic trip distances
        if 'trip_distance' in self.df_clean.columns:
            very_long = (self.df_clean['trip_distance'] > 100).sum()  # > 100 miles
            if very_long > 0:
                issues.append(f"{very_long:,} trips > 100 miles")

        if issues:
            logger.warning("Data quality issues found:")
            for issue in issues:
                logger.warning(f"  - {issue}")
        else:
            logger.info("No major data quality issues found")

        # Log summary statistics
        logger.info("\nCleaned Data Summary:")
        logger.info(f"  Total records: {len(self.df_clean):,}")
        logger.info(f"  Total columns: {len(self.df_clean.columns)}")
        logger.info(f"  Memory usage: {self.df_clean.memory_usage(deep=True).sum() / (1024**2):.2f} MB")

    def save_cleaned_data(self, output_path: str = None, s3_bucket: str = None,
                         s3_key: str = None):
        """
        Save cleaned data to parquet file.

        Args:
            output_path: Local path to save file
            s3_bucket: S3 bucket name (optional)
            s3_key: S3 object key (optional)
        """
        if self.df_clean is None:
            raise ValueError("No cleaned data to save.")

        # Save locally
        if output_path is None:
            script_dir = Path(__file__).parent
            processed_dir = script_dir.parent.parent / 'data' / 'processed'
            processed_dir.mkdir(parents=True, exist_ok=True)
            output_path = processed_dir / 'yellow_tripdata_2024-01_cleaned.parquet'
        else:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Saving cleaned data to {output_path}...")

        try:
            self.df_clean.to_parquet(output_path, index=False, compression='snappy')
            file_size = output_path.stat().st_size / (1024 * 1024)
            logger.info(f"Successfully saved cleaned data ({file_size:.2f} MB)")

            # Upload to S3 if specified
            if s3_bucket and s3_key:
                self._upload_to_s3(output_path, s3_bucket, s3_key)

            return output_path

        except Exception as e:
            logger.error(f"Failed to save cleaned data: {e}")
            raise

    def _upload_to_s3(self, filepath: Path, bucket: str, key: str):
        """Upload file to S3."""
        if self.s3_client is None:
            self.s3_client = boto3.client('s3')

        try:
            logger.info(f"Uploading to s3://{bucket}/{key}...")
            self.s3_client.upload_file(str(filepath), bucket, key)
            logger.info("Successfully uploaded to S3")

        except ClientError as e:
            logger.error(f"Failed to upload to S3: {e}")

    def print_cleaning_summary(self):
        """Print summary of cleaning operations."""
        if not self.cleaning_stats:
            logger.warning("No cleaning statistics available")
            return

        print("\n" + "="*80)
        print("DATA CLEANING SUMMARY")
        print("="*80)
        print(f"Initial rows:           {self.cleaning_stats['initial_rows']:,}")
        print(f"Rows removed:           {self.cleaning_stats['rows_removed']:,} "
              f"({self.cleaning_stats['rows_removed']/self.cleaning_stats['initial_rows']*100:.2f}%)")
        print(f"Final rows:             {self.cleaning_stats['final_rows']:,}")
        print(f"Missing values before:  {self.cleaning_stats['missing_before']:,}")
        print(f"Missing values after:   {self.cleaning_stats['missing_after']:,}")
        print("="*80 + "\n")


def main():
    """Main execution function."""
    try:
        # Initialize cleaner
        cleaner = TaxiDataCleaner()

        # Load data
        script_dir = Path(__file__).parent
        input_file = script_dir.parent.parent / 'data' / 'raw' / 'yellow_tripdata_2024-01.parquet'

        logger.info("Starting NYC Yellow Taxi data cleaning process")

        if not input_file.exists():
            logger.error(f"Input file not found: {input_file}")
            logger.info("Please run download_taxi_data.py first to download the data.")
            return

        cleaner.load_data(str(input_file))

        # Clean data
        cleaner.handle_missing_values()
        cleaner.add_calculated_columns()
        cleaner.validate_data()

        # Save cleaned data
        output_path = cleaner.save_cleaned_data()

        # Print summary
        cleaner.print_cleaning_summary()

        logger.info(f"Cleaning completed successfully. Output: {output_path}")

    except Exception as e:
        logger.error(f"Cleaning process failed: {e}")
        raise


if __name__ == "__main__":
    main()
