#!/usr/bin/env python3
"""
Sample NYC Yellow Taxi Trip Data
Create a representative sample for testing and development (e.g., RDS testing).
"""

import logging
import pandas as pd
import boto3
from pathlib import Path
from typing import Optional
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TaxiDataSampler:
    """Create samples from NYC taxi trip data."""

    def __init__(self):
        """Initialize the sampler."""
        self.df: Optional[pd.DataFrame] = None
        self.df_sample: Optional[pd.DataFrame] = None
        self.s3_client = None

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

    def create_random_sample(self, n: int = 10000, random_state: int = 42) -> pd.DataFrame:
        """
        Create a random sample of the dataset.

        Args:
            n: Number of records to sample (default: 10000)
            random_state: Random seed for reproducibility (default: 42)

        Returns:
            Sampled DataFrame
        """
        if self.df is None:
            raise ValueError("No data loaded. Please load data first.")

        if n > len(self.df):
            logger.warning(f"Requested sample size {n:,} exceeds dataset size {len(self.df):,}")
            logger.warning(f"Returning entire dataset")
            self.df_sample = self.df.copy()
            return self.df_sample

        logger.info(f"Creating random sample of {n:,} records...")

        self.df_sample = self.df.sample(n=n, random_state=random_state)

        logger.info(f"Sample created: {len(self.df_sample):,} records")
        logger.info(f"Sample represents {len(self.df_sample)/len(self.df)*100:.2f}% of original data")

        return self.df_sample

    def create_stratified_sample(self, n: int = 10000, stratify_column: str = None,
                                random_state: int = 42) -> pd.DataFrame:
        """
        Create a stratified sample to maintain distribution of a specific column.

        Args:
            n: Number of records to sample (default: 10000)
            stratify_column: Column to stratify by (e.g., 'pickup_hour', 'payment_type')
            random_state: Random seed for reproducibility (default: 42)

        Returns:
            Sampled DataFrame
        """
        if self.df is None:
            raise ValueError("No data loaded. Please load data first.")

        if stratify_column and stratify_column not in self.df.columns:
            raise ValueError(f"Column '{stratify_column}' not found in dataset")

        if n > len(self.df):
            logger.warning(f"Requested sample size {n:,} exceeds dataset size {len(self.df):,}")
            logger.warning(f"Returning entire dataset")
            self.df_sample = self.df.copy()
            return self.df_sample

        logger.info(f"Creating stratified sample of {n:,} records (stratified by {stratify_column})...")

        # Calculate sampling fraction
        frac = n / len(self.df)

        # Perform stratified sampling
        self.df_sample = self.df.groupby(stratify_column, group_keys=False).apply(
            lambda x: x.sample(frac=frac, random_state=random_state)
        )

        # If we don't get exactly n records due to rounding, adjust
        if len(self.df_sample) < n:
            # Add more records randomly
            remaining = n - len(self.df_sample)
            additional = self.df[~self.df.index.isin(self.df_sample.index)].sample(
                n=remaining, random_state=random_state
            )
            self.df_sample = pd.concat([self.df_sample, additional])
        elif len(self.df_sample) > n:
            # Remove excess records randomly
            self.df_sample = self.df_sample.sample(n=n, random_state=random_state)

        logger.info(f"Stratified sample created: {len(self.df_sample):,} records")

        return self.df_sample

    def create_time_based_sample(self, n: int = 10000, date_column: str = 'tpep_pickup_datetime',
                                start_date: str = None, end_date: str = None,
                                random_state: int = 42) -> pd.DataFrame:
        """
        Create a sample from a specific time period.

        Args:
            n: Number of records to sample (default: 10000)
            date_column: Column containing datetime (default: 'tpep_pickup_datetime')
            start_date: Start date (format: 'YYYY-MM-DD'), None for beginning
            end_date: End date (format: 'YYYY-MM-DD'), None for end
            random_state: Random seed for reproducibility (default: 42)

        Returns:
            Sampled DataFrame
        """
        if self.df is None:
            raise ValueError("No data loaded. Please load data first.")

        if date_column not in self.df.columns:
            raise ValueError(f"Column '{date_column}' not found in dataset")

        logger.info(f"Creating time-based sample of {n:,} records...")

        # Filter by date range if specified
        df_filtered = self.df.copy()

        if start_date:
            df_filtered = df_filtered[df_filtered[date_column] >= pd.to_datetime(start_date)]
            logger.info(f"Filtered from {start_date}")

        if end_date:
            df_filtered = df_filtered[df_filtered[date_column] <= pd.to_datetime(end_date)]
            logger.info(f"Filtered to {end_date}")

        logger.info(f"Records in time range: {len(df_filtered):,}")

        if len(df_filtered) == 0:
            raise ValueError("No records found in specified date range")

        # Sample from filtered data
        if n >= len(df_filtered):
            logger.warning(f"Requested sample size exceeds filtered data. Using all {len(df_filtered):,} records")
            self.df_sample = df_filtered
        else:
            self.df_sample = df_filtered.sample(n=n, random_state=random_state)

        logger.info(f"Time-based sample created: {len(self.df_sample):,} records")

        return self.df_sample

    def validate_sample(self):
        """Validate sample representativeness."""
        if self.df is None or self.df_sample is None:
            raise ValueError("Both original and sample data must be loaded")

        logger.info("Validating sample representativeness...")

        print("\n" + "="*80)
        print("SAMPLE VALIDATION")
        print("="*80)

        print(f"\nOriginal dataset: {len(self.df):,} records")
        print(f"Sample dataset:   {len(self.df_sample):,} records")
        print(f"Sample ratio:     {len(self.df_sample)/len(self.df)*100:.2f}%")

        # Compare numeric columns
        numeric_cols = self.df.select_dtypes(include=['int64', 'float64']).columns

        if len(numeric_cols) > 0:
            print("\n" + "-"*80)
            print("Numeric Column Comparison (Mean values):")
            print("-"*80)

            for col in numeric_cols[:10]:  # Limit to first 10 columns
                orig_mean = self.df[col].mean()
                sample_mean = self.df_sample[col].mean()
                diff_pct = abs(orig_mean - sample_mean) / orig_mean * 100 if orig_mean != 0 else 0

                print(f"{col:30s} | Original: {orig_mean:12.2f} | "
                      f"Sample: {sample_mean:12.2f} | Diff: {diff_pct:5.2f}%")

        print("\n" + "="*80 + "\n")

    def save_sample(self, output_path: str = None, format: str = 'parquet',
                   s3_bucket: str = None, s3_key: str = None):
        """
        Save sample data to file.

        Args:
            output_path: Local path to save file
            format: Output format ('parquet' or 'csv')
            s3_bucket: S3 bucket name (optional)
            s3_key: S3 object key (optional)

        Returns:
            Path to saved file
        """
        if self.df_sample is None:
            raise ValueError("No sample data to save. Create a sample first.")

        # Determine output path
        if output_path is None:
            script_dir = Path(__file__).parent
            processed_dir = script_dir.parent.parent / 'data' / 'processed'
            processed_dir.mkdir(parents=True, exist_ok=True)

            if format == 'parquet':
                output_path = processed_dir / 'yellow_tripdata_2024-01_sample.parquet'
            else:
                output_path = processed_dir / 'yellow_tripdata_2024-01_sample.csv'
        else:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Saving sample data to {output_path}...")

        try:
            if format == 'parquet':
                self.df_sample.to_parquet(output_path, index=False, compression='snappy')
            elif format == 'csv':
                self.df_sample.to_csv(output_path, index=False)
            else:
                raise ValueError(f"Unsupported format: {format}. Use 'parquet' or 'csv'")

            file_size = output_path.stat().st_size / (1024 * 1024)
            logger.info(f"Successfully saved sample data ({file_size:.2f} MB)")

            # Upload to S3 if specified
            if s3_bucket and s3_key:
                self._upload_to_s3(output_path, s3_bucket, s3_key)

            return output_path

        except Exception as e:
            logger.error(f"Failed to save sample data: {e}")
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

    def export_to_csv_for_rds(self, output_path: str = None) -> Path:
        """
        Export sample to CSV format optimized for RDS import.

        Args:
            output_path: Path to save CSV file

        Returns:
            Path to saved CSV file
        """
        if self.df_sample is None:
            raise ValueError("No sample data available. Create a sample first.")

        logger.info("Preparing sample for RDS import...")

        # Create a copy for RDS export
        df_rds = self.df_sample.copy()

        # Convert datetime columns to string format
        for col in df_rds.select_dtypes(include=['datetime64']).columns:
            df_rds[col] = df_rds[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Handle any remaining data type issues
        for col in df_rds.columns:
            if df_rds[col].dtype == 'object':
                df_rds[col] = df_rds[col].astype(str)

        # Save to CSV
        if output_path is None:
            script_dir = Path(__file__).parent
            processed_dir = script_dir.parent.parent / 'data' / 'processed'
            processed_dir.mkdir(parents=True, exist_ok=True)
            output_path = processed_dir / 'yellow_tripdata_2024-01_sample_rds.csv'
        else:
            output_path = Path(output_path)

        logger.info(f"Saving RDS-ready CSV to {output_path}...")

        df_rds.to_csv(output_path, index=False)

        file_size = output_path.stat().st_size / (1024 * 1024)
        logger.info(f"RDS-ready CSV saved ({file_size:.2f} MB)")

        return output_path


def main():
    """Main execution function."""
    try:
        # Initialize sampler
        sampler = TaxiDataSampler()

        # Load cleaned data (or raw data if cleaned not available)
        script_dir = Path(__file__).parent
        data_dir = script_dir.parent.parent / 'data'

        # Try cleaned data first, fall back to raw
        cleaned_file = data_dir / 'processed' / 'yellow_tripdata_2024-01_cleaned.parquet'
        raw_file = data_dir / 'raw' / 'yellow_tripdata_2024-01.parquet'

        logger.info("Starting NYC Yellow Taxi data sampling process")

        if cleaned_file.exists():
            logger.info(f"Loading cleaned data from {cleaned_file}")
            sampler.load_data(str(cleaned_file))
        elif raw_file.exists():
            logger.info(f"Loading raw data from {raw_file}")
            sampler.load_data(str(raw_file))
        else:
            logger.error("No data file found. Please run download_taxi_data.py first.")
            return

        # Create random sample of 10,000 records
        logger.info("Creating 10,000 record sample for RDS testing...")
        sampler.create_random_sample(n=10000, random_state=42)

        # Validate sample
        sampler.validate_sample()

        # Save in both formats
        parquet_path = sampler.save_sample(format='parquet')
        logger.info(f"Parquet sample saved to: {parquet_path}")

        csv_path = sampler.export_to_csv_for_rds()
        logger.info(f"RDS-ready CSV saved to: {csv_path}")

        logger.info("Sampling completed successfully")

        print("\n" + "="*80)
        print("SAMPLING COMPLETE")
        print("="*80)
        print(f"Sample size:     {len(sampler.df_sample):,} records")
        print(f"Parquet output:  {parquet_path}")
        print(f"CSV output:      {csv_path}")
        print("="*80 + "\n")

    except Exception as e:
        logger.error(f"Sampling process failed: {e}")
        raise


if __name__ == "__main__":
    main()
