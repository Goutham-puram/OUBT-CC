#!/usr/bin/env python3
"""
Explore NYC Yellow Taxi Trip Data
Load parquet file with pandas and display schema and basic statistics.
"""

import logging
import pandas as pd
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


class TaxiDataExplorer:
    """Explore and analyze NYC taxi trip data."""

    def __init__(self):
        """Initialize the explorer."""
        self.df: Optional[pd.DataFrame] = None
        self.s3_client = None

    def load_from_local(self, filepath: str) -> pd.DataFrame:
        """
        Load parquet file from local filesystem.

        Args:
            filepath: Path to parquet file

        Returns:
            Loaded DataFrame

        Raises:
            FileNotFoundError: If file doesn't exist
            Exception: If loading fails
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

    def load_from_s3(self, bucket: str, key: str) -> pd.DataFrame:
        """
        Load parquet file from S3.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            Loaded DataFrame

        Raises:
            ClientError: If S3 operation fails
        """
        if self.s3_client is None:
            self.s3_client = boto3.client('s3')

        logger.info(f"Loading data from s3://{bucket}/{key}...")

        try:
            # Download file to memory
            obj = self.s3_client.get_object(Bucket=bucket, Key=key)
            data = obj['Body'].read()

            # Load into pandas
            self.df = pd.read_parquet(BytesIO(data))
            logger.info(f"Successfully loaded {len(self.df):,} records from S3")
            return self.df

        except ClientError as e:
            logger.error(f"Failed to load from S3: {e}")
            raise

    def show_schema(self):
        """Display dataset schema information."""
        if self.df is None:
            logger.error("No data loaded. Please load data first.")
            return

        print("\n" + "="*80)
        print("DATASET SCHEMA")
        print("="*80)

        print(f"\nDataset Shape: {self.df.shape[0]:,} rows × {self.df.shape[1]} columns")
        print(f"Memory Usage: {self.df.memory_usage(deep=True).sum() / (1024**2):.2f} MB")

        print("\n" + "-"*80)
        print("Column Information:")
        print("-"*80)

        # Create schema info
        schema_info = pd.DataFrame({
            'Column': self.df.columns,
            'Type': self.df.dtypes.values,
            'Non-Null Count': [self.df[col].count() for col in self.df.columns],
            'Null Count': [self.df[col].isna().sum() for col in self.df.columns],
            'Null %': [f"{(self.df[col].isna().sum() / len(self.df) * 100):.2f}%"
                      for col in self.df.columns]
        })

        print(schema_info.to_string(index=False))
        print("="*80 + "\n")

    def show_statistics(self):
        """Display basic statistical analysis."""
        if self.df is None:
            logger.error("No data loaded. Please load data first.")
            return

        print("\n" + "="*80)
        print("NUMERICAL STATISTICS")
        print("="*80)

        # Get numeric columns
        numeric_cols = self.df.select_dtypes(include=['int64', 'float64']).columns

        if len(numeric_cols) > 0:
            stats = self.df[numeric_cols].describe()
            print(stats.to_string())
        else:
            print("No numeric columns found.")

        print("\n" + "="*80)
        print("CATEGORICAL STATISTICS")
        print("="*80)

        # Get categorical/object columns
        categorical_cols = self.df.select_dtypes(include=['object', 'category']).columns

        if len(categorical_cols) > 0:
            for col in categorical_cols:
                print(f"\n{col}:")
                print(f"  Unique values: {self.df[col].nunique()}")
                print(f"  Most common:")
                value_counts = self.df[col].value_counts().head(5)
                for value, count in value_counts.items():
                    print(f"    {value}: {count:,} ({count/len(self.df)*100:.2f}%)")
        else:
            print("No categorical columns found.")

        print("\n" + "="*80)
        print("DATETIME STATISTICS")
        print("="*80)

        # Get datetime columns
        datetime_cols = self.df.select_dtypes(include=['datetime64']).columns

        if len(datetime_cols) > 0:
            for col in datetime_cols:
                print(f"\n{col}:")
                print(f"  Min: {self.df[col].min()}")
                print(f"  Max: {self.df[col].max()}")
                print(f"  Range: {self.df[col].max() - self.df[col].min()}")
        else:
            print("No datetime columns found.")

        print("\n" + "="*80 + "\n")

    def show_sample_data(self, n: int = 10):
        """
        Display sample records.

        Args:
            n: Number of records to display (default: 10)
        """
        if self.df is None:
            logger.error("No data loaded. Please load data first.")
            return

        print("\n" + "="*80)
        print(f"SAMPLE DATA (First {n} records)")
        print("="*80 + "\n")

        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 50)

        print(self.df.head(n).to_string())
        print("\n" + "="*80 + "\n")

    def generate_report(self):
        """Generate comprehensive exploration report."""
        if self.df is None:
            logger.error("No data loaded. Please load data first.")
            return

        logger.info("Generating exploration report...")

        self.show_schema()
        self.show_sample_data(5)
        self.show_statistics()

        # Additional insights
        print("="*80)
        print("ADDITIONAL INSIGHTS")
        print("="*80)

        # Check for duplicates
        duplicates = self.df.duplicated().sum()
        print(f"\nDuplicate rows: {duplicates:,} ({duplicates/len(self.df)*100:.2f}%)")

        # Missing data summary
        missing_data = self.df.isna().sum()
        if missing_data.sum() > 0:
            print("\nColumns with missing data:")
            for col, count in missing_data[missing_data > 0].items():
                print(f"  {col}: {count:,} ({count/len(self.df)*100:.2f}%)")
        else:
            print("\nNo missing data found.")

        print("\n" + "="*80 + "\n")


def main():
    """Main execution function."""
    try:
        # Initialize explorer
        explorer = TaxiDataExplorer()

        # Determine data source
        script_dir = Path(__file__).parent
        default_file = script_dir.parent.parent / 'data' / 'raw' / 'yellow_tripdata_2024-01.parquet'

        logger.info("Starting NYC Yellow Taxi data exploration")

        # Try to load from local file
        if default_file.exists():
            logger.info(f"Loading from local file: {default_file}")
            explorer.load_from_local(str(default_file))
        else:
            logger.error(f"Data file not found: {default_file}")
            logger.info("Please run download_taxi_data.py first to download the data.")
            return

        # Generate comprehensive report
        explorer.generate_report()

        logger.info("Exploration completed successfully")

    except Exception as e:
        logger.error(f"Exploration failed: {e}")
        raise


if __name__ == "__main__":
    main()
