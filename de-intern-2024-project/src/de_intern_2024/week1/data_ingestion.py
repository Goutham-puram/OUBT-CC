"""Data ingestion utilities for NYC Taxi data."""

import os
from pathlib import Path
from typing import Optional
import requests
import pandas as pd
from datetime import datetime

from ..utils.logger import get_logger
from .rds_connector import RDSConnector

logger = get_logger(__name__)

# NYC Taxi data URL pattern
NYC_TAXI_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
YELLOW_TAXI_PATTERN = "yellow_tripdata_{year}-{month:02d}.parquet"


def download_taxi_data(
    year: int,
    month: int,
    output_dir: str = "data/raw",
    force: bool = False
) -> Optional[str]:
    """
    Download NYC Yellow Taxi data for a specific month.

    Args:
        year: Year (e.g., 2023)
        month: Month (1-12)
        output_dir: Directory to save the file
        force: If True, download even if file exists

    Returns:
        Path to downloaded file, or None if failed.
    """
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Generate filename and URL
    filename = YELLOW_TAXI_PATTERN.format(year=year, month=month)
    url = f"{NYC_TAXI_BASE_URL}/{filename}"
    file_path = output_path / filename

    # Check if file already exists
    if file_path.exists() and not force:
        logger.info(f"File already exists: {file_path}")
        return str(file_path)

    try:
        logger.info(f"Downloading data from {url}")
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()

        # Download with progress
        total_size = int(response.headers.get('content-length', 0))
        logger.info(f"File size: {total_size / (1024*1024):.2f} MB")

        with open(file_path, 'wb') as f:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0 and downloaded % (1024*1024*10) == 0:  # Log every 10MB
                    progress = (downloaded / total_size) * 100
                    logger.info(f"Download progress: {progress:.1f}%")

        logger.info(f"Successfully downloaded to {file_path}")
        return str(file_path)

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download data: {e}")
        return None


def load_taxi_parquet(file_path: str, nrows: Optional[int] = None) -> pd.DataFrame:
    """
    Load taxi data from parquet file.

    Args:
        file_path: Path to parquet file
        nrows: Number of rows to load (None for all)

    Returns:
        DataFrame with taxi trip data.
    """
    try:
        logger.info(f"Loading data from {file_path}")
        df = pd.read_parquet(file_path)

        if nrows:
            df = df.head(nrows)

        logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
        logger.info(f"Columns: {list(df.columns)}")
        return df

    except Exception as e:
        logger.error(f"Failed to load parquet file: {e}")
        raise


def clean_taxi_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate taxi trip data.

    Args:
        df: Raw taxi data DataFrame

    Returns:
        Cleaned DataFrame.
    """
    logger.info("Cleaning taxi data")
    initial_rows = len(df)

    # Make a copy to avoid modifying original
    df = df.copy()

    # Remove rows with missing critical fields
    df = df.dropna(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

    # Remove invalid trips (negative or zero values)
    df = df[df['trip_distance'] > 0]
    df = df[df['fare_amount'] > 0]
    df = df[df['passenger_count'] > 0]

    # Remove outliers
    df = df[df['trip_distance'] < 100]  # Reasonable max distance
    df = df[df['fare_amount'] < 500]    # Reasonable max fare
    df = df[df['passenger_count'] <= 6] # Reasonable max passengers

    # Ensure datetime columns are datetime type
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    # Remove trips where dropoff is before pickup
    df = df[df['tpep_dropoff_datetime'] > df['tpep_pickup_datetime']]

    # Calculate trip duration in minutes
    df['trip_duration_minutes'] = (
        df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']
    ).dt.total_seconds() / 60

    # Remove unreasonably short or long trips
    df = df[df['trip_duration_minutes'] > 1]
    df = df[df['trip_duration_minutes'] < 300]  # 5 hours max

    rows_removed = initial_rows - len(df)
    logger.info(f"Removed {rows_removed} invalid rows ({rows_removed/initial_rows*100:.1f}%)")
    logger.info(f"Final dataset: {len(df)} rows")

    return df


def ingest_to_postgres(
    df: pd.DataFrame,
    table_name: str = "taxi_trips",
    connector: Optional[RDSConnector] = None,
    if_exists: str = "append"
) -> bool:
    """
    Ingest taxi data into PostgreSQL.

    Args:
        df: DataFrame to ingest
        table_name: Target table name
        connector: RDSConnector instance (creates new if None)
        if_exists: Action if table exists ('fail', 'replace', 'append')

    Returns:
        True if successful, False otherwise.
    """
    try:
        # Create connector if not provided
        if connector is None:
            connector = RDSConnector()

        # Test connection first
        if not connector.test_connection():
            logger.error("Database connection failed")
            return False

        logger.info(f"Ingesting {len(df)} rows to table {table_name}")

        # Write DataFrame to database
        connector.write_dataframe(df, table_name, if_exists=if_exists)

        logger.info(f"Successfully ingested data to {table_name}")
        return True

    except Exception as e:
        logger.error(f"Failed to ingest data: {e}")
        return False


def get_sample_statistics(df: pd.DataFrame) -> dict:
    """
    Get basic statistics about the taxi data.

    Args:
        df: Taxi data DataFrame

    Returns:
        Dictionary with statistics.
    """
    stats = {
        "total_trips": len(df),
        "date_range": {
            "start": df['tpep_pickup_datetime'].min().isoformat(),
            "end": df['tpep_pickup_datetime'].max().isoformat(),
        },
        "trip_distance": {
            "mean": float(df['trip_distance'].mean()),
            "median": float(df['trip_distance'].median()),
            "max": float(df['trip_distance'].max()),
        },
        "fare_amount": {
            "mean": float(df['fare_amount'].mean()),
            "median": float(df['fare_amount'].median()),
            "total": float(df['fare_amount'].sum()),
        },
        "passenger_count": {
            "mean": float(df['passenger_count'].mean()),
            "mode": int(df['passenger_count'].mode()[0]),
        }
    }

    logger.info(f"Statistics: {stats}")
    return stats
