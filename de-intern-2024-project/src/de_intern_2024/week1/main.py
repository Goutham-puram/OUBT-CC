"""
Week 1 Main Script - AWS Setup and RDS PostgreSQL

This script demonstrates:
1. Downloading NYC Taxi data
2. Basic data exploration
3. Connecting to RDS PostgreSQL
4. Loading data into PostgreSQL
"""

import argparse
from pathlib import Path

from ..utils.logger import get_logger
from .data_ingestion import (
    download_taxi_data,
    load_taxi_parquet,
    clean_taxi_data,
    ingest_to_postgres,
    get_sample_statistics
)
from .rds_connector import RDSConnector

logger = get_logger(__name__)


def main():
    """Main execution function for Week 1."""
    parser = argparse.ArgumentParser(description="Week 1: Data Ingestion and RDS Setup")
    parser.add_argument("--year", type=int, default=2023, help="Year to download")
    parser.add_argument("--month", type=int, default=1, help="Month to download")
    parser.add_argument("--sample", type=int, help="Load only N rows (for testing)")
    parser.add_argument("--skip-download", action="store_true", help="Skip download step")
    parser.add_argument("--skip-ingest", action="store_true", help="Skip PostgreSQL ingestion")

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Week 1: AWS Setup and RDS PostgreSQL")
    logger.info("=" * 60)

    # Step 1: Download data
    file_path = None
    if not args.skip_download:
        logger.info(f"\n[Step 1] Downloading taxi data for {args.year}-{args.month:02d}")
        file_path = download_taxi_data(args.year, args.month)
        if not file_path:
            logger.error("Download failed!")
            return
    else:
        # Look for existing file
        filename = f"yellow_tripdata_{args.year}-{args.month:02d}.parquet"
        file_path = Path("data/raw") / filename
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return
        file_path = str(file_path)

    # Step 2: Load and explore data
    logger.info("\n[Step 2] Loading and exploring data")
    df = load_taxi_parquet(file_path, nrows=args.sample)

    logger.info("\nData Preview:")
    logger.info(f"\n{df.head()}")

    logger.info("\nData Info:")
    logger.info(f"Shape: {df.shape}")
    logger.info(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

    # Step 3: Clean data
    logger.info("\n[Step 3] Cleaning data")
    df_clean = clean_taxi_data(df)

    # Step 4: Get statistics
    logger.info("\n[Step 4] Computing statistics")
    stats = get_sample_statistics(df_clean)
    logger.info(f"\nStatistics:\n{stats}")

    # Step 5: Ingest to PostgreSQL
    if not args.skip_ingest:
        logger.info("\n[Step 5] Ingesting to PostgreSQL")
        connector = RDSConnector()

        # Test connection
        if connector.test_connection():
            logger.info("Database connection successful!")

            # Ingest data
            success = ingest_to_postgres(df_clean, connector=connector)
            if success:
                logger.info("Data ingestion completed successfully!")
            else:
                logger.error("Data ingestion failed!")

            # Verify ingestion
            logger.info("\nVerifying ingestion...")
            row_count = connector.execute_query(
                "SELECT COUNT(*) as count FROM taxi_trips"
            )
            logger.info(f"Total rows in database: {row_count[0]['count']}")

            connector.close()
        else:
            logger.error("Could not connect to database!")
    else:
        logger.info("\n[Step 5] Skipping PostgreSQL ingestion")

    logger.info("\n" + "=" * 60)
    logger.info("Week 1 completed successfully!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
