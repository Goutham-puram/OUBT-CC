#!/usr/bin/env python3
"""
Script to download NYC Yellow Taxi data for a specified date range.

Usage:
    python download_taxi_data.py --year 2023 --month 1
    python download_taxi_data.py --year 2023 --month-start 1 --month-end 3
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path to import project modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from de_intern_2024.week1.data_ingestion import download_taxi_data
from de_intern_2024.utils.logger import get_logger

logger = get_logger(__name__)


def main():
    """Main function to download taxi data."""
    parser = argparse.ArgumentParser(
        description="Download NYC Yellow Taxi trip data"
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Year to download (e.g., 2023)"
    )
    parser.add_argument(
        "--month",
        type=int,
        help="Single month to download (1-12)"
    )
    parser.add_argument(
        "--month-start",
        type=int,
        default=1,
        help="Start month for range (1-12)"
    )
    parser.add_argument(
        "--month-end",
        type=int,
        default=12,
        help="End month for range (1-12)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/raw",
        help="Output directory for downloaded files"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download even if file exists"
    )

    args = parser.parse_args()

    # Determine month range
    if args.month:
        months = [args.month]
    else:
        months = range(args.month_start, args.month_end + 1)

    logger.info(f"Downloading NYC Taxi data for {args.year}, months: {list(months)}")

    success_count = 0
    fail_count = 0

    for month in months:
        logger.info(f"\n{'='*60}")
        logger.info(f"Downloading {args.year}-{month:02d}")
        logger.info('='*60)

        result = download_taxi_data(
            year=args.year,
            month=month,
            output_dir=args.output_dir,
            force=args.force
        )

        if result:
            success_count += 1
            logger.info(f"✓ Successfully downloaded {args.year}-{month:02d}")
        else:
            fail_count += 1
            logger.error(f"✗ Failed to download {args.year}-{month:02d}")

    logger.info(f"\n{'='*60}")
    logger.info("Download Summary")
    logger.info('='*60)
    logger.info(f"Successful: {success_count}")
    logger.info(f"Failed: {fail_count}")
    logger.info(f"Total: {success_count + fail_count}")

    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
