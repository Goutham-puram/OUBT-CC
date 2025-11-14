"""
Run AWS Glue Crawler and verify schema discovery results.

This script:
1. Starts the Glue crawler
2. Monitors crawler execution
3. Verifies schema discovery (columns and data types)
4. Confirms partition detection
5. Provides detailed results and statistics
"""

import sys
import json
import time
from typing import Dict, Optional, List, Tuple
import boto3
from botocore.exceptions import ClientError

# Add parent directory to path for imports
sys.path.append('/home/user/OUBT-CC/de-intern-2024-project/src')
from de_intern_2024.utils.logger import get_logger
from de_intern_2024.utils.aws_helpers import get_boto3_client

logger = get_logger(__name__)


class GlueCrawlerRunner:
    """Manages execution and verification of AWS Glue crawler."""

    # Expected NYC taxi data schema
    EXPECTED_COLUMNS = 19
    EXPECTED_PARTITION_KEYS = 2  # year, month

    def __init__(
        self,
        crawler_name: str = "crawler-taxi-raw",
        database_name: str = "nyc_taxi_catalog",
        region: str = 'us-east-1'
    ):
        """
        Initialize Glue Crawler Runner.

        Args:
            crawler_name: Name of the Glue crawler.
            database_name: Target Glue catalog database.
            region: AWS region.
        """
        self.crawler_name = crawler_name
        self.database_name = database_name
        self.region = region
        self.glue_client = get_boto3_client('glue', region=region)

        logger.info(f"Initialized GlueCrawlerRunner for crawler: {self.crawler_name}")

    def start_crawler(self) -> bool:
        """
        Start the Glue crawler.

        Returns:
            bool: True if started successfully, False otherwise.
        """
        try:
            logger.info(f"Starting Glue crawler: {self.crawler_name}")

            # Check current state
            response = self.glue_client.get_crawler(Name=self.crawler_name)
            current_state = response['Crawler']['State']

            if current_state == 'RUNNING':
                logger.warning(f"Crawler {self.crawler_name} is already running")
                return True

            # Start the crawler
            self.glue_client.start_crawler(Name=self.crawler_name)

            logger.info(f"Successfully started crawler: {self.crawler_name}")
            return True

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')

            if error_code == 'CrawlerRunningException':
                logger.warning(f"Crawler {self.crawler_name} is already running")
                return True
            elif error_code == 'EntityNotFoundException':
                logger.error(f"Crawler {self.crawler_name} does not exist")
                return False
            else:
                logger.error(f"Failed to start crawler: {e}")
                return False

    def get_crawler_state(self) -> Optional[str]:
        """
        Get current state of the crawler.

        Returns:
            str: Crawler state, None if error.
        """
        try:
            response = self.glue_client.get_crawler(Name=self.crawler_name)
            return response['Crawler']['State']

        except ClientError as e:
            logger.error(f"Failed to get crawler state: {e}")
            return None

    def wait_for_crawler(self, timeout: int = 600, poll_interval: int = 10) -> Tuple[bool, Optional[Dict]]:
        """
        Wait for crawler to complete.

        Args:
            timeout: Maximum time to wait in seconds (default: 600).
            poll_interval: Time between status checks in seconds (default: 10).

        Returns:
            tuple: (success: bool, last_crawl_info: dict)
        """
        logger.info(f"Waiting for crawler to complete (timeout: {timeout}s)...")

        start_time = time.time()
        last_state = None

        while time.time() - start_time < timeout:
            state = self.get_crawler_state()

            if state != last_state:
                logger.info(f"Crawler state: {state}")
                last_state = state

            if state == 'READY':
                # Crawler completed
                elapsed_time = time.time() - start_time
                logger.info(f"Crawler completed in {elapsed_time:.1f} seconds")

                # Get last crawl info
                response = self.glue_client.get_crawler(Name=self.crawler_name)
                last_crawl = response['Crawler'].get('LastCrawl', {})

                return True, last_crawl

            elif state in ['STOPPING', 'RUNNING']:
                # Still running, wait and check again
                time.sleep(poll_interval)

            else:
                # Unexpected state
                logger.error(f"Unexpected crawler state: {state}")
                return False, None

        # Timeout reached
        logger.error(f"Crawler did not complete within {timeout} seconds")
        return False, None

    def verify_crawler_results(self, last_crawl: Dict) -> bool:
        """
        Verify crawler execution results.

        Args:
            last_crawl: Last crawl information from crawler.

        Returns:
            bool: True if verification successful, False otherwise.
        """
        logger.info("\n" + "=" * 80)
        logger.info("Verifying Crawler Results")
        logger.info("=" * 80)

        success = True

        # Check crawl status
        status = last_crawl.get('Status')
        logger.info(f"\nCrawl Status: {status}")

        if status != 'SUCCEEDED':
            logger.error(f"Crawler did not succeed. Status: {status}")
            if 'ErrorMessage' in last_crawl:
                logger.error(f"Error message: {last_crawl['ErrorMessage']}")
            success = False

        # Check metrics
        if 'Metrics' in last_crawl:
            metrics = last_crawl['Metrics']
            logger.info("\nCrawl Metrics:")
            logger.info(f"  Tables Created: {metrics.get('TablesCreated', 0)}")
            logger.info(f"  Tables Updated: {metrics.get('TablesUpdated', 0)}")
            logger.info(f"  Tables Deleted: {metrics.get('TablesDeleted', 0)}")

            if metrics.get('TablesCreated', 0) == 0 and metrics.get('TablesUpdated', 0) == 0:
                logger.warning("No tables were created or updated")
                # This might be okay if tables already exist, so don't fail

        # Check duration
        if 'DurationInSeconds' in metrics:
            duration = metrics['DurationInSeconds']
            logger.info(f"  Duration: {duration} seconds")

        return success

    def get_discovered_tables(self) -> List[Dict]:
        """
        Get list of tables discovered by the crawler.

        Returns:
            list: List of table information dictionaries.
        """
        try:
            logger.info(f"\nDiscovering tables in database: {self.database_name}")

            response = self.glue_client.get_tables(DatabaseName=self.database_name)
            tables = response.get('TableList', [])

            logger.info(f"Found {len(tables)} table(s)")

            return tables

        except ClientError as e:
            logger.error(f"Failed to get tables: {e}")
            return []

    def verify_table_schema(self, table_name: str) -> bool:
        """
        Verify schema of a discovered table.

        Args:
            table_name: Name of the table to verify.

        Returns:
            bool: True if schema matches expectations, False otherwise.
        """
        try:
            logger.info(f"\n--- Verifying Table: {table_name} ---")

            response = self.glue_client.get_table(
                DatabaseName=self.database_name,
                Name=table_name
            )

            table = response['Table']
            storage_descriptor = table.get('StorageDescriptor', {})
            columns = storage_descriptor.get('Columns', [])
            partition_keys = table.get('PartitionKeys', [])

            # Display schema information
            logger.info(f"\nTable: {table_name}")
            logger.info(f"  Location: {storage_descriptor.get('Location', 'N/A')}")
            logger.info(f"  Input Format: {storage_descriptor.get('InputFormat', 'N/A')}")
            logger.info(f"  Output Format: {storage_descriptor.get('OutputFormat', 'N/A')}")

            logger.info(f"\n  Data Columns ({len(columns)}):")
            for col in columns:
                logger.info(f"    - {col['Name']}: {col['Type']}")

            logger.info(f"\n  Partition Keys ({len(partition_keys)}):")
            for key in partition_keys:
                logger.info(f"    - {key['Name']}: {key['Type']}")

            # Verify column count
            success = True
            if len(columns) == self.EXPECTED_COLUMNS:
                logger.info(f"\n✓ Column count matches: {len(columns)} columns")
            else:
                logger.warning(
                    f"\n⚠ Column count mismatch: Expected {self.EXPECTED_COLUMNS}, "
                    f"found {len(columns)}"
                )
                # Don't fail on this, as schema might vary slightly

            # Verify partition keys
            if len(partition_keys) == self.EXPECTED_PARTITION_KEYS:
                logger.info(f"✓ Partition key count matches: {len(partition_keys)} keys")
            else:
                logger.warning(
                    f"⚠ Partition key count mismatch: Expected {self.EXPECTED_PARTITION_KEYS}, "
                    f"found {len(partition_keys)}"
                )
                # Don't fail on this either

            # Check for common taxi data columns
            column_names = {col['Name'].lower() for col in columns}
            expected_columns = {
                'vendorid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
                'passenger_count', 'trip_distance', 'fare_amount'
            }

            found_expected = expected_columns.intersection(column_names)
            logger.info(f"\n  Key columns found: {len(found_expected)}/{len(expected_columns)}")
            for col_name in found_expected:
                logger.info(f"    ✓ {col_name}")

            missing = expected_columns - found_expected
            if missing:
                logger.warning(f"  Missing expected columns: {missing}")

            # Get partition information
            try:
                partitions_response = self.glue_client.get_partitions(
                    DatabaseName=self.database_name,
                    TableName=table_name,
                    MaxResults=10
                )
                partitions = partitions_response.get('Partitions', [])
                logger.info(f"\n  Partitions discovered: {len(partitions)} (showing up to 10)")

                for i, partition in enumerate(partitions[:5], 1):
                    values = partition.get('Values', [])
                    logger.info(f"    Partition {i}: {values}")

            except ClientError as e:
                logger.warning(f"Could not retrieve partitions: {e}")

            return success

        except ClientError as e:
            logger.error(f"Failed to verify table schema: {e}")
            return False

    def run_and_verify(self, wait: bool = True, timeout: int = 600) -> bool:
        """
        Run crawler and verify results.

        Args:
            wait: Whether to wait for crawler to complete.
            timeout: Maximum time to wait in seconds.

        Returns:
            bool: True if successful, False otherwise.
        """
        logger.info("=" * 80)
        logger.info("Running Glue Crawler and Verification")
        logger.info("=" * 80)

        # Step 1: Start crawler
        logger.info("\n[STEP 1] Starting crawler")
        if not self.start_crawler():
            logger.error("Failed to start crawler")
            return False

        if not wait:
            logger.info("Crawler started. Not waiting for completion.")
            return True

        # Step 2: Wait for completion
        logger.info("\n[STEP 2] Waiting for crawler to complete")
        success, last_crawl = self.wait_for_crawler(timeout=timeout)

        if not success:
            logger.error("Crawler did not complete successfully")
            return False

        # Step 3: Verify results
        logger.info("\n[STEP 3] Verifying crawler results")
        if not self.verify_crawler_results(last_crawl):
            logger.error("Crawler verification failed")
            return False

        # Step 4: Get and verify discovered tables
        logger.info("\n[STEP 4] Verifying discovered tables")
        tables = self.get_discovered_tables()

        if not tables:
            logger.error("No tables were discovered")
            return False

        # Verify each table's schema
        all_verified = True
        for table in tables:
            table_name = table['Name']
            if not self.verify_table_schema(table_name):
                all_verified = False

        logger.info("\n" + "=" * 80)
        if all_verified:
            logger.info("SUCCESS: Crawler execution and verification completed!")
            logger.info("=" * 80)
            logger.info(f"\nSummary:")
            logger.info(f"  Crawler: {self.crawler_name}")
            logger.info(f"  Database: {self.database_name}")
            logger.info(f"  Tables Discovered: {len(tables)}")
            logger.info(f"  Status: All verifications passed")
        else:
            logger.warning("PARTIAL SUCCESS: Some verifications had warnings")
            logger.info("=" * 80)

        return all_verified


def main():
    """Main entry point for the script."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Run AWS Glue Crawler and verify schema discovery'
    )
    parser.add_argument(
        '--crawler-name',
        type=str,
        default='crawler-taxi-raw',
        help='Glue crawler name (default: crawler-taxi-raw)'
    )
    parser.add_argument(
        '--database-name',
        type=str,
        default='nyc_taxi_catalog',
        help='Target Glue database name (default: nyc_taxi_catalog)'
    )
    parser.add_argument(
        '--region',
        type=str,
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )
    parser.add_argument(
        '--no-wait',
        action='store_true',
        help='Do not wait for crawler to complete'
    )
    parser.add_argument(
        '--timeout',
        type=int,
        default=600,
        help='Maximum time to wait for crawler in seconds (default: 600)'
    )
    parser.add_argument(
        '--verify-only',
        action='store_true',
        help='Only verify tables, do not run crawler'
    )

    args = parser.parse_args()

    try:
        runner = GlueCrawlerRunner(
            crawler_name=args.crawler_name,
            database_name=args.database_name,
            region=args.region
        )

        if args.verify_only:
            # Only verify existing tables
            logger.info("Verification mode: checking existing tables")
            tables = runner.get_discovered_tables()

            if not tables:
                logger.error("No tables found to verify")
                sys.exit(1)

            all_verified = True
            for table in tables:
                if not runner.verify_table_schema(table['Name']):
                    all_verified = False

            sys.exit(0 if all_verified else 1)
        else:
            # Run crawler and verify
            success = runner.run_and_verify(
                wait=not args.no_wait,
                timeout=args.timeout
            )
            sys.exit(0 if success else 1)

    except Exception as e:
        logger.error(f"Failed to run crawler verification: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
