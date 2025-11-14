"""
Configure AWS Glue Crawler for NYC taxi data schema discovery.

This script creates and configures a Glue crawler to automatically
discover the schema of NYC taxi data stored in S3. The crawler will:
- Scan the raw taxi data in S3
- Detect schema (columns and data types)
- Identify partitions
- Create/update Glue catalog tables
"""

import sys
import json
from typing import Dict, Optional
import boto3
from botocore.exceptions import ClientError

# Add parent directory to path for imports
sys.path.append('/home/user/OUBT-CC/de-intern-2024-project/src')
from de_intern_2024.utils.logger import get_logger
from de_intern_2024.utils.aws_helpers import get_boto3_client

logger = get_logger(__name__)


class GlueCrawlerConfigurator:
    """Manages configuration and creation of AWS Glue crawler."""

    def __init__(
        self,
        crawler_name: str = "crawler-taxi-raw",
        database_name: str = "nyc_taxi_catalog",
        role_name: str = "AWSGlueServiceRole-intern",
        region: str = 'us-east-1'
    ):
        """
        Initialize Glue Crawler Configurator.

        Args:
            crawler_name: Name for the Glue crawler.
            database_name: Target Glue catalog database.
            role_name: IAM role name for the crawler.
            region: AWS region.
        """
        self.crawler_name = crawler_name
        self.database_name = database_name
        self.role_name = role_name
        self.region = region
        self.glue_client = get_boto3_client('glue', region=region)
        self.sts_client = get_boto3_client('sts', region=region)
        self.iam_client = get_boto3_client('iam', region=region)

        # Get account ID and construct S3 path
        self.account_id = self.sts_client.get_caller_identity()['Account']
        self.bucket_name = f"{self.account_id}-oubt-datalake"
        self.s3_target_path = f"s3://{self.bucket_name}/raw/taxi/"

        logger.info(f"Initialized GlueCrawlerConfigurator for crawler: {self.crawler_name}")

    def get_role_arn(self) -> Optional[str]:
        """
        Get ARN for the IAM role.

        Returns:
            str: Role ARN, None if role doesn't exist.
        """
        try:
            response = self.iam_client.get_role(RoleName=self.role_name)
            role_arn = response['Role']['Arn']
            logger.info(f"Found IAM role: {role_arn}")
            return role_arn

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'NoSuchEntity':
                logger.error(f"IAM role {self.role_name} does not exist. Please create it first.")
                return None
            else:
                logger.error(f"Failed to get role ARN: {e}")
                return None

    def verify_database_exists(self) -> bool:
        """
        Verify that the target database exists.

        Returns:
            bool: True if database exists, False otherwise.
        """
        try:
            self.glue_client.get_database(Name=self.database_name)
            logger.info(f"Found Glue database: {self.database_name}")
            return True

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'EntityNotFoundException':
                logger.error(f"Database {self.database_name} does not exist. Please create it first.")
                return False
            else:
                logger.error(f"Failed to verify database: {e}")
                return False

    def create_crawler(self) -> bool:
        """
        Create and configure Glue crawler.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info(f"Creating Glue crawler: {self.crawler_name}")

            # Get role ARN
            role_arn = self.get_role_arn()
            if not role_arn:
                return False

            # Verify database exists
            if not self.verify_database_exists():
                return False

            # Create crawler configuration
            crawler_config = {
                'Name': self.crawler_name,
                'Role': role_arn,
                'DatabaseName': self.database_name,
                'Description': 'Crawler for NYC taxi raw data in S3',
                'Targets': {
                    'S3Targets': [
                        {
                            'Path': self.s3_target_path
                        }
                    ]
                },
                'SchemaChangePolicy': {
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'LOG'
                },
                'RecrawlPolicy': {
                    'RecrawlBehavior': 'CRAWL_EVERYTHING'
                },
                'LineageConfiguration': {
                    'CrawlerLineageSettings': 'ENABLE'
                },
                'Configuration': json.dumps({
                    'Version': 1.0,
                    'CrawlerOutput': {
                        'Partitions': {'AddOrUpdateBehavior': 'InheritFromTable'}
                    },
                    'Grouping': {
                        'TableGroupingPolicy': 'CombineCompatibleSchemas'
                    }
                }),
                'Tags': {
                    'Project': 'OUBT-DataEngineering',
                    'Environment': 'Production',
                    'Purpose': 'SchemaDiscovery',
                    'ManagedBy': 'Automation',
                    'DataZone': 'Raw'
                }
            }

            self.glue_client.create_crawler(**crawler_config)

            logger.info(f"Successfully created crawler: {self.crawler_name}")
            logger.info(f"  Target: {self.s3_target_path}")
            logger.info(f"  Database: {self.database_name}")
            logger.info(f"  Schedule: On-demand")
            return True

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'AlreadyExistsException':
                logger.warning(f"Crawler {self.crawler_name} already exists")
                return True
            else:
                logger.error(f"Failed to create crawler: {e}")
                return False

    def update_crawler(self) -> bool:
        """
        Update existing crawler configuration.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info(f"Updating Glue crawler: {self.crawler_name}")

            # Get role ARN
            role_arn = self.get_role_arn()
            if not role_arn:
                return False

            # Update crawler configuration
            crawler_config = {
                'Name': self.crawler_name,
                'Role': role_arn,
                'DatabaseName': self.database_name,
                'Description': 'Crawler for NYC taxi raw data in S3',
                'Targets': {
                    'S3Targets': [
                        {
                            'Path': self.s3_target_path
                        }
                    ]
                },
                'SchemaChangePolicy': {
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'LOG'
                },
                'RecrawlPolicy': {
                    'RecrawlBehavior': 'CRAWL_EVERYTHING'
                },
                'LineageConfiguration': {
                    'CrawlerLineageSettings': 'ENABLE'
                }
            }

            self.glue_client.update_crawler(**crawler_config)

            logger.info(f"Successfully updated crawler: {self.crawler_name}")
            return True

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'EntityNotFoundException':
                logger.error(f"Crawler {self.crawler_name} does not exist")
                return False
            else:
                logger.error(f"Failed to update crawler: {e}")
                return False

    def get_crawler_info(self) -> Optional[Dict]:
        """
        Get information about the crawler.

        Returns:
            dict: Crawler information, None if crawler doesn't exist.
        """
        try:
            response = self.glue_client.get_crawler(Name=self.crawler_name)
            crawler = response['Crawler']

            return {
                'name': crawler['Name'],
                'state': crawler['State'],
                'database': crawler.get('DatabaseName', 'N/A'),
                'role': crawler.get('Role', 'N/A'),
                'targets': crawler.get('Targets', {}),
                'last_crawl': crawler.get('LastCrawl', {}),
                'crawl_elapsed_time': crawler.get('CrawlElapsedTime', 0),
                'creation_time': crawler.get('CreationTime', 'N/A'),
                'last_updated': crawler.get('LastUpdated', 'N/A')
            }

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'EntityNotFoundException':
                logger.warning(f"Crawler {self.crawler_name} does not exist")
                return None
            else:
                logger.error(f"Failed to get crawler info: {e}")
                return None

    def delete_crawler(self) -> bool:
        """
        Delete Glue crawler.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info(f"Deleting Glue crawler: {self.crawler_name}")

            self.glue_client.delete_crawler(Name=self.crawler_name)

            logger.info(f"Successfully deleted crawler: {self.crawler_name}")
            return True

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'EntityNotFoundException':
                logger.warning(f"Crawler {self.crawler_name} does not exist")
                return True
            else:
                logger.error(f"Failed to delete crawler: {e}")
                return False

    def setup_crawler(self) -> bool:
        """
        Complete setup of Glue crawler.

        Returns:
            bool: True if successful, False otherwise.
        """
        logger.info("=" * 80)
        logger.info("Starting Glue Crawler Configuration")
        logger.info("=" * 80)

        logger.info("\n[STEP] Create Glue crawler")
        success = self.create_crawler()

        logger.info("\n" + "=" * 80)
        if success:
            logger.info("SUCCESS: Glue crawler configuration completed successfully!")
            logger.info("=" * 80)

            # Print crawler information
            info = self.get_crawler_info()
            if info:
                logger.info("\nCrawler Configuration:")
                logger.info(f"  Name: {info['name']}")
                logger.info(f"  State: {info['state']}")
                logger.info(f"  Database: {info['database']}")
                logger.info(f"  Role: {info['role']}")

                if info['targets'].get('S3Targets'):
                    s3_targets = info['targets']['S3Targets']
                    logger.info(f"  S3 Targets:")
                    for target in s3_targets:
                        logger.info(f"    - {target.get('Path', 'N/A')}")

                if info.get('creation_time') != 'N/A':
                    logger.info(f"  Created: {info['creation_time']}")

                last_crawl = info.get('last_crawl', {})
                if last_crawl:
                    logger.info(f"  Last Crawl Status: {last_crawl.get('Status', 'Never run')}")
                    if 'StartTime' in last_crawl:
                        logger.info(f"  Last Crawl Time: {last_crawl['StartTime']}")

                logger.info("\nNext Steps:")
                logger.info(f"  1. Run the crawler using: python src/glue/run_crawler_verification.py")
                logger.info(f"  2. Or via AWS CLI: aws glue start-crawler --name {self.crawler_name}")

        else:
            logger.error("FAILED: Crawler configuration failed")
            logger.info("=" * 80)

        return success


def main():
    """Main entry point for the script."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Configure AWS Glue Crawler for schema discovery'
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
        '--role-name',
        type=str,
        default='AWSGlueServiceRole-intern',
        help='IAM role name (default: AWSGlueServiceRole-intern)'
    )
    parser.add_argument(
        '--region',
        type=str,
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )
    parser.add_argument(
        '--info-only',
        action='store_true',
        help='Only display crawler information, do not create'
    )
    parser.add_argument(
        '--delete',
        action='store_true',
        help='Delete the crawler'
    )
    parser.add_argument(
        '--update',
        action='store_true',
        help='Update existing crawler'
    )

    args = parser.parse_args()

    try:
        configurator = GlueCrawlerConfigurator(
            crawler_name=args.crawler_name,
            database_name=args.database_name,
            role_name=args.role_name,
            region=args.region
        )

        if args.delete:
            success = configurator.delete_crawler()
            sys.exit(0 if success else 1)
        elif args.update:
            success = configurator.update_crawler()
            sys.exit(0 if success else 1)
        elif args.info_only:
            info = configurator.get_crawler_info()
            if info:
                print(json.dumps(info, indent=2, default=str))
            else:
                logger.error("Crawler does not exist")
                sys.exit(1)
        else:
            success = configurator.setup_crawler()
            sys.exit(0 if success else 1)

    except Exception as e:
        logger.error(f"Failed to configure Glue crawler: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
