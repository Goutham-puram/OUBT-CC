"""
Create AWS Glue Data Catalog Database for NYC taxi data.

This script creates a Glue catalog database to store metadata
for the NYC taxi data lake. The database serves as a central
repository for table schemas, partitions, and other metadata.
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


class GlueDatabaseCreator:
    """Manages creation and configuration of AWS Glue catalog database."""

    def __init__(
        self,
        database_name: str = "nyc_taxi_catalog",
        description: str = "NYC taxi data lake metadata repository",
        region: str = 'us-east-1'
    ):
        """
        Initialize Glue Database Creator.

        Args:
            database_name: Name for the Glue catalog database.
            description: Description for the database.
            region: AWS region.
        """
        self.database_name = database_name
        self.description = description
        self.region = region
        self.glue_client = get_boto3_client('glue', region=region)
        self.sts_client = get_boto3_client('sts', region=region)

        # Get account ID
        self.account_id = self.sts_client.get_caller_identity()['Account']

        logger.info(f"Initialized GlueDatabaseCreator for database: {self.database_name}")

    def create_database(self) -> bool:
        """
        Create Glue catalog database.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info(f"Creating Glue catalog database: {self.database_name}")

            database_input = {
                'Name': self.database_name,
                'Description': self.description,
                'LocationUri': f's3://{self.account_id}-oubt-datalake/',
                'Parameters': {
                    'classification': 'parquet',
                    'project': 'OUBT-DataEngineering',
                    'environment': 'production'
                }
            }

            self.glue_client.create_database(
                DatabaseInput=database_input
            )

            logger.info(f"Successfully created database: {self.database_name}")
            return True

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'AlreadyExistsException':
                logger.warning(f"Database {self.database_name} already exists")
                return True
            else:
                logger.error(f"Failed to create database: {e}")
                return False

    def get_database_info(self) -> Optional[Dict]:
        """
        Get information about the database.

        Returns:
            dict: Database information, None if database doesn't exist.
        """
        try:
            response = self.glue_client.get_database(Name=self.database_name)
            database = response['Database']

            # Get table count
            try:
                tables_response = self.glue_client.get_tables(DatabaseName=self.database_name)
                table_count = len(tables_response.get('TableList', []))
                table_names = [t['Name'] for t in tables_response.get('TableList', [])]
            except ClientError:
                table_count = 0
                table_names = []

            return {
                'name': database['Name'],
                'description': database.get('Description', 'N/A'),
                'location_uri': database.get('LocationUri', 'N/A'),
                'create_time': database['CreateTime'].isoformat() if 'CreateTime' in database else 'N/A',
                'parameters': database.get('Parameters', {}),
                'table_count': table_count,
                'tables': table_names
            }

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'EntityNotFoundException':
                logger.warning(f"Database {self.database_name} does not exist")
                return None
            else:
                logger.error(f"Failed to get database info: {e}")
                return None

    def delete_database(self) -> bool:
        """
        Delete Glue catalog database.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info(f"Deleting Glue catalog database: {self.database_name}")

            self.glue_client.delete_database(Name=self.database_name)

            logger.info(f"Successfully deleted database: {self.database_name}")
            return True

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'EntityNotFoundException':
                logger.warning(f"Database {self.database_name} does not exist")
                return True
            else:
                logger.error(f"Failed to delete database: {e}")
                return False

    def update_database(self, new_description: Optional[str] = None) -> bool:
        """
        Update database description.

        Args:
            new_description: New description for the database.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info(f"Updating Glue catalog database: {self.database_name}")

            # Get current database info
            response = self.glue_client.get_database(Name=self.database_name)
            database = response['Database']

            # Update description
            database_input = {
                'Name': database['Name'],
                'Description': new_description or database.get('Description', ''),
            }

            if 'LocationUri' in database:
                database_input['LocationUri'] = database['LocationUri']

            if 'Parameters' in database:
                database_input['Parameters'] = database['Parameters']

            self.glue_client.update_database(
                Name=self.database_name,
                DatabaseInput=database_input
            )

            logger.info(f"Successfully updated database: {self.database_name}")
            return True

        except ClientError as e:
            logger.error(f"Failed to update database: {e}")
            return False

    def setup_database(self) -> bool:
        """
        Complete setup of Glue catalog database.

        Returns:
            bool: True if successful, False otherwise.
        """
        logger.info("=" * 80)
        logger.info("Starting Glue Catalog Database Setup")
        logger.info("=" * 80)

        logger.info("\n[STEP] Create Glue catalog database")
        success = self.create_database()

        logger.info("\n" + "=" * 80)
        if success:
            logger.info("SUCCESS: Glue catalog database setup completed successfully!")
            logger.info("=" * 80)

            # Print database information
            info = self.get_database_info()
            if info:
                logger.info("\nDatabase Configuration:")
                logger.info(f"  Name: {info['name']}")
                logger.info(f"  Description: {info['description']}")
                logger.info(f"  Location URI: {info['location_uri']}")
                logger.info(f"  Created: {info['create_time']}")
                logger.info(f"  Table Count: {info['table_count']}")
                if info['tables']:
                    logger.info(f"  Tables: {', '.join(info['tables'])}")
                if info['parameters']:
                    logger.info(f"  Parameters:")
                    for key, value in info['parameters'].items():
                        logger.info(f"    {key}: {value}")

        else:
            logger.error("FAILED: Database setup failed")
            logger.info("=" * 80)

        return success


def main():
    """Main entry point for the script."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Create AWS Glue Data Catalog Database'
    )
    parser.add_argument(
        '--database-name',
        type=str,
        default='nyc_taxi_catalog',
        help='Glue catalog database name (default: nyc_taxi_catalog)'
    )
    parser.add_argument(
        '--description',
        type=str,
        default='NYC taxi data lake metadata repository',
        help='Database description'
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
        help='Only display database information, do not create'
    )
    parser.add_argument(
        '--delete',
        action='store_true',
        help='Delete the database'
    )

    args = parser.parse_args()

    try:
        creator = GlueDatabaseCreator(
            database_name=args.database_name,
            description=args.description,
            region=args.region
        )

        if args.delete:
            success = creator.delete_database()
            sys.exit(0 if success else 1)
        elif args.info_only:
            info = creator.get_database_info()
            if info:
                print(json.dumps(info, indent=2, default=str))
            else:
                logger.error("Database does not exist")
                sys.exit(1)
        else:
            success = creator.setup_database()
            sys.exit(0 if success else 1)

    except Exception as e:
        logger.error(f"Failed to setup Glue database: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
