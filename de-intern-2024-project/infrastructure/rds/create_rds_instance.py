#!/usr/bin/env python3
"""
Create RDS PostgreSQL Instance for NYC Taxi Data
Instance Type: db.t4g.micro
Database: oubt_ptg
Port: 5432
"""

import logging
import sys
import time
import json
from pathlib import Path
from typing import Optional, Dict, Any
import boto3
from botocore.exceptions import ClientError, BotoCoreError

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.de_intern_2024.utils.logger import setup_logger

# Configure logging
logger = setup_logger(__name__)


class RDSInstanceManager:
    """Manage RDS PostgreSQL instance creation and configuration."""

    def __init__(
        self,
        instance_identifier: str = "oubt-taxi-db",
        database_name: str = "oubt_ptg",
        master_username: str = "oubt_admin",
        master_password: Optional[str] = None,
        instance_class: str = "db.t4g.micro",
        allocated_storage: int = 20,
        port: int = 5432,
        publicly_accessible: bool = True,
        region: str = "us-east-1"
    ):
        """
        Initialize RDS instance manager.

        Args:
            instance_identifier: Unique identifier for the RDS instance
            database_name: Name of the initial database
            master_username: Master username for database
            master_password: Master password (will prompt if not provided)
            instance_class: RDS instance class
            allocated_storage: Storage in GB
            port: Database port
            publicly_accessible: Whether instance is publicly accessible
            region: AWS region
        """
        self.instance_identifier = instance_identifier
        self.database_name = database_name
        self.master_username = master_username
        self.master_password = master_password
        self.instance_class = instance_class
        self.allocated_storage = allocated_storage
        self.port = port
        self.publicly_accessible = publicly_accessible
        self.region = region

        # Initialize AWS clients
        try:
            self.rds_client = boto3.client('rds', region_name=region)
            self.ec2_client = boto3.client('ec2', region_name=region)
            logger.info(f"Initialized AWS clients for region: {region}")
        except Exception as e:
            logger.error(f"Failed to initialize AWS clients: {e}")
            raise

        self.instance_info: Optional[Dict[str, Any]] = None

    def check_instance_exists(self) -> bool:
        """
        Check if RDS instance already exists.

        Returns:
            True if instance exists, False otherwise
        """
        try:
            response = self.rds_client.describe_db_instances(
                DBInstanceIdentifier=self.instance_identifier
            )
            if response['DBInstances']:
                logger.info(f"Instance {self.instance_identifier} already exists")
                return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'DBInstanceNotFound':
                return False
            else:
                logger.error(f"Error checking instance existence: {e}")
                raise
        return False

    def create_security_group(self) -> str:
        """
        Create security group for RDS instance with PostgreSQL access.

        Returns:
            Security group ID
        """
        try:
            # Get default VPC
            vpcs = self.ec2_client.describe_vpcs(
                Filters=[{'Name': 'isDefault', 'Values': ['true']}]
            )

            if not vpcs['Vpcs']:
                logger.error("No default VPC found")
                raise ValueError("Default VPC not found")

            vpc_id = vpcs['Vpcs'][0]['VpcId']
            logger.info(f"Using VPC: {vpc_id}")

            # Create security group
            sg_name = f"{self.instance_identifier}-sg"
            sg_description = f"Security group for {self.instance_identifier}"

            try:
                response = self.ec2_client.create_security_group(
                    GroupName=sg_name,
                    Description=sg_description,
                    VpcId=vpc_id
                )
                sg_id = response['GroupId']
                logger.info(f"Created security group: {sg_id}")

                # Add inbound rule for PostgreSQL
                self.ec2_client.authorize_security_group_ingress(
                    GroupId=sg_id,
                    IpPermissions=[
                        {
                            'IpProtocol': 'tcp',
                            'FromPort': self.port,
                            'ToPort': self.port,
                            'IpRanges': [{'CidrIp': '0.0.0.0/0', 'Description': 'PostgreSQL access'}]
                        }
                    ]
                )
                logger.info(f"Added inbound rule for port {self.port}")

                return sg_id

            except ClientError as e:
                if e.response['Error']['Code'] == 'InvalidGroup.Duplicate':
                    # Security group already exists, find it
                    sgs = self.ec2_client.describe_security_groups(
                        Filters=[
                            {'Name': 'group-name', 'Values': [sg_name]},
                            {'Name': 'vpc-id', 'Values': [vpc_id]}
                        ]
                    )
                    if sgs['SecurityGroups']:
                        sg_id = sgs['SecurityGroups'][0]['GroupId']
                        logger.info(f"Using existing security group: {sg_id}")
                        return sg_id
                raise

        except Exception as e:
            logger.error(f"Failed to create security group: {e}")
            raise

    def create_instance(self) -> Dict[str, Any]:
        """
        Create RDS PostgreSQL instance.

        Returns:
            Instance information dictionary

        Raises:
            ValueError: If instance already exists or creation fails
        """
        # Check if instance already exists
        if self.check_instance_exists():
            logger.warning(f"Instance {self.instance_identifier} already exists")
            return self.get_instance_info()

        # Get password if not provided
        if not self.master_password:
            import getpass
            self.master_password = getpass.getpass("Enter master password: ")

        if len(self.master_password) < 8:
            raise ValueError("Password must be at least 8 characters long")

        logger.info(f"Creating RDS instance: {self.instance_identifier}")
        logger.info(f"  Database: {self.database_name}")
        logger.info(f"  Instance class: {self.instance_class}")
        logger.info(f"  Storage: {self.allocated_storage} GB")
        logger.info(f"  Port: {self.port}")

        try:
            # Create security group
            security_group_id = self.create_security_group()

            # Create RDS instance
            response = self.rds_client.create_db_instance(
                DBInstanceIdentifier=self.instance_identifier,
                DBName=self.database_name,
                DBInstanceClass=self.instance_class,
                Engine='postgres',
                EngineVersion='15.4',  # Latest PostgreSQL 15 version
                MasterUsername=self.master_username,
                MasterUserPassword=self.master_password,
                AllocatedStorage=self.allocated_storage,
                Port=self.port,
                VpcSecurityGroupIds=[security_group_id],
                PubliclyAccessible=self.publicly_accessible,
                BackupRetentionPeriod=7,  # 7 days backup retention
                StorageType='gp3',  # General Purpose SSD
                StorageEncrypted=True,
                EnableCloudwatchLogsExports=['postgresql'],
                DeletionProtection=False,  # For testing, can be changed to True for production
                Tags=[
                    {'Key': 'Name', 'Value': self.instance_identifier},
                    {'Key': 'Project', 'Value': 'NYC-Taxi-Data'},
                    {'Key': 'Environment', 'Value': 'development'}
                ]
            )

            logger.info("RDS instance creation initiated")
            logger.info("Waiting for instance to become available...")

            return response['DBInstance']

        except ClientError as e:
            logger.error(f"Failed to create RDS instance: {e}")
            raise

    def wait_for_instance_available(self, timeout: int = 900) -> bool:
        """
        Wait for RDS instance to become available.

        Args:
            timeout: Maximum time to wait in seconds (default: 15 minutes)

        Returns:
            True if instance is available, False if timeout

        Raises:
            Exception: If instance creation fails
        """
        logger.info("Waiting for instance to become available (this may take 10-15 minutes)...")

        start_time = time.time()
        waiter = self.rds_client.get_waiter('db_instance_available')

        try:
            waiter.wait(
                DBInstanceIdentifier=self.instance_identifier,
                WaiterConfig={
                    'Delay': 30,  # Check every 30 seconds
                    'MaxAttempts': timeout // 30
                }
            )

            elapsed_time = time.time() - start_time
            logger.info(f"Instance is now available (took {elapsed_time:.0f} seconds)")

            # Get instance information
            self.instance_info = self.get_instance_info()
            return True

        except Exception as e:
            logger.error(f"Error waiting for instance: {e}")
            raise

    def get_instance_info(self) -> Dict[str, Any]:
        """
        Get current instance information.

        Returns:
            Dictionary with instance details
        """
        try:
            response = self.rds_client.describe_db_instances(
                DBInstanceIdentifier=self.instance_identifier
            )

            if not response['DBInstances']:
                raise ValueError(f"Instance {self.instance_identifier} not found")

            instance = response['DBInstances'][0]

            info = {
                'identifier': instance['DBInstanceIdentifier'],
                'status': instance['DBInstanceStatus'],
                'endpoint': instance.get('Endpoint', {}).get('Address', 'N/A'),
                'port': instance.get('Endpoint', {}).get('Port', self.port),
                'database': instance['DBName'],
                'engine': instance['Engine'],
                'engine_version': instance['EngineVersion'],
                'instance_class': instance['DBInstanceClass'],
                'storage': instance['AllocatedStorage'],
                'publicly_accessible': instance['PubliclyAccessible'],
                'master_username': instance['MasterUsername']
            }

            return info

        except ClientError as e:
            logger.error(f"Failed to get instance info: {e}")
            raise

    def test_connection(self) -> bool:
        """
        Test connection to RDS instance using psycopg2.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            import psycopg2

            if not self.instance_info:
                self.instance_info = self.get_instance_info()

            endpoint = self.instance_info['endpoint']

            if endpoint == 'N/A':
                logger.error("Instance endpoint not available yet")
                return False

            logger.info(f"Testing connection to {endpoint}:{self.port}")

            conn = psycopg2.connect(
                host=endpoint,
                port=self.port,
                database=self.database_name,
                user=self.master_username,
                password=self.master_password,
                connect_timeout=10
            )

            # Test query
            cur = conn.cursor()
            cur.execute("SELECT version();")
            version = cur.fetchone()
            logger.info(f"PostgreSQL version: {version[0]}")

            cur.close()
            conn.close()

            logger.info("Connection test successful!")
            return True

        except ImportError:
            logger.warning("psycopg2 not installed. Skipping connection test.")
            logger.info("Install with: pip install psycopg2-binary")
            return False

        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def delete_instance(self, skip_final_snapshot: bool = True) -> bool:
        """
        Delete RDS instance (rollback).

        Args:
            skip_final_snapshot: Whether to skip final snapshot

        Returns:
            True if deletion initiated successfully
        """
        try:
            logger.warning(f"Deleting RDS instance: {self.instance_identifier}")

            self.rds_client.delete_db_instance(
                DBInstanceIdentifier=self.instance_identifier,
                SkipFinalSnapshot=skip_final_snapshot,
                DeleteAutomatedBackups=True
            )

            logger.info("RDS instance deletion initiated")
            return True

        except ClientError as e:
            logger.error(f"Failed to delete instance: {e}")
            return False

    def save_connection_info(self, output_file: str = None):
        """
        Save connection information to file.

        Args:
            output_file: Path to output file (default: config/rds_connection.json)
        """
        if not self.instance_info:
            self.instance_info = self.get_instance_info()

        if output_file is None:
            config_dir = Path(__file__).parent.parent.parent / 'config'
            config_dir.mkdir(exist_ok=True)
            output_file = config_dir / 'rds_connection.json'
        else:
            output_file = Path(output_file)
            output_file.parent.mkdir(parents=True, exist_ok=True)

        connection_info = {
            'host': self.instance_info['endpoint'],
            'port': self.instance_info['port'],
            'database': self.instance_info['database'],
            'username': self.instance_info['master_username'],
            'instance_identifier': self.instance_info['identifier'],
            'engine': self.instance_info['engine'],
            'engine_version': self.instance_info['engine_version']
        }

        with open(output_file, 'w') as f:
            json.dump(connection_info, f, indent=2)

        logger.info(f"Connection info saved to: {output_file}")

    def print_summary(self):
        """Print summary of RDS instance."""
        if not self.instance_info:
            self.instance_info = self.get_instance_info()

        print("\n" + "="*80)
        print("RDS INSTANCE SUMMARY")
        print("="*80)
        print(f"Identifier:        {self.instance_info['identifier']}")
        print(f"Status:            {self.instance_info['status']}")
        print(f"Endpoint:          {self.instance_info['endpoint']}")
        print(f"Port:              {self.instance_info['port']}")
        print(f"Database:          {self.instance_info['database']}")
        print(f"Engine:            {self.instance_info['engine']} {self.instance_info['engine_version']}")
        print(f"Instance Class:    {self.instance_info['instance_class']}")
        print(f"Storage:           {self.instance_info['storage']} GB")
        print(f"Publicly Accessible: {self.instance_info['publicly_accessible']}")
        print(f"Master Username:   {self.instance_info['master_username']}")
        print("="*80)
        print("\nConnection String:")
        print(f"psql -h {self.instance_info['endpoint']} -p {self.instance_info['port']} "
              f"-U {self.instance_info['master_username']} -d {self.instance_info['database']}")
        print("\nPython psycopg2:")
        print(f"import psycopg2")
        print(f"conn = psycopg2.connect(")
        print(f"    host='{self.instance_info['endpoint']}',")
        print(f"    port={self.instance_info['port']},")
        print(f"    database='{self.instance_info['database']}',")
        print(f"    user='{self.instance_info['master_username']}',")
        print(f"    password='YOUR_PASSWORD'")
        print(f")")
        print("="*80 + "\n")


def main():
    """Main execution function."""
    import argparse

    parser = argparse.ArgumentParser(description='Create RDS PostgreSQL instance for NYC Taxi data')
    parser.add_argument('--instance-id', default='oubt-taxi-db',
                       help='RDS instance identifier')
    parser.add_argument('--database', default='oubt_ptg',
                       help='Database name')
    parser.add_argument('--username', default='oubt_admin',
                       help='Master username')
    parser.add_argument('--password', help='Master password (will prompt if not provided)')
    parser.add_argument('--instance-class', default='db.t4g.micro',
                       help='RDS instance class')
    parser.add_argument('--storage', type=int, default=20,
                       help='Allocated storage in GB')
    parser.add_argument('--port', type=int, default=5432,
                       help='Database port')
    parser.add_argument('--region', default='us-east-1',
                       help='AWS region')
    parser.add_argument('--delete', action='store_true',
                       help='Delete the instance instead of creating')
    parser.add_argument('--test-only', action='store_true',
                       help='Only test connection to existing instance')

    args = parser.parse_args()

    try:
        # Initialize manager
        manager = RDSInstanceManager(
            instance_identifier=args.instance_id,
            database_name=args.database,
            master_username=args.username,
            master_password=args.password,
            instance_class=args.instance_class,
            allocated_storage=args.storage,
            port=args.port,
            region=args.region
        )

        if args.delete:
            # Delete instance
            logger.info("Deleting RDS instance...")
            if manager.check_instance_exists():
                if manager.delete_instance():
                    logger.info("Instance deletion initiated successfully")
                else:
                    logger.error("Failed to delete instance")
                    sys.exit(1)
            else:
                logger.info("Instance does not exist")
            return

        if args.test_only:
            # Test connection only
            logger.info("Testing connection to existing instance...")
            if manager.test_connection():
                logger.info("Connection test passed")
                manager.print_summary()
            else:
                logger.error("Connection test failed")
                sys.exit(1)
            return

        # Create instance
        logger.info("Starting RDS instance creation...")

        # Create instance
        instance = manager.create_instance()

        # Wait for instance to be available
        if manager.wait_for_instance_available():
            logger.info("RDS instance is ready!")

            # Save connection info
            manager.save_connection_info()

            # Print summary
            manager.print_summary()

            # Test connection
            logger.info("Testing database connection...")
            if manager.test_connection():
                logger.info("Setup completed successfully!")
            else:
                logger.warning("Instance created but connection test failed")
                logger.info("You may need to configure security groups or wait a bit longer")

        else:
            logger.error("Timeout waiting for instance to become available")
            logger.info("Check AWS console for instance status")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.warning("\nOperation cancelled by user")
        logger.info("Note: If instance creation was started, it may still be in progress")
        logger.info("Check AWS console or use --delete flag to remove the instance")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Failed to create RDS instance: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
