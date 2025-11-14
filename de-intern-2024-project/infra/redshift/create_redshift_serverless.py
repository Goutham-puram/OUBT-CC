#!/usr/bin/env python3
"""
Create Redshift Serverless Data Warehouse for NYC Taxi Analytics

This script creates a Redshift Serverless namespace and workgroup configured for
data analytics with external schema integration via AWS Glue Data Catalog.

Resources Created:
- Redshift Serverless Namespace: oubt-analytics
- Redshift Serverless Workgroup: intern-workgroup
- IAM Role for Redshift Spectrum (Glue Data Catalog and S3 access)
- CloudWatch billing alarms for cost monitoring
"""

import sys
import json
import time
from pathlib import Path
from typing import Optional, Dict, Any, List
import boto3
from botocore.exceptions import ClientError

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.de_intern_2024.utils.logger import setup_logger

# Configure logging
logger = setup_logger(__name__)


class RedshiftServerlessManager:
    """Manage Redshift Serverless namespace and workgroup creation."""

    def __init__(
        self,
        namespace_name: str = "oubt-analytics",
        workgroup_name: str = "intern-workgroup",
        database_name: str = "analytics_db",
        admin_username: str = "admin",
        admin_password: Optional[str] = None,
        base_capacity: int = 32,
        region: str = "us-east-1"
    ):
        """
        Initialize Redshift Serverless manager.

        Args:
            namespace_name: Name for the Redshift namespace
            workgroup_name: Name for the Redshift workgroup
            database_name: Name of the default database
            admin_username: Admin username
            admin_password: Admin password (will prompt if not provided)
            base_capacity: Base RPU capacity (minimum 32)
            region: AWS region
        """
        self.namespace_name = namespace_name
        self.workgroup_name = workgroup_name
        self.database_name = database_name
        self.admin_username = admin_username
        self.admin_password = admin_password
        self.base_capacity = max(32, base_capacity)  # Ensure minimum 32 RPU
        self.region = region

        # Initialize AWS clients
        try:
            self.redshift_serverless = boto3.client('redshift-serverless', region_name=region)
            self.iam_client = boto3.client('iam', region_name=region)
            self.sts_client = boto3.client('sts', region_name=region)
            self.cloudwatch = boto3.client('cloudwatch', region_name=region)
            self.sns = boto3.client('sns', region_name=region)
            logger.info(f"Initialized AWS clients for region: {region}")

            # Get account ID
            self.account_id = self.sts_client.get_caller_identity()['Account']
            self.bucket_name = f"{self.account_id}-oubt-datalake"

        except Exception as e:
            logger.error(f"Failed to initialize AWS clients: {e}")
            raise

        self.namespace_info: Optional[Dict[str, Any]] = None
        self.workgroup_info: Optional[Dict[str, Any]] = None
        self.spectrum_role_arn: Optional[str] = None

    def create_spectrum_role(self) -> str:
        """
        Create IAM role for Redshift Spectrum to access Glue Data Catalog and S3.

        Returns:
            Role ARN
        """
        role_name = f"RedshiftSpectrumRole-{self.namespace_name}"

        try:
            logger.info(f"Creating Spectrum IAM role: {role_name}")

            # Trust policy for Redshift
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "redshift.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }

            # Create role
            try:
                response = self.iam_client.create_role(
                    RoleName=role_name,
                    AssumeRolePolicyDocument=json.dumps(trust_policy),
                    Description=f"IAM role for Redshift Spectrum access to Glue Data Catalog and S3",
                    Tags=[
                        {'Key': 'Project', 'Value': 'OUBT-DataEngineering'},
                        {'Key': 'Environment', 'Value': 'Production'},
                        {'Key': 'Purpose', 'Value': 'RedshiftSpectrum'},
                        {'Key': 'ManagedBy', 'Value': 'Automation'}
                    ]
                )
                role_arn = response['Role']['Arn']
                logger.info(f"Created role: {role_arn}")
            except ClientError as e:
                if e.response['Error']['Code'] == 'EntityAlreadyExists':
                    logger.warning(f"Role {role_name} already exists, retrieving ARN")
                    response = self.iam_client.get_role(RoleName=role_name)
                    role_arn = response['Role']['Arn']
                else:
                    raise

            # Attach AWS managed policy for Glue access
            glue_policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
            try:
                self.iam_client.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=glue_policy_arn
                )
                logger.info("Attached Glue access policy")
            except ClientError as e:
                if e.response['Error']['Code'] != 'EntityAlreadyExists':
                    raise

            # Create inline S3 access policy
            s3_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:ListBucket",
                            "s3:GetBucketLocation"
                        ],
                        "Resource": [
                            f"arn:aws:s3:::{self.bucket_name}",
                            f"arn:aws:s3:::{self.bucket_name}/*"
                        ]
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "glue:GetDatabase",
                            "glue:GetDatabases",
                            "glue:GetTable",
                            "glue:GetTables",
                            "glue:GetPartition",
                            "glue:GetPartitions"
                        ],
                        "Resource": "*"
                    }
                ]
            }

            self.iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName=f"{role_name}-S3GlueAccess",
                PolicyDocument=json.dumps(s3_policy)
            )
            logger.info("Created inline S3 and Glue access policy")

            self.spectrum_role_arn = role_arn
            return role_arn

        except Exception as e:
            logger.error(f"Failed to create Spectrum role: {e}")
            raise

    def check_namespace_exists(self) -> bool:
        """
        Check if Redshift Serverless namespace exists.

        Returns:
            True if namespace exists, False otherwise
        """
        try:
            response = self.redshift_serverless.get_namespace(
                namespaceName=self.namespace_name
            )
            if response['namespace']:
                logger.info(f"Namespace {self.namespace_name} already exists")
                return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return False
            else:
                logger.error(f"Error checking namespace existence: {e}")
                raise
        return False

    def create_namespace(self) -> Dict[str, Any]:
        """
        Create Redshift Serverless namespace.

        Returns:
            Namespace information
        """
        if self.check_namespace_exists():
            logger.warning(f"Namespace {self.namespace_name} already exists")
            return self.get_namespace_info()

        # Get password if not provided
        if not self.admin_password:
            import getpass
            self.admin_password = getpass.getpass("Enter admin password (min 8 chars): ")

        if len(self.admin_password) < 8:
            raise ValueError("Password must be at least 8 characters long")

        logger.info(f"Creating Redshift Serverless namespace: {self.namespace_name}")
        logger.info(f"  Database: {self.database_name}")
        logger.info(f"  Admin username: {self.admin_username}")

        try:
            # Create Spectrum role first
            spectrum_role_arn = self.create_spectrum_role()

            # Create namespace
            response = self.redshift_serverless.create_namespace(
                namespaceName=self.namespace_name,
                dbName=self.database_name,
                adminUsername=self.admin_username,
                adminUserPassword=self.admin_password,
                iamRoles=[spectrum_role_arn],
                tags=[
                    {'key': 'Name', 'value': self.namespace_name},
                    {'key': 'Project', 'value': 'NYC-Taxi-Analytics'},
                    {'key': 'Environment', 'value': 'production'}
                ]
            )

            logger.info("Namespace creation initiated")
            self.namespace_info = response['namespace']

            # Wait for namespace to be available
            self.wait_for_namespace_available()

            return self.namespace_info

        except ClientError as e:
            logger.error(f"Failed to create namespace: {e}")
            raise

    def wait_for_namespace_available(self, timeout: int = 600) -> bool:
        """
        Wait for namespace to become available.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if available, False if timeout
        """
        logger.info("Waiting for namespace to become available...")
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                response = self.redshift_serverless.get_namespace(
                    namespaceName=self.namespace_name
                )
                status = response['namespace']['status']

                if status == 'AVAILABLE':
                    logger.info("Namespace is now available")
                    self.namespace_info = response['namespace']
                    return True
                elif status in ['DELETING', 'DELETED']:
                    raise Exception(f"Namespace in invalid state: {status}")

                logger.info(f"Namespace status: {status}, waiting...")
                time.sleep(15)

            except ClientError as e:
                logger.error(f"Error checking namespace status: {e}")
                raise

        logger.error("Timeout waiting for namespace to become available")
        return False

    def check_workgroup_exists(self) -> bool:
        """
        Check if workgroup exists.

        Returns:
            True if exists, False otherwise
        """
        try:
            response = self.redshift_serverless.get_workgroup(
                workgroupName=self.workgroup_name
            )
            if response['workgroup']:
                logger.info(f"Workgroup {self.workgroup_name} already exists")
                return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return False
            else:
                logger.error(f"Error checking workgroup existence: {e}")
                raise
        return False

    def create_workgroup(self) -> Dict[str, Any]:
        """
        Create Redshift Serverless workgroup.

        Returns:
            Workgroup information
        """
        if self.check_workgroup_exists():
            logger.warning(f"Workgroup {self.workgroup_name} already exists")
            return self.get_workgroup_info()

        logger.info(f"Creating Redshift Serverless workgroup: {self.workgroup_name}")
        logger.info(f"  Base capacity: {self.base_capacity} RPU")

        try:
            response = self.redshift_serverless.create_workgroup(
                workgroupName=self.workgroup_name,
                namespaceName=self.namespace_name,
                baseCapacity=self.base_capacity,
                publiclyAccessible=True,
                tags=[
                    {'key': 'Name', 'value': self.workgroup_name},
                    {'key': 'Project', 'value': 'NYC-Taxi-Analytics'},
                    {'key': 'Environment', 'value': 'production'}
                ]
            )

            logger.info("Workgroup creation initiated")
            self.workgroup_info = response['workgroup']

            # Wait for workgroup to be available
            self.wait_for_workgroup_available()

            return self.workgroup_info

        except ClientError as e:
            logger.error(f"Failed to create workgroup: {e}")
            raise

    def wait_for_workgroup_available(self, timeout: int = 600) -> bool:
        """
        Wait for workgroup to become available.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if available, False if timeout
        """
        logger.info("Waiting for workgroup to become available...")
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                response = self.redshift_serverless.get_workgroup(
                    workgroupName=self.workgroup_name
                )
                status = response['workgroup']['status']

                if status == 'AVAILABLE':
                    logger.info("Workgroup is now available")
                    self.workgroup_info = response['workgroup']
                    return True
                elif status in ['DELETING', 'DELETED']:
                    raise Exception(f"Workgroup in invalid state: {status}")

                logger.info(f"Workgroup status: {status}, waiting...")
                time.sleep(15)

            except ClientError as e:
                logger.error(f"Error checking workgroup status: {e}")
                raise

        logger.error("Timeout waiting for workgroup to become available")
        return False

    def get_namespace_info(self) -> Dict[str, Any]:
        """Get namespace information."""
        try:
            response = self.redshift_serverless.get_namespace(
                namespaceName=self.namespace_name
            )
            return response['namespace']
        except ClientError as e:
            logger.error(f"Failed to get namespace info: {e}")
            raise

    def get_workgroup_info(self) -> Dict[str, Any]:
        """Get workgroup information."""
        try:
            response = self.redshift_serverless.get_workgroup(
                workgroupName=self.workgroup_name
            )
            return response['workgroup']
        except ClientError as e:
            logger.error(f"Failed to get workgroup info: {e}")
            raise

    def create_cost_alarm(self, threshold_usd: float = 100.0) -> Optional[str]:
        """
        Create CloudWatch billing alarm for Redshift Serverless costs.

        Args:
            threshold_usd: Cost threshold in USD for alarm

        Returns:
            SNS topic ARN or None if creation fails
        """
        try:
            logger.info(f"Creating cost monitoring alarm (threshold: ${threshold_usd})")

            # Create SNS topic for alerts
            topic_name = f"redshift-serverless-cost-alerts-{self.namespace_name}"
            try:
                response = self.sns.create_topic(Name=topic_name)
                topic_arn = response['TopicArn']
                logger.info(f"Created SNS topic: {topic_arn}")
            except ClientError as e:
                if e.response['Error']['Code'] == 'TopicAlreadyExists':
                    topics = self.sns.list_topics()
                    topic_arn = next(
                        (t['TopicArn'] for t in topics['Topics'] if topic_name in t['TopicArn']),
                        None
                    )
                    logger.info(f"Using existing SNS topic: {topic_arn}")
                else:
                    raise

            # Create CloudWatch alarm for estimated charges
            alarm_name = f"RedshiftServerless-Cost-{self.namespace_name}"

            self.cloudwatch.put_metric_alarm(
                AlarmName=alarm_name,
                AlarmDescription=f"Alert when Redshift Serverless costs exceed ${threshold_usd}",
                ActionsEnabled=True,
                AlarmActions=[topic_arn],
                MetricName='EstimatedCharges',
                Namespace='AWS/Billing',
                Statistic='Maximum',
                Dimensions=[
                    {
                        'Name': 'ServiceName',
                        'Value': 'Amazon Redshift'
                    },
                    {
                        'Name': 'Currency',
                        'Value': 'USD'
                    }
                ],
                Period=21600,  # 6 hours
                EvaluationPeriods=1,
                Threshold=threshold_usd,
                ComparisonOperator='GreaterThanThreshold',
                TreatMissingData='notBreaching'
            )

            logger.info(f"Created billing alarm: {alarm_name}")
            logger.info(f"Subscribe to alerts: aws sns subscribe --topic-arn {topic_arn} --protocol email --notification-endpoint YOUR_EMAIL")

            return topic_arn

        except Exception as e:
            logger.error(f"Failed to create cost alarm: {e}")
            return None

    def print_summary(self):
        """Print summary of Redshift Serverless setup."""
        if not self.namespace_info:
            self.namespace_info = self.get_namespace_info()
        if not self.workgroup_info:
            self.workgroup_info = self.get_workgroup_info()

        endpoint = self.workgroup_info.get('endpoint', {})

        print("\n" + "="*80)
        print("REDSHIFT SERVERLESS SUMMARY")
        print("="*80)
        print(f"Namespace:         {self.namespace_info['namespaceName']}")
        print(f"Namespace ARN:     {self.namespace_info['namespaceArn']}")
        print(f"Database:          {self.namespace_info['dbName']}")
        print(f"Admin Username:    {self.namespace_info['adminUsername']}")
        print(f"\nWorkgroup:         {self.workgroup_info['workgroupName']}")
        print(f"Workgroup ARN:     {self.workgroup_info['workgroupArn']}")
        print(f"Base Capacity:     {self.workgroup_info['baseCapacity']} RPU")
        print(f"Status:            {self.workgroup_info['status']}")
        print(f"\nEndpoint:          {endpoint.get('address', 'N/A')}")
        print(f"Port:              {endpoint.get('port', 5439)}")
        print(f"Publicly Accessible: {self.workgroup_info.get('publiclyAccessible', False)}")

        if self.spectrum_role_arn:
            print(f"\nSpectrum Role ARN: {self.spectrum_role_arn}")

        print("="*80)
        print("\nConnection String (psql):")
        print(f"psql -h {endpoint.get('address', 'N/A')} -p {endpoint.get('port', 5439)} "
              f"-U {self.namespace_info['adminUsername']} -d {self.namespace_info['dbName']}")
        print("\nConnection String (Python):")
        print("import redshift_connector")
        print(f"conn = redshift_connector.connect(")
        print(f"    host='{endpoint.get('address', 'N/A')}',")
        print(f"    port={endpoint.get('port', 5439)},")
        print(f"    database='{self.namespace_info['dbName']}',")
        print(f"    user='{self.namespace_info['adminUsername']}',")
        print(f"    password='YOUR_PASSWORD'")
        print(f")")
        print("="*80 + "\n")

    def save_connection_info(self, output_file: str = None):
        """
        Save connection information to file.

        Args:
            output_file: Path to output file
        """
        if not self.namespace_info:
            self.namespace_info = self.get_namespace_info()
        if not self.workgroup_info:
            self.workgroup_info = self.get_workgroup_info()

        if output_file is None:
            config_dir = Path(__file__).parent.parent.parent / 'config'
            config_dir.mkdir(exist_ok=True)
            output_file = config_dir / 'redshift_serverless_connection.json'
        else:
            output_file = Path(output_file)
            output_file.parent.mkdir(parents=True, exist_ok=True)

        endpoint = self.workgroup_info.get('endpoint', {})

        connection_info = {
            'namespace': self.namespace_info['namespaceName'],
            'namespace_arn': self.namespace_info['namespaceArn'],
            'workgroup': self.workgroup_info['workgroupName'],
            'workgroup_arn': self.workgroup_info['workgroupArn'],
            'host': endpoint.get('address'),
            'port': endpoint.get('port', 5439),
            'database': self.namespace_info['dbName'],
            'username': self.namespace_info['adminUsername'],
            'base_capacity_rpu': self.workgroup_info['baseCapacity'],
            'spectrum_role_arn': self.spectrum_role_arn,
            'region': self.region
        }

        with open(output_file, 'w') as f:
            json.dump(connection_info, f, indent=2)

        logger.info(f"Connection info saved to: {output_file}")


def main():
    """Main execution function."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Create Redshift Serverless data warehouse for NYC Taxi analytics'
    )
    parser.add_argument('--namespace', default='oubt-analytics',
                       help='Namespace name')
    parser.add_argument('--workgroup', default='intern-workgroup',
                       help='Workgroup name')
    parser.add_argument('--database', default='analytics_db',
                       help='Database name')
    parser.add_argument('--username', default='admin',
                       help='Admin username')
    parser.add_argument('--password', help='Admin password (will prompt if not provided)')
    parser.add_argument('--base-capacity', type=int, default=32,
                       help='Base RPU capacity (default: 32, minimum: 32)')
    parser.add_argument('--region', default='us-east-1',
                       help='AWS region')
    parser.add_argument('--cost-threshold', type=float, default=100.0,
                       help='Cost alarm threshold in USD (default: 100)')
    parser.add_argument('--delete', action='store_true',
                       help='Delete the resources instead of creating')

    args = parser.parse_args()

    try:
        manager = RedshiftServerlessManager(
            namespace_name=args.namespace,
            workgroup_name=args.workgroup,
            database_name=args.database,
            admin_username=args.username,
            admin_password=args.password,
            base_capacity=args.base_capacity,
            region=args.region
        )

        if args.delete:
            logger.info("Deleting Redshift Serverless resources...")

            # Delete workgroup
            try:
                manager.redshift_serverless.delete_workgroup(
                    workgroupName=args.workgroup
                )
                logger.info(f"Workgroup {args.workgroup} deletion initiated")
            except ClientError as e:
                if e.response['Error']['Code'] != 'ResourceNotFoundException':
                    logger.error(f"Failed to delete workgroup: {e}")

            # Delete namespace
            try:
                manager.redshift_serverless.delete_namespace(
                    namespaceName=args.namespace
                )
                logger.info(f"Namespace {args.namespace} deletion initiated")
            except ClientError as e:
                if e.response['Error']['Code'] != 'ResourceNotFoundException':
                    logger.error(f"Failed to delete namespace: {e}")

            return

        # Create namespace
        logger.info("="*80)
        logger.info("Creating Redshift Serverless Data Warehouse")
        logger.info("="*80)

        namespace = manager.create_namespace()
        logger.info("✓ Namespace created successfully")

        # Create workgroup
        workgroup = manager.create_workgroup()
        logger.info("✓ Workgroup created successfully")

        # Create cost monitoring
        topic_arn = manager.create_cost_alarm(threshold_usd=args.cost_threshold)
        if topic_arn:
            logger.info("✓ Cost monitoring alarm created")

        # Save connection info
        manager.save_connection_info()

        # Print summary
        manager.print_summary()

        logger.info("="*80)
        logger.info("Setup completed successfully!")
        logger.info("="*80)
        logger.info("\nNext steps:")
        logger.info("1. Run SQL scripts to create external schema:")
        logger.info("   sql/redshift/001_create_external_schema.sql")
        logger.info("2. Create analytics views:")
        logger.info("   sql/redshift/002_create_analytics_views.sql")
        logger.info("3. Run analysis queries:")
        logger.info("   sql/redshift/003_analysis_queries.sql")

        if topic_arn:
            logger.info(f"\n4. Subscribe to cost alerts:")
            logger.info(f"   aws sns subscribe --topic-arn {topic_arn} \\")
            logger.info(f"       --protocol email --notification-endpoint YOUR_EMAIL")

    except KeyboardInterrupt:
        logger.warning("\nOperation cancelled by user")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Failed to create Redshift Serverless: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
