"""
Create and configure production-grade S3 Data Lake with multiple zones and governance.

This script creates an S3 bucket with:
- Multiple zones (raw, processed, curated)
- Versioning enabled
- Server-side encryption (AES-256)
- CloudWatch metrics for monitoring
- Proper tagging for governance
"""

import sys
import json
from typing import Dict, List, Optional
import boto3
from botocore.exceptions import ClientError

# Add parent directory to path for imports
sys.path.append('/home/user/OUBT-CC/de-intern-2024-project/src')
from de_intern_2024.utils.logger import get_logger
from de_intern_2024.utils.aws_helpers import get_boto3_client

logger = get_logger(__name__)


class S3DataLakeCreator:
    """Manages creation and configuration of S3 Data Lake."""

    ZONES = ['raw/', 'processed/', 'curated/']

    def __init__(self, account_id: Optional[str] = None, region: str = 'us-east-1'):
        """
        Initialize S3 Data Lake Creator.

        Args:
            account_id: AWS account ID. If None, fetches from STS.
            region: AWS region for bucket creation.
        """
        self.region = region
        self.s3_client = get_boto3_client('s3', region=region)
        self.cloudwatch_client = get_boto3_client('cloudwatch', region=region)

        # Get account ID if not provided
        if account_id is None:
            sts_client = get_boto3_client('sts', region=region)
            account_id = sts_client.get_caller_identity()['Account']

        self.account_id = account_id
        self.bucket_name = f"{account_id}-oubt-datalake"
        logger.info(f"Initialized S3DataLakeCreator for bucket: {self.bucket_name}")

    def create_bucket(self) -> bool:
        """
        Create S3 bucket with appropriate configuration.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info(f"Creating S3 bucket: {self.bucket_name}")

            # Create bucket with location constraint if not in us-east-1
            if self.region == 'us-east-1':
                self.s3_client.create_bucket(Bucket=self.bucket_name)
            else:
                self.s3_client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': self.region}
                )

            logger.info(f"Successfully created bucket: {self.bucket_name}")
            return True

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'BucketAlreadyOwnedByYou':
                logger.warning(f"Bucket {self.bucket_name} already exists and is owned by you")
                return True
            elif error_code == 'BucketAlreadyExists':
                logger.error(f"Bucket {self.bucket_name} already exists but is owned by another account")
                return False
            else:
                logger.error(f"Failed to create bucket: {e}")
                return False

    def enable_versioning(self) -> bool:
        """
        Enable versioning on the S3 bucket.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info(f"Enabling versioning for bucket: {self.bucket_name}")

            self.s3_client.put_bucket_versioning(
                Bucket=self.bucket_name,
                VersioningConfiguration={'Status': 'Enabled'}
            )

            logger.info("Successfully enabled versioning")
            return True

        except ClientError as e:
            logger.error(f"Failed to enable versioning: {e}")
            return False

    def enable_encryption(self) -> bool:
        """
        Enable default server-side encryption (AES-256) for the bucket.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info(f"Enabling encryption for bucket: {self.bucket_name}")

            self.s3_client.put_bucket_encryption(
                Bucket=self.bucket_name,
                ServerSideEncryptionConfiguration={
                    'Rules': [
                        {
                            'ApplyServerSideEncryptionByDefault': {
                                'SSEAlgorithm': 'AES256'
                            },
                            'BucketKeyEnabled': True
                        }
                    ]
                }
            )

            logger.info("Successfully enabled AES-256 encryption")
            return True

        except ClientError as e:
            logger.error(f"Failed to enable encryption: {e}")
            return False

    def enable_public_access_block(self) -> bool:
        """
        Enable public access block to prevent accidental public exposure.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info(f"Enabling public access block for bucket: {self.bucket_name}")

            self.s3_client.put_public_access_block(
                Bucket=self.bucket_name,
                PublicAccessBlockConfiguration={
                    'BlockPublicAcls': True,
                    'IgnorePublicAcls': True,
                    'BlockPublicPolicy': True,
                    'RestrictPublicBuckets': True
                }
            )

            logger.info("Successfully enabled public access block")
            return True

        except ClientError as e:
            logger.error(f"Failed to enable public access block: {e}")
            return False

    def add_bucket_tags(self) -> bool:
        """
        Add governance tags to the bucket.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info(f"Adding tags to bucket: {self.bucket_name}")

            tags = [
                {'Key': 'Project', 'Value': 'OUBT-DataEngineering'},
                {'Key': 'Environment', 'Value': 'Production'},
                {'Key': 'Purpose', 'Value': 'DataLake'},
                {'Key': 'ManagedBy', 'Value': 'Automation'},
                {'Key': 'CostCenter', 'Value': 'DataEngineering'},
                {'Key': 'DataClassification', 'Value': 'Internal'}
            ]

            self.s3_client.put_bucket_tagging(
                Bucket=self.bucket_name,
                Tagging={'TagSet': tags}
            )

            logger.info(f"Successfully added {len(tags)} tags to bucket")
            return True

        except ClientError as e:
            logger.error(f"Failed to add tags: {e}")
            return False

    def create_zone_structure(self) -> bool:
        """
        Create folder structure for different data zones.
        Creates placeholder objects for raw/, processed/, and curated/ zones.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info("Creating data lake zone structure")

            for zone in self.ZONES:
                logger.info(f"Creating zone: {zone}")
                # Create a placeholder object to establish the folder structure
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=zone,
                    Body=b'',
                    ServerSideEncryption='AES256'
                )

            logger.info(f"Successfully created {len(self.ZONES)} zones")
            return True

        except ClientError as e:
            logger.error(f"Failed to create zone structure: {e}")
            return False

    def enable_request_metrics(self) -> bool:
        """
        Enable request metrics for CloudWatch monitoring.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info("Enabling CloudWatch request metrics")

            # Enable metrics for each zone
            for zone in self.ZONES:
                zone_name = zone.rstrip('/').capitalize()

                self.s3_client.put_bucket_metrics_configuration(
                    Bucket=self.bucket_name,
                    Id=f"{zone_name}ZoneMetrics",
                    MetricsConfiguration={
                        'Id': f"{zone_name}ZoneMetrics",
                        'Filter': {
                            'Prefix': zone
                        }
                    }
                )
                logger.info(f"Enabled metrics for {zone}")

            # Enable overall bucket metrics
            self.s3_client.put_bucket_metrics_configuration(
                Bucket=self.bucket_name,
                Id="EntireBucketMetrics",
                MetricsConfiguration={
                    'Id': "EntireBucketMetrics"
                }
            )

            logger.info("Successfully enabled CloudWatch metrics")
            return True

        except ClientError as e:
            logger.error(f"Failed to enable request metrics: {e}")
            return False

    def enable_intelligent_tiering(self) -> bool:
        """
        Configure S3 Intelligent-Tiering for automatic cost optimization.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info("Enabling Intelligent-Tiering configuration")

            self.s3_client.put_bucket_intelligent_tiering_configuration(
                Bucket=self.bucket_name,
                Id='IntelligentTieringConfig',
                IntelligentTieringConfiguration={
                    'Id': 'IntelligentTieringConfig',
                    'Status': 'Enabled',
                    'Tierings': [
                        {
                            'Days': 90,
                            'AccessTier': 'ARCHIVE_ACCESS'
                        },
                        {
                            'Days': 180,
                            'AccessTier': 'DEEP_ARCHIVE_ACCESS'
                        }
                    ]
                }
            )

            logger.info("Successfully enabled Intelligent-Tiering")
            return True

        except ClientError as e:
            logger.error(f"Failed to enable Intelligent-Tiering: {e}")
            return False

    def get_bucket_info(self) -> Dict:
        """
        Get current bucket configuration information.

        Returns:
            dict: Bucket configuration details.
        """
        info = {
            'bucket_name': self.bucket_name,
            'region': self.region,
            'account_id': self.account_id,
            'zones': self.ZONES
        }

        try:
            # Get versioning status
            versioning = self.s3_client.get_bucket_versioning(Bucket=self.bucket_name)
            info['versioning'] = versioning.get('Status', 'Disabled')

            # Get encryption status
            try:
                encryption = self.s3_client.get_bucket_encryption(Bucket=self.bucket_name)
                info['encryption'] = 'Enabled'
            except ClientError:
                info['encryption'] = 'Disabled'

            # Get tags
            try:
                tags = self.s3_client.get_bucket_tagging(Bucket=self.bucket_name)
                info['tags'] = {tag['Key']: tag['Value'] for tag in tags['TagSet']}
            except ClientError:
                info['tags'] = {}

        except ClientError as e:
            logger.error(f"Failed to get bucket info: {e}")

        return info

    def setup_data_lake(self) -> bool:
        """
        Complete setup of the S3 data lake with all configurations.

        Returns:
            bool: True if all steps successful, False otherwise.
        """
        logger.info("=" * 80)
        logger.info("Starting S3 Data Lake Setup")
        logger.info("=" * 80)

        steps = [
            ("Create S3 bucket", self.create_bucket),
            ("Enable versioning", self.enable_versioning),
            ("Enable encryption", self.enable_encryption),
            ("Enable public access block", self.enable_public_access_block),
            ("Add bucket tags", self.add_bucket_tags),
            ("Create zone structure", self.create_zone_structure),
            ("Enable CloudWatch metrics", self.enable_request_metrics),
            ("Enable Intelligent-Tiering", self.enable_intelligent_tiering)
        ]

        all_successful = True
        for step_name, step_func in steps:
            logger.info(f"\n[STEP] {step_name}")
            if not step_func():
                logger.error(f"Failed: {step_name}")
                all_successful = False
            else:
                logger.info(f"Completed: {step_name}")

        logger.info("\n" + "=" * 80)
        if all_successful:
            logger.info("SUCCESS: Data lake setup completed successfully!")
            logger.info("=" * 80)

            # Print bucket information
            info = self.get_bucket_info()
            logger.info("\nBucket Configuration:")
            logger.info(f"  Bucket Name: {info['bucket_name']}")
            logger.info(f"  Region: {info['region']}")
            logger.info(f"  Account ID: {info['account_id']}")
            logger.info(f"  Versioning: {info.get('versioning', 'Unknown')}")
            logger.info(f"  Encryption: {info.get('encryption', 'Unknown')}")
            logger.info(f"  Zones: {', '.join(info['zones'])}")

        else:
            logger.error("FAILED: Some steps failed during data lake setup")
            logger.info("=" * 80)

        return all_successful


def main():
    """Main entry point for the script."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Create production-grade S3 Data Lake with governance'
    )
    parser.add_argument(
        '--account-id',
        type=str,
        help='AWS Account ID (optional, will auto-detect if not provided)'
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
        help='Only display bucket information, do not create'
    )

    args = parser.parse_args()

    try:
        creator = S3DataLakeCreator(
            account_id=args.account_id,
            region=args.region
        )

        if args.info_only:
            info = creator.get_bucket_info()
            print(json.dumps(info, indent=2))
        else:
            success = creator.setup_data_lake()
            sys.exit(0 if success else 1)

    except Exception as e:
        logger.error(f"Failed to create data lake: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
