"""
Configure S3 lifecycle policies for cost optimization in the Data Lake.

This script sets up lifecycle rules for different zones:
- raw/: Transition to Standard-IA after 30 days
- processed/: Transition to Intelligent-Tiering after 7 days
- curated/: Keep in Standard storage class
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


class S3LifecycleConfigurator:
    """Manages S3 lifecycle policies for Data Lake zones."""

    def __init__(self, bucket_name: str, region: str = 'us-east-1'):
        """
        Initialize S3 Lifecycle Configurator.

        Args:
            bucket_name: Name of the S3 bucket.
            region: AWS region.
        """
        self.bucket_name = bucket_name
        self.region = region
        self.s3_client = get_boto3_client('s3', region=region)
        logger.info(f"Initialized S3LifecycleConfigurator for bucket: {bucket_name}")

    def create_lifecycle_rules(self) -> List[Dict]:
        """
        Create lifecycle rules for each data lake zone.

        Returns:
            list: List of lifecycle rule configurations.
        """
        rules = []

        # Rule 1: Raw Zone - Transition to Standard-IA after 30 days
        rules.append({
            'ID': 'raw-zone-lifecycle',
            'Status': 'Enabled',
            'Filter': {
                'Prefix': 'raw/'
            },
            'Transitions': [
                {
                    'Days': 30,
                    'StorageClass': 'STANDARD_IA'
                },
                {
                    'Days': 90,
                    'StorageClass': 'GLACIER_IR'  # Glacier Instant Retrieval
                },
                {
                    'Days': 180,
                    'StorageClass': 'DEEP_ARCHIVE'
                }
            ],
            'NoncurrentVersionTransitions': [
                {
                    'NoncurrentDays': 30,
                    'StorageClass': 'STANDARD_IA'
                },
                {
                    'NoncurrentDays': 60,
                    'StorageClass': 'GLACIER_IR'
                }
            ],
            'NoncurrentVersionExpiration': {
                'NoncurrentDays': 365
            }
        })

        # Rule 2: Processed Zone - Transition to Intelligent-Tiering after 7 days
        rules.append({
            'ID': 'processed-zone-lifecycle',
            'Status': 'Enabled',
            'Filter': {
                'Prefix': 'processed/'
            },
            'Transitions': [
                {
                    'Days': 7,
                    'StorageClass': 'INTELLIGENT_TIERING'
                }
            ],
            'NoncurrentVersionTransitions': [
                {
                    'NoncurrentDays': 7,
                    'StorageClass': 'INTELLIGENT_TIERING'
                }
            ],
            'NoncurrentVersionExpiration': {
                'NoncurrentDays': 180
            }
        })

        # Rule 3: Curated Zone - Keep in Standard, but clean up old versions
        rules.append({
            'ID': 'curated-zone-lifecycle',
            'Status': 'Enabled',
            'Filter': {
                'Prefix': 'curated/'
            },
            'NoncurrentVersionExpiration': {
                'NoncurrentDays': 90
            }
        })

        # Rule 4: Clean up incomplete multipart uploads after 7 days
        rules.append({
            'ID': 'cleanup-incomplete-multipart-uploads',
            'Status': 'Enabled',
            'Filter': {
                'Prefix': ''
            },
            'AbortIncompleteMultipartUpload': {
                'DaysAfterInitiation': 7
            }
        })

        # Rule 5: Delete expired object delete markers
        rules.append({
            'ID': 'delete-expired-object-delete-markers',
            'Status': 'Enabled',
            'Filter': {
                'Prefix': ''
            },
            'Expiration': {
                'ExpiredObjectDeleteMarker': True
            }
        })

        logger.info(f"Created {len(rules)} lifecycle rules")
        return rules

    def apply_lifecycle_configuration(self, rules: Optional[List[Dict]] = None) -> bool:
        """
        Apply lifecycle configuration to the S3 bucket.

        Args:
            rules: Optional list of lifecycle rules. If None, uses default rules.

        Returns:
            bool: True if successful, False otherwise.
        """
        if rules is None:
            rules = self.create_lifecycle_rules()

        try:
            logger.info(f"Applying lifecycle configuration to bucket: {self.bucket_name}")
            logger.info(f"Total rules to apply: {len(rules)}")

            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=self.bucket_name,
                LifecycleConfiguration={
                    'Rules': rules
                }
            )

            logger.info("Successfully applied lifecycle configuration")
            self._log_rule_summary(rules)
            return True

        except ClientError as e:
            logger.error(f"Failed to apply lifecycle configuration: {e}")
            return False

    def _log_rule_summary(self, rules: List[Dict]) -> None:
        """
        Log a summary of applied lifecycle rules.

        Args:
            rules: List of lifecycle rules.
        """
        logger.info("\nLifecycle Rules Summary:")
        logger.info("=" * 80)

        for rule in rules:
            logger.info(f"\nRule ID: {rule['ID']}")
            logger.info(f"  Status: {rule['Status']}")
            logger.info(f"  Filter: {rule.get('Filter', {})}")

            if 'Transitions' in rule:
                logger.info("  Transitions:")
                for transition in rule['Transitions']:
                    logger.info(f"    - After {transition['Days']} days -> {transition['StorageClass']}")

            if 'NoncurrentVersionTransitions' in rule:
                logger.info("  Noncurrent Version Transitions:")
                for transition in rule['NoncurrentVersionTransitions']:
                    logger.info(f"    - After {transition['NoncurrentDays']} days -> {transition['StorageClass']}")

            if 'NoncurrentVersionExpiration' in rule:
                days = rule['NoncurrentVersionExpiration']['NoncurrentDays']
                logger.info(f"  Noncurrent Version Expiration: After {days} days")

            if 'AbortIncompleteMultipartUpload' in rule:
                days = rule['AbortIncompleteMultipartUpload']['DaysAfterInitiation']
                logger.info(f"  Abort Incomplete Multipart Uploads: After {days} days")

            if 'Expiration' in rule:
                if rule['Expiration'].get('ExpiredObjectDeleteMarker'):
                    logger.info("  Expiration: Delete expired object delete markers")

        logger.info("\n" + "=" * 80)

    def get_lifecycle_configuration(self) -> Optional[Dict]:
        """
        Get current lifecycle configuration for the bucket.

        Returns:
            dict: Current lifecycle configuration, or None if not set.
        """
        try:
            logger.info(f"Fetching lifecycle configuration for bucket: {self.bucket_name}")

            response = self.s3_client.get_bucket_lifecycle_configuration(
                Bucket=self.bucket_name
            )

            rules = response.get('Rules', [])
            logger.info(f"Found {len(rules)} lifecycle rules")
            return response

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'NoSuchLifecycleConfiguration':
                logger.warning("No lifecycle configuration found for bucket")
                return None
            else:
                logger.error(f"Failed to get lifecycle configuration: {e}")
                return None

    def delete_lifecycle_configuration(self) -> bool:
        """
        Delete lifecycle configuration from the bucket.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info(f"Deleting lifecycle configuration from bucket: {self.bucket_name}")

            self.s3_client.delete_bucket_lifecycle(Bucket=self.bucket_name)

            logger.info("Successfully deleted lifecycle configuration")
            return True

        except ClientError as e:
            logger.error(f"Failed to delete lifecycle configuration: {e}")
            return False

    def validate_lifecycle_rules(self, rules: List[Dict]) -> bool:
        """
        Validate lifecycle rules before applying.

        Args:
            rules: List of lifecycle rules to validate.

        Returns:
            bool: True if valid, False otherwise.
        """
        logger.info("Validating lifecycle rules...")

        if not rules:
            logger.error("No rules provided")
            return False

        for i, rule in enumerate(rules):
            # Check required fields
            if 'ID' not in rule:
                logger.error(f"Rule {i}: Missing 'ID' field")
                return False

            if 'Status' not in rule:
                logger.error(f"Rule {i}: Missing 'Status' field")
                return False

            if rule['Status'] not in ['Enabled', 'Disabled']:
                logger.error(f"Rule {i}: Invalid status '{rule['Status']}'")
                return False

            # Check that rule has at least one action
            actions = [
                'Transitions',
                'NoncurrentVersionTransitions',
                'Expiration',
                'NoncurrentVersionExpiration',
                'AbortIncompleteMultipartUpload'
            ]

            if not any(action in rule for action in actions):
                logger.error(f"Rule {i}: No lifecycle actions defined")
                return False

        logger.info(f"All {len(rules)} rules are valid")
        return True

    def estimate_cost_savings(self) -> Dict:
        """
        Estimate potential cost savings from lifecycle policies.
        This is a simplified estimation based on storage class pricing.

        Returns:
            dict: Estimated cost savings information.
        """
        # Simplified pricing per GB/month (us-east-1)
        pricing = {
            'STANDARD': 0.023,
            'STANDARD_IA': 0.0125,
            'INTELLIGENT_TIERING': 0.023,  # Base price, auto-optimizes
            'GLACIER_IR': 0.004,
            'DEEP_ARCHIVE': 0.00099
        }

        savings = {
            'raw_zone': {
                'after_30_days': {
                    'from': 'STANDARD',
                    'to': 'STANDARD_IA',
                    'savings_per_gb_month': pricing['STANDARD'] - pricing['STANDARD_IA']
                },
                'after_90_days': {
                    'from': 'STANDARD',
                    'to': 'GLACIER_IR',
                    'savings_per_gb_month': pricing['STANDARD'] - pricing['GLACIER_IR']
                }
            },
            'processed_zone': {
                'after_7_days': {
                    'from': 'STANDARD',
                    'to': 'INTELLIGENT_TIERING',
                    'note': 'Auto-optimizes based on access patterns'
                }
            },
            'pricing': pricing
        }

        logger.info("\nEstimated Cost Savings:")
        logger.info("=" * 80)
        logger.info("Raw Zone:")
        logger.info(f"  - Savings after 30 days: ${savings['raw_zone']['after_30_days']['savings_per_gb_month']:.4f} per GB/month")
        logger.info(f"  - Savings after 90 days: ${savings['raw_zone']['after_90_days']['savings_per_gb_month']:.4f} per GB/month")
        logger.info("\nProcessed Zone:")
        logger.info("  - Intelligent-Tiering auto-optimizes based on access patterns")
        logger.info("=" * 80)

        return savings


def main():
    """Main entry point for the script."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Configure S3 lifecycle policies for Data Lake zones'
    )
    parser.add_argument(
        '--bucket-name',
        type=str,
        required=True,
        help='Name of the S3 bucket'
    )
    parser.add_argument(
        '--region',
        type=str,
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )
    parser.add_argument(
        '--show-current',
        action='store_true',
        help='Show current lifecycle configuration'
    )
    parser.add_argument(
        '--delete',
        action='store_true',
        help='Delete lifecycle configuration'
    )
    parser.add_argument(
        '--estimate-savings',
        action='store_true',
        help='Show estimated cost savings'
    )

    args = parser.parse_args()

    try:
        configurator = S3LifecycleConfigurator(
            bucket_name=args.bucket_name,
            region=args.region
        )

        if args.show_current:
            config = configurator.get_lifecycle_configuration()
            if config:
                print(json.dumps(config, indent=2, default=str))
            sys.exit(0)

        elif args.delete:
            success = configurator.delete_lifecycle_configuration()
            sys.exit(0 if success else 1)

        elif args.estimate_savings:
            savings = configurator.estimate_cost_savings()
            print(json.dumps(savings, indent=2))
            sys.exit(0)

        else:
            # Apply lifecycle configuration
            logger.info("=" * 80)
            logger.info("Starting Lifecycle Configuration")
            logger.info("=" * 80)

            rules = configurator.create_lifecycle_rules()

            if configurator.validate_lifecycle_rules(rules):
                success = configurator.apply_lifecycle_configuration(rules)

                if success:
                    logger.info("\nSUCCESS: Lifecycle policies configured successfully!")
                    configurator.estimate_cost_savings()
                else:
                    logger.error("\nFAILED: Could not apply lifecycle configuration")
                    sys.exit(1)
            else:
                logger.error("\nFAILED: Lifecycle rules validation failed")
                sys.exit(1)

    except Exception as e:
        logger.error(f"Failed to configure lifecycle policies: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
