"""
Apply S3 bucket policy to the Data Lake bucket.

This script applies the bucket policy from infra/policies/s3_bucket_policy.json
with proper replacements for BUCKET_NAME and ACCOUNT_ID.
"""

import sys
import json
from pathlib import Path
from typing import Optional
import boto3
from botocore.exceptions import ClientError

# Add parent directory to path for imports
sys.path.append('/home/user/OUBT-CC/de-intern-2024-project/src')
from de_intern_2024.utils.logger import get_logger
from de_intern_2024.utils.aws_helpers import get_boto3_client

logger = get_logger(__name__)


class BucketPolicyManager:
    """Manages S3 bucket policies."""

    def __init__(
        self,
        bucket_name: str,
        account_id: Optional[str] = None,
        region: str = 'us-east-1'
    ):
        """
        Initialize Bucket Policy Manager.

        Args:
            bucket_name: Name of the S3 bucket.
            account_id: AWS account ID. If None, fetches from STS.
            region: AWS region.
        """
        self.bucket_name = bucket_name
        self.region = region
        self.s3_client = get_boto3_client('s3', region=region)

        # Get account ID if not provided
        if account_id is None:
            sts_client = get_boto3_client('sts', region=region)
            account_id = sts_client.get_caller_identity()['Account']

        self.account_id = account_id
        logger.info(f"Initialized BucketPolicyManager for bucket: {bucket_name}")
        logger.info(f"Account ID: {account_id}")

    def load_policy_template(self, policy_file: str) -> dict:
        """
        Load bucket policy template from JSON file.

        Args:
            policy_file: Path to the policy JSON file.

        Returns:
            dict: Policy document.
        """
        try:
            logger.info(f"Loading policy template from: {policy_file}")

            with open(policy_file, 'r') as f:
                policy = json.load(f)

            logger.info("Successfully loaded policy template")
            return policy

        except FileNotFoundError:
            logger.error(f"Policy file not found: {policy_file}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in policy file: {e}")
            raise

    def replace_policy_placeholders(self, policy: dict) -> dict:
        """
        Replace placeholders in the policy document.

        Args:
            policy: Policy document with placeholders.

        Returns:
            dict: Policy document with replaced values.
        """
        logger.info("Replacing policy placeholders...")

        # Convert to string, replace, and convert back
        policy_str = json.dumps(policy)

        replacements = {
            'BUCKET_NAME': self.bucket_name,
            'ACCOUNT_ID': self.account_id
        }

        for placeholder, value in replacements.items():
            policy_str = policy_str.replace(placeholder, value)
            logger.info(f"  Replaced {placeholder} with {value}")

        policy = json.loads(policy_str)
        return policy

    def validate_policy(self, policy: dict) -> bool:
        """
        Validate the policy document structure.

        Args:
            policy: Policy document to validate.

        Returns:
            bool: True if valid, False otherwise.
        """
        logger.info("Validating policy document...")

        required_keys = ['Version', 'Statement']
        for key in required_keys:
            if key not in policy:
                logger.error(f"Missing required key: {key}")
                return False

        if not isinstance(policy['Statement'], list):
            logger.error("Statement must be a list")
            return False

        if len(policy['Statement']) == 0:
            logger.error("Statement list is empty")
            return False

        # Check for placeholders that weren't replaced
        policy_str = json.dumps(policy)
        placeholders = ['BUCKET_NAME', 'ACCOUNT_ID']
        for placeholder in placeholders:
            if placeholder in policy_str:
                logger.error(f"Unreplaced placeholder found: {placeholder}")
                return False

        logger.info("✓ Policy validation passed")
        return True

    def apply_policy(self, policy: dict) -> bool:
        """
        Apply the bucket policy to the S3 bucket.

        Args:
            policy: Policy document to apply.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info(f"Applying bucket policy to: {self.bucket_name}")

            # Validate before applying
            if not self.validate_policy(policy):
                logger.error("Policy validation failed, aborting")
                return False

            # Convert policy to JSON string
            policy_str = json.dumps(policy, indent=2)

            # Apply the policy
            self.s3_client.put_bucket_policy(
                Bucket=self.bucket_name,
                Policy=policy_str
            )

            logger.info("✓ Successfully applied bucket policy")
            return True

        except ClientError as e:
            logger.error(f"Failed to apply bucket policy: {e}")
            return False

    def get_current_policy(self) -> Optional[dict]:
        """
        Get the current bucket policy.

        Returns:
            dict: Current policy document, or None if not set.
        """
        try:
            logger.info(f"Fetching current policy for bucket: {self.bucket_name}")

            response = self.s3_client.get_bucket_policy(Bucket=self.bucket_name)
            policy = json.loads(response['Policy'])

            logger.info("✓ Successfully retrieved current policy")
            return policy

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'NoSuchBucketPolicy':
                logger.warning("No bucket policy currently set")
                return None
            else:
                logger.error(f"Failed to get bucket policy: {e}")
                return None

    def delete_policy(self) -> bool:
        """
        Delete the bucket policy.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info(f"Deleting bucket policy from: {self.bucket_name}")

            self.s3_client.delete_bucket_policy(Bucket=self.bucket_name)

            logger.info("✓ Successfully deleted bucket policy")
            return True

        except ClientError as e:
            logger.error(f"Failed to delete bucket policy: {e}")
            return False

    def display_policy_summary(self, policy: dict) -> None:
        """
        Display a summary of the policy.

        Args:
            policy: Policy document to summarize.
        """
        logger.info("\n" + "=" * 80)
        logger.info("Bucket Policy Summary")
        logger.info("=" * 80)

        statements = policy.get('Statement', [])
        logger.info(f"\nTotal Statements: {len(statements)}\n")

        for i, stmt in enumerate(statements, 1):
            sid = stmt.get('Sid', f'Statement-{i}')
            effect = stmt.get('Effect', 'Unknown')
            actions = stmt.get('Action', [])
            if isinstance(actions, str):
                actions = [actions]

            logger.info(f"{i}. {sid}")
            logger.info(f"   Effect: {effect}")
            logger.info(f"   Actions: {len(actions)} action(s)")

            # Show principals
            if 'Principal' in stmt:
                principal = stmt['Principal']
                if isinstance(principal, dict):
                    for key, value in principal.items():
                        if isinstance(value, list):
                            logger.info(f"   Principals ({key}): {len(value)} principal(s)")
                        else:
                            logger.info(f"   Principal ({key}): {value}")
                else:
                    logger.info(f"   Principal: {principal}")

            # Show conditions
            if 'Condition' in stmt:
                logger.info(f"   Conditions: {len(stmt['Condition'])} condition(s)")

            logger.info("")

        logger.info("=" * 80)


def main():
    """Main entry point for the script."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Apply S3 bucket policy to Data Lake'
    )
    parser.add_argument(
        '--bucket-name',
        type=str,
        required=True,
        help='Name of the S3 bucket'
    )
    parser.add_argument(
        '--policy-file',
        type=str,
        default='/home/user/OUBT-CC/de-intern-2024-project/infra/policies/s3_bucket_policy.json',
        help='Path to bucket policy JSON file'
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
        '--show-current',
        action='store_true',
        help='Show current bucket policy'
    )
    parser.add_argument(
        '--delete',
        action='store_true',
        help='Delete bucket policy'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate policy without applying'
    )

    args = parser.parse_args()

    try:
        manager = BucketPolicyManager(
            bucket_name=args.bucket_name,
            account_id=args.account_id,
            region=args.region
        )

        if args.show_current:
            policy = manager.get_current_policy()
            if policy:
                print(json.dumps(policy, indent=2))
                manager.display_policy_summary(policy)
            sys.exit(0)

        elif args.delete:
            success = manager.delete_policy()
            sys.exit(0 if success else 1)

        else:
            # Load and apply policy
            logger.info("=" * 80)
            logger.info("Starting Bucket Policy Application")
            logger.info("=" * 80)

            policy = manager.load_policy_template(args.policy_file)
            policy = manager.replace_policy_placeholders(policy)

            manager.display_policy_summary(policy)

            if args.dry_run:
                logger.info("\n[DRY RUN] Policy validation successful, not applying")
                sys.exit(0)

            success = manager.apply_policy(policy)

            if success:
                logger.info("\n✓ SUCCESS: Bucket policy applied successfully!")
            else:
                logger.error("\n✗ FAILED: Could not apply bucket policy")
                sys.exit(1)

    except Exception as e:
        logger.error(f"Failed to manage bucket policy: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
