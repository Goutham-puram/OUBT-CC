"""
Create IAM role for AWS Glue service with appropriate permissions.

This script creates an IAM role for AWS Glue to access S3 data lake resources.
The role includes:
- Trust policy for glue.amazonaws.com
- AWS managed policy for Glue service
- Custom S3 read access policy for the data lake
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


class GlueRoleCreator:
    """Manages creation and configuration of AWS Glue IAM role."""

    def __init__(self, role_name: str = "AWSGlueServiceRole-intern", region: str = 'us-east-1'):
        """
        Initialize Glue Role Creator.

        Args:
            role_name: Name for the IAM role.
            region: AWS region.
        """
        self.role_name = role_name
        self.region = region
        self.iam_client = get_boto3_client('iam', region=region)
        self.sts_client = get_boto3_client('sts', region=region)

        # Get account ID
        self.account_id = self.sts_client.get_caller_identity()['Account']
        self.bucket_name = f"{self.account_id}-oubt-datalake"

        logger.info(f"Initialized GlueRoleCreator for role: {self.role_name}")

    def get_trust_policy(self) -> Dict:
        """
        Get trust policy for Glue service.

        Returns:
            dict: Trust policy document.
        """
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "glue.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }

    def get_s3_access_policy(self) -> Dict:
        """
        Get S3 access policy for data lake.

        Returns:
            dict: S3 access policy document.
        """
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{self.bucket_name}",
                        f"arn:aws:s3:::{self.bucket_name}/*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:PutObject",
                        "s3:DeleteObject"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{self.bucket_name}/raw/*",
                        f"arn:aws:s3:::{self.bucket_name}/processed/*",
                        f"arn:aws:s3:::{self.bucket_name}/curated/*"
                    ]
                }
            ]
        }

    def create_role(self) -> bool:
        """
        Create IAM role with trust policy.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info(f"Creating IAM role: {self.role_name}")

            trust_policy = self.get_trust_policy()

            response = self.iam_client.create_role(
                RoleName=self.role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description="IAM role for AWS Glue service to access S3 data lake",
                Tags=[
                    {'Key': 'Project', 'Value': 'OUBT-DataEngineering'},
                    {'Key': 'Environment', 'Value': 'Production'},
                    {'Key': 'Purpose', 'Value': 'GlueService'},
                    {'Key': 'ManagedBy', 'Value': 'Automation'}
                ]
            )

            role_arn = response['Role']['Arn']
            logger.info(f"Successfully created role: {role_arn}")
            return True

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'EntityAlreadyExists':
                logger.warning(f"Role {self.role_name} already exists")
                return True
            else:
                logger.error(f"Failed to create role: {e}")
                return False

    def attach_managed_policy(self) -> bool:
        """
        Attach AWS managed Glue service policy.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info("Attaching AWS managed Glue service policy")

            policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"

            self.iam_client.attach_role_policy(
                RoleName=self.role_name,
                PolicyArn=policy_arn
            )

            logger.info("Successfully attached managed policy")
            return True

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'EntityAlreadyExists':
                logger.warning("Policy already attached to role")
                return True
            else:
                logger.error(f"Failed to attach managed policy: {e}")
                return False

    def create_s3_access_policy(self) -> Optional[str]:
        """
        Create inline S3 access policy.

        Returns:
            str: Policy ARN if successful, None otherwise.
        """
        try:
            logger.info("Creating S3 access policy")

            policy_name = f"{self.role_name}-S3Access"
            s3_policy = self.get_s3_access_policy()

            self.iam_client.put_role_policy(
                RoleName=self.role_name,
                PolicyName=policy_name,
                PolicyDocument=json.dumps(s3_policy)
            )

            logger.info(f"Successfully created inline policy: {policy_name}")
            return policy_name

        except ClientError as e:
            logger.error(f"Failed to create S3 access policy: {e}")
            return None

    def get_role_info(self) -> Optional[Dict]:
        """
        Get information about the role.

        Returns:
            dict: Role information, None if role doesn't exist.
        """
        try:
            response = self.iam_client.get_role(RoleName=self.role_name)
            role = response['Role']

            # Get attached policies
            policies_response = self.iam_client.list_attached_role_policies(
                RoleName=self.role_name
            )
            attached_policies = [p['PolicyName'] for p in policies_response['AttachedPolicies']]

            # Get inline policies
            inline_response = self.iam_client.list_role_policies(
                RoleName=self.role_name
            )
            inline_policies = inline_response['PolicyNames']

            return {
                'role_name': role['RoleName'],
                'role_arn': role['Arn'],
                'created_date': role['CreateDate'].isoformat(),
                'attached_policies': attached_policies,
                'inline_policies': inline_policies
            }

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'NoSuchEntity':
                logger.warning(f"Role {self.role_name} does not exist")
                return None
            else:
                logger.error(f"Failed to get role info: {e}")
                return None

    def setup_role(self) -> bool:
        """
        Complete setup of Glue IAM role.

        Returns:
            bool: True if all steps successful, False otherwise.
        """
        logger.info("=" * 80)
        logger.info("Starting Glue IAM Role Setup")
        logger.info("=" * 80)

        steps = [
            ("Create IAM role", self.create_role),
            ("Attach managed Glue service policy", self.attach_managed_policy),
            ("Create S3 access policy", lambda: self.create_s3_access_policy() is not None)
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
            logger.info("SUCCESS: Glue IAM role setup completed successfully!")
            logger.info("=" * 80)

            # Print role information
            info = self.get_role_info()
            if info:
                logger.info("\nRole Configuration:")
                logger.info(f"  Role Name: {info['role_name']}")
                logger.info(f"  Role ARN: {info['role_arn']}")
                logger.info(f"  Created: {info['created_date']}")
                logger.info(f"  Attached Policies: {', '.join(info['attached_policies'])}")
                logger.info(f"  Inline Policies: {', '.join(info['inline_policies'])}")
                logger.info(f"  S3 Bucket Access: {self.bucket_name}")

        else:
            logger.error("FAILED: Some steps failed during role setup")
            logger.info("=" * 80)

        return all_successful


def main():
    """Main entry point for the script."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Create IAM role for AWS Glue service'
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
        help='Only display role information, do not create'
    )

    args = parser.parse_args()

    try:
        creator = GlueRoleCreator(
            role_name=args.role_name,
            region=args.region
        )

        if args.info_only:
            info = creator.get_role_info()
            if info:
                print(json.dumps(info, indent=2))
            else:
                logger.error("Role does not exist")
                sys.exit(1)
        else:
            success = creator.setup_role()
            sys.exit(0 if success else 1)

    except Exception as e:
        logger.error(f"Failed to setup Glue role: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
