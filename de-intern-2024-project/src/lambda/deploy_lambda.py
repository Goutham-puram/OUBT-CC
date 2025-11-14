"""
Deploy script for S3 notification Lambda function.

Creates or updates the Lambda function with proper configuration:
- Function name: s3-file-notification
- Runtime: Python 3.12
- Memory: 128 MB
- Timeout: 3 seconds
"""

import os
import sys
import json
import zipfile
import boto3
import argparse
from pathlib import Path
from typing import Dict, Any

# AWS clients
lambda_client = boto3.client('lambda')
iam_client = boto3.client('iam')

# Lambda configuration
FUNCTION_NAME = 's3-file-notification'
RUNTIME = 'python3.12'
MEMORY_SIZE = 128  # MB
TIMEOUT = 3  # seconds
HANDLER = 'lambda_function.lambda_handler'


def create_deployment_package(source_dir: Path, output_path: Path) -> None:
    """
    Create a deployment package (ZIP file) for the Lambda function.

    Args:
        source_dir: Directory containing the Lambda function code
        output_path: Path where the ZIP file will be created
    """
    print(f"Creating deployment package from {source_dir}")

    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Add lambda_function.py
        lambda_file = source_dir / 'lambda_function.py'
        if not lambda_file.exists():
            raise FileNotFoundError(f"Lambda function not found: {lambda_file}")

        zipf.write(lambda_file, 'lambda_function.py')

        # Add __init__.py if it exists
        init_file = source_dir / '__init__.py'
        if init_file.exists():
            zipf.write(init_file, '__init__.py')

    print(f"Deployment package created: {output_path}")
    print(f"Package size: {output_path.stat().st_size / 1024:.2f} KB")


def get_or_create_lambda_role(role_name: str = 's3-notification-lambda-role') -> str:
    """
    Get existing Lambda execution role or create a new one.

    Args:
        role_name: Name of the IAM role

    Returns:
        ARN of the IAM role
    """
    try:
        # Try to get existing role
        response = iam_client.get_role(RoleName=role_name)
        role_arn = response['Role']['Arn']
        print(f"Using existing IAM role: {role_arn}")
        return role_arn

    except iam_client.exceptions.NoSuchEntityException:
        # Create new role
        print(f"Creating new IAM role: {role_name}")

        # Trust policy for Lambda
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "lambda.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }

        response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='Execution role for S3 notification Lambda function'
        )

        role_arn = response['Role']['Arn']

        # Attach basic Lambda execution policy
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
        )

        # Attach S3 read-only policy for accessing S3 metadata
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
        )

        print(f"Created IAM role: {role_arn}")
        print("Note: Wait a few seconds for IAM role to propagate before deploying Lambda")

        return role_arn


def deploy_lambda_function(
    zip_path: Path,
    role_arn: str,
    description: str = "S3 file notification handler (Week 2)"
) -> Dict[str, Any]:
    """
    Deploy or update the Lambda function.

    Args:
        zip_path: Path to the deployment package ZIP file
        role_arn: ARN of the IAM execution role
        description: Function description

    Returns:
        Lambda function configuration
    """
    # Read deployment package
    with open(zip_path, 'rb') as f:
        zip_content = f.read()

    try:
        # Try to update existing function
        print(f"Updating existing Lambda function: {FUNCTION_NAME}")

        response = lambda_client.update_function_code(
            FunctionName=FUNCTION_NAME,
            ZipFile=zip_content
        )

        # Update function configuration
        lambda_client.update_function_configuration(
            FunctionName=FUNCTION_NAME,
            Runtime=RUNTIME,
            MemorySize=MEMORY_SIZE,
            Timeout=TIMEOUT,
            Handler=HANDLER,
            Description=description
        )

        print(f"Successfully updated Lambda function: {FUNCTION_NAME}")
        return response

    except lambda_client.exceptions.ResourceNotFoundException:
        # Create new function
        print(f"Creating new Lambda function: {FUNCTION_NAME}")

        response = lambda_client.create_function(
            FunctionName=FUNCTION_NAME,
            Runtime=RUNTIME,
            Role=role_arn,
            Handler=HANDLER,
            Code={'ZipFile': zip_content},
            Description=description,
            Timeout=TIMEOUT,
            MemorySize=MEMORY_SIZE,
            Publish=True
        )

        print(f"Successfully created Lambda function: {FUNCTION_NAME}")
        return response


def main():
    """Main deployment function."""
    parser = argparse.ArgumentParser(description='Deploy S3 notification Lambda function')
    parser.add_argument(
        '--role-arn',
        help='IAM role ARN for Lambda execution (optional, will create if not provided)',
        default=None
    )
    parser.add_argument(
        '--region',
        help='AWS region (default: from AWS CLI config)',
        default=None
    )

    args = parser.parse_args()

    # Set region if provided
    if args.region:
        global lambda_client, iam_client
        lambda_client = boto3.client('lambda', region_name=args.region)
        iam_client = boto3.client('iam', region_name=args.region)

    # Get current script directory
    script_dir = Path(__file__).parent
    lambda_source_dir = script_dir / 's3_notification'
    output_dir = script_dir / 'dist'
    output_dir.mkdir(exist_ok=True)

    zip_path = output_dir / 's3_notification.zip'

    try:
        # Step 1: Create deployment package
        print("\n" + "=" * 60)
        print("Step 1: Creating deployment package")
        print("=" * 60)
        create_deployment_package(lambda_source_dir, zip_path)

        # Step 2: Get or create IAM role
        print("\n" + "=" * 60)
        print("Step 2: Setting up IAM role")
        print("=" * 60)
        role_arn = args.role_arn or get_or_create_lambda_role()

        # Wait for IAM role propagation if newly created
        if not args.role_arn:
            import time
            print("Waiting 10 seconds for IAM role to propagate...")
            time.sleep(10)

        # Step 3: Deploy Lambda function
        print("\n" + "=" * 60)
        print("Step 3: Deploying Lambda function")
        print("=" * 60)
        response = deploy_lambda_function(zip_path, role_arn)

        # Print success message with details
        print("\n" + "=" * 60)
        print("Deployment successful!")
        print("=" * 60)
        print(f"Function Name: {response['FunctionName']}")
        print(f"Function ARN: {response['FunctionArn']}")
        print(f"Runtime: {response['Runtime']}")
        print(f"Memory: {response['MemorySize']} MB")
        print(f"Timeout: {response['Timeout']} seconds")
        print(f"Last Modified: {response['LastModified']}")
        print("\nNext steps:")
        print("1. Run configure_s3_trigger.py to set up S3 event notifications")
        print("2. Upload a test file to S3 raw/taxi/ prefix")
        print("3. Check CloudWatch Logs for the notification")

        return 0

    except Exception as e:
        print(f"\nError during deployment: {str(e)}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
