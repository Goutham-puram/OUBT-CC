"""
Configure S3 event notifications to trigger Lambda function.

Sets up S3 bucket notifications for:
- Event: s3:ObjectCreated:*
- Prefix: raw/taxi/
- Suffix: .parquet
"""

import sys
import json
import boto3
import argparse
from typing import Optional

# AWS clients
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

# Configuration
FUNCTION_NAME = 's3-file-notification'
EVENT_PREFIX = 'raw/taxi/'
EVENT_SUFFIX = '.parquet'


def get_lambda_arn(function_name: str) -> str:
    """
    Get the ARN of the Lambda function.

    Args:
        function_name: Name of the Lambda function

    Returns:
        ARN of the Lambda function

    Raises:
        Exception: If function not found
    """
    try:
        response = lambda_client.get_function(FunctionName=function_name)
        return response['Configuration']['FunctionArn']
    except lambda_client.exceptions.ResourceNotFoundException:
        raise Exception(
            f"Lambda function '{function_name}' not found. "
            "Please deploy the Lambda function first using deploy_lambda.py"
        )


def add_lambda_permission(function_name: str, bucket_name: str) -> None:
    """
    Add permission for S3 to invoke the Lambda function.

    Args:
        function_name: Name of the Lambda function
        bucket_name: Name of the S3 bucket
    """
    statement_id = f's3-invoke-permission-{bucket_name}'

    try:
        # Try to add permission
        lambda_client.add_permission(
            FunctionName=function_name,
            StatementId=statement_id,
            Action='lambda:InvokeFunction',
            Principal='s3.amazonaws.com',
            SourceArn=f'arn:aws:s3:::{bucket_name}'
        )
        print(f"Added Lambda permission for S3 bucket: {bucket_name}")

    except lambda_client.exceptions.ResourceConflictException:
        # Permission already exists
        print(f"Lambda permission already exists for bucket: {bucket_name}")


def configure_s3_notification(
    bucket_name: str,
    lambda_arn: str,
    prefix: str = EVENT_PREFIX,
    suffix: str = EVENT_SUFFIX
) -> None:
    """
    Configure S3 bucket notification to trigger Lambda function.

    Args:
        bucket_name: Name of the S3 bucket
        lambda_arn: ARN of the Lambda function
        prefix: S3 key prefix filter
        suffix: S3 key suffix filter
    """
    # Get existing notification configuration
    try:
        response = s3_client.get_bucket_notification_configuration(Bucket=bucket_name)
        existing_config = response
    except s3_client.exceptions.NoSuchBucket:
        raise Exception(f"S3 bucket '{bucket_name}' not found")
    except Exception:
        existing_config = {}

    # Remove ResponseMetadata if present
    existing_config.pop('ResponseMetadata', None)

    # Get existing Lambda configurations
    lambda_configs = existing_config.get('LambdaFunctionConfigurations', [])

    # Check if configuration already exists
    notification_id = f'{FUNCTION_NAME}-notification'
    config_exists = False

    for config in lambda_configs:
        if config.get('Id') == notification_id:
            config_exists = True
            print(f"Updating existing notification configuration: {notification_id}")
            # Update existing configuration
            config['LambdaFunctionArn'] = lambda_arn
            config['Events'] = ['s3:ObjectCreated:*']
            config['Filter'] = {
                'Key': {
                    'FilterRules': [
                        {'Name': 'prefix', 'Value': prefix},
                        {'Name': 'suffix', 'Value': suffix}
                    ]
                }
            }
            break

    # Add new configuration if it doesn't exist
    if not config_exists:
        print(f"Creating new notification configuration: {notification_id}")
        new_config = {
            'Id': notification_id,
            'LambdaFunctionArn': lambda_arn,
            'Events': ['s3:ObjectCreated:*'],
            'Filter': {
                'Key': {
                    'FilterRules': [
                        {'Name': 'prefix', 'Value': prefix},
                        {'Name': 'suffix', 'Value': suffix}
                    ]
                }
            }
        }
        lambda_configs.append(new_config)

    # Update notification configuration
    existing_config['LambdaFunctionConfigurations'] = lambda_configs

    # Apply the configuration
    s3_client.put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration=existing_config
    )

    print(f"Successfully configured S3 notification for bucket: {bucket_name}")


def verify_configuration(bucket_name: str) -> None:
    """
    Verify the S3 notification configuration.

    Args:
        bucket_name: Name of the S3 bucket
    """
    response = s3_client.get_bucket_notification_configuration(Bucket=bucket_name)

    lambda_configs = response.get('LambdaFunctionConfigurations', [])

    if not lambda_configs:
        print("Warning: No Lambda function configurations found!")
        return

    print("\nCurrent S3 Notification Configurations:")
    print("=" * 60)

    for config in lambda_configs:
        print(f"\nConfiguration ID: {config.get('Id')}")
        print(f"Lambda ARN: {config.get('LambdaFunctionArn')}")
        print(f"Events: {', '.join(config.get('Events', []))}")

        filter_rules = config.get('Filter', {}).get('Key', {}).get('FilterRules', [])
        for rule in filter_rules:
            print(f"{rule['Name'].capitalize()}: {rule['Value']}")


def main():
    """Main configuration function."""
    parser = argparse.ArgumentParser(
        description='Configure S3 event notifications for Lambda function'
    )
    parser.add_argument(
        'bucket_name',
        help='Name of the S3 bucket (e.g., de-intern-2024-datalake-dev)'
    )
    parser.add_argument(
        '--function-name',
        default=FUNCTION_NAME,
        help=f'Lambda function name (default: {FUNCTION_NAME})'
    )
    parser.add_argument(
        '--prefix',
        default=EVENT_PREFIX,
        help=f'S3 key prefix filter (default: {EVENT_PREFIX})'
    )
    parser.add_argument(
        '--suffix',
        default=EVENT_SUFFIX,
        help=f'S3 key suffix filter (default: {EVENT_SUFFIX})'
    )
    parser.add_argument(
        '--region',
        help='AWS region (default: from AWS CLI config)',
        default=None
    )
    parser.add_argument(
        '--verify-only',
        action='store_true',
        help='Only verify existing configuration without making changes'
    )

    args = parser.parse_args()

    # Set region if provided
    if args.region:
        global s3_client, lambda_client
        s3_client = boto3.client('s3', region_name=args.region)
        lambda_client = boto3.client('lambda', region_name=args.region)

    try:
        # Verify only mode
        if args.verify_only:
            print(f"Verifying S3 notification configuration for bucket: {args.bucket_name}")
            verify_configuration(args.bucket_name)
            return 0

        # Step 1: Get Lambda function ARN
        print("\n" + "=" * 60)
        print("Step 1: Getting Lambda function ARN")
        print("=" * 60)
        lambda_arn = get_lambda_arn(args.function_name)
        print(f"Lambda ARN: {lambda_arn}")

        # Step 2: Add Lambda permission
        print("\n" + "=" * 60)
        print("Step 2: Adding Lambda invoke permission")
        print("=" * 60)
        add_lambda_permission(args.function_name, args.bucket_name)

        # Step 3: Configure S3 notification
        print("\n" + "=" * 60)
        print("Step 3: Configuring S3 bucket notification")
        print("=" * 60)
        configure_s3_notification(
            args.bucket_name,
            lambda_arn,
            args.prefix,
            args.suffix
        )

        # Step 4: Verify configuration
        print("\n" + "=" * 60)
        print("Step 4: Verifying configuration")
        print("=" * 60)
        verify_configuration(args.bucket_name)

        # Print success message
        print("\n" + "=" * 60)
        print("Configuration successful!")
        print("=" * 60)
        print(f"Bucket: {args.bucket_name}")
        print(f"Lambda: {args.function_name}")
        print(f"Trigger: s3:ObjectCreated:* events")
        print(f"Filter: prefix='{args.prefix}', suffix='{args.suffix}'")
        print("\nTest the configuration:")
        print(f"  aws s3 cp test.parquet s3://{args.bucket_name}/{args.prefix}test.parquet")
        print(f"Then check CloudWatch Logs for function: {args.function_name}")

        return 0

    except Exception as e:
        print(f"\nError during configuration: {str(e)}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
