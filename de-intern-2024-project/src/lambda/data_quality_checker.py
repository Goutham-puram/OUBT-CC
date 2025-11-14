"""
Lambda function for data quality checks.

Validates data quality of processed files before moving to curated zone.
"""

import json
import boto3
import pandas as pd
from io import BytesIO
from typing import Dict, Any, List
import os

# Initialize AWS clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

# Environment variables
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', '')
MIN_ROWS = int(os.environ.get('MIN_ROWS', '1000'))


def check_data_quality(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Perform data quality checks on DataFrame.

    Args:
        df: DataFrame to validate

    Returns:
        Dictionary with validation results.
    """
    issues = []
    warnings = []

    # Check 1: Minimum row count
    if len(df) < MIN_ROWS:
        issues.append(f"Insufficient rows: {len(df)} < {MIN_ROWS}")

    # Check 2: Required columns
    required_columns = [
        'tpep_pickup_datetime',
        'tpep_dropoff_datetime',
        'passenger_count',
        'trip_distance',
        'fare_amount'
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        issues.append(f"Missing required columns: {missing_columns}")

    # Check 3: Null values in critical columns
    if not missing_columns:  # Only if columns exist
        for col in required_columns:
            null_count = df[col].isnull().sum()
            null_pct = (null_count / len(df)) * 100
            if null_pct > 5:  # More than 5% nulls
                warnings.append(f"High null percentage in {col}: {null_pct:.2f}%")

    # Check 4: Data ranges
    if 'trip_distance' in df.columns:
        if (df['trip_distance'] < 0).any():
            issues.append("Found negative trip distances")
        if (df['trip_distance'] > 100).any():
            warnings.append("Found unusually long trips (>100 miles)")

    if 'fare_amount' in df.columns:
        if (df['fare_amount'] < 0).any():
            issues.append("Found negative fare amounts")
        if (df['fare_amount'] > 500).any():
            warnings.append("Found unusually high fares (>$500)")

    # Check 5: Datetime validity
    if 'tpep_pickup_datetime' in df.columns and 'tpep_dropoff_datetime' in df.columns:
        try:
            pickup = pd.to_datetime(df['tpep_pickup_datetime'])
            dropoff = pd.to_datetime(df['tpep_dropoff_datetime'])
            if (dropoff < pickup).any():
                issues.append("Found trips where dropoff is before pickup")
        except Exception as e:
            issues.append(f"Invalid datetime format: {str(e)}")

    return {
        'passed': len(issues) == 0,
        'row_count': len(df),
        'column_count': len(df.columns),
        'issues': issues,
        'warnings': warnings
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for data quality checks.

    Args:
        event: Event with S3 file information
        context: Lambda context

    Returns:
        Response with validation results.
    """
    print(f"Received event: {json.dumps(event)}")

    try:
        bucket = event['bucket']
        key = event['key']

        print(f"Checking data quality for: s3://{bucket}/{key}")

        # Download file from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()

        # Load into DataFrame
        df = pd.read_parquet(BytesIO(content))

        # Run quality checks
        results = check_data_quality(df)

        print(f"Quality check results: {json.dumps(results)}")

        # Send notification if there are issues
        if not results['passed'] and SNS_TOPIC_ARN:
            message = f"""
Data Quality Check Failed

File: s3://{bucket}/{key}
Issues: {results['issues']}
Warnings: {results['warnings']}
            """
            sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject='Data Quality Check Failed',
                Message=message
            )

        return {
            'statusCode': 200 if results['passed'] else 400,
            'body': json.dumps(results)
        }

    except Exception as e:
        print(f"Error in quality check: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
