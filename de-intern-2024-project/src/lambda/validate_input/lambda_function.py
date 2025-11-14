"""
Lambda function for validating S3 event input in Step Functions workflow.

This function is invoked by Step Functions to validate S3 event data
before triggering the Glue ETL job.
"""

import json
import logging
from typing import Dict, Any
import boto3
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')


def validate_file_exists(bucket: str, key: str) -> bool:
    """
    Verify that the S3 object exists.

    Args:
        bucket: S3 bucket name
        key: S3 object key

    Returns:
        True if file exists, False otherwise
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        logger.info(f"Validated file exists: s3://{bucket}/{key}")
        return True
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == '404':
            logger.error(f"File not found: s3://{bucket}/{key}")
        else:
            logger.error(f"Error checking file existence: {e}")
        return False


def validate_file_size(size: int, min_size: int = 100, max_size: int = 10 * 1024 * 1024 * 1024) -> tuple:
    """
    Validate file size is within acceptable range.

    Args:
        size: File size in bytes
        min_size: Minimum acceptable size in bytes (default: 100 bytes)
        max_size: Maximum acceptable size in bytes (default: 10 GB)

    Returns:
        Tuple of (is_valid: bool, message: str)
    """
    if size < min_size:
        return False, f"File too small: {size} bytes (minimum: {min_size} bytes)"
    if size > max_size:
        return False, f"File too large: {size} bytes (maximum: {max_size} bytes)"
    return True, f"File size valid: {round(size / (1024 * 1024), 2)} MB"


def validate_file_type(key: str) -> tuple:
    """
    Validate file type based on extension.

    Args:
        key: S3 object key

    Returns:
        Tuple of (is_valid: bool, message: str)
    """
    valid_extensions = ['.parquet', '.csv', '.csv.gz']
    key_lower = key.lower()

    for ext in valid_extensions:
        if key_lower.endswith(ext):
            return True, f"Valid file type: {ext}"

    return False, f"Unsupported file type. Supported: {', '.join(valid_extensions)}"


def validate_prefix(key: str, required_prefix: str = 'raw/taxi/') -> tuple:
    """
    Validate that file is in the correct S3 prefix.

    Args:
        key: S3 object key
        required_prefix: Required prefix path

    Returns:
        Tuple of (is_valid: bool, message: str)
    """
    if key.startswith(required_prefix):
        return True, f"Valid prefix: {required_prefix}"
    return False, f"Invalid prefix. Expected: {required_prefix}, Got: {key[:20]}..."


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for validating S3 event input.

    Args:
        event: Input from Step Functions with bucket, key, size, eventTime
        context: Lambda context object

    Returns:
        Dictionary with validation results
    """
    logger.info("=" * 80)
    logger.info("Input Validation Lambda")
    logger.info("=" * 80)
    logger.info(f"Input: {json.dumps(event, indent=2)}")

    try:
        # Extract input parameters
        bucket = event.get('bucket', '')
        key = event.get('key', '')
        size = event.get('size', 0)
        event_time = event.get('eventTime', '')

        # Validation results
        validations = []
        is_valid = True

        # 1. Validate required fields present
        if not bucket or not key:
            logger.error("Missing required fields: bucket or key")
            return {
                'isValid': False,
                'message': 'Missing required fields: bucket or key',
                'metadata': {}
            }

        logger.info(f"Validating: s3://{bucket}/{key}")

        # 2. Validate file prefix
        prefix_valid, prefix_msg = validate_prefix(key)
        validations.append({'check': 'prefix', 'valid': prefix_valid, 'message': prefix_msg})
        if not prefix_valid:
            is_valid = False

        # 3. Validate file type
        type_valid, type_msg = validate_file_type(key)
        validations.append({'check': 'file_type', 'valid': type_valid, 'message': type_msg})
        if not type_valid:
            is_valid = False

        # 4. Validate file size
        size_valid, size_msg = validate_file_size(size)
        validations.append({'check': 'file_size', 'valid': size_valid, 'message': size_msg})
        if not size_valid:
            is_valid = False

        # 5. Validate file exists (only if other validations passed)
        if is_valid:
            file_exists = validate_file_exists(bucket, key)
            validations.append({'check': 'file_exists', 'valid': file_exists, 'message': 'File exists in S3'})
            if not file_exists:
                is_valid = False

        # Prepare metadata
        metadata = {
            'bucket': bucket,
            'key': key,
            'size': size,
            'size_mb': round(size / (1024 * 1024), 2),
            'event_time': event_time,
            'file_name': key.split('/')[-1],
            'validations': validations
        }

        # Prepare response
        if is_valid:
            message = "All validations passed"
            logger.info(f"✓ Validation PASSED for: s3://{bucket}/{key}")
        else:
            failed_checks = [v['message'] for v in validations if not v['valid']]
            message = f"Validation failed: {'; '.join(failed_checks)}"
            logger.warning(f"✗ Validation FAILED for: s3://{bucket}/{key}")
            logger.warning(f"Failed checks: {failed_checks}")

        # Log validation summary
        logger.info("\nValidation Results:")
        for validation in validations:
            status = "✓" if validation['valid'] else "✗"
            logger.info(f"  {status} {validation['check']}: {validation['message']}")

        response = {
            'isValid': is_valid,
            'message': message,
            'metadata': metadata
        }

        logger.info(f"\nFinal Result: {'VALID' if is_valid else 'INVALID'}")
        logger.info("=" * 80)

        return response

    except Exception as e:
        error_msg = f"Error during validation: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            'isValid': False,
            'message': error_msg,
            'metadata': {
                'error': str(e)
            }
        }


# For local testing
if __name__ == '__main__':
    # Sample test event
    test_event = {
        'bucket': '123456789012-oubt-datalake',
        'key': 'raw/taxi/yellow_tripdata_2024-01.parquet',
        'size': 52428800,
        'eventTime': '2024-01-15T10:30:00.000Z'
    }

    # Mock context
    class MockContext:
        request_id = 'test-request-123'
        function_name = 'validate-input-test'

    result = lambda_handler(test_event, MockContext())
    print(json.dumps(result, indent=2, default=str))
