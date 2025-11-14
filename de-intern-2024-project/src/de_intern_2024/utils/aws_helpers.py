"""AWS helper functions using Boto3."""

from typing import Any, Optional
import boto3
from botocore.exceptions import ClientError

from ..config import config
from .logger import get_logger

logger = get_logger(__name__)


def get_boto3_client(service_name: str, region: Optional[str] = None) -> Any:
    """
    Get a Boto3 client for the specified AWS service.

    Args:
        service_name: AWS service name (e.g., 's3', 'glue', 'rds')
        region: AWS region. If None, uses config default.

    Returns:
        Boto3 client instance.
    """
    region = region or config.aws.region
    logger.debug(f"Creating Boto3 client for {service_name} in {region}")
    return boto3.client(service_name, region_name=region)


def get_boto3_resource(service_name: str, region: Optional[str] = None) -> Any:
    """
    Get a Boto3 resource for the specified AWS service.

    Args:
        service_name: AWS service name (e.g., 's3', 'dynamodb')
        region: AWS region. If None, uses config default.

    Returns:
        Boto3 resource instance.
    """
    region = region or config.aws.region
    logger.debug(f"Creating Boto3 resource for {service_name} in {region}")
    return boto3.resource(service_name, region_name=region)


def upload_to_s3(
    file_path: str,
    bucket: str,
    key: str,
    extra_args: Optional[dict] = None
) -> bool:
    """
    Upload a file to S3.

    Args:
        file_path: Local file path
        bucket: S3 bucket name
        key: S3 object key
        extra_args: Extra arguments for upload (e.g., metadata, ACL)

    Returns:
        True if successful, False otherwise.
    """
    try:
        s3_client = get_boto3_client('s3')
        logger.info(f"Uploading {file_path} to s3://{bucket}/{key}")
        s3_client.upload_file(file_path, bucket, key, ExtraArgs=extra_args)
        logger.info(f"Successfully uploaded to s3://{bucket}/{key}")
        return True
    except ClientError as e:
        logger.error(f"Failed to upload to S3: {e}")
        return False


def download_from_s3(
    bucket: str,
    key: str,
    file_path: str
) -> bool:
    """
    Download a file from S3.

    Args:
        bucket: S3 bucket name
        key: S3 object key
        file_path: Local file path to save to

    Returns:
        True if successful, False otherwise.
    """
    try:
        s3_client = get_boto3_client('s3')
        logger.info(f"Downloading s3://{bucket}/{key} to {file_path}")
        s3_client.download_file(bucket, key, file_path)
        logger.info(f"Successfully downloaded to {file_path}")
        return True
    except ClientError as e:
        logger.error(f"Failed to download from S3: {e}")
        return False


def list_s3_objects(bucket: str, prefix: str = "") -> list:
    """
    List objects in an S3 bucket with optional prefix.

    Args:
        bucket: S3 bucket name
        prefix: Object key prefix filter

    Returns:
        List of object keys.
    """
    try:
        s3_client = get_boto3_client('s3')
        logger.info(f"Listing objects in s3://{bucket}/{prefix}")

        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        objects = []
        for page in pages:
            if 'Contents' in page:
                objects.extend([obj['Key'] for obj in page['Contents']])

        logger.info(f"Found {len(objects)} objects")
        return objects
    except ClientError as e:
        logger.error(f"Failed to list S3 objects: {e}")
        return []


def check_s3_bucket_exists(bucket: str) -> bool:
    """
    Check if an S3 bucket exists and is accessible.

    Args:
        bucket: S3 bucket name

    Returns:
        True if bucket exists and is accessible, False otherwise.
    """
    try:
        s3_client = get_boto3_client('s3')
        s3_client.head_bucket(Bucket=bucket)
        logger.info(f"Bucket {bucket} exists and is accessible")
        return True
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == '404':
            logger.warning(f"Bucket {bucket} does not exist")
        else:
            logger.error(f"Error checking bucket {bucket}: {e}")
        return False
