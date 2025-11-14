"""
Lambda function to handle S3 events.

Triggered when new data is uploaded to the raw S3 bucket.
Starts Glue crawler to catalog the new data.
"""

import json
import boto3
import os
from typing import Dict, Any

# Initialize AWS clients
glue_client = boto3.client('glue')
s3_client = boto3.client('s3')

# Environment variables
CRAWLER_NAME = os.environ.get('CRAWLER_NAME', 'de-intern-2024-raw-taxi-crawler')
GLUE_DATABASE = os.environ.get('GLUE_DATABASE', 'de_intern_2024_catalog')


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for S3 events.

    Args:
        event: S3 event data
        context: Lambda context

    Returns:
        Response dictionary with status.
    """
    print(f"Received event: {json.dumps(event)}")

    try:
        # Parse S3 event
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']

            print(f"Processing file: s3://{bucket}/{key}")

            # Check if it's a parquet file
            if not key.endswith('.parquet'):
                print(f"Skipping non-parquet file: {key}")
                continue

            # Start Glue Crawler
            print(f"Starting Glue crawler: {CRAWLER_NAME}")
            try:
                response = glue_client.start_crawler(Name=CRAWLER_NAME)
                print(f"Crawler started successfully: {response}")
            except glue_client.exceptions.CrawlerRunningException:
                print(f"Crawler {CRAWLER_NAME} is already running")

        return {
            'statusCode': 200,
            'body': json.dumps('Successfully processed S3 event')
        }

    except Exception as e:
        print(f"Error processing event: {str(e)}")
        raise
