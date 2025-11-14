"""
Lambda function for S3 event notifications (Week 2 scope).

Triggered by S3 ObjectCreated events in raw/taxi/ prefix.
Logs structured notification to CloudWatch with file metadata.
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any

# Configure structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for S3 event notifications.

    This function is triggered when new files are uploaded to S3 with the
    raw/taxi/ prefix. It extracts metadata and logs structured information
    to CloudWatch for monitoring and audit purposes.

    Args:
        event: S3 event data containing Records with S3 object information
        context: Lambda context object with runtime information

    Returns:
        Response dictionary with statusCode and processing results
    """
    logger.info(f"Received S3 event with {len(event.get('Records', []))} record(s)")

    processed_files = []

    try:
        # Process each S3 event record
        for record in event.get('Records', []):
            # Extract S3 event information
            event_name = record.get('eventName', 'Unknown')
            event_time = record.get('eventTime', datetime.utcnow().isoformat())

            # Extract S3 object metadata
            s3_info = record.get('s3', {})
            bucket_name = s3_info.get('bucket', {}).get('name', 'Unknown')
            object_info = s3_info.get('object', {})
            object_key = object_info.get('key', 'Unknown')
            object_size = object_info.get('size', 0)

            # Log structured notification
            notification = {
                'event': 'S3_FILE_UPLOADED',
                'timestamp': event_time,
                'bucket': bucket_name,
                'key': object_key,
                'size_bytes': object_size,
                'size_mb': round(object_size / (1024 * 1024), 2),
                'event_type': event_name,
                'request_id': context.request_id,
                'function_name': context.function_name
            }

            # Log the structured notification
            logger.info(f"S3 File Notification: {json.dumps(notification, indent=2)}")

            # Add to processed files list
            processed_files.append({
                'bucket': bucket_name,
                'key': object_key,
                'size_bytes': object_size
            })

            # Log summary for this file
            logger.info(
                f"Processed file upload: s3://{bucket_name}/{object_key} "
                f"(Size: {notification['size_mb']} MB, Time: {event_time})"
            )

        # Return success response
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'S3 notifications processed successfully',
                'files_processed': len(processed_files),
                'files': processed_files
            })
        }

        logger.info(f"Successfully processed {len(processed_files)} file(s)")
        return response

    except Exception as e:
        # Log error with full details
        error_message = f"Error processing S3 event: {str(e)}"
        logger.error(error_message, exc_info=True)

        # Return error response
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error processing S3 notifications',
                'error': str(e)
            })
        }
