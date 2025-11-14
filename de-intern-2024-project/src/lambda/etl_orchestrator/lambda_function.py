"""
Lambda function for ETL orchestration using AWS Step Functions.

Updated from Week 2 notification function to trigger Step Functions workflow
for comprehensive ETL pipeline orchestration. This function:
- Receives S3 ObjectCreated events from raw/taxi/ prefix
- Validates and extracts S3 event metadata
- Triggers Step Functions state machine for ETL orchestration
- Handles errors and provides detailed logging
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError

# Configure structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
stepfunctions_client = boto3.client('stepfunctions')

# Environment variables
STATE_MACHINE_ARN = os.environ.get('STATE_MACHINE_ARN', '')
ENABLE_ORCHESTRATION = os.environ.get('ENABLE_ORCHESTRATION', 'true').lower() == 'true'


def validate_s3_event(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Validate and extract metadata from S3 event record.

    Args:
        record: S3 event record

    Returns:
        Dictionary with validated metadata, None if validation fails
    """
    try:
        # Extract S3 event information
        event_name = record.get('eventName', '')
        if not event_name.startswith('ObjectCreated'):
            logger.warning(f"Skipping non-creation event: {event_name}")
            return None

        # Extract S3 object metadata
        s3_info = record.get('s3', {})
        bucket_name = s3_info.get('bucket', {}).get('name', '')
        object_info = s3_info.get('object', {})
        object_key = object_info.get('key', '')
        object_size = object_info.get('size', 0)
        event_time = record.get('eventTime', datetime.utcnow().isoformat())

        # Validate required fields
        if not bucket_name or not object_key:
            logger.error("Missing required S3 metadata: bucket or key")
            return None

        # Validate that file is in the correct prefix
        if not object_key.startswith('raw/taxi/'):
            logger.info(f"Skipping file not in raw/taxi/ prefix: {object_key}")
            return None

        # Validate file type (should be Parquet or CSV)
        valid_extensions = ['.parquet', '.csv', '.csv.gz']
        if not any(object_key.lower().endswith(ext) for ext in valid_extensions):
            logger.warning(f"Skipping unsupported file type: {object_key}")
            return None

        # Return validated metadata
        metadata = {
            'bucket': bucket_name,
            'key': object_key,
            'size': object_size,
            'size_mb': round(object_size / (1024 * 1024), 2),
            'eventTime': event_time,
            'eventName': event_name
        }

        logger.info(f"Validated S3 event: s3://{bucket_name}/{object_key} ({metadata['size_mb']} MB)")
        return metadata

    except Exception as e:
        logger.error(f"Error validating S3 event: {str(e)}", exc_info=True)
        return None


def trigger_step_functions(metadata: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Trigger Step Functions state machine for ETL orchestration.

    Args:
        metadata: Validated S3 event metadata
        context: Lambda context object

    Returns:
        Response dictionary with execution details
    """
    try:
        # Prepare input for Step Functions
        execution_input = {
            'bucket': metadata['bucket'],
            'key': metadata['key'],
            'size': metadata['size'],
            'eventTime': metadata['eventTime'],
            'triggeredBy': 'lambda',
            'lambdaRequestId': context.request_id,
            'lambdaFunctionName': context.function_name
        }

        # Generate unique execution name
        timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        file_name = metadata['key'].split('/')[-1].replace('.', '-')[:50]
        execution_name = f"etl-{timestamp}-{file_name}"

        logger.info(f"Starting Step Functions execution: {execution_name}")
        logger.info(f"State Machine ARN: {STATE_MACHINE_ARN}")
        logger.info(f"Input: {json.dumps(execution_input, indent=2)}")

        # Start Step Functions execution
        response = stepfunctions_client.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=execution_name,
            input=json.dumps(execution_input)
        )

        execution_arn = response['executionArn']
        started_at = response['startDate'].isoformat()

        logger.info(f"Step Functions execution started successfully")
        logger.info(f"Execution ARN: {execution_arn}")
        logger.info(f"Started at: {started_at}")

        return {
            'statusCode': 200,
            'executionArn': execution_arn,
            'executionName': execution_name,
            'startDate': started_at,
            'message': 'ETL orchestration workflow triggered successfully'
        }

    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        error_message = e.response.get('Error', {}).get('Message', '')

        logger.error(f"AWS API error triggering Step Functions: {error_code} - {error_message}")

        if error_code == 'ExecutionAlreadyExists':
            logger.warning("Execution already exists, treating as success")
            return {
                'statusCode': 200,
                'message': 'Execution already in progress',
                'executionName': execution_name
            }

        raise

    except Exception as e:
        logger.error(f"Unexpected error triggering Step Functions: {str(e)}", exc_info=True)
        raise


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for ETL orchestration via Step Functions.

    This function is triggered when new files are uploaded to S3 with the
    raw/taxi/ prefix. It validates the event, extracts metadata, and triggers
    a Step Functions state machine to orchestrate the ETL pipeline.

    Args:
        event: S3 event data containing Records with S3 object information
        context: Lambda context object with runtime information

    Returns:
        Response dictionary with statusCode and processing results
    """
    logger.info("=" * 80)
    logger.info("ETL Orchestrator Lambda - Processing S3 Event")
    logger.info("=" * 80)
    logger.info(f"Received S3 event with {len(event.get('Records', []))} record(s)")
    logger.info(f"Orchestration enabled: {ENABLE_ORCHESTRATION}")

    # Validate environment configuration
    if ENABLE_ORCHESTRATION and not STATE_MACHINE_ARN:
        error_msg = "STATE_MACHINE_ARN environment variable not set"
        logger.error(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Configuration error',
                'error': error_msg
            })
        }

    processed_files = []
    triggered_executions = []
    errors = []

    try:
        # Process each S3 event record
        for i, record in enumerate(event.get('Records', []), 1):
            logger.info(f"\n[Record {i}/{len(event.get('Records', []))}]")

            # Validate S3 event
            metadata = validate_s3_event(record)
            if not metadata:
                logger.info("Skipping invalid or irrelevant S3 event")
                continue

            # Log structured notification (backward compatible with Week 2)
            notification = {
                'event': 'S3_FILE_UPLOADED',
                'timestamp': metadata['eventTime'],
                'bucket': metadata['bucket'],
                'key': metadata['key'],
                'size_bytes': metadata['size'],
                'size_mb': metadata['size_mb'],
                'event_type': metadata['eventName'],
                'request_id': context.request_id,
                'function_name': context.function_name
            }
            logger.info(f"S3 File Notification: {json.dumps(notification, indent=2)}")

            # Add to processed files list
            processed_files.append({
                'bucket': metadata['bucket'],
                'key': metadata['key'],
                'size_bytes': metadata['size']
            })

            # Trigger Step Functions orchestration
            if ENABLE_ORCHESTRATION:
                try:
                    execution_result = trigger_step_functions(metadata, context)
                    triggered_executions.append({
                        'file': f"s3://{metadata['bucket']}/{metadata['key']}",
                        'executionArn': execution_result.get('executionArn'),
                        'executionName': execution_result.get('executionName'),
                        'status': 'triggered'
                    })
                    logger.info(f"✓ Successfully triggered orchestration for: {metadata['key']}")

                except Exception as e:
                    error_msg = f"Failed to trigger Step Functions for {metadata['key']}: {str(e)}"
                    logger.error(error_msg, exc_info=True)
                    errors.append({
                        'file': metadata['key'],
                        'error': str(e)
                    })
            else:
                logger.info("Orchestration disabled - notification only mode")

        # Prepare response
        response = {
            'statusCode': 200 if not errors else 207,  # 207 = Multi-Status
            'body': json.dumps({
                'message': 'S3 events processed',
                'files_processed': len(processed_files),
                'orchestrations_triggered': len(triggered_executions),
                'errors': len(errors),
                'files': processed_files,
                'executions': triggered_executions,
                'failed': errors if errors else None
            }, default=str)
        }

        # Log summary
        logger.info("\n" + "=" * 80)
        logger.info("Processing Summary")
        logger.info("=" * 80)
        logger.info(f"Files processed: {len(processed_files)}")
        logger.info(f"Orchestrations triggered: {len(triggered_executions)}")
        logger.info(f"Errors: {len(errors)}")

        if triggered_executions:
            logger.info("\nTriggered Executions:")
            for exec_info in triggered_executions:
                logger.info(f"  - {exec_info['executionName']}")
                logger.info(f"    File: {exec_info['file']}")
                if exec_info.get('executionArn'):
                    logger.info(f"    ARN: {exec_info['executionArn']}")

        if errors:
            logger.error("\nErrors encountered:")
            for error_info in errors:
                logger.error(f"  - {error_info['file']}: {error_info['error']}")

        logger.info("=" * 80)

        return response

    except Exception as e:
        # Log error with full details
        error_message = f"Critical error processing S3 events: {str(e)}"
        logger.error(error_message, exc_info=True)

        # Return error response
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error processing S3 notifications',
                'error': str(e),
                'files_processed': len(processed_files),
                'orchestrations_triggered': len(triggered_executions)
            })
        }


# For local testing
if __name__ == '__main__':
    # Sample test event
    test_event = {
        'Records': [
            {
                'eventName': 'ObjectCreated:Put',
                'eventTime': '2024-01-15T10:30:00.000Z',
                's3': {
                    'bucket': {
                        'name': '123456789012-oubt-datalake'
                    },
                    'object': {
                        'key': 'raw/taxi/yellow_tripdata_2024-01.parquet',
                        'size': 52428800
                    }
                }
            }
        ]
    }

    # Mock context
    class MockContext:
        request_id = 'test-request-123'
        function_name = 'etl-orchestrator-test'

    # Set test environment
    os.environ['STATE_MACHINE_ARN'] = 'arn:aws:states:us-east-1:123456789012:stateMachine:etl-pipeline-workflow'
    os.environ['ENABLE_ORCHESTRATION'] = 'false'  # Set to 'false' for local testing

    result = lambda_handler(test_event, MockContext())
    print(json.dumps(result, indent=2, default=str))
