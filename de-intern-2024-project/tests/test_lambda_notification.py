"""
Unit tests for S3 notification Lambda function.

Tests verify:
1. Correct parsing of S3 events
2. Structured logging to CloudWatch
3. Error handling and edge cases
"""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Import Lambda function
import sys
from pathlib import Path

# Add src directory to path for imports
src_path = Path(__file__).parent.parent / 'src' / 'lambda' / 's3_notification'
sys.path.insert(0, str(src_path))

from lambda_function import lambda_handler


@pytest.fixture
def mock_context():
    """Mock Lambda context object."""
    context = Mock()
    context.request_id = 'test-request-id-12345'
    context.function_name = 's3-file-notification'
    context.invoked_function_arn = 'arn:aws:lambda:us-east-1:123456789012:function:s3-file-notification'
    context.memory_limit_in_mb = 128
    context.aws_request_id = 'test-request-id-12345'
    return context


@pytest.fixture
def sample_s3_event():
    """Sample S3 ObjectCreated event."""
    return {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventTime": "2024-11-14T10:30:00.000Z",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "s3-file-notification",
                    "bucket": {
                        "name": "de-intern-2024-datalake-dev",
                        "ownerIdentity": {
                            "principalId": "EXAMPLE"
                        },
                        "arn": "arn:aws:s3:::de-intern-2024-datalake-dev"
                    },
                    "object": {
                        "key": "raw/taxi/yellow_tripdata_2024_01.parquet",
                        "size": 52428800,
                        "eTag": "0123456789abcdef0123456789abcdef",
                        "sequencer": "0A1B2C3D4E5F678901"
                    }
                }
            }
        ]
    }


@pytest.fixture
def multi_file_s3_event():
    """S3 event with multiple file uploads."""
    return {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "eventTime": "2024-11-14T10:30:00.000Z",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {
                        "name": "de-intern-2024-datalake-dev"
                    },
                    "object": {
                        "key": "raw/taxi/yellow_tripdata_2024_01.parquet",
                        "size": 52428800
                    }
                }
            },
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "eventTime": "2024-11-14T10:31:00.000Z",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {
                        "name": "de-intern-2024-datalake-dev"
                    },
                    "object": {
                        "key": "raw/taxi/yellow_tripdata_2024_02.parquet",
                        "size": 104857600
                    }
                }
            }
        ]
    }


class TestLambdaNotificationBasic:
    """Basic functionality tests for Lambda notification handler."""

    @patch('lambda_function.logger')
    def test_successful_single_file_notification(self, mock_logger, sample_s3_event, mock_context):
        """Test successful processing of single file notification."""
        # Execute Lambda handler
        response = lambda_handler(sample_s3_event, mock_context)

        # Verify response
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['message'] == 'S3 notifications processed successfully'
        assert body['files_processed'] == 1
        assert len(body['files']) == 1

        # Verify file details
        file_info = body['files'][0]
        assert file_info['bucket'] == 'de-intern-2024-datalake-dev'
        assert file_info['key'] == 'raw/taxi/yellow_tripdata_2024_01.parquet'
        assert file_info['size_bytes'] == 52428800

        # Verify logging was called
        assert mock_logger.info.call_count >= 3

    @patch('lambda_function.logger')
    def test_multiple_files_notification(self, mock_logger, multi_file_s3_event, mock_context):
        """Test processing of multiple file notifications in single event."""
        # Execute Lambda handler
        response = lambda_handler(multi_file_s3_event, mock_context)

        # Verify response
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['files_processed'] == 2
        assert len(body['files']) == 2

        # Verify both files are processed
        assert body['files'][0]['key'] == 'raw/taxi/yellow_tripdata_2024_01.parquet'
        assert body['files'][1]['key'] == 'raw/taxi/yellow_tripdata_2024_02.parquet'

    @patch('lambda_function.logger')
    def test_empty_records_list(self, mock_logger, mock_context):
        """Test handling of event with no records."""
        event = {"Records": []}

        response = lambda_handler(event, mock_context)

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['files_processed'] == 0
        assert body['files'] == []


class TestLambdaNotificationLogging:
    """Tests for CloudWatch logging behavior."""

    @patch('lambda_function.logger')
    def test_structured_log_format(self, mock_logger, sample_s3_event, mock_context):
        """Verify structured notification log contains required fields."""
        lambda_handler(sample_s3_event, mock_context)

        # Find the structured notification log call
        info_calls = [call for call in mock_logger.info.call_args_list]
        notification_log = None

        for call in info_calls:
            log_message = call[0][0]
            if 'S3 File Notification:' in log_message:
                # Extract JSON from log message
                json_start = log_message.index('{')
                notification_log = json.loads(log_message[json_start:])
                break

        assert notification_log is not None, "Structured notification log not found"

        # Verify required fields
        assert notification_log['event'] == 'S3_FILE_UPLOADED'
        assert notification_log['bucket'] == 'de-intern-2024-datalake-dev'
        assert notification_log['key'] == 'raw/taxi/yellow_tripdata_2024_01.parquet'
        assert notification_log['size_bytes'] == 52428800
        assert notification_log['size_mb'] == 50.0  # 52428800 bytes = 50 MB
        assert 'timestamp' in notification_log
        assert 'event_type' in notification_log
        assert notification_log['request_id'] == 'test-request-id-12345'
        assert notification_log['function_name'] == 's3-file-notification'

    @patch('lambda_function.logger')
    def test_file_metadata_logging(self, mock_logger, sample_s3_event, mock_context):
        """Test that file metadata is properly logged."""
        lambda_handler(sample_s3_event, mock_context)

        # Verify initial event log
        initial_call = mock_logger.info.call_args_list[0]
        assert 'Received S3 event with 1 record(s)' in initial_call[0][0]

        # Verify summary log
        summary_calls = [call for call in mock_logger.info.call_args_list]
        summary_found = False

        for call in summary_calls:
            log_message = call[0][0]
            if 'Processed file upload:' in log_message:
                assert 's3://de-intern-2024-datalake-dev/raw/taxi/yellow_tripdata_2024_01.parquet' in log_message
                assert 'Size: 50.0 MB' in log_message
                summary_found = True
                break

        assert summary_found, "Summary log not found"

    @patch('lambda_function.logger')
    def test_success_summary_logging(self, mock_logger, multi_file_s3_event, mock_context):
        """Test that success summary is logged correctly."""
        lambda_handler(multi_file_s3_event, mock_context)

        # Find success summary log
        success_found = False
        for call in mock_logger.info.call_args_list:
            if 'Successfully processed 2 file(s)' in call[0][0]:
                success_found = True
                break

        assert success_found, "Success summary log not found"


class TestLambdaNotificationErrorHandling:
    """Tests for error handling and edge cases."""

    @patch('lambda_function.logger')
    def test_malformed_event_error_handling(self, mock_logger, mock_context):
        """Test handling of malformed event data."""
        # Event missing critical fields
        malformed_event = {
            "Records": [
                {
                    "eventName": "ObjectCreated:Put"
                    # Missing s3 field
                }
            ]
        }

        response = lambda_handler(malformed_event, mock_context)

        # Should return error response
        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert body['message'] == 'Error processing S3 notifications'
        assert 'error' in body

        # Verify error was logged
        mock_logger.error.assert_called()

    @patch('lambda_function.logger')
    def test_exception_logging_with_traceback(self, mock_logger, mock_context):
        """Test that exceptions are logged with traceback."""
        malformed_event = {"Records": [{"invalid": "data"}]}

        response = lambda_handler(malformed_event, mock_context)

        assert response['statusCode'] == 500

        # Verify error was logged with exc_info
        error_calls = [call for call in mock_logger.error.call_args_list]
        assert len(error_calls) > 0

        # Check that exc_info was set to True (for traceback)
        error_call = error_calls[0]
        assert error_call[1].get('exc_info') is True

    @patch('lambda_function.logger')
    def test_missing_optional_fields(self, mock_logger, mock_context):
        """Test handling when optional fields are missing."""
        # Event with minimal required fields
        minimal_event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {},
                        "object": {}
                    }
                }
            ]
        }

        response = lambda_handler(minimal_event, mock_context)

        # Should still process without crashing
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['files_processed'] == 1


class TestLambdaNotificationCalculations:
    """Tests for size calculations and data transformations."""

    @patch('lambda_function.logger')
    def test_size_conversion_to_mb(self, mock_logger, mock_context):
        """Test that file size is correctly converted to MB."""
        test_cases = [
            (1048576, 1.0),      # 1 MB
            (52428800, 50.0),    # 50 MB
            (104857600, 100.0),  # 100 MB
            (1536, 0.0),         # < 1 MB, rounds to 0.0
        ]

        for size_bytes, expected_mb in test_cases:
            event = {
                "Records": [
                    {
                        "eventTime": "2024-11-14T10:30:00.000Z",
                        "eventName": "ObjectCreated:Put",
                        "s3": {
                            "bucket": {"name": "test-bucket"},
                            "object": {
                                "key": "raw/taxi/test.parquet",
                                "size": size_bytes
                            }
                        }
                    }
                ]
            }

            response = lambda_handler(event, mock_context)

            body = json.loads(response['body'])
            assert body['files'][0]['size_bytes'] == size_bytes

            # Verify MB conversion in log
            # Extract from logged notification
            for call in mock_logger.info.call_args_list:
                log_message = call[0][0]
                if 'S3 File Notification:' in log_message:
                    json_start = log_message.index('{')
                    notification_log = json.loads(log_message[json_start:])
                    assert notification_log['size_mb'] == expected_mb
                    break

            # Reset mock for next iteration
            mock_logger.reset_mock()


class TestLambdaNotificationIntegration:
    """Integration-style tests for complete workflow."""

    @patch('lambda_function.logger')
    def test_realistic_taxi_data_upload_event(self, mock_logger, mock_context):
        """Test with realistic NYC taxi data upload scenario."""
        # Simulate real-world taxi data upload
        event = {
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "awsRegion": "us-east-1",
                    "eventTime": "2024-11-14T15:45:30.000Z",
                    "eventName": "ObjectCreated:Put",
                    "s3": {
                        "bucket": {
                            "name": "de-intern-2024-datalake-prod",
                            "arn": "arn:aws:s3:::de-intern-2024-datalake-prod"
                        },
                        "object": {
                            "key": "raw/taxi/yellow_tripdata_2024_01.parquet",
                            "size": 123456789,  # ~117.7 MB
                            "eTag": "abc123def456"
                        }
                    }
                }
            ]
        }

        response = lambda_handler(event, mock_context)

        # Verify successful processing
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['files_processed'] == 1

        # Verify correct bucket and key
        file_info = body['files'][0]
        assert file_info['bucket'] == 'de-intern-2024-datalake-prod'
        assert file_info['key'] == 'raw/taxi/yellow_tripdata_2024_01.parquet'
        assert file_info['size_bytes'] == 123456789

        # Verify all expected logs were created
        assert mock_logger.info.call_count >= 3


def test_lambda_handler_exists():
    """Test that lambda_handler function exists and is callable."""
    assert callable(lambda_handler)


def test_lambda_handler_signature():
    """Test that lambda_handler has correct signature."""
    import inspect

    sig = inspect.signature(lambda_handler)
    params = list(sig.parameters.keys())

    assert 'event' in params
    assert 'context' in params
    assert len(params) == 2
