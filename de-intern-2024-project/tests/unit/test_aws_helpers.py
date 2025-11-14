"""Unit tests for AWS helper functions."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from de_intern_2024.utils.aws_helpers import (
    get_boto3_client,
    get_boto3_resource,
    upload_to_s3,
    download_from_s3,
    check_s3_bucket_exists
)


class TestBoto3Helpers:
    """Test Boto3 helper functions."""

    @patch('de_intern_2024.utils.aws_helpers.boto3')
    def test_get_boto3_client(self, mock_boto3):
        """Test getting Boto3 client."""
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        client = get_boto3_client('s3')

        mock_boto3.client.assert_called_once_with('s3', region_name='us-east-1')
        assert client == mock_client

    @patch('de_intern_2024.utils.aws_helpers.boto3')
    def test_get_boto3_resource(self, mock_boto3):
        """Test getting Boto3 resource."""
        mock_resource = Mock()
        mock_boto3.resource.return_value = mock_resource

        resource = get_boto3_resource('s3')

        mock_boto3.resource.assert_called_once_with('s3', region_name='us-east-1')
        assert resource == mock_resource


class TestS3Operations:
    """Test S3 operation functions."""

    @patch('de_intern_2024.utils.aws_helpers.get_boto3_client')
    def test_upload_to_s3_success(self, mock_get_client):
        """Test successful S3 upload."""
        mock_s3_client = Mock()
        mock_get_client.return_value = mock_s3_client

        result = upload_to_s3('/path/to/file.txt', 'my-bucket', 'key.txt')

        assert result is True
        mock_s3_client.upload_file.assert_called_once()

    @patch('de_intern_2024.utils.aws_helpers.get_boto3_client')
    def test_download_from_s3_success(self, mock_get_client):
        """Test successful S3 download."""
        mock_s3_client = Mock()
        mock_get_client.return_value = mock_s3_client

        result = download_from_s3('my-bucket', 'key.txt', '/path/to/file.txt')

        assert result is True
        mock_s3_client.download_file.assert_called_once()

    @patch('de_intern_2024.utils.aws_helpers.get_boto3_client')
    def test_check_s3_bucket_exists_true(self, mock_get_client):
        """Test checking if S3 bucket exists."""
        mock_s3_client = Mock()
        mock_get_client.return_value = mock_s3_client

        result = check_s3_bucket_exists('my-bucket')

        assert result is True
        mock_s3_client.head_bucket.assert_called_once_with(Bucket='my-bucket')
