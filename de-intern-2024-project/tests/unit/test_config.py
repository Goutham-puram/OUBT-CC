"""Unit tests for configuration module."""

import pytest
from de_intern_2024.config import Config, AWSConfig, S3Config, RDSConfig


class TestAWSConfig:
    """Test AWS configuration."""

    def test_default_region(self):
        """Test default AWS region."""
        config = AWSConfig()
        assert config.region == "us-east-1"


class TestS3Config:
    """Test S3 configuration."""

    def test_s3_config_initialization(self):
        """Test S3 config can be initialized."""
        config = S3Config()
        assert isinstance(config, S3Config)


class TestRDSConfig:
    """Test RDS configuration."""

    def test_connection_string_format(self):
        """Test RDS connection string format."""
        config = RDSConfig(
            host="localhost",
            port=5432,
            database="testdb",
            username="testuser",
            password="testpass"
        )
        expected = "postgresql://testuser:testpass@localhost:5432/testdb"
        assert config.connection_string == expected


class TestConfig:
    """Test main configuration object."""

    def test_config_initialization(self):
        """Test config object can be initialized."""
        config = Config()
        assert config.project_name == "de-intern-2024"
        assert isinstance(config.aws, AWSConfig)
        assert isinstance(config.s3, S3Config)
        assert isinstance(config.rds, RDSConfig)
