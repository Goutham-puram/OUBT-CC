"""Pytest configuration and fixtures."""

import pytest
import pandas as pd
from datetime import datetime, timedelta


@pytest.fixture
def sample_taxi_data():
    """Create sample taxi trip data for testing."""
    base_time = datetime(2023, 1, 1, 12, 0, 0)

    data = {
        'VendorID': [1, 1, 2, 2, 1],
        'tpep_pickup_datetime': [
            base_time,
            base_time + timedelta(hours=1),
            base_time + timedelta(hours=2),
            base_time + timedelta(hours=3),
            base_time + timedelta(hours=4),
        ],
        'tpep_dropoff_datetime': [
            base_time + timedelta(minutes=15),
            base_time + timedelta(hours=1, minutes=20),
            base_time + timedelta(hours=2, minutes=10),
            base_time + timedelta(hours=3, minutes=30),
            base_time + timedelta(hours=4, minutes=25),
        ],
        'passenger_count': [1, 2, 1, 4, 2],
        'trip_distance': [2.5, 5.0, 1.5, 8.0, 3.5],
        'fare_amount': [12.5, 25.0, 8.5, 35.0, 15.0],
        'tip_amount': [2.0, 5.0, 1.5, 7.0, 3.0],
        'total_amount': [15.5, 31.0, 10.5, 43.0, 19.0],
        'payment_type': [1, 1, 2, 1, 1],
        'PULocationID': [100, 200, 150, 100, 250],
        'DOLocationID': [200, 100, 250, 200, 100],
    }

    return pd.DataFrame(data)


@pytest.fixture
def mock_aws_credentials(monkeypatch):
    """Mock AWS credentials for testing."""
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
    monkeypatch.setenv('AWS_SECURITY_TOKEN', 'testing')
    monkeypatch.setenv('AWS_SESSION_TOKEN', 'testing')
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
