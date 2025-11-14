"""
Week 1: AWS Setup, Python/Boto3, and RDS PostgreSQL

Topics covered:
- AWS account setup and IAM configuration
- Python virtual environment and dependencies
- Boto3 basics for AWS SDK
- RDS PostgreSQL connection and queries
- Initial data ingestion and exploration
"""

from .rds_connector import RDSConnector
from .data_ingestion import download_taxi_data, ingest_to_postgres

__all__ = [
    "RDSConnector",
    "download_taxi_data",
    "ingest_to_postgres",
]
