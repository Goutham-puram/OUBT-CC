"""Configuration management for the data engineering project."""

import os
from typing import Optional
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class AWSConfig(BaseModel):
    """AWS configuration settings."""

    region: str = Field(default_factory=lambda: os.getenv("AWS_REGION", "us-east-1"))
    account_id: Optional[str] = Field(default_factory=lambda: os.getenv("AWS_ACCOUNT_ID"))


class S3Config(BaseModel):
    """S3 bucket configuration."""

    raw_bucket: str = Field(default_factory=lambda: os.getenv("S3_RAW_BUCKET", ""))
    processed_bucket: str = Field(default_factory=lambda: os.getenv("S3_PROCESSED_BUCKET", ""))
    curated_bucket: str = Field(default_factory=lambda: os.getenv("S3_CURATED_BUCKET", ""))
    scripts_bucket: str = Field(default_factory=lambda: os.getenv("S3_SCRIPTS_BUCKET", ""))


class RDSConfig(BaseModel):
    """RDS PostgreSQL configuration."""

    host: str = Field(default_factory=lambda: os.getenv("RDS_HOST", "localhost"))
    port: int = Field(default_factory=lambda: int(os.getenv("RDS_PORT", "5432")))
    database: str = Field(default_factory=lambda: os.getenv("RDS_DATABASE", "taxi_data"))
    username: str = Field(default_factory=lambda: os.getenv("RDS_USERNAME", "admin"))
    password: str = Field(default_factory=lambda: os.getenv("RDS_PASSWORD", ""))

    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class RedshiftConfig(BaseModel):
    """Redshift configuration."""

    host: str = Field(default_factory=lambda: os.getenv("REDSHIFT_HOST", ""))
    port: int = Field(default_factory=lambda: int(os.getenv("REDSHIFT_PORT", "5439")))
    database: str = Field(default_factory=lambda: os.getenv("REDSHIFT_DATABASE", "taxi_warehouse"))
    username: str = Field(default_factory=lambda: os.getenv("REDSHIFT_USERNAME", "admin"))
    password: str = Field(default_factory=lambda: os.getenv("REDSHIFT_PASSWORD", ""))

    @property
    def connection_string(self) -> str:
        """Generate Redshift connection string."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class GlueConfig(BaseModel):
    """AWS Glue configuration."""

    catalog_database: str = Field(default_factory=lambda: os.getenv("GLUE_CATALOG_DATABASE", "de_intern_2024_catalog"))
    raw_to_processed_job: str = Field(default_factory=lambda: os.getenv("GLUE_RAW_TO_PROCESSED_JOB", ""))
    processed_to_curated_job: str = Field(default_factory=lambda: os.getenv("GLUE_PROCESSED_TO_CURATED_JOB", ""))


class Config(BaseModel):
    """Main configuration object."""

    aws: AWSConfig = Field(default_factory=AWSConfig)
    s3: S3Config = Field(default_factory=S3Config)
    rds: RDSConfig = Field(default_factory=RDSConfig)
    redshift: RedshiftConfig = Field(default_factory=RedshiftConfig)
    glue: GlueConfig = Field(default_factory=GlueConfig)

    # Project settings
    project_name: str = "de-intern-2024"
    environment: str = Field(default_factory=lambda: os.getenv("ENVIRONMENT", "dev"))
    log_level: str = Field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))


# Global configuration instance
config = Config()
