#!/usr/bin/env python3
"""
Download NYC Yellow Taxi Trip Data
Downloads January 2024 parquet file from NYC TLC data repository.
"""

import os
import logging
import requests
import boto3
from pathlib import Path
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TaxiDataDownloader:
    """Download and manage NYC taxi trip data."""

    def __init__(self, data_dir: str = None):
        """
        Initialize the downloader.

        Args:
            data_dir: Directory to store downloaded files. Defaults to ../../data/raw/
        """
        if data_dir is None:
            # Default to project data/raw directory
            script_dir = Path(__file__).parent
            self.data_dir = script_dir.parent.parent / 'data' / 'raw'
        else:
            self.data_dir = Path(data_dir)

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.s3_client = None

    def download_from_url(self, url: str, filename: str) -> Path:
        """
        Download file from URL.

        Args:
            url: URL to download from
            filename: Local filename to save

        Returns:
            Path to downloaded file

        Raises:
            requests.RequestException: If download fails
        """
        filepath = self.data_dir / filename

        # Check if file already exists
        if filepath.exists():
            logger.info(f"File {filename} already exists. Skipping download.")
            return filepath

        logger.info(f"Downloading {url}...")

        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()

            # Get file size for progress tracking
            total_size = int(response.headers.get('content-length', 0))

            with open(filepath, 'wb') as f:
                downloaded = 0
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

                        # Log progress every 10MB
                        if total_size > 0 and downloaded % (10 * 1024 * 1024) < 8192:
                            progress = (downloaded / total_size) * 100
                            logger.info(f"Progress: {progress:.1f}%")

            logger.info(f"Successfully downloaded {filename}")
            logger.info(f"File size: {filepath.stat().st_size / (1024 * 1024):.2f} MB")
            return filepath

        except requests.RequestException as e:
            logger.error(f"Failed to download {url}: {e}")
            # Clean up partial download
            if filepath.exists():
                filepath.unlink()
            raise

    def upload_to_s3(self, filepath: Path, bucket: str, key: str = None) -> bool:
        """
        Upload file to S3.

        Args:
            filepath: Local file path
            bucket: S3 bucket name
            key: S3 object key. If None, uses filename

        Returns:
            True if successful, False otherwise
        """
        if self.s3_client is None:
            self.s3_client = boto3.client('s3')

        if key is None:
            key = f"raw/{filepath.name}"

        try:
            logger.info(f"Uploading {filepath.name} to s3://{bucket}/{key}...")

            self.s3_client.upload_file(
                str(filepath),
                bucket,
                key,
                Callback=self._upload_progress
            )

            logger.info(f"Successfully uploaded to s3://{bucket}/{key}")
            return True

        except ClientError as e:
            logger.error(f"Failed to upload to S3: {e}")
            return False

    def _upload_progress(self, bytes_uploaded: int):
        """Callback for upload progress."""
        if bytes_uploaded % (10 * 1024 * 1024) < 8192:  # Log every ~10MB
            logger.info(f"Uploaded: {bytes_uploaded / (1024 * 1024):.2f} MB")

    def download_yellow_taxi_data(self, year: int = 2024, month: int = 1) -> Path:
        """
        Download NYC Yellow Taxi trip data for specified month.

        Args:
            year: Year (default: 2024)
            month: Month (default: 1)

        Returns:
            Path to downloaded file
        """
        # NYC TLC data URL pattern
        filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"

        return self.download_from_url(url, filename)


def main():
    """Main execution function."""
    try:
        # Initialize downloader
        downloader = TaxiDataDownloader()

        # Download January 2024 Yellow Taxi data
        logger.info("Starting NYC Yellow Taxi data download for January 2024")
        filepath = downloader.download_yellow_taxi_data(year=2024, month=1)
        logger.info(f"Data downloaded to: {filepath}")

        # Optional: Upload to S3 if bucket is specified
        s3_bucket = os.getenv('S3_BUCKET')
        if s3_bucket:
            logger.info(f"S3 bucket specified: {s3_bucket}")
            success = downloader.upload_to_s3(filepath, s3_bucket)
            if success:
                logger.info("Data successfully uploaded to S3")
            else:
                logger.warning("S3 upload failed, but local file is available")
        else:
            logger.info("No S3_BUCKET environment variable set. Skipping S3 upload.")

        logger.info("Download process completed successfully")

    except Exception as e:
        logger.error(f"Download process failed: {e}")
        raise


if __name__ == "__main__":
    main()
