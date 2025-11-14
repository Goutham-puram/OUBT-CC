"""
Upload NYC taxi data to S3 Data Lake with intelligent multipart upload support.

This script:
- Uploads taxi data to raw/taxi/year=YYYY/month=MM/ partition structure
- Uses multipart upload for files > 100MB for better performance
- Provides progress tracking and retry logic
- Supports both single files and directory uploads
"""

import sys
import os
from pathlib import Path
from typing import Optional, List, Callable
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

# Add parent directory to path for imports
sys.path.append('/home/user/OUBT-CC/de-intern-2024-project/src')
from de_intern_2024.utils.logger import get_logger
from de_intern_2024.utils.aws_helpers import get_boto3_client

logger = get_logger(__name__)


class ProgressPercentage:
    """Callback class for tracking upload progress."""

    def __init__(self, filename: str, filesize: int):
        """
        Initialize progress tracker.

        Args:
            filename: Name of the file being uploaded.
            filesize: Size of the file in bytes.
        """
        self._filename = filename
        self._size = filesize
        self._seen_so_far = 0
        self._lock = None

    def __call__(self, bytes_amount: int):
        """
        Callback function called by boto3 during upload.

        Args:
            bytes_amount: Number of bytes transferred.
        """
        self._seen_so_far += bytes_amount
        percentage = (self._seen_so_far / self._size) * 100

        logger.info(
            f"  {self._filename}: {self._seen_so_far}/{self._size} bytes "
            f"({percentage:.2f}%)"
        )


class TaxiDataUploader:
    """Manages upload of NYC taxi data to S3 Data Lake."""

    # Multipart upload threshold: 100MB
    MULTIPART_THRESHOLD = 100 * 1024 * 1024

    # Multipart chunk size: 50MB
    MULTIPART_CHUNKSIZE = 50 * 1024 * 1024

    def __init__(
        self,
        bucket_name: str,
        region: str = 'us-east-1',
        use_multipart: bool = True
    ):
        """
        Initialize Taxi Data Uploader.

        Args:
            bucket_name: Name of the S3 bucket.
            region: AWS region.
            use_multipart: Whether to use multipart upload for large files.
        """
        self.bucket_name = bucket_name
        self.region = region
        self.use_multipart = use_multipart
        self.s3_client = get_boto3_client('s3', region=region)

        # Configure transfer settings
        self.transfer_config = TransferConfig(
            multipart_threshold=self.MULTIPART_THRESHOLD if use_multipart else sys.maxsize,
            multipart_chunksize=self.MULTIPART_CHUNKSIZE,
            max_concurrency=10,
            use_threads=True
        )

        logger.info(f"Initialized TaxiDataUploader for bucket: {bucket_name}")
        logger.info(f"Multipart upload: {'Enabled' if use_multipart else 'Disabled'}")
        logger.info(f"Multipart threshold: {self.MULTIPART_THRESHOLD / (1024*1024):.0f}MB")

    def generate_s3_key(
        self,
        filename: str,
        year: int,
        month: int,
        prefix: str = 'raw/taxi/'
    ) -> str:
        """
        Generate S3 key with partitioned structure.

        Args:
            filename: Name of the file.
            year: Year for partitioning.
            month: Month for partitioning.
            prefix: S3 key prefix (default: raw/taxi/).

        Returns:
            str: Complete S3 key path.
        """
        key = f"{prefix}year={year}/month={month:02d}/{filename}"
        return key

    def upload_file(
        self,
        file_path: str,
        year: int,
        month: int,
        s3_key: Optional[str] = None,
        show_progress: bool = True,
        extra_args: Optional[dict] = None
    ) -> bool:
        """
        Upload a single file to S3.

        Args:
            file_path: Path to the local file.
            year: Year for partitioning.
            month: Month for partitioning.
            s3_key: Optional custom S3 key. If None, auto-generates.
            show_progress: Whether to show upload progress.
            extra_args: Extra arguments for upload (metadata, encryption, etc.).

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            # Validate file exists
            if not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                return False

            file_path_obj = Path(file_path)
            filename = file_path_obj.name
            filesize = file_path_obj.stat().st_size

            # Generate S3 key if not provided
            if s3_key is None:
                s3_key = self.generate_s3_key(filename, year, month)

            logger.info(f"Uploading: {filename}")
            logger.info(f"  Size: {filesize / (1024*1024):.2f}MB")
            logger.info(f"  Destination: s3://{self.bucket_name}/{s3_key}")

            # Determine upload method
            if filesize > self.MULTIPART_THRESHOLD and self.use_multipart:
                logger.info(f"  Using multipart upload (file > {self.MULTIPART_THRESHOLD/(1024*1024):.0f}MB)")
            else:
                logger.info("  Using standard upload")

            # Set default extra args if not provided
            if extra_args is None:
                extra_args = {
                    'ServerSideEncryption': 'AES256',
                    'Metadata': {
                        'uploaded-by': 'taxi-data-uploader',
                        'upload-timestamp': datetime.utcnow().isoformat(),
                        'original-filename': filename
                    }
                }

            # Create progress callback
            callback = None
            if show_progress:
                callback = ProgressPercentage(filename, filesize)

            # Upload the file
            self.s3_client.upload_file(
                file_path,
                self.bucket_name,
                s3_key,
                ExtraArgs=extra_args,
                Config=self.transfer_config,
                Callback=callback
            )

            logger.info(f"✓ Successfully uploaded: {filename}")
            logger.info(f"  S3 URI: s3://{self.bucket_name}/{s3_key}")

            # Verify upload
            if self._verify_upload(s3_key, filesize):
                logger.info("✓ Upload verified successfully")
                return True
            else:
                logger.warning("⚠ Upload verification failed")
                return False

        except ClientError as e:
            logger.error(f"Failed to upload {filename}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error uploading {filename}: {e}", exc_info=True)
            return False

    def _verify_upload(self, s3_key: str, expected_size: int) -> bool:
        """
        Verify that the file was uploaded correctly.

        Args:
            s3_key: S3 key of the uploaded file.
            expected_size: Expected file size in bytes.

        Returns:
            bool: True if verified, False otherwise.
        """
        try:
            response = self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )

            actual_size = response['ContentLength']

            if actual_size == expected_size:
                return True
            else:
                logger.error(
                    f"Size mismatch: expected {expected_size}, got {actual_size}"
                )
                return False

        except ClientError as e:
            logger.error(f"Failed to verify upload: {e}")
            return False

    def upload_directory(
        self,
        directory_path: str,
        year: int,
        month: int,
        file_pattern: str = '*.parquet',
        show_progress: bool = True
    ) -> dict:
        """
        Upload all files matching pattern from a directory.

        Args:
            directory_path: Path to the directory.
            year: Year for partitioning.
            month: Month for partitioning.
            file_pattern: Glob pattern for files to upload.
            show_progress: Whether to show upload progress.

        Returns:
            dict: Summary of upload results.
        """
        logger.info("=" * 80)
        logger.info(f"Starting directory upload: {directory_path}")
        logger.info(f"Pattern: {file_pattern}")
        logger.info("=" * 80)

        directory = Path(directory_path)

        if not directory.exists() or not directory.is_dir():
            logger.error(f"Directory not found: {directory_path}")
            return {'success': False, 'error': 'Directory not found'}

        # Find matching files
        files = list(directory.glob(file_pattern))

        if not files:
            logger.warning(f"No files matching pattern '{file_pattern}' found")
            return {'success': False, 'uploaded': 0, 'failed': 0, 'total': 0}

        logger.info(f"Found {len(files)} files to upload\n")

        # Upload each file
        results = {
            'uploaded': 0,
            'failed': 0,
            'total': len(files),
            'files': []
        }

        for i, file_path in enumerate(files, 1):
            logger.info(f"[{i}/{len(files)}] Processing: {file_path.name}")

            success = self.upload_file(
                str(file_path),
                year=year,
                month=month,
                show_progress=show_progress
            )

            results['files'].append({
                'filename': file_path.name,
                'success': success,
                'size': file_path.stat().st_size
            })

            if success:
                results['uploaded'] += 1
            else:
                results['failed'] += 1

            logger.info("")  # Blank line between files

        # Log summary
        logger.info("=" * 80)
        logger.info("Upload Summary:")
        logger.info(f"  Total files: {results['total']}")
        logger.info(f"  Successful: {results['uploaded']}")
        logger.info(f"  Failed: {results['failed']}")
        logger.info("=" * 80)

        results['success'] = results['failed'] == 0
        return results

    def list_uploaded_files(
        self,
        year: Optional[int] = None,
        month: Optional[int] = None,
        prefix: str = 'raw/taxi/'
    ) -> List[dict]:
        """
        List files uploaded to the data lake.

        Args:
            year: Optional year filter.
            month: Optional month filter.
            prefix: S3 key prefix.

        Returns:
            list: List of file information dictionaries.
        """
        try:
            # Build prefix based on filters
            if year and month:
                list_prefix = f"{prefix}year={year}/month={month:02d}/"
            elif year:
                list_prefix = f"{prefix}year={year}/"
            else:
                list_prefix = prefix

            logger.info(f"Listing files with prefix: {list_prefix}")

            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=list_prefix)

            files = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        files.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'],
                            'storage_class': obj.get('StorageClass', 'STANDARD')
                        })

            logger.info(f"Found {len(files)} files")
            return files

        except ClientError as e:
            logger.error(f"Failed to list files: {e}")
            return []

    def delete_file(self, s3_key: str) -> bool:
        """
        Delete a file from S3.

        Args:
            s3_key: S3 key of the file to delete.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info(f"Deleting: s3://{self.bucket_name}/{s3_key}")

            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )

            logger.info("✓ File deleted successfully")
            return True

        except ClientError as e:
            logger.error(f"Failed to delete file: {e}")
            return False


def main():
    """Main entry point for the script."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Upload NYC taxi data to S3 Data Lake'
    )
    parser.add_argument(
        '--bucket-name',
        type=str,
        required=True,
        help='Name of the S3 bucket'
    )
    parser.add_argument(
        '--file-path',
        type=str,
        help='Path to single file to upload'
    )
    parser.add_argument(
        '--directory',
        type=str,
        help='Path to directory to upload'
    )
    parser.add_argument(
        '--year',
        type=int,
        default=2024,
        help='Year for partitioning (default: 2024)'
    )
    parser.add_argument(
        '--month',
        type=int,
        default=1,
        help='Month for partitioning (default: 1)'
    )
    parser.add_argument(
        '--region',
        type=str,
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )
    parser.add_argument(
        '--pattern',
        type=str,
        default='*.parquet',
        help='File pattern for directory upload (default: *.parquet)'
    )
    parser.add_argument(
        '--no-multipart',
        action='store_true',
        help='Disable multipart upload'
    )
    parser.add_argument(
        '--list',
        action='store_true',
        help='List uploaded files'
    )
    parser.add_argument(
        '--no-progress',
        action='store_true',
        help='Disable progress display'
    )

    args = parser.parse_args()

    try:
        uploader = TaxiDataUploader(
            bucket_name=args.bucket_name,
            region=args.region,
            use_multipart=not args.no_multipart
        )

        if args.list:
            files = uploader.list_uploaded_files(year=args.year, month=args.month)
            for f in files:
                print(f"{f['key']} - {f['size']/(1024*1024):.2f}MB - {f['last_modified']}")
            sys.exit(0)

        elif args.file_path:
            success = uploader.upload_file(
                file_path=args.file_path,
                year=args.year,
                month=args.month,
                show_progress=not args.no_progress
            )
            sys.exit(0 if success else 1)

        elif args.directory:
            results = uploader.upload_directory(
                directory_path=args.directory,
                year=args.year,
                month=args.month,
                file_pattern=args.pattern,
                show_progress=not args.no_progress
            )
            sys.exit(0 if results['success'] else 1)

        else:
            parser.print_help()
            logger.error("\nError: Must specify either --file-path or --directory")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Failed to upload taxi data: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
