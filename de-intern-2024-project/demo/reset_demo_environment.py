"""
Demo Environment Reset Script.

Resets the demo environment to a clean state:
- Clears processed and curated S3 zones
- Resets Glue job bookmarks
- Clears CloudWatch logs
- Stops any running Step Functions executions

Use this before running the demo to ensure a clean slate.
"""

import argparse
import logging
import sys
import time
from typing import List, Dict, Any
from datetime import datetime

import boto3
from botocore.exceptions import ClientError, BotoCoreError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DemoReset:
    """Reset the demo environment to a clean state."""

    def __init__(
        self,
        region: str = 'us-east-1',
        project_name: str = 'de-intern-2024',
        dry_run: bool = False
    ):
        """
        Initialize the demo reset utility.

        Args:
            region: AWS region
            project_name: Project name prefix for resources
            dry_run: If True, only show what would be deleted
        """
        self.region = region
        self.project_name = project_name
        self.dry_run = dry_run

        # Initialize AWS clients
        self.s3_client = boto3.client('s3', region_name=region)
        self.s3_resource = boto3.resource('s3', region_name=region)
        self.logs_client = boto3.client('logs', region_name=region)
        self.sfn_client = boto3.client('stepfunctions', region_name=region)
        self.glue_client = boto3.client('glue', region_name=region)
        self.sts_client = boto3.client('sts', region_name=region)

        # Get account ID
        self.account_id = self.sts_client.get_caller_identity()['Account']

        # Resource names
        self.raw_bucket = f"{project_name}-raw-data-{self.account_id}"
        self.processed_bucket = f"{project_name}-processed-data-{self.account_id}"
        self.curated_bucket = f"{project_name}-curated-data-{self.account_id}"
        self.state_machine_arn = f"arn:aws:states:{region}:{self.account_id}:stateMachine:{project_name}-etl-pipeline"

        logger.info(f"Initialized demo reset for account {self.account_id} in region {region}")
        if dry_run:
            logger.info("DRY RUN MODE - No changes will be made")

    def clear_s3_bucket(self, bucket_name: str, prefix: str = '') -> int:
        """
        Clear objects from an S3 bucket.

        Args:
            bucket_name: Name of the S3 bucket
            prefix: Object prefix to filter (default: all objects)

        Returns:
            Number of objects deleted
        """
        logger.info(f"Clearing bucket: s3://{bucket_name}/{prefix}")

        try:
            bucket = self.s3_resource.Bucket(bucket_name)
            objects_to_delete = []

            # List objects
            for obj in bucket.objects.filter(Prefix=prefix):
                objects_to_delete.append({'Key': obj.key})

            if not objects_to_delete:
                logger.info(f"No objects found in s3://{bucket_name}/{prefix}")
                return 0

            logger.info(f"Found {len(objects_to_delete)} objects to delete")

            if self.dry_run:
                logger.info("DRY RUN - Would delete:")
                for obj in objects_to_delete[:10]:  # Show first 10
                    logger.info(f"  - {obj['Key']}")
                if len(objects_to_delete) > 10:
                    logger.info(f"  ... and {len(objects_to_delete) - 10} more")
                return len(objects_to_delete)

            # Delete objects in batches
            deleted_count = 0
            batch_size = 1000

            for i in range(0, len(objects_to_delete), batch_size):
                batch = objects_to_delete[i:i + batch_size]

                response = self.s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': batch}
                )

                deleted_count += len(response.get('Deleted', []))

                if response.get('Errors'):
                    logger.warning(f"Some objects failed to delete: {response['Errors']}")

            logger.info(f"Deleted {deleted_count} objects from s3://{bucket_name}/{prefix}")
            return deleted_count

        except ClientError as e:
            logger.error(f"Failed to clear bucket {bucket_name}: {str(e)}")
            return 0

    def reset_processed_zone(self) -> bool:
        """
        Clear the processed data zone.

        Returns:
            True if successful, False otherwise
        """
        logger.info("\n=== Clearing Processed Zone ===")

        try:
            deleted = self.clear_s3_bucket(self.processed_bucket, prefix='taxi/')

            if deleted > 0 or self.dry_run:
                logger.info(f"✓ Processed zone cleared ({deleted} objects)")
                return True
            else:
                logger.info("✓ Processed zone already empty")
                return True

        except Exception as e:
            logger.error(f"Failed to clear processed zone: {str(e)}")
            return False

    def reset_curated_zone(self) -> bool:
        """
        Clear the curated data zone.

        Returns:
            True if successful, False otherwise
        """
        logger.info("\n=== Clearing Curated Zone ===")

        try:
            deleted = self.clear_s3_bucket(self.curated_bucket, prefix='taxi/')

            if deleted > 0 or self.dry_run:
                logger.info(f"✓ Curated zone cleared ({deleted} objects)")
                return True
            else:
                logger.info("✓ Curated zone already empty")
                return True

        except Exception as e:
            logger.error(f"Failed to clear curated zone: {str(e)}")
            return False

    def reset_job_bookmarks(self) -> bool:
        """
        Reset Glue job bookmarks.

        Returns:
            True if successful, False otherwise
        """
        logger.info("\n=== Resetting Glue Job Bookmarks ===")

        job_names = [
            f"{self.project_name}-raw-to-processed",
            f"{self.project_name}-processed-to-curated"
        ]

        all_successful = True

        for job_name in job_names:
            try:
                logger.info(f"Resetting bookmark for job: {job_name}")

                if self.dry_run:
                    logger.info(f"DRY RUN - Would reset bookmark for {job_name}")
                    continue

                self.glue_client.reset_job_bookmark(
                    JobName=job_name
                )

                logger.info(f"✓ Reset bookmark for {job_name}")

            except ClientError as e:
                if e.response['Error']['Code'] == 'EntityNotFoundException':
                    logger.warning(f"Job not found: {job_name}")
                else:
                    logger.error(f"Failed to reset bookmark for {job_name}: {str(e)}")
                    all_successful = False

        return all_successful

    def clear_cloudwatch_logs(self) -> bool:
        """
        Clear CloudWatch log groups.

        Returns:
            True if successful, False otherwise
        """
        logger.info("\n=== Clearing CloudWatch Logs ===")

        log_groups = [
            f"/aws/lambda/{self.project_name}-s3-notification",
            f"/aws/lambda/{self.project_name}-etl-orchestrator",
            f"/aws/stepfunctions/{self.project_name}-etl-pipeline",
            f"/aws/glue/jobs/output",
            f"/aws/glue/jobs/error",
        ]

        all_successful = True

        for log_group in log_groups:
            try:
                logger.info(f"Clearing log group: {log_group}")

                # Get all log streams
                paginator = self.logs_client.get_paginator('describe_log_streams')
                page_iterator = paginator.paginate(logGroupName=log_group)

                stream_count = 0

                for page in page_iterator:
                    for stream in page.get('logStreams', []):
                        stream_name = stream['logStreamName']

                        if self.dry_run:
                            logger.info(f"DRY RUN - Would delete stream: {stream_name}")
                            stream_count += 1
                            continue

                        try:
                            self.logs_client.delete_log_stream(
                                logGroupName=log_group,
                                logStreamName=stream_name
                            )
                            stream_count += 1

                        except ClientError as e:
                            logger.warning(f"Failed to delete stream {stream_name}: {str(e)}")

                if stream_count > 0:
                    logger.info(f"✓ Cleared {stream_count} log streams from {log_group}")
                else:
                    logger.info(f"✓ No log streams in {log_group}")

            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    logger.info(f"Log group not found (skipping): {log_group}")
                else:
                    logger.error(f"Failed to clear log group {log_group}: {str(e)}")
                    all_successful = False

        return all_successful

    def stop_running_executions(self) -> bool:
        """
        Stop any running Step Functions executions.

        Returns:
            True if successful, False otherwise
        """
        logger.info("\n=== Stopping Running Executions ===")

        try:
            # List running executions
            response = self.sfn_client.list_executions(
                stateMachineArn=self.state_machine_arn,
                statusFilter='RUNNING',
                maxResults=100
            )

            executions = response.get('executions', [])

            if not executions:
                logger.info("✓ No running executions found")
                return True

            logger.info(f"Found {len(executions)} running execution(s)")

            for execution in executions:
                execution_arn = execution['executionArn']
                execution_name = execution['name']

                logger.info(f"Stopping execution: {execution_name}")

                if self.dry_run:
                    logger.info(f"DRY RUN - Would stop: {execution_name}")
                    continue

                self.sfn_client.stop_execution(
                    executionArn=execution_arn,
                    error='DemoReset',
                    cause='Demo environment reset'
                )

                logger.info(f"✓ Stopped execution: {execution_name}")

            return True

        except ClientError as e:
            logger.error(f"Failed to stop executions: {str(e)}")
            return False

    def clear_athena_results(self) -> bool:
        """
        Clear Athena query results.

        Returns:
            True if successful, False otherwise
        """
        logger.info("\n=== Clearing Athena Query Results ===")

        try:
            deleted = self.clear_s3_bucket(
                self.processed_bucket,
                prefix='athena-results/'
            )

            if deleted > 0 or self.dry_run:
                logger.info(f"✓ Cleared {deleted} Athena result files")
            else:
                logger.info("✓ No Athena results to clear")

            return True

        except Exception as e:
            logger.error(f"Failed to clear Athena results: {str(e)}")
            return False

    def run_reset(self) -> int:
        """
        Run the complete reset process.

        Returns:
            Exit code (0 for success, 1 for failure)
        """
        logger.info("\n" + "="*80)
        logger.info(" Demo Environment Reset")
        logger.info("="*80)

        logger.info(f"\nConfiguration:")
        logger.info(f"  Region: {self.region}")
        logger.info(f"  Account ID: {self.account_id}")
        logger.info(f"  Project: {self.project_name}")
        logger.info(f"  Dry Run: {self.dry_run}")

        if not self.dry_run:
            logger.warning("\n⚠️  This will delete data from your AWS account!")
            logger.warning("Press Ctrl+C within 5 seconds to cancel...")
            try:
                time.sleep(5)
            except KeyboardInterrupt:
                logger.info("\nReset cancelled by user")
                return 1

        # Run all reset operations
        operations = [
            ("Stop Running Executions", self.stop_running_executions),
            ("Clear Processed Zone", self.reset_processed_zone),
            ("Clear Curated Zone", self.reset_curated_zone),
            ("Reset Job Bookmarks", self.reset_job_bookmarks),
            ("Clear CloudWatch Logs", self.clear_cloudwatch_logs),
            ("Clear Athena Results", self.clear_athena_results),
        ]

        results = []

        for operation_name, operation_func in operations:
            try:
                result = operation_func()
                results.append((operation_name, result))

            except Exception as e:
                logger.error(f"{operation_name} failed: {str(e)}")
                results.append((operation_name, False))

        # Summary
        logger.info("\n" + "="*80)
        logger.info(" Reset Summary")
        logger.info("="*80 + "\n")

        for operation_name, result in results:
            status = "✓ SUCCESS" if result else "✗ FAILED"
            logger.info(f"  {operation_name}: {status}")

        successful_ops = sum(1 for _, result in results if result)
        total_ops = len(results)

        logger.info(f"\nResults: {successful_ops}/{total_ops} operations completed successfully\n")

        if successful_ops == total_ops:
            if self.dry_run:
                logger.info("✓ Dry run completed - no changes made")
            else:
                logger.info("✓ Demo environment reset complete!")
            return 0
        else:
            logger.warning("⚠️  Reset completed with some errors")
            return 1


def main():
    """Main entry point for the demo reset script."""
    parser = argparse.ArgumentParser(
        description='Reset the demo environment to a clean state'
    )
    parser.add_argument(
        '--region',
        type=str,
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )
    parser.add_argument(
        '--project-name',
        type=str,
        default='de-intern-2024',
        help='Project name (default: de-intern-2024)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )
    parser.add_argument(
        '--yes',
        action='store_true',
        help='Skip confirmation prompt (use with caution!)'
    )

    args = parser.parse_args()

    try:
        # Additional confirmation if not dry run and not --yes
        if not args.dry_run and not args.yes:
            response = input(
                "\n⚠️  WARNING: This will delete data from your AWS account!\n"
                "Type 'yes' to continue: "
            )
            if response.lower() != 'yes':
                print("Reset cancelled")
                sys.exit(0)

        reset = DemoReset(
            region=args.region,
            project_name=args.project_name,
            dry_run=args.dry_run
        )

        exit_code = reset.run_reset()
        sys.exit(exit_code)

    except KeyboardInterrupt:
        print("\n\nReset interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Reset failed: {str(e)}")
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
