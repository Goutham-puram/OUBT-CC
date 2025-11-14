"""
Create and configure AWS Glue ETL job for taxi data transformation pipeline.

This script creates a Glue ETL job with the following specifications:
- Job name: job-process-taxi-data
- Type: Spark ETL (glueetl)
- Worker type: G.1X (standard Spark executor)
- Number of workers: 2
- Glue version: 4.0
- Job bookmarks: Enabled for incremental processing
- Script location: S3
"""

import sys
import json
from typing import Dict, Optional
import boto3
from botocore.exceptions import ClientError

# Add parent directory to path for imports
sys.path.append('/home/user/OUBT-CC/de-intern-2024-project/src')
from de_intern_2024.utils.logger import get_logger
from de_intern_2024.utils.aws_helpers import get_boto3_client

logger = get_logger(__name__)


class GlueETLJobCreator:
    """
    Manages creation and configuration of AWS Glue ETL job for taxi data processing.
    """

    def __init__(self,
                 job_name: str = "job-process-taxi-data",
                 region: str = 'us-east-1',
                 role_name: str = "AWSGlueServiceRole-intern"):
        """
        Initialize Glue ETL Job Creator.

        Args:
            job_name: Name for the Glue ETL job.
            region: AWS region.
            role_name: IAM role name for Glue job execution.
        """
        self.job_name = job_name
        self.region = region
        self.role_name = role_name

        # Initialize AWS clients
        self.glue_client = get_boto3_client('glue', region=region)
        self.s3_client = get_boto3_client('s3', region=region)
        self.sts_client = get_boto3_client('sts', region=region)
        self.iam_client = get_boto3_client('iam', region=region)

        # Get account ID and construct bucket name
        self.account_id = self.sts_client.get_caller_identity()['Account']
        self.bucket_name = f"{self.account_id}-oubt-datalake"

        logger.info(f"Initialized GlueETLJobCreator for job: {self.job_name}")
        logger.info(f"Using bucket: {self.bucket_name}")

    def get_role_arn(self) -> str:
        """
        Get ARN of the IAM role for Glue.

        Returns:
            str: Role ARN.

        Raises:
            Exception: If role doesn't exist.
        """
        try:
            response = self.iam_client.get_role(RoleName=self.role_name)
            role_arn = response['Role']['Arn']
            logger.info(f"Found IAM role: {role_arn}")
            return role_arn
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'NoSuchEntity':
                logger.error(f"IAM role '{self.role_name}' does not exist")
                logger.error("Please create the role first using infrastructure/iam/create_glue_role.py")
                raise Exception(f"IAM role '{self.role_name}' not found")
            else:
                raise

    def upload_script_to_s3(self, local_script_path: str) -> str:
        """
        Upload ETL script to S3.

        Args:
            local_script_path: Local path to the ETL script.

        Returns:
            str: S3 path to the uploaded script.
        """
        script_key = f"scripts/glue/glue_etl_job.py"
        s3_path = f"s3://{self.bucket_name}/{script_key}"

        logger.info(f"Uploading script from {local_script_path} to {s3_path}")

        try:
            with open(local_script_path, 'rb') as f:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=script_key,
                    Body=f,
                    ServerSideEncryption='AES256'
                )
            logger.info("Script uploaded successfully")
            return s3_path
        except Exception as e:
            logger.error(f"Failed to upload script: {e}")
            raise

    def create_job(self, script_location: str) -> bool:
        """
        Create the Glue ETL job.

        Args:
            script_location: S3 location of the ETL script.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info(f"Creating Glue ETL job: {self.job_name}")

            role_arn = self.get_role_arn()

            # Job configuration
            job_config = {
                'Name': self.job_name,
                'Description': 'ETL job to process taxi data from raw to processed zone with data quality checks',
                'Role': role_arn,
                'ExecutionProperty': {
                    'MaxConcurrentRuns': 1  # Only one instance at a time
                },
                'Command': {
                    'Name': 'glueetl',  # Spark ETL job type
                    'ScriptLocation': script_location,
                    'PythonVersion': '3'
                },
                'DefaultArguments': {
                    '--job-language': 'python',
                    '--job-bookmark-option': 'job-bookmark-enable',  # Enable job bookmarks
                    '--enable-metrics': 'true',
                    '--enable-continuous-cloudwatch-log': 'true',
                    '--enable-spark-ui': 'true',
                    '--spark-event-logs-path': f's3://{self.bucket_name}/logs/spark-ui/',
                    '--TempDir': f's3://{self.bucket_name}/temp/glue/',
                    '--bucket_name': self.bucket_name,
                    '--source_prefix': 'raw/taxi/',
                    '--target_prefix': 'processed/taxi/',
                    '--enable-glue-datacatalog': 'true'
                },
                'MaxRetries': 1,
                'Timeout': 2880,  # 48 hours in minutes
                'GlueVersion': '4.0',  # Glue 4.0 (latest stable)
                'NumberOfWorkers': 2,  # Number of workers
                'WorkerType': 'G.1X',  # Standard worker type (4 vCPU, 16 GB memory, 64 GB disk)
                'Tags': {
                    'Project': 'OUBT-DataEngineering',
                    'Environment': 'Production',
                    'Purpose': 'TaxiDataProcessing',
                    'ManagedBy': 'Automation'
                }
            }

            response = self.glue_client.create_job(**job_config)

            logger.info(f"Successfully created job: {response['Name']}")
            return True

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'AlreadyExistsException':
                logger.warning(f"Job '{self.job_name}' already exists")
                return True
            else:
                logger.error(f"Failed to create job: {e}")
                return False
        except Exception as e:
            logger.error(f"Unexpected error creating job: {e}")
            return False

    def update_job(self, script_location: str) -> bool:
        """
        Update existing Glue ETL job.

        Args:
            script_location: S3 location of the ETL script.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info(f"Updating Glue ETL job: {self.job_name}")

            role_arn = self.get_role_arn()

            # Job update configuration
            job_update = {
                'Description': 'ETL job to process taxi data from raw to processed zone with data quality checks',
                'Role': role_arn,
                'ExecutionProperty': {
                    'MaxConcurrentRuns': 1
                },
                'Command': {
                    'Name': 'glueetl',
                    'ScriptLocation': script_location,
                    'PythonVersion': '3'
                },
                'DefaultArguments': {
                    '--job-language': 'python',
                    '--job-bookmark-option': 'job-bookmark-enable',
                    '--enable-metrics': 'true',
                    '--enable-continuous-cloudwatch-log': 'true',
                    '--enable-spark-ui': 'true',
                    '--spark-event-logs-path': f's3://{self.bucket_name}/logs/spark-ui/',
                    '--TempDir': f's3://{self.bucket_name}/temp/glue/',
                    '--bucket_name': self.bucket_name,
                    '--source_prefix': 'raw/taxi/',
                    '--target_prefix': 'processed/taxi/',
                    '--enable-glue-datacatalog': 'true'
                },
                'MaxRetries': 1,
                'Timeout': 2880,
                'GlueVersion': '4.0',
                'NumberOfWorkers': 2,
                'WorkerType': 'G.1X'
            }

            self.glue_client.update_job(
                JobName=self.job_name,
                JobUpdate=job_update
            )

            logger.info(f"Successfully updated job: {self.job_name}")
            return True

        except ClientError as e:
            logger.error(f"Failed to update job: {e}")
            return False

    def get_job_info(self) -> Optional[Dict]:
        """
        Get information about the job.

        Returns:
            dict: Job information, None if job doesn't exist.
        """
        try:
            response = self.glue_client.get_job(JobName=self.job_name)
            job = response['Job']

            return {
                'name': job['Name'],
                'role': job['Role'],
                'created_on': job.get('CreatedOn', 'N/A'),
                'last_modified': job.get('LastModifiedOn', 'N/A'),
                'glue_version': job.get('GlueVersion', 'N/A'),
                'worker_type': job.get('WorkerType', 'N/A'),
                'number_of_workers': job.get('NumberOfWorkers', 'N/A'),
                'max_concurrent_runs': job.get('ExecutionProperty', {}).get('MaxConcurrentRuns', 'N/A'),
                'script_location': job.get('Command', {}).get('ScriptLocation', 'N/A'),
                'job_bookmark': job.get('DefaultArguments', {}).get('--job-bookmark-option', 'disabled')
            }

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'EntityNotFoundException':
                logger.warning(f"Job '{self.job_name}' does not exist")
                return None
            else:
                logger.error(f"Failed to get job info: {e}")
                return None

    def start_job_run(self, arguments: Optional[Dict[str, str]] = None) -> Optional[str]:
        """
        Start a job run.

        Args:
            arguments: Optional job arguments to override defaults.

        Returns:
            str: Job run ID if successful, None otherwise.
        """
        try:
            logger.info(f"Starting job run for: {self.job_name}")

            params = {'JobName': self.job_name}
            if arguments:
                params['Arguments'] = arguments

            response = self.glue_client.start_job_run(**params)
            job_run_id = response['JobRunId']

            logger.info(f"Job run started successfully. Run ID: {job_run_id}")
            return job_run_id

        except ClientError as e:
            logger.error(f"Failed to start job run: {e}")
            return None

    def setup_job(self, script_path: str, update_if_exists: bool = True) -> bool:
        """
        Complete setup of Glue ETL job.

        Args:
            script_path: Local path to the ETL script.
            update_if_exists: Update job if it already exists.

        Returns:
            bool: True if successful, False otherwise.
        """
        logger.info("=" * 80)
        logger.info("Starting Glue ETL Job Setup")
        logger.info("=" * 80)

        # Check if job exists
        existing_job = self.get_job_info()
        job_exists = existing_job is not None

        # Upload script to S3
        logger.info("\n[STEP 1] Uploading ETL script to S3")
        try:
            script_location = self.upload_script_to_s3(script_path)
        except Exception as e:
            logger.error(f"Failed to upload script: {e}")
            return False

        # Create or update job
        if job_exists:
            if update_if_exists:
                logger.info("\n[STEP 2] Job exists - updating job configuration")
                success = self.update_job(script_location)
            else:
                logger.info("\n[STEP 2] Job exists - skipping creation")
                success = True
        else:
            logger.info("\n[STEP 2] Creating new Glue ETL job")
            success = self.create_job(script_location)

        if not success:
            logger.error("Failed to create/update job")
            return False

        # Display job information
        logger.info("\n" + "=" * 80)
        logger.info("Glue ETL Job Configuration")
        logger.info("=" * 80)

        info = self.get_job_info()
        if info:
            logger.info(f"Job Name: {info['name']}")
            logger.info(f"Role: {info['role']}")
            logger.info(f"Glue Version: {info['glue_version']}")
            logger.info(f"Worker Type: {info['worker_type']}")
            logger.info(f"Number of Workers: {info['number_of_workers']}")
            logger.info(f"Max Concurrent Runs: {info['max_concurrent_runs']}")
            logger.info(f"Job Bookmark: {info['job_bookmark']}")
            logger.info(f"Script Location: {info['script_location']}")
            logger.info(f"Created On: {info['created_on']}")
            logger.info(f"Last Modified: {info['last_modified']}")

        logger.info("\n" + "=" * 80)
        logger.info("SUCCESS: Glue ETL job setup completed!")
        logger.info("=" * 80)

        return True


def main():
    """Main entry point for the script."""
    import argparse
    import os

    parser = argparse.ArgumentParser(
        description='Create or update AWS Glue ETL job for taxi data processing'
    )
    parser.add_argument(
        '--job-name',
        type=str,
        default='job-process-taxi-data',
        help='Glue job name (default: job-process-taxi-data)'
    )
    parser.add_argument(
        '--role-name',
        type=str,
        default='AWSGlueServiceRole-intern',
        help='IAM role name (default: AWSGlueServiceRole-intern)'
    )
    parser.add_argument(
        '--region',
        type=str,
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )
    parser.add_argument(
        '--script-path',
        type=str,
        default='/home/user/OUBT-CC/de-intern-2024-project/src/transform/glue_etl_job.py',
        help='Local path to ETL script'
    )
    parser.add_argument(
        '--update',
        action='store_true',
        help='Update job if it already exists'
    )
    parser.add_argument(
        '--start',
        action='store_true',
        help='Start a job run after setup'
    )
    parser.add_argument(
        '--info-only',
        action='store_true',
        help='Only display job information, do not create/update'
    )

    args = parser.parse_args()

    # Validate script path
    if not args.info_only and not os.path.exists(args.script_path):
        logger.error(f"Script file not found: {args.script_path}")
        sys.exit(1)

    try:
        creator = GlueETLJobCreator(
            job_name=args.job_name,
            region=args.region,
            role_name=args.role_name
        )

        if args.info_only:
            info = creator.get_job_info()
            if info:
                print(json.dumps(info, indent=2, default=str))
            else:
                logger.error("Job does not exist")
                sys.exit(1)
        else:
            success = creator.setup_job(
                script_path=args.script_path,
                update_if_exists=args.update
            )

            if not success:
                sys.exit(1)

            if args.start:
                logger.info("\nStarting job run...")
                job_run_id = creator.start_job_run()
                if job_run_id:
                    logger.info(f"Job run started: {job_run_id}")
                    logger.info("Monitor the job in AWS Glue Console")
                else:
                    logger.error("Failed to start job run")
                    sys.exit(1)

            sys.exit(0)

    except Exception as e:
        logger.error(f"Failed to setup Glue ETL job: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
