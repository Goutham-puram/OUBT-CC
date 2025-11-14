"""
Create CloudWatch Alarms for DE Intern Pipeline monitoring.

Alarms:
- alarm-glue-job-failure: Alert when Glue job fails
- alarm-data-freshness: Alert when data is stale (> 24 hours)
- alarm-lambda-errors: Alert when Lambda has > 5 errors in 5 minutes
- alarm-cost-anomaly: Alert when daily cost exceeds $10
"""

import sys
import json
from typing import Dict, List, Any, Optional
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.append('/home/user/OUBT-CC/de-intern-2024-project/src')
from de_intern_2024.utils.logger import get_logger
from de_intern_2024.utils.aws_helpers import get_boto3_client

logger = get_logger(__name__)


class CloudWatchAlarmsCreator:
    """
    Creates and manages CloudWatch Alarms for pipeline monitoring.
    """

    def __init__(
        self,
        sns_topic_name: str = "etl-pipeline-alarms",
        region: str = 'us-east-1'
    ):
        """
        Initialize CloudWatch Alarms Creator.

        Args:
            sns_topic_name: SNS topic name for alarm notifications
            region: AWS region
        """
        self.sns_topic_name = sns_topic_name
        self.region = region

        # Initialize AWS clients
        self.cloudwatch_client = get_boto3_client('cloudwatch', region=region)
        self.sns_client = get_boto3_client('sns', region=region)
        self.sts_client = get_boto3_client('sts', region=region)
        self.ce_client = get_boto3_client('ce', region='us-east-1')  # Cost Explorer is always us-east-1
        self.lambda_client = get_boto3_client('lambda', region=region)

        # Get account information
        self.account_id = self.sts_client.get_caller_identity()['Account']
        self.bucket_name = f"{self.account_id}-oubt-datalake"

        logger.info(f"Initialized CloudWatchAlarmsCreator")
        logger.info(f"SNS Topic: {self.sns_topic_name}")
        logger.info(f"Region: {self.region}")
        logger.info(f"Account ID: {self.account_id}")

    def create_sns_topic(self) -> str:
        """
        Create SNS topic for alarm notifications.

        Returns:
            SNS topic ARN
        """
        try:
            logger.info(f"Creating SNS topic: {self.sns_topic_name}")

            response = self.sns_client.create_topic(
                Name=self.sns_topic_name,
                Tags=[
                    {'Key': 'Project', 'Value': 'OUBT-DataEngineering'},
                    {'Key': 'Purpose', 'Value': 'AlarmNotifications'},
                    {'Key': 'ManagedBy', 'Value': 'Automation'}
                ]
            )
            topic_arn = response['TopicArn']
            logger.info(f"Created SNS topic: {topic_arn}")

            return topic_arn

        except ClientError as e:
            if 'already exists' in str(e).lower():
                # Get existing topic ARN
                topic_arn = f"arn:aws:sns:{self.region}:{self.account_id}:{self.sns_topic_name}"
                logger.info(f"SNS topic already exists: {topic_arn}")
                return topic_arn
            else:
                logger.error(f"Failed to create SNS topic: {e}")
                raise

    def subscribe_email_to_topic(self, topic_arn: str, email: str) -> bool:
        """
        Subscribe email address to SNS topic.

        Args:
            topic_arn: SNS topic ARN
            email: Email address to subscribe

        Returns:
            True if successful
        """
        try:
            logger.info(f"Subscribing {email} to SNS topic")

            response = self.sns_client.subscribe(
                TopicArn=topic_arn,
                Protocol='email',
                Endpoint=email
            )

            logger.info(f"Subscription created. Please check {email} to confirm subscription.")
            logger.info(f"Subscription ARN: {response.get('SubscriptionArn', 'pending confirmation')}")

            return True

        except Exception as e:
            logger.error(f"Failed to subscribe email: {e}")
            return False

    def create_glue_job_failure_alarm(self, topic_arn: str) -> bool:
        """
        Create alarm for Glue job failures.

        Args:
            topic_arn: SNS topic ARN for notifications

        Returns:
            True if successful
        """
        alarm_name = "alarm-glue-job-failure"
        glue_job_name = "job-process-taxi-data"

        try:
            logger.info(f"Creating alarm: {alarm_name}")

            self.cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                AlarmDescription='Alert when Glue ETL job fails',
                ActionsEnabled=True,
                AlarmActions=[topic_arn],
                MetricName='glue.driver.aggregate.numFailedTasks',
                Namespace='AWS/Glue',
                Statistic='Sum',
                Dimensions=[
                    {
                        'Name': 'JobName',
                        'Value': glue_job_name
                    },
                    {
                        'Name': 'Type',
                        'Value': 'count'
                    }
                ],
                Period=300,  # 5 minutes
                EvaluationPeriods=1,
                Threshold=1.0,
                ComparisonOperator='GreaterThanOrEqualToThreshold',
                TreatMissingData='notBreaching',
                Tags=[
                    {'Key': 'Project', 'Value': 'OUBT-DataEngineering'},
                    {'Key': 'Severity', 'Value': 'High'},
                    {'Key': 'ManagedBy', 'Value': 'Automation'}
                ]
            )

            logger.info(f"✓ Created alarm: {alarm_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to create alarm {alarm_name}: {e}")
            return False

    def create_lambda_errors_alarm(self, topic_arn: str) -> bool:
        """
        Create alarm for Lambda errors (> 5 in 5 minutes).

        Args:
            topic_arn: SNS topic ARN for notifications

        Returns:
            True if successful
        """
        alarm_name = "alarm-lambda-errors"

        try:
            logger.info(f"Creating alarm: {alarm_name}")

            # Get Lambda function names
            lambda_functions = []
            try:
                response = self.lambda_client.list_functions()
                lambda_functions = [
                    f['FunctionName'] for f in response.get('Functions', [])
                    if 'etl' in f['FunctionName'].lower() or 's3-notification' in f['FunctionName'].lower()
                ]
            except Exception as e:
                logger.warning(f"Could not list Lambda functions: {e}")
                lambda_functions = ["s3-notification-handler"]  # Default

            # Create alarm for each Lambda function
            for func_name in lambda_functions[:5]:  # Limit to 5 functions
                func_alarm_name = f"{alarm_name}-{func_name}"

                self.cloudwatch_client.put_metric_alarm(
                    AlarmName=func_alarm_name,
                    AlarmDescription=f'Alert when Lambda function {func_name} has > 5 errors in 5 minutes',
                    ActionsEnabled=True,
                    AlarmActions=[topic_arn],
                    MetricName='Errors',
                    Namespace='AWS/Lambda',
                    Statistic='Sum',
                    Dimensions=[
                        {
                            'Name': 'FunctionName',
                            'Value': func_name
                        }
                    ],
                    Period=300,  # 5 minutes
                    EvaluationPeriods=1,
                    Threshold=5.0,
                    ComparisonOperator='GreaterThanThreshold',
                    TreatMissingData='notBreaching',
                    Tags=[
                        {'Key': 'Project', 'Value': 'OUBT-DataEngineering'},
                        {'Key': 'Severity', 'Value': 'High'},
                        {'Key': 'ManagedBy', 'Value': 'Automation'}
                    ]
                )

                logger.info(f"  ✓ Created alarm: {func_alarm_name}")

            logger.info(f"✓ Created {len(lambda_functions)} Lambda error alarms")
            return True

        except Exception as e:
            logger.error(f"Failed to create Lambda error alarms: {e}")
            return False

    def create_data_freshness_alarm(self, topic_arn: str) -> bool:
        """
        Create alarm for data freshness (> 24 hours old).
        Uses a custom CloudWatch metric that should be published by the ETL pipeline.

        Args:
            topic_arn: SNS topic ARN for notifications

        Returns:
            True if successful
        """
        alarm_name = "alarm-data-freshness"

        try:
            logger.info(f"Creating alarm: {alarm_name}")

            # Create alarm based on custom metric
            # The ETL pipeline should publish this metric
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                AlarmDescription='Alert when data has not been updated for > 24 hours',
                ActionsEnabled=True,
                AlarmActions=[topic_arn],
                MetricName='DataFreshnessHours',
                Namespace='ETL/Pipeline',
                Statistic='Maximum',
                Dimensions=[
                    {
                        'Name': 'Pipeline',
                        'Value': 'TaxiDataETL'
                    }
                ],
                Period=3600,  # 1 hour
                EvaluationPeriods=1,
                Threshold=24.0,  # 24 hours
                ComparisonOperator='GreaterThanThreshold',
                TreatMissingData='breaching',  # Treat missing data as a breach
                Tags=[
                    {'Key': 'Project', 'Value': 'OUBT-DataEngineering'},
                    {'Key': 'Severity', 'Value': 'Medium'},
                    {'Key': 'ManagedBy', 'Value': 'Automation'}
                ]
            )

            logger.info(f"✓ Created alarm: {alarm_name}")
            logger.info("  Note: This alarm requires the ETL pipeline to publish 'DataFreshnessHours' metric")

            return True

        except Exception as e:
            logger.error(f"Failed to create alarm {alarm_name}: {e}")
            return False

    def create_step_functions_failure_alarm(self, topic_arn: str) -> bool:
        """
        Create alarm for Step Functions execution failures.

        Args:
            topic_arn: SNS topic ARN for notifications

        Returns:
            True if successful
        """
        alarm_name = "alarm-stepfunctions-failure"
        state_machine_name = "etl-pipeline-workflow"
        state_machine_arn = f"arn:aws:states:{self.region}:{self.account_id}:stateMachine:{state_machine_name}"

        try:
            logger.info(f"Creating alarm: {alarm_name}")

            self.cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                AlarmDescription='Alert when Step Functions workflow fails',
                ActionsEnabled=True,
                AlarmActions=[topic_arn],
                MetricName='ExecutionsFailed',
                Namespace='AWS/States',
                Statistic='Sum',
                Dimensions=[
                    {
                        'Name': 'StateMachineArn',
                        'Value': state_machine_arn
                    }
                ],
                Period=300,  # 5 minutes
                EvaluationPeriods=1,
                Threshold=1.0,
                ComparisonOperator='GreaterThanOrEqualToThreshold',
                TreatMissingData='notBreaching',
                Tags=[
                    {'Key': 'Project', 'Value': 'OUBT-DataEngineering'},
                    {'Key': 'Severity', 'Value': 'High'},
                    {'Key': 'ManagedBy', 'Value': 'Automation'}
                ]
            )

            logger.info(f"✓ Created alarm: {alarm_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to create alarm {alarm_name}: {e}")
            return False

    def create_cost_anomaly_alarm(self, topic_arn: str) -> bool:
        """
        Create alarm for daily cost exceeding $10.
        Uses a custom CloudWatch metric for cost tracking.

        Args:
            topic_arn: SNS topic ARN for notifications

        Returns:
            True if successful
        """
        alarm_name = "alarm-cost-anomaly"

        try:
            logger.info(f"Creating alarm: {alarm_name}")

            # Create alarm based on custom cost metric
            # A separate Lambda or script should publish daily costs
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                AlarmDescription='Alert when estimated daily AWS cost exceeds $10',
                ActionsEnabled=True,
                AlarmActions=[topic_arn],
                MetricName='EstimatedDailyCost',
                Namespace='AWS/Billing',
                Statistic='Maximum',
                Dimensions=[
                    {
                        'Name': 'Service',
                        'Value': 'DataEngineering'
                    }
                ],
                Period=86400,  # 24 hours
                EvaluationPeriods=1,
                Threshold=10.0,  # $10
                ComparisonOperator='GreaterThanThreshold',
                TreatMissingData='notBreaching',
                Tags=[
                    {'Key': 'Project', 'Value': 'OUBT-DataEngineering'},
                    {'Key': 'Severity', 'Value': 'Medium'},
                    {'Key': 'ManagedBy', 'Value': 'Automation'}
                ]
            )

            logger.info(f"✓ Created alarm: {alarm_name}")
            logger.info("  Note: This alarm requires a cost tracking metric to be published")
            logger.info("  Consider using AWS Budgets for comprehensive cost monitoring")

            return True

        except Exception as e:
            logger.error(f"Failed to create alarm {alarm_name}: {e}")
            return False

    def create_redshift_query_performance_alarm(self, topic_arn: str) -> bool:
        """
        Create alarm for slow Redshift queries.

        Args:
            topic_arn: SNS topic ARN for notifications

        Returns:
            True if successful
        """
        alarm_name = "alarm-redshift-slow-queries"
        workgroup_name = "de-intern-workgroup"

        try:
            logger.info(f"Creating alarm: {alarm_name}")

            self.cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                AlarmDescription='Alert when Redshift query duration exceeds 5 minutes',
                ActionsEnabled=True,
                AlarmActions=[topic_arn],
                MetricName='QueryDuration',
                Namespace='AWS/Redshift-Serverless',
                Statistic='Maximum',
                Dimensions=[
                    {
                        'Name': 'WorkgroupName',
                        'Value': workgroup_name
                    }
                ],
                Period=300,  # 5 minutes
                EvaluationPeriods=2,  # 2 consecutive periods
                Threshold=300000.0,  # 5 minutes in milliseconds
                ComparisonOperator='GreaterThanThreshold',
                TreatMissingData='notBreaching',
                Tags=[
                    {'Key': 'Project', 'Value': 'OUBT-DataEngineering'},
                    {'Key': 'Severity', 'Value': 'Low'},
                    {'Key': 'ManagedBy', 'Value': 'Automation'}
                ]
            )

            logger.info(f"✓ Created alarm: {alarm_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to create alarm {alarm_name}: {e}")
            return False

    def create_s3_bucket_size_alarm(self, topic_arn: str) -> bool:
        """
        Create alarm for S3 bucket size exceeding threshold.

        Args:
            topic_arn: SNS topic ARN for notifications

        Returns:
            True if successful
        """
        alarm_name = "alarm-s3-bucket-size"

        try:
            logger.info(f"Creating alarm: {alarm_name}")

            # Alert when bucket size exceeds 100 GB
            threshold_bytes = 100 * 1024 * 1024 * 1024  # 100 GB in bytes

            self.cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                AlarmDescription='Alert when S3 bucket size exceeds 100 GB',
                ActionsEnabled=True,
                AlarmActions=[topic_arn],
                MetricName='BucketSizeBytes',
                Namespace='AWS/S3',
                Statistic='Average',
                Dimensions=[
                    {
                        'Name': 'BucketName',
                        'Value': self.bucket_name
                    },
                    {
                        'Name': 'StorageType',
                        'Value': 'StandardStorage'
                    }
                ],
                Period=86400,  # 24 hours
                EvaluationPeriods=1,
                Threshold=threshold_bytes,
                ComparisonOperator='GreaterThanThreshold',
                TreatMissingData='notBreaching',
                Tags=[
                    {'Key': 'Project', 'Value': 'OUBT-DataEngineering'},
                    {'Key': 'Severity', 'Value': 'Low'},
                    {'Key': 'ManagedBy', 'Value': 'Automation'}
                ]
            )

            logger.info(f"✓ Created alarm: {alarm_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to create alarm {alarm_name}: {e}")
            return False

    def list_alarms(self) -> List[Dict[str, Any]]:
        """
        List all alarms with 'alarm-' prefix.

        Returns:
            List of alarm information dictionaries
        """
        try:
            response = self.cloudwatch_client.describe_alarms(
                AlarmNamePrefix='alarm-',
                MaxRecords=100
            )

            alarms = []
            for alarm in response.get('MetricAlarms', []):
                alarms.append({
                    'name': alarm['AlarmName'],
                    'description': alarm.get('AlarmDescription', 'N/A'),
                    'state': alarm['StateValue'],
                    'metric': alarm['MetricName'],
                    'namespace': alarm['Namespace'],
                    'threshold': alarm['Threshold'],
                    'comparison': alarm['ComparisonOperator']
                })

            return alarms

        except Exception as e:
            logger.error(f"Failed to list alarms: {e}")
            return []

    def delete_all_alarms(self) -> bool:
        """
        Delete all alarms with 'alarm-' prefix.

        Returns:
            True if successful
        """
        try:
            alarms = self.list_alarms()
            alarm_names = [a['name'] for a in alarms]

            if not alarm_names:
                logger.info("No alarms to delete")
                return True

            logger.info(f"Deleting {len(alarm_names)} alarms...")

            self.cloudwatch_client.delete_alarms(
                AlarmNames=alarm_names
            )

            logger.info(f"✓ Deleted {len(alarm_names)} alarms")
            return True

        except Exception as e:
            logger.error(f"Failed to delete alarms: {e}")
            return False

    def create_all_alarms(self, topic_arn: str) -> bool:
        """
        Create all monitoring alarms.

        Args:
            topic_arn: SNS topic ARN for notifications

        Returns:
            True if all successful
        """
        logger.info("=" * 80)
        logger.info("Creating CloudWatch Alarms")
        logger.info("=" * 80)

        results = []

        # Create each alarm
        logger.info("\n[1] Creating Glue job failure alarm")
        results.append(self.create_glue_job_failure_alarm(topic_arn))

        logger.info("\n[2] Creating Lambda errors alarm")
        results.append(self.create_lambda_errors_alarm(topic_arn))

        logger.info("\n[3] Creating data freshness alarm")
        results.append(self.create_data_freshness_alarm(topic_arn))

        logger.info("\n[4] Creating Step Functions failure alarm")
        results.append(self.create_step_functions_failure_alarm(topic_arn))

        logger.info("\n[5] Creating cost anomaly alarm")
        results.append(self.create_cost_anomaly_alarm(topic_arn))

        logger.info("\n[6] Creating Redshift query performance alarm")
        results.append(self.create_redshift_query_performance_alarm(topic_arn))

        logger.info("\n[7] Creating S3 bucket size alarm")
        results.append(self.create_s3_bucket_size_alarm(topic_arn))

        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("Alarm Creation Summary")
        logger.info("=" * 80)

        total = len(results)
        successful = sum(results)

        logger.info(f"Total alarms: {total}")
        logger.info(f"Successful: {successful}")
        logger.info(f"Failed: {total - successful}")

        if successful == total:
            logger.info("\n✓ All alarms created successfully!")
        else:
            logger.warning(f"\n⚠ {total - successful} alarm(s) failed to create")

        return all(results)

    def setup_alarms(self, email: Optional[str] = None) -> bool:
        """
        Complete setup of CloudWatch alarms.

        Args:
            email: Email address to subscribe to alarm notifications

        Returns:
            True if successful
        """
        logger.info("=" * 80)
        logger.info("CloudWatch Alarms Setup")
        logger.info("=" * 80)

        try:
            # Step 1: Create SNS topic
            logger.info("\n[STEP 1] Creating SNS topic for alarm notifications")
            topic_arn = self.create_sns_topic()

            # Step 2: Subscribe email if provided
            if email:
                logger.info(f"\n[STEP 2] Subscribing {email} to alarm notifications")
                self.subscribe_email_to_topic(topic_arn, email)
            else:
                logger.info("\n[STEP 2] Skipping email subscription (no email provided)")

            # Step 3: Create all alarms
            logger.info("\n[STEP 3] Creating all CloudWatch alarms")
            success = self.create_all_alarms(topic_arn)

            # Step 4: List all alarms
            logger.info("\n[STEP 4] Listing all alarms")
            alarms = self.list_alarms()

            logger.info("\n" + "=" * 80)
            logger.info("Alarm Summary")
            logger.info("=" * 80)

            for alarm in alarms:
                logger.info(f"\n{alarm['name']}")
                logger.info(f"  Description: {alarm['description']}")
                logger.info(f"  State: {alarm['state']}")
                logger.info(f"  Metric: {alarm['namespace']}/{alarm['metric']}")
                logger.info(f"  Condition: {alarm['comparison']} {alarm['threshold']}")

            logger.info("\n" + "=" * 80)
            logger.info("SUCCESS: CloudWatch alarms setup completed!")
            logger.info("=" * 80)

            logger.info(f"\nSNS Topic ARN: {topic_arn}")
            if email:
                logger.info(f"\nPlease check {email} and confirm the SNS subscription to receive alarm notifications.")

            logger.info("\nView alarms in AWS Console:")
            logger.info(f"https://console.aws.amazon.com/cloudwatch/home?region={self.region}#alarmsV2:")

            return success

        except Exception as e:
            logger.error(f"\nAlarm setup failed: {e}", exc_info=True)
            return False


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Create CloudWatch Alarms for DE Intern Pipeline'
    )
    parser.add_argument(
        '--sns-topic-name',
        type=str,
        default='etl-pipeline-alarms',
        help='SNS topic name (default: etl-pipeline-alarms)'
    )
    parser.add_argument(
        '--region',
        type=str,
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )
    parser.add_argument(
        '--email',
        type=str,
        help='Email address to subscribe to alarm notifications'
    )
    parser.add_argument(
        '--delete-all',
        action='store_true',
        help='Delete all alarms with alarm- prefix'
    )
    parser.add_argument(
        '--list',
        action='store_true',
        help='List all alarms'
    )

    args = parser.parse_args()

    try:
        creator = CloudWatchAlarmsCreator(
            sns_topic_name=args.sns_topic_name,
            region=args.region
        )

        if args.delete_all:
            success = creator.delete_all_alarms()
            sys.exit(0 if success else 1)
        elif args.list:
            alarms = creator.list_alarms()
            print(json.dumps(alarms, indent=2))
            sys.exit(0)
        else:
            success = creator.setup_alarms(email=args.email)
            sys.exit(0 if success else 1)

    except Exception as e:
        logger.error(f"Failed to manage alarms: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
