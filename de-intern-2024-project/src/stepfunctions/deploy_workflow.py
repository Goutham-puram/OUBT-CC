"""
Deploy AWS Step Functions workflow for ETL orchestration.

This script:
- Creates IAM role for Step Functions execution (StepFunctionsExecutionRole)
- Deploys validation Lambda function
- Creates SNS topic for notifications
- Creates Step Functions state machine (etl-pipeline-workflow)
- Sets up CloudWatch alarms for monitoring
- Configures error handling and retry logic
"""

import sys
import json
import time
from typing import Dict, Optional, List
import boto3
from botocore.exceptions import ClientError

# Add parent directory to path for imports
sys.path.append('/home/user/OUBT-CC/de-intern-2024-project/src')
from de_intern_2024.utils.logger import get_logger
from de_intern_2024.utils.aws_helpers import get_boto3_client

logger = get_logger(__name__)


class StepFunctionsDeployer:
    """
    Manages deployment of Step Functions workflow for ETL orchestration.
    """

    def __init__(
        self,
        state_machine_name: str = "etl-pipeline-workflow",
        role_name: str = "StepFunctionsExecutionRole",
        region: str = 'us-east-1'
    ):
        """
        Initialize Step Functions Deployer.

        Args:
            state_machine_name: Name for the Step Functions state machine
            role_name: IAM role name for Step Functions execution
            region: AWS region
        """
        self.state_machine_name = state_machine_name
        self.role_name = role_name
        self.region = region

        # Initialize AWS clients
        self.sfn_client = get_boto3_client('stepfunctions', region=region)
        self.iam_client = get_boto3_client('iam', region=region)
        self.sns_client = get_boto3_client('sns', region=region)
        self.cloudwatch_client = get_boto3_client('cloudwatch', region=region)
        self.sts_client = get_boto3_client('sts', region=region)

        # Get account information
        self.account_id = self.sts_client.get_caller_identity()['Account']
        self.sns_topic_name = f"etl-pipeline-notifications"

        logger.info(f"Initialized StepFunctionsDeployer")
        logger.info(f"State Machine: {self.state_machine_name}")
        logger.info(f"Role: {self.role_name}")
        logger.info(f"Region: {self.region}")
        logger.info(f"Account ID: {self.account_id}")

    def create_iam_role(self) -> str:
        """
        Create IAM role for Step Functions execution.

        Returns:
            Role ARN
        """
        try:
            logger.info(f"Creating IAM role: {self.role_name}")

            # Trust policy for Step Functions
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "states.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }

            # Create role
            try:
                response = self.iam_client.create_role(
                    RoleName=self.role_name,
                    AssumeRolePolicyDocument=json.dumps(trust_policy),
                    Description='Execution role for ETL Pipeline Step Functions workflow',
                    Tags=[
                        {'Key': 'Project', 'Value': 'OUBT-DataEngineering'},
                        {'Key': 'Purpose', 'Value': 'StepFunctionsExecution'},
                        {'Key': 'ManagedBy', 'Value': 'Automation'}
                    ]
                )
                role_arn = response['Role']['Arn']
                logger.info(f"Created role: {role_arn}")
                time.sleep(10)  # Wait for role to propagate

            except ClientError as e:
                if e.response['Error']['Code'] == 'EntityAlreadyExists':
                    logger.info(f"Role already exists: {self.role_name}")
                    response = self.iam_client.get_role(RoleName=self.role_name)
                    role_arn = response['Role']['Arn']
                else:
                    raise

            # Attach policies
            self._attach_role_policies()

            return role_arn

        except Exception as e:
            logger.error(f"Failed to create IAM role: {e}")
            raise

    def _attach_role_policies(self):
        """Attach required policies to Step Functions execution role."""
        logger.info("Attaching policies to role...")

        # Policy for Lambda invocation
        lambda_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "lambda:InvokeFunction"
                    ],
                    "Resource": [
                        f"arn:aws:lambda:{self.region}:{self.account_id}:function:etl-orchestrator-*"
                    ]
                }
            ]
        }

        # Policy for Glue operations
        glue_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "glue:StartJobRun",
                        "glue:GetJobRun",
                        "glue:GetJobRuns",
                        "glue:BatchStopJobRun",
                        "glue:StartCrawler",
                        "glue:GetCrawler",
                        "glue:GetCrawlerMetrics"
                    ],
                    "Resource": [
                        f"arn:aws:glue:{self.region}:{self.account_id}:job/job-process-taxi-data",
                        f"arn:aws:glue:{self.region}:{self.account_id}:crawler/crawler-taxi-*"
                    ]
                }
            ]
        }

        # Policy for SNS notifications
        sns_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "sns:Publish"
                    ],
                    "Resource": [
                        f"arn:aws:sns:{self.region}:{self.account_id}:{self.sns_topic_name}"
                    ]
                }
            ]
        }

        # Policy for CloudWatch Logs
        logs_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                        "logs:CreateLogDelivery",
                        "logs:GetLogDelivery",
                        "logs:UpdateLogDelivery",
                        "logs:DeleteLogDelivery",
                        "logs:ListLogDeliveries",
                        "logs:PutResourcePolicy",
                        "logs:DescribeResourcePolicies",
                        "logs:DescribeLogGroups"
                    ],
                    "Resource": "*"
                }
            ]
        }

        # Policy for X-Ray tracing
        xray_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "xray:PutTraceSegments",
                        "xray:PutTelemetryRecords",
                        "xray:GetSamplingRules",
                        "xray:GetSamplingTargets"
                    ],
                    "Resource": "*"
                }
            ]
        }

        policies = {
            'StepFunctionsLambdaPolicy': lambda_policy,
            'StepFunctionsGluePolicy': glue_policy,
            'StepFunctionsSNSPolicy': sns_policy,
            'StepFunctionsLogsPolicy': logs_policy,
            'StepFunctionsXRayPolicy': xray_policy
        }

        for policy_name, policy_document in policies.items():
            try:
                self.iam_client.put_role_policy(
                    RoleName=self.role_name,
                    PolicyName=policy_name,
                    PolicyDocument=json.dumps(policy_document)
                )
                logger.info(f"  ✓ Attached policy: {policy_name}")
            except ClientError as e:
                logger.warning(f"  ! Failed to attach {policy_name}: {e}")

    def create_sns_topic(self) -> str:
        """
        Create SNS topic for notifications.

        Returns:
            SNS topic ARN
        """
        try:
            logger.info(f"Creating SNS topic: {self.sns_topic_name}")

            response = self.sns_client.create_topic(
                Name=self.sns_topic_name,
                Tags=[
                    {'Key': 'Project', 'Value': 'OUBT-DataEngineering'},
                    {'Key': 'Purpose', 'Value': 'ETLNotifications'},
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

    def load_state_machine_definition(self, definition_path: str, sns_topic_arn: str) -> str:
        """
        Load and prepare state machine definition.

        Args:
            definition_path: Path to state machine definition JSON file
            sns_topic_arn: SNS topic ARN for notifications

        Returns:
            State machine definition as string
        """
        try:
            logger.info(f"Loading state machine definition from: {definition_path}")

            with open(definition_path, 'r') as f:
                definition = f.read()

            # Replace SNS topic ARN placeholder
            definition = definition.replace('${SNSTopicArn}', sns_topic_arn)

            # Validate JSON
            json.loads(definition)

            logger.info("State machine definition loaded and validated")
            return definition

        except FileNotFoundError:
            logger.error(f"State machine definition file not found: {definition_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in state machine definition: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to load state machine definition: {e}")
            raise

    def create_state_machine(self, definition: str, role_arn: str) -> str:
        """
        Create Step Functions state machine.

        Args:
            definition: State machine definition (ASL JSON)
            role_arn: IAM role ARN for execution

        Returns:
            State machine ARN
        """
        try:
            logger.info(f"Creating state machine: {self.state_machine_name}")

            response = self.sfn_client.create_state_machine(
                name=self.state_machine_name,
                definition=definition,
                roleArn=role_arn,
                type='STANDARD',  # STANDARD for long-running workflows
                loggingConfiguration={
                    'level': 'ALL',
                    'includeExecutionData': True,
                    'destinations': [
                        {
                            'cloudWatchLogsLogGroup': {
                                'logGroupArn': f'arn:aws:logs:{self.region}:{self.account_id}:log-group:/aws/vendedlogs/states/{self.state_machine_name}:*'
                            }
                        }
                    ]
                },
                tracingConfiguration={
                    'enabled': True  # Enable X-Ray tracing
                },
                tags=[
                    {'key': 'Project', 'value': 'OUBT-DataEngineering'},
                    {'key': 'Purpose', 'value': 'ETLOrchestration'},
                    {'key': 'ManagedBy', 'value': 'Automation'}
                ]
            )

            state_machine_arn = response['stateMachineArn']
            logger.info(f"Created state machine: {state_machine_arn}")

            return state_machine_arn

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'StateMachineAlreadyExists':
                logger.info(f"State machine already exists: {self.state_machine_name}")
                # Get existing state machine ARN
                state_machine_arn = f"arn:aws:states:{self.region}:{self.account_id}:stateMachine:{self.state_machine_name}"
                return state_machine_arn
            else:
                logger.error(f"Failed to create state machine: {e}")
                raise
        except Exception as e:
            logger.error(f"Unexpected error creating state machine: {e}")
            raise

    def update_state_machine(self, state_machine_arn: str, definition: str, role_arn: str) -> bool:
        """
        Update existing state machine.

        Args:
            state_machine_arn: ARN of state machine to update
            definition: New state machine definition
            role_arn: IAM role ARN

        Returns:
            True if successful
        """
        try:
            logger.info(f"Updating state machine: {self.state_machine_name}")

            self.sfn_client.update_state_machine(
                stateMachineArn=state_machine_arn,
                definition=definition,
                roleArn=role_arn,
                loggingConfiguration={
                    'level': 'ALL',
                    'includeExecutionData': True,
                    'destinations': [
                        {
                            'cloudWatchLogsLogGroup': {
                                'logGroupArn': f'arn:aws:logs:{self.region}:{self.account_id}:log-group:/aws/vendedlogs/states/{self.state_machine_name}:*'
                            }
                        }
                    ]
                },
                tracingConfiguration={
                    'enabled': True
                }
            )

            logger.info("State machine updated successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to update state machine: {e}")
            raise

    def create_cloudwatch_alarms(self, state_machine_arn: str, sns_topic_arn: str):
        """
        Create CloudWatch alarms for monitoring.

        Args:
            state_machine_arn: State machine ARN
            sns_topic_arn: SNS topic ARN for alarm notifications
        """
        logger.info("Creating CloudWatch alarms...")

        alarms = [
            {
                'AlarmName': f'{self.state_machine_name}-ExecutionsFailed',
                'AlarmDescription': 'Alert when Step Functions executions fail',
                'MetricName': 'ExecutionsFailed',
                'Threshold': 1.0,
                'ComparisonOperator': 'GreaterThanOrEqualToThreshold',
                'EvaluationPeriods': 1,
                'Period': 300,  # 5 minutes
                'Statistic': 'Sum'
            },
            {
                'AlarmName': f'{self.state_machine_name}-ExecutionThrottled',
                'AlarmDescription': 'Alert when Step Functions executions are throttled',
                'MetricName': 'ExecutionThrottled',
                'Threshold': 1.0,
                'ComparisonOperator': 'GreaterThanOrEqualToThreshold',
                'EvaluationPeriods': 1,
                'Period': 300,
                'Statistic': 'Sum'
            },
            {
                'AlarmName': f'{self.state_machine_name}-ExecutionTime',
                'AlarmDescription': 'Alert when execution time exceeds threshold',
                'MetricName': 'ExecutionTime',
                'Threshold': 3600000.0,  # 1 hour in milliseconds
                'ComparisonOperator': 'GreaterThanThreshold',
                'EvaluationPeriods': 1,
                'Period': 300,
                'Statistic': 'Maximum'
            }
        ]

        for alarm_config in alarms:
            try:
                self.cloudwatch_client.put_metric_alarm(
                    AlarmName=alarm_config['AlarmName'],
                    AlarmDescription=alarm_config['AlarmDescription'],
                    ActionsEnabled=True,
                    AlarmActions=[sns_topic_arn],
                    MetricName=alarm_config['MetricName'],
                    Namespace='AWS/States',
                    Statistic=alarm_config['Statistic'],
                    Dimensions=[
                        {
                            'Name': 'StateMachineArn',
                            'Value': state_machine_arn
                        }
                    ],
                    Period=alarm_config['Period'],
                    EvaluationPeriods=alarm_config['EvaluationPeriods'],
                    Threshold=alarm_config['Threshold'],
                    ComparisonOperator=alarm_config['ComparisonOperator'],
                    Tags=[
                        {'Key': 'Project', 'Value': 'OUBT-DataEngineering'},
                        {'Key': 'ManagedBy', 'Value': 'Automation'}
                    ]
                )
                logger.info(f"  ✓ Created alarm: {alarm_config['AlarmName']}")

            except Exception as e:
                logger.warning(f"  ! Failed to create alarm {alarm_config['AlarmName']}: {e}")

    def get_state_machine_info(self) -> Optional[Dict]:
        """
        Get information about the state machine.

        Returns:
            State machine information dictionary, None if doesn't exist
        """
        try:
            state_machine_arn = f"arn:aws:states:{self.region}:{self.account_id}:stateMachine:{self.state_machine_name}"
            response = self.sfn_client.describe_state_machine(
                stateMachineArn=state_machine_arn
            )

            return {
                'name': response['name'],
                'arn': response['stateMachineArn'],
                'status': response['status'],
                'roleArn': response['roleArn'],
                'type': response['type'],
                'creationDate': response['creationDate'],
                'loggingConfiguration': response.get('loggingConfiguration', {}),
                'tracingConfiguration': response.get('tracingConfiguration', {})
            }

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'StateMachineDoesNotExist':
                return None
            else:
                logger.error(f"Failed to get state machine info: {e}")
                return None

    def deploy(self, definition_path: str, update_if_exists: bool = True) -> bool:
        """
        Complete deployment of Step Functions workflow.

        Args:
            definition_path: Path to state machine definition JSON
            update_if_exists: Update if state machine already exists

        Returns:
            True if successful
        """
        logger.info("=" * 80)
        logger.info("Step Functions Workflow Deployment")
        logger.info("=" * 80)

        try:
            # Step 1: Create IAM role
            logger.info("\n[STEP 1] Creating IAM role for Step Functions execution")
            role_arn = self.create_iam_role()

            # Step 2: Create SNS topic
            logger.info("\n[STEP 2] Creating SNS topic for notifications")
            sns_topic_arn = self.create_sns_topic()

            # Step 3: Load state machine definition
            logger.info("\n[STEP 3] Loading state machine definition")
            definition = self.load_state_machine_definition(definition_path, sns_topic_arn)

            # Step 4: Check if state machine exists
            logger.info("\n[STEP 4] Checking if state machine exists")
            existing_info = self.get_state_machine_info()

            if existing_info:
                if update_if_exists:
                    logger.info("State machine exists - updating")
                    self.update_state_machine(existing_info['arn'], definition, role_arn)
                    state_machine_arn = existing_info['arn']
                else:
                    logger.info("State machine exists - skipping creation")
                    state_machine_arn = existing_info['arn']
            else:
                logger.info("Creating new state machine")
                state_machine_arn = self.create_state_machine(definition, role_arn)

            # Step 5: Create CloudWatch alarms
            logger.info("\n[STEP 5] Creating CloudWatch alarms")
            self.create_cloudwatch_alarms(state_machine_arn, sns_topic_arn)

            # Display deployment summary
            logger.info("\n" + "=" * 80)
            logger.info("Deployment Summary")
            logger.info("=" * 80)

            info = self.get_state_machine_info()
            if info:
                logger.info(f"State Machine Name: {info['name']}")
                logger.info(f"State Machine ARN: {info['arn']}")
                logger.info(f"Status: {info['status']}")
                logger.info(f"Type: {info['type']}")
                logger.info(f"Role ARN: {info['roleArn']}")
                logger.info(f"Created: {info['creationDate']}")
                logger.info(f"Logging: {info['loggingConfiguration'].get('level', 'N/A')}")
                logger.info(f"Tracing: {'Enabled' if info['tracingConfiguration'].get('enabled') else 'Disabled'}")

            logger.info(f"\nSNS Topic ARN: {sns_topic_arn}")

            logger.info("\n" + "=" * 80)
            logger.info("SUCCESS: Step Functions workflow deployed!")
            logger.info("=" * 80)

            logger.info("\nNext Steps:")
            logger.info(f"  1. Subscribe to SNS topic for notifications:")
            logger.info(f"     aws sns subscribe --topic-arn {sns_topic_arn} --protocol email --notification-endpoint your-email@example.com")
            logger.info(f"\n  2. Update Lambda orchestrator with state machine ARN:")
            logger.info(f"     STATE_MACHINE_ARN={state_machine_arn}")
            logger.info(f"\n  3. Test the workflow:")
            logger.info(f"     python src/stepfunctions/test_workflow.py")

            return True

        except Exception as e:
            logger.error(f"\nDeployment failed: {e}", exc_info=True)
            return False


def main():
    """Main entry point."""
    import argparse
    import os

    parser = argparse.ArgumentParser(
        description='Deploy AWS Step Functions workflow for ETL orchestration'
    )
    parser.add_argument(
        '--state-machine-name',
        type=str,
        default='etl-pipeline-workflow',
        help='State machine name (default: etl-pipeline-workflow)'
    )
    parser.add_argument(
        '--role-name',
        type=str,
        default='StepFunctionsExecutionRole',
        help='IAM role name (default: StepFunctionsExecutionRole)'
    )
    parser.add_argument(
        '--region',
        type=str,
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )
    parser.add_argument(
        '--definition-path',
        type=str,
        default='/home/user/OUBT-CC/de-intern-2024-project/infra/stepfunctions/etl_workflow.json',
        help='Path to state machine definition JSON'
    )
    parser.add_argument(
        '--update',
        action='store_true',
        help='Update state machine if it already exists'
    )
    parser.add_argument(
        '--info-only',
        action='store_true',
        help='Only display state machine information'
    )

    args = parser.parse_args()

    # Validate definition path
    if not args.info_only and not os.path.exists(args.definition_path):
        logger.error(f"State machine definition not found: {args.definition_path}")
        sys.exit(1)

    try:
        deployer = StepFunctionsDeployer(
            state_machine_name=args.state_machine_name,
            role_name=args.role_name,
            region=args.region
        )

        if args.info_only:
            info = deployer.get_state_machine_info()
            if info:
                print(json.dumps(info, indent=2, default=str))
            else:
                logger.error("State machine does not exist")
                sys.exit(1)
        else:
            success = deployer.deploy(
                definition_path=args.definition_path,
                update_if_exists=args.update
            )

            sys.exit(0 if success else 1)

    except Exception as e:
        logger.error(f"Failed to deploy Step Functions workflow: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
