"""
Manual test script for Step Functions ETL workflow.

This script provides utilities to:
- Trigger test executions
- Monitor execution progress
- Verify workflow configuration
- Generate test reports
"""

import json
import time
import sys
from typing import Dict, Optional, List
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

# Add parent directory to path
sys.path.append('/home/user/OUBT-CC/de-intern-2024-project/src')
from de_intern_2024.utils.logger import get_logger
from de_intern_2024.utils.aws_helpers import get_boto3_client

logger = get_logger(__name__)


class WorkflowTester:
    """Test and validate Step Functions workflow."""

    def __init__(self, region: str = 'us-east-1'):
        """
        Initialize workflow tester.

        Args:
            region: AWS region
        """
        self.region = region
        self.sfn_client = get_boto3_client('stepfunctions', region=region)
        self.sts_client = get_boto3_client('sts', region=region)

        self.account_id = self.sts_client.get_caller_identity()['Account']
        self.state_machine_name = 'etl-pipeline-workflow'
        self.state_machine_arn = (
            f'arn:aws:states:{region}:{self.account_id}:'
            f'stateMachine:{self.state_machine_name}'
        )

        logger.info(f"Initialized WorkflowTester")
        logger.info(f"State Machine: {self.state_machine_name}")
        logger.info(f"Region: {region}")

    def verify_deployment(self) -> bool:
        """
        Verify that workflow is properly deployed.

        Returns:
            True if deployed correctly, False otherwise
        """
        logger.info("=" * 80)
        logger.info("Verifying Workflow Deployment")
        logger.info("=" * 80)

        try:
            response = self.sfn_client.describe_state_machine(
                stateMachineArn=self.state_machine_arn
            )

            logger.info(f"\n✓ State Machine: {response['name']}")
            logger.info(f"  ARN: {response['stateMachineArn']}")
            logger.info(f"  Status: {response['status']}")
            logger.info(f"  Type: {response['type']}")
            logger.info(f"  Role: {response['roleArn']}")
            logger.info(f"  Created: {response['creationDate']}")

            # Verify state machine is active
            if response['status'] != 'ACTIVE':
                logger.error(f"State machine is not active: {response['status']}")
                return False

            # Verify definition
            definition = json.loads(response['definition'])
            expected_states = [
                'ValidateInput',
                'RunGlueETLJob',
                'CheckJobStatus',
                'RunCrawlerUpdate',
                'NotifySuccess',
                'NotifyFailure'
            ]

            states = definition.get('States', {})
            missing_states = [s for s in expected_states if s not in states]

            if missing_states:
                logger.error(f"Missing states: {missing_states}")
                return False

            logger.info(f"\n✓ All required states present:")
            for state_name in expected_states:
                logger.info(f"  - {state_name}")

            logger.info("\n✓ Workflow deployment verified successfully")
            return True

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'StateMachineDoesNotExist':
                logger.error("State machine not found. Please deploy first:")
                logger.error("  python src/stepfunctions/deploy_workflow.py")
            else:
                logger.error(f"Failed to verify deployment: {e}")
            return False

    def start_test_execution(
        self,
        bucket: str,
        key: str,
        size: int = 1048576,
        wait: bool = False,
        timeout: int = 300
    ) -> Optional[str]:
        """
        Start a test execution of the workflow.

        Args:
            bucket: S3 bucket name
            key: S3 object key
            size: File size in bytes (default: 1 MB)
            wait: Wait for execution to complete
            timeout: Maximum wait time in seconds (default: 300)

        Returns:
            Execution ARN if successful, None otherwise
        """
        logger.info("=" * 80)
        logger.info("Starting Test Execution")
        logger.info("=" * 80)

        execution_input = {
            'bucket': bucket,
            'key': key,
            'size': size,
            'eventTime': datetime.utcnow().isoformat(),
            'triggeredBy': 'test_script'
        }

        # Generate unique execution name
        timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        execution_name = f'test-execution-{timestamp}'

        logger.info(f"Execution Name: {execution_name}")
        logger.info(f"Input: {json.dumps(execution_input, indent=2)}")

        try:
            response = self.sfn_client.start_execution(
                stateMachineArn=self.state_machine_arn,
                name=execution_name,
                input=json.dumps(execution_input)
            )

            execution_arn = response['executionArn']
            logger.info(f"\n✓ Execution started successfully")
            logger.info(f"  ARN: {execution_arn}")
            logger.info(f"  Started: {response['startDate']}")

            if wait:
                logger.info(f"\nWaiting for execution to complete (timeout: {timeout}s)...")
                self.wait_for_execution(execution_arn, timeout)

            return execution_arn

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'ExecutionAlreadyExists':
                logger.warning(f"Execution already exists: {execution_name}")
                execution_arn = f"{self.state_machine_arn.replace('stateMachine', 'execution')}:{execution_name}"
                return execution_arn
            else:
                logger.error(f"Failed to start execution: {e}")
                return None

    def wait_for_execution(
        self,
        execution_arn: str,
        timeout: int = 300,
        poll_interval: int = 5
    ) -> Dict:
        """
        Wait for execution to complete and return final status.

        Args:
            execution_arn: Execution ARN
            timeout: Maximum wait time in seconds
            poll_interval: Polling interval in seconds

        Returns:
            Final execution details
        """
        start_time = time.time()
        last_status = None

        while (time.time() - start_time) < timeout:
            try:
                response = self.sfn_client.describe_execution(
                    executionArn=execution_arn
                )

                current_status = response['status']

                # Log status change
                if current_status != last_status:
                    logger.info(f"  Status: {current_status}")
                    last_status = current_status

                # Check if execution completed
                if current_status in ['SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED']:
                    elapsed = time.time() - start_time
                    logger.info(f"\n✓ Execution completed in {elapsed:.1f}s")
                    logger.info(f"  Final Status: {current_status}")

                    if current_status == 'SUCCEEDED':
                        logger.info(f"  Output: {response.get('output', 'N/A')}")
                    elif current_status == 'FAILED':
                        logger.error(f"  Error: {response.get('error', 'N/A')}")
                        logger.error(f"  Cause: {response.get('cause', 'N/A')}")

                    return response

                # Wait before next poll
                time.sleep(poll_interval)

            except ClientError as e:
                logger.error(f"Error checking execution status: {e}")
                return {}

        logger.warning(f"Execution timed out after {timeout}s")
        return {}

    def get_execution_history(self, execution_arn: str) -> List[Dict]:
        """
        Get execution history with all events.

        Args:
            execution_arn: Execution ARN

        Returns:
            List of execution events
        """
        logger.info("=" * 80)
        logger.info("Execution History")
        logger.info("=" * 80)

        try:
            events = []
            next_token = None

            while True:
                params = {'executionArn': execution_arn}
                if next_token:
                    params['nextToken'] = next_token

                response = self.sfn_client.get_execution_history(**params)
                events.extend(response['events'])

                next_token = response.get('nextToken')
                if not next_token:
                    break

            logger.info(f"\nTotal Events: {len(events)}\n")

            for i, event in enumerate(events, 1):
                event_type = event['type']
                timestamp = event['timestamp']
                event_id = event['id']

                logger.info(f"{i}. [{event_id}] {event_type}")
                logger.info(f"   Time: {timestamp}")

                # Log specific details based on event type
                if event_type == 'ExecutionStarted':
                    details = event.get('executionStartedEventDetails', {})
                    if details.get('input'):
                        logger.info(f"   Input: {details['input'][:100]}...")

                elif event_type == 'ExecutionFailed':
                    details = event.get('executionFailedEventDetails', {})
                    logger.info(f"   Error: {details.get('error', 'N/A')}")
                    logger.info(f"   Cause: {details.get('cause', 'N/A')}")

                elif event_type == 'ExecutionSucceeded':
                    details = event.get('executionSucceededEventDetails', {})
                    if details.get('output'):
                        logger.info(f"   Output: {details['output'][:100]}...")

                elif 'StateEntered' in event_type:
                    details = event.get('stateEnteredEventDetails', {})
                    logger.info(f"   State: {details.get('name', 'N/A')}")

                elif 'StateFailed' in event_type or 'StateTimeout' in event_type:
                    logger.error(f"   State failure detected")

            return events

        except ClientError as e:
            logger.error(f"Failed to get execution history: {e}")
            return []

    def list_recent_executions(self, max_results: int = 10) -> List[Dict]:
        """
        List recent executions.

        Args:
            max_results: Maximum number of executions to return

        Returns:
            List of execution details
        """
        logger.info("=" * 80)
        logger.info("Recent Executions")
        logger.info("=" * 80)

        try:
            response = self.sfn_client.list_executions(
                stateMachineArn=self.state_machine_arn,
                maxResults=max_results
            )

            executions = response.get('executions', [])

            if not executions:
                logger.info("\nNo executions found")
                return []

            logger.info(f"\nFound {len(executions)} execution(s):\n")

            for i, execution in enumerate(executions, 1):
                logger.info(f"{i}. {execution['name']}")
                logger.info(f"   Status: {execution['status']}")
                logger.info(f"   Started: {execution['startDate']}")
                if execution.get('stopDate'):
                    duration = (execution['stopDate'] - execution['startDate']).total_seconds()
                    logger.info(f"   Stopped: {execution['stopDate']}")
                    logger.info(f"   Duration: {duration:.1f}s")
                logger.info(f"   ARN: {execution['executionArn']}\n")

            return executions

        except ClientError as e:
            logger.error(f"Failed to list executions: {e}")
            return []

    def stop_execution(self, execution_arn: str, reason: str = "Manual stop") -> bool:
        """
        Stop a running execution.

        Args:
            execution_arn: Execution ARN
            reason: Reason for stopping

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Stopping execution: {execution_arn}")
            logger.info(f"Reason: {reason}")

            self.sfn_client.stop_execution(
                executionArn=execution_arn,
                error='ManualStop',
                cause=reason
            )

            logger.info("✓ Execution stopped successfully")
            return True

        except ClientError as e:
            logger.error(f"Failed to stop execution: {e}")
            return False


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Test and validate Step Functions ETL workflow'
    )
    parser.add_argument(
        '--action',
        type=str,
        choices=['verify', 'start', 'list', 'history', 'stop'],
        default='verify',
        help='Action to perform (default: verify)'
    )
    parser.add_argument(
        '--bucket',
        type=str,
        help='S3 bucket name for test execution'
    )
    parser.add_argument(
        '--key',
        type=str,
        default='raw/taxi/test_data.parquet',
        help='S3 object key for test execution'
    )
    parser.add_argument(
        '--execution-arn',
        type=str,
        help='Execution ARN for history/stop actions'
    )
    parser.add_argument(
        '--wait',
        action='store_true',
        help='Wait for execution to complete'
    )
    parser.add_argument(
        '--region',
        type=str,
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )

    args = parser.parse_args()

    try:
        tester = WorkflowTester(region=args.region)

        if args.action == 'verify':
            success = tester.verify_deployment()
            sys.exit(0 if success else 1)

        elif args.action == 'start':
            if not args.bucket:
                # Use default bucket
                account_id = tester.account_id
                args.bucket = f'{account_id}-oubt-datalake'
                logger.info(f"Using default bucket: {args.bucket}")

            execution_arn = tester.start_test_execution(
                bucket=args.bucket,
                key=args.key,
                wait=args.wait
            )
            sys.exit(0 if execution_arn else 1)

        elif args.action == 'list':
            executions = tester.list_recent_executions()
            sys.exit(0 if executions else 1)

        elif args.action == 'history':
            if not args.execution_arn:
                logger.error("--execution-arn required for history action")
                sys.exit(1)
            events = tester.get_execution_history(args.execution_arn)
            sys.exit(0 if events else 1)

        elif args.action == 'stop':
            if not args.execution_arn:
                logger.error("--execution-arn required for stop action")
                sys.exit(1)
            success = tester.stop_execution(args.execution_arn)
            sys.exit(0 if success else 1)

    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
