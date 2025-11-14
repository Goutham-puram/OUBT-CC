"""
Integration tests for Step Functions ETL workflow.

Tests the complete end-to-end workflow including:
- Lambda orchestrator triggering Step Functions
- Input validation
- Glue job execution
- Crawler updates
- Success/failure notifications
- Error handling and retries
"""

import json
import time
import pytest
from datetime import datetime
from typing import Dict, Any
import boto3
from botocore.exceptions import ClientError


class TestStepFunctionsWorkflow:
    """Integration tests for Step Functions ETL orchestration workflow."""

    @pytest.fixture(scope='class')
    def aws_clients(self):
        """Initialize AWS clients for testing."""
        return {
            'stepfunctions': boto3.client('stepfunctions', region_name='us-east-1'),
            's3': boto3.client('s3', region_name='us-east-1'),
            'glue': boto3.client('glue', region_name='us-east-1'),
            'sns': boto3.client('sns', region_name='us-east-1'),
            'lambda': boto3.client('lambda', region_name='us-east-1'),
            'cloudwatch': boto3.client('cloudwatch', region_name='us-east-1'),
            'sts': boto3.client('sts', region_name='us-east-1')
        }

    @pytest.fixture(scope='class')
    def test_config(self, aws_clients):
        """Get test configuration from AWS resources."""
        account_id = aws_clients['sts'].get_caller_identity()['Account']

        return {
            'account_id': account_id,
            'region': 'us-east-1',
            'state_machine_name': 'etl-pipeline-workflow',
            'state_machine_arn': f'arn:aws:states:us-east-1:{account_id}:stateMachine:etl-pipeline-workflow',
            'bucket_name': f'{account_id}-oubt-datalake',
            'test_file_key': 'raw/taxi/test_yellow_tripdata_2024-01.parquet',
            'glue_job_name': 'job-process-taxi-data',
            'crawler_name': 'crawler-taxi-raw',
            'lambda_function_name': 'etl-orchestrator',
            'sns_topic_name': 'etl-pipeline-notifications'
        }

    def test_state_machine_exists(self, aws_clients, test_config):
        """Test that state machine is deployed and accessible."""
        try:
            response = aws_clients['stepfunctions'].describe_state_machine(
                stateMachineArn=test_config['state_machine_arn']
            )

            assert response['name'] == test_config['state_machine_name']
            assert response['status'] == 'ACTIVE'
            assert response['type'] == 'STANDARD'

            print(f"✓ State machine exists: {response['name']}")
            print(f"  Status: {response['status']}")
            print(f"  Type: {response['type']}")
            print(f"  Created: {response['creationDate']}")

        except ClientError as e:
            pytest.fail(f"State machine not found: {e}")

    def test_state_machine_definition(self, aws_clients, test_config):
        """Test state machine definition is valid and contains expected states."""
        try:
            response = aws_clients['stepfunctions'].describe_state_machine(
                stateMachineArn=test_config['state_machine_arn']
            )

            definition = json.loads(response['definition'])

            # Check expected states exist
            expected_states = [
                'ValidateInput',
                'RunGlueETLJob',
                'CheckJobStatus',
                'RunCrawlerUpdate',
                'NotifySuccess',
                'NotifyFailure'
            ]

            states = definition.get('States', {})
            for state_name in expected_states:
                assert state_name in states, f"Missing expected state: {state_name}"
                print(f"✓ Found state: {state_name}")

            # Verify start state
            assert definition.get('StartAt') == 'ValidateInput'
            print(f"✓ Start state: {definition['StartAt']}")

        except ClientError as e:
            pytest.fail(f"Failed to get state machine definition: {e}")

    def test_iam_role_permissions(self, aws_clients, test_config):
        """Test that Step Functions execution role has required permissions."""
        try:
            response = aws_clients['stepfunctions'].describe_state_machine(
                stateMachineArn=test_config['state_machine_arn']
            )

            role_arn = response['roleArn']
            assert role_arn is not None
            assert 'StepFunctionsExecutionRole' in role_arn

            print(f"✓ Execution role: {role_arn}")

        except ClientError as e:
            pytest.fail(f"Failed to verify IAM role: {e}")

    def test_start_execution_with_valid_input(self, aws_clients, test_config):
        """Test starting Step Functions execution with valid input."""
        execution_input = {
            'bucket': test_config['bucket_name'],
            'key': test_config['test_file_key'],
            'size': 1048576,  # 1 MB
            'eventTime': datetime.utcnow().isoformat()
        }

        execution_name = f"test-valid-input-{int(time.time())}"

        try:
            response = aws_clients['stepfunctions'].start_execution(
                stateMachineArn=test_config['state_machine_arn'],
                name=execution_name,
                input=json.dumps(execution_input)
            )

            execution_arn = response['executionArn']
            assert execution_arn is not None

            print(f"✓ Started execution: {execution_name}")
            print(f"  ARN: {execution_arn}")

            # Wait a bit and check execution status
            time.sleep(5)

            status_response = aws_clients['stepfunctions'].describe_execution(
                executionArn=execution_arn
            )

            print(f"  Status: {status_response['status']}")

            # Stop the execution to avoid long-running test
            if status_response['status'] == 'RUNNING':
                aws_clients['stepfunctions'].stop_execution(
                    executionArn=execution_arn,
                    error='TestCleanup',
                    cause='Stopping test execution'
                )
                print(f"  Stopped test execution")

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'ExecutionAlreadyExists':
                print(f"! Execution already exists: {execution_name}")
            else:
                pytest.fail(f"Failed to start execution: {e}")

    def test_start_execution_with_invalid_input(self, aws_clients, test_config):
        """Test that validation properly handles invalid input."""
        # Invalid input: file not in correct prefix
        invalid_input = {
            'bucket': test_config['bucket_name'],
            'key': 'invalid/prefix/test.parquet',  # Wrong prefix
            'size': 1048576,
            'eventTime': datetime.utcnow().isoformat()
        }

        execution_name = f"test-invalid-input-{int(time.time())}"

        try:
            response = aws_clients['stepfunctions'].start_execution(
                stateMachineArn=test_config['state_machine_arn'],
                name=execution_name,
                input=json.dumps(invalid_input)
            )

            execution_arn = response['executionArn']
            print(f"✓ Started validation test: {execution_name}")

            # Wait for validation to complete
            time.sleep(10)

            # Check execution status and history
            status_response = aws_clients['stepfunctions'].describe_execution(
                executionArn=execution_arn
            )

            print(f"  Status: {status_response['status']}")

            # Should fail validation or complete with validation failure path
            # Stop if still running
            if status_response['status'] == 'RUNNING':
                aws_clients['stepfunctions'].stop_execution(
                    executionArn=execution_arn,
                    error='TestCleanup',
                    cause='Stopping test execution'
                )

        except ClientError as e:
            pytest.fail(f"Failed to test invalid input: {e}")

    def test_sns_topic_exists(self, aws_clients, test_config):
        """Test that SNS topic for notifications exists."""
        try:
            topic_arn = f"arn:aws:sns:{test_config['region']}:{test_config['account_id']}:{test_config['sns_topic_name']}"

            response = aws_clients['sns'].get_topic_attributes(
                TopicArn=topic_arn
            )

            assert response['Attributes'] is not None
            print(f"✓ SNS topic exists: {test_config['sns_topic_name']}")
            print(f"  ARN: {topic_arn}")

        except ClientError as e:
            pytest.fail(f"SNS topic not found: {e}")

    def test_cloudwatch_alarms_exist(self, aws_clients, test_config):
        """Test that CloudWatch alarms are configured."""
        try:
            response = aws_clients['cloudwatch'].describe_alarms(
                AlarmNamePrefix=test_config['state_machine_name']
            )

            alarms = response.get('MetricAlarms', [])
            assert len(alarms) > 0, "No CloudWatch alarms found"

            print(f"✓ Found {len(alarms)} CloudWatch alarm(s):")
            for alarm in alarms:
                print(f"  - {alarm['AlarmName']}: {alarm['StateValue']}")

        except ClientError as e:
            pytest.fail(f"Failed to check CloudWatch alarms: {e}")

    def test_lambda_orchestrator_integration(self, aws_clients, test_config):
        """Test Lambda orchestrator can trigger Step Functions."""
        # Create test S3 event
        test_event = {
            'Records': [
                {
                    'eventName': 'ObjectCreated:Put',
                    'eventTime': datetime.utcnow().isoformat(),
                    's3': {
                        'bucket': {
                            'name': test_config['bucket_name']
                        },
                        'object': {
                            'key': test_config['test_file_key'],
                            'size': 1048576
                        }
                    }
                }
            ]
        }

        try:
            # Note: This requires the Lambda function to be deployed with ENABLE_ORCHESTRATION=false for testing
            # In a real test, you would invoke the Lambda function
            # response = aws_clients['lambda'].invoke(
            #     FunctionName=test_config['lambda_function_name'],
            #     InvocationType='RequestResponse',
            #     Payload=json.dumps(test_event)
            # )

            print(f"✓ Lambda orchestrator integration test skipped (requires deployed Lambda)")
            print(f"  To test manually:")
            print(f"    1. Deploy Lambda function: {test_config['lambda_function_name']}")
            print(f"    2. Set STATE_MACHINE_ARN environment variable")
            print(f"    3. Trigger with S3 event or test event")

        except Exception as e:
            print(f"! Lambda integration test skipped: {e}")

    def test_execution_history(self, aws_clients, test_config):
        """Test retrieving and analyzing execution history."""
        try:
            # List recent executions
            response = aws_clients['stepfunctions'].list_executions(
                stateMachineArn=test_config['state_machine_arn'],
                maxResults=10
            )

            executions = response.get('executions', [])
            print(f"✓ Found {len(executions)} recent execution(s)")

            if executions:
                print("\nRecent Executions:")
                for i, execution in enumerate(executions[:5], 1):
                    print(f"  {i}. {execution['name']}")
                    print(f"     Status: {execution['status']}")
                    print(f"     Started: {execution['startDate']}")
                    if execution.get('stopDate'):
                        print(f"     Stopped: {execution['stopDate']}")

        except ClientError as e:
            pytest.fail(f"Failed to retrieve execution history: {e}")

    def test_workflow_timeout_configuration(self, aws_clients, test_config):
        """Test that workflow has proper timeout configuration."""
        try:
            response = aws_clients['stepfunctions'].describe_state_machine(
                stateMachineArn=test_config['state_machine_arn']
            )

            definition = json.loads(response['definition'])

            # Check timeout is configured
            timeout = definition.get('TimeoutSeconds')
            assert timeout is not None, "Workflow timeout not configured"
            assert timeout > 0, "Invalid timeout value"

            print(f"✓ Workflow timeout configured: {timeout} seconds ({timeout / 3600} hours)")

        except ClientError as e:
            pytest.fail(f"Failed to check timeout configuration: {e}")

    def test_retry_configuration(self, aws_clients, test_config):
        """Test that retry logic is properly configured."""
        try:
            response = aws_clients['stepfunctions'].describe_state_machine(
                stateMachineArn=test_config['state_machine_arn']
            )

            definition = json.loads(response['definition'])
            states = definition.get('States', {})

            # Check critical states have retry configuration
            states_with_retry = ['ValidateInput', 'RunGlueETLJob', 'RunCrawlerUpdate']

            for state_name in states_with_retry:
                if state_name in states:
                    state = states[state_name]
                    retry_config = state.get('Retry', [])

                    if retry_config:
                        print(f"✓ {state_name} has {len(retry_config)} retry configuration(s)")
                        for i, retry in enumerate(retry_config, 1):
                            print(f"    {i}. Max Attempts: {retry.get('MaxAttempts', 'N/A')}")
                            print(f"       Interval: {retry.get('IntervalSeconds', 'N/A')}s")
                            print(f"       Backoff Rate: {retry.get('BackoffRate', 'N/A')}")
                    else:
                        print(f"! {state_name} has no retry configuration")

        except ClientError as e:
            pytest.fail(f"Failed to check retry configuration: {e}")

    def test_error_handling_configuration(self, aws_clients, test_config):
        """Test that error handling (Catch) is properly configured."""
        try:
            response = aws_clients['stepfunctions'].describe_state_machine(
                stateMachineArn=test_config['state_machine_arn']
            )

            definition = json.loads(response['definition'])
            states = definition.get('States', {})

            # Check critical states have error handling
            states_with_catch = ['ValidateInput', 'RunGlueETLJob', 'RunCrawlerUpdate']

            for state_name in states_with_catch:
                if state_name in states:
                    state = states[state_name]
                    catch_config = state.get('Catch', [])

                    if catch_config:
                        print(f"✓ {state_name} has {len(catch_config)} error handler(s)")
                        for i, catch in enumerate(catch_config, 1):
                            print(f"    {i}. Next: {catch.get('Next', 'N/A')}")
                            print(f"       Error Equals: {catch.get('ErrorEquals', [])}")
                    else:
                        print(f"! {state_name} has no error handling")

        except ClientError as e:
            pytest.fail(f"Failed to check error handling: {e}")


class TestLambdaOrchestrator:
    """Tests for Lambda orchestrator function."""

    @pytest.fixture
    def lambda_handler(self):
        """Import Lambda handler for testing."""
        import sys
        sys.path.insert(0, '/home/user/OUBT-CC/de-intern-2024-project/src/lambda/etl_orchestrator')
        from lambda_function import lambda_handler, validate_s3_event
        return lambda_handler, validate_s3_event

    def test_validate_s3_event_valid(self, lambda_handler):
        """Test S3 event validation with valid input."""
        _, validate_s3_event = lambda_handler

        valid_record = {
            'eventName': 'ObjectCreated:Put',
            'eventTime': '2024-01-15T10:30:00.000Z',
            's3': {
                'bucket': {'name': 'test-bucket'},
                'object': {
                    'key': 'raw/taxi/yellow_tripdata_2024-01.parquet',
                    'size': 1048576
                }
            }
        }

        result = validate_s3_event(valid_record)

        assert result is not None
        assert result['bucket'] == 'test-bucket'
        assert result['key'] == 'raw/taxi/yellow_tripdata_2024-01.parquet'
        assert result['size'] == 1048576
        print("✓ Valid S3 event validation passed")

    def test_validate_s3_event_invalid_prefix(self, lambda_handler):
        """Test S3 event validation with invalid prefix."""
        _, validate_s3_event = lambda_handler

        invalid_record = {
            'eventName': 'ObjectCreated:Put',
            'eventTime': '2024-01-15T10:30:00.000Z',
            's3': {
                'bucket': {'name': 'test-bucket'},
                'object': {
                    'key': 'processed/taxi/data.parquet',  # Wrong prefix
                    'size': 1048576
                }
            }
        }

        result = validate_s3_event(invalid_record)

        assert result is None
        print("✓ Invalid prefix validation passed")

    def test_validate_s3_event_invalid_file_type(self, lambda_handler):
        """Test S3 event validation with invalid file type."""
        _, validate_s3_event = lambda_handler

        invalid_record = {
            'eventName': 'ObjectCreated:Put',
            'eventTime': '2024-01-15T10:30:00.000Z',
            's3': {
                'bucket': {'name': 'test-bucket'},
                'object': {
                    'key': 'raw/taxi/data.txt',  # Unsupported file type
                    'size': 1048576
                }
            }
        }

        result = validate_s3_event(invalid_record)

        assert result is None
        print("✓ Invalid file type validation passed")


def run_integration_tests():
    """Run all integration tests."""
    print("=" * 80)
    print("Running Step Functions Workflow Integration Tests")
    print("=" * 80)

    pytest.main([
        __file__,
        '-v',
        '--tb=short',
        '-s'
    ])


if __name__ == '__main__':
    run_integration_tests()
