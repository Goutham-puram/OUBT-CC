# Step Functions ETL Orchestration Workflow

## Overview

This document describes the AWS Step Functions workflow for orchestrating the ETL pipeline. The workflow automates the complete data processing pipeline from S3 file upload to Glue ETL job execution and Data Catalog updates.

## Architecture

```
S3 Upload (raw/taxi/)
    ↓
Lambda Orchestrator (etl_orchestrator)
    ↓
Step Functions State Machine (etl-pipeline-workflow)
    ↓
├── ValidateInput (Lambda)
├── RunGlueETLJob (Glue Job)
├── CheckJobStatus (Choice State)
├── RunCrawlerUpdate (Glue Crawler)
└── NotifySuccess/NotifyFailure (SNS)
```

## Workflow States

### 1. ValidateInput
- **Type**: Lambda Task
- **Purpose**: Validate S3 event data and file metadata
- **Checks**:
  - File exists in S3
  - Correct prefix (raw/taxi/)
  - Valid file type (.parquet, .csv, .csv.gz)
  - File size within acceptable range
- **Retry**: 3 attempts with exponential backoff
- **Error Handling**: Routes to NotifyValidationFailure

### 2. CheckValidation
- **Type**: Choice State
- **Purpose**: Route based on validation result
- **Paths**:
  - Valid → RunGlueETLJob
  - Invalid → NotifyValidationFailure

### 3. RunGlueETLJob
- **Type**: Glue Task (Sync)
- **Purpose**: Execute Glue ETL job and wait for completion
- **Job**: job-process-taxi-data
- **Retry Logic**:
  - ConcurrentRunsExceeded: 5 attempts, 60s interval
  - InternalServiceException: 2 attempts, 30s interval
- **Error Handling**: Routes to NotifyGlueJobFailure

### 4. CheckGlueJobStatus
- **Type**: Choice State
- **Purpose**: Route based on Glue job result
- **Paths**:
  - SUCCEEDED → RunCrawlerUpdate
  - FAILED/TIMEOUT/STOPPED → NotifyGlueJobFailure

### 5. RunCrawlerUpdate
- **Type**: Glue Task (Async)
- **Purpose**: Update Glue Data Catalog with new data
- **Crawler**: crawler-taxi-raw
- **Retry Logic**:
  - CrawlerRunning: 3 attempts, 60s interval
  - OperationTimeout: 2 attempts, 30s interval
- **Error Handling**: Routes to HandleCrawlerFailure (non-critical)

### 6. WaitForCrawler
- **Type**: Wait State
- **Duration**: 30 seconds
- **Purpose**: Allow crawler to start and process data

### 7. CheckCrawlerStatus
- **Type**: Task State (SDK:Glue:GetCrawler)
- **Purpose**: Check current crawler state

### 8. IsCrawlerRunning
- **Type**: Choice State
- **Purpose**: Route based on crawler status
- **Paths**:
  - RUNNING → WaitForCrawler (loop)
  - READY → NotifySuccess
  - Other → HandleCrawlerFailure

### 9. NotifySuccess
- **Type**: SNS Task
- **Purpose**: Send success notification with execution details
- **Includes**:
  - Job run ID
  - Execution time
  - File details
  - Workflow execution ID

### 10. NotifyFailure
- **Type**: SNS Task
- **Purpose**: Send failure notification with error details

## Deployment

### Prerequisites

1. AWS Glue job deployed (`job-process-taxi-data`)
2. AWS Glue crawler configured (`crawler-taxi-raw`)
3. S3 bucket for data lake
4. Python 3.8+ with boto3

### Step 1: Deploy Step Functions Workflow

```bash
# Deploy workflow with default settings
python src/stepfunctions/deploy_workflow.py

# Deploy with custom settings
python src/stepfunctions/deploy_workflow.py \
    --state-machine-name etl-pipeline-workflow \
    --role-name StepFunctionsExecutionRole \
    --region us-east-1 \
    --definition-path infra/stepfunctions/etl_workflow.json \
    --update
```

The deployment script will:
1. Create IAM role for Step Functions execution
2. Attach required policies (Lambda, Glue, SNS, CloudWatch)
3. Create SNS topic for notifications
4. Deploy state machine
5. Create CloudWatch alarms

### Step 2: Subscribe to SNS Notifications

```bash
# Get SNS topic ARN
aws sns list-topics --query 'Topics[?contains(TopicArn, `etl-pipeline-notifications`)].TopicArn' --output text

# Subscribe with email
aws sns subscribe \
    --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:etl-pipeline-notifications \
    --protocol email \
    --notification-endpoint your-email@example.com

# Confirm subscription via email
```

### Step 3: Deploy Validation Lambda

```bash
# Package Lambda function
cd src/lambda/validate_input
zip -r validate_input.zip lambda_function.py

# Create Lambda function
aws lambda create-function \
    --function-name etl-orchestrator-validate-input \
    --runtime python3.11 \
    --role arn:aws:iam::ACCOUNT_ID:role/LambdaExecutionRole \
    --handler lambda_function.lambda_handler \
    --zip-file fileb://validate_input.zip \
    --timeout 60 \
    --memory-size 256
```

### Step 4: Update Lambda Orchestrator

```bash
# Package Lambda function
cd src/lambda/etl_orchestrator
zip -r etl_orchestrator.zip lambda_function.py

# Update existing Lambda or create new one
aws lambda update-function-code \
    --function-name etl-orchestrator \
    --zip-file fileb://etl_orchestrator.zip

# Set environment variables
aws lambda update-function-configuration \
    --function-name etl-orchestrator \
    --environment "Variables={STATE_MACHINE_ARN=arn:aws:states:us-east-1:ACCOUNT_ID:stateMachine:etl-pipeline-workflow,ENABLE_ORCHESTRATION=true}"
```

### Step 5: Configure S3 Event Notification

```bash
# Configure S3 bucket to trigger Lambda on object creation
python src/lambda/configure_s3_trigger.py \
    --function-name etl-orchestrator \
    --bucket-name ACCOUNT_ID-oubt-datalake \
    --prefix raw/taxi/ \
    --events s3:ObjectCreated:*
```

## Testing

### Verify Deployment

```bash
# Verify workflow is deployed correctly
python src/stepfunctions/test_workflow.py --action verify
```

### Manual Test Execution

```bash
# Start a test execution
python src/stepfunctions/test_workflow.py \
    --action start \
    --bucket ACCOUNT_ID-oubt-datalake \
    --key raw/taxi/test_data.parquet \
    --wait

# List recent executions
python src/stepfunctions/test_workflow.py --action list

# View execution history
python src/stepfunctions/test_workflow.py \
    --action history \
    --execution-arn arn:aws:states:us-east-1:ACCOUNT_ID:execution:etl-pipeline-workflow:EXECUTION_ID
```

### Integration Tests

```bash
# Run complete integration test suite
cd tests/integration
pytest test_stepfunctions_workflow.py -v

# Run specific test
pytest test_stepfunctions_workflow.py::TestStepFunctionsWorkflow::test_state_machine_exists -v
```

### End-to-End Test

```bash
# Upload a test file to trigger the complete workflow
aws s3 cp test_data.parquet s3://ACCOUNT_ID-oubt-datalake/raw/taxi/test_yellow_tripdata_2024-01.parquet

# Monitor execution in AWS Console or CLI
aws stepfunctions list-executions \
    --state-machine-arn arn:aws:states:us-east-1:ACCOUNT_ID:stateMachine:etl-pipeline-workflow \
    --max-results 5
```

## Monitoring

### CloudWatch Alarms

The deployment creates three CloudWatch alarms:

1. **ExecutionsFailed**: Alerts when executions fail
   - Threshold: ≥1 failure in 5 minutes
   - Action: SNS notification

2. **ExecutionThrottled**: Alerts when executions are throttled
   - Threshold: ≥1 throttle in 5 minutes
   - Action: SNS notification

3. **ExecutionTime**: Alerts when execution exceeds threshold
   - Threshold: >1 hour
   - Action: SNS notification

### CloudWatch Logs

Logs are available in:
- `/aws/vendedlogs/states/etl-pipeline-workflow` - Step Functions execution logs
- `/aws/lambda/etl-orchestrator` - Lambda orchestrator logs
- `/aws/lambda/etl-orchestrator-validate-input` - Validation Lambda logs

### X-Ray Tracing

X-Ray tracing is enabled for the workflow. View traces in the AWS X-Ray console to:
- Identify bottlenecks
- Debug errors
- Analyze execution paths

### Metrics

Monitor these key metrics in CloudWatch:
- `ExecutionsStarted` - Number of workflow executions started
- `ExecutionsSucceeded` - Number of successful executions
- `ExecutionsFailed` - Number of failed executions
- `ExecutionTime` - Duration of executions
- `ExecutionThrottled` - Number of throttled executions

## Error Handling

### Retry Logic

| State | Error Type | Max Attempts | Interval | Backoff |
|-------|-----------|--------------|----------|---------|
| ValidateInput | Lambda Service | 3 | 2s | 2.0 |
| RunGlueETLJob | ConcurrentRuns | 5 | 60s | 1.5 |
| RunGlueETLJob | InternalService | 2 | 30s | 2.0 |
| RunCrawlerUpdate | CrawlerRunning | 3 | 60s | 1.5 |
| RunCrawlerUpdate | OperationTimeout | 2 | 30s | 2.0 |

### Error Paths

1. **Validation Failure**: NotifyValidationFailure → End
2. **Glue Job Failure**: NotifyGlueJobFailure → NotifyFailure → End
3. **Crawler Failure**: HandleCrawlerFailure → NotifyPartialSuccess → End

### Partial Success

If Glue job succeeds but crawler fails, the workflow sends a "Partial Success" notification. This is non-critical as the catalog can be manually updated later.

## Notifications

### Success Notification

```json
{
  "status": "SUCCESS",
  "message": "ETL pipeline completed successfully",
  "jobName": "job-process-taxi-data",
  "jobRunId": "jr_abc123...",
  "executionTimeSeconds": 245,
  "bucket": "123456789012-oubt-datalake",
  "key": "raw/taxi/yellow_tripdata_2024-01.parquet",
  "crawlerState": "READY",
  "timestamp": "2024-01-15T10:35:00.000Z",
  "workflowExecutionId": "arn:aws:states:..."
}
```

### Failure Notification

```json
{
  "status": "FAILED",
  "message": "ETL pipeline execution failed",
  "bucket": "123456789012-oubt-datalake",
  "key": "raw/taxi/yellow_tripdata_2024-01.parquet",
  "error": {
    "Error": "States.TaskFailed",
    "Cause": "Glue job failed..."
  },
  "timestamp": "2024-01-15T10:35:00.000Z",
  "workflowExecutionId": "arn:aws:states:..."
}
```

## Troubleshooting

### Common Issues

#### 1. Execution Fails at ValidateInput

**Symptoms**: Execution stops after ValidateInput state
**Causes**:
- Lambda function not deployed
- Incorrect Lambda function name
- File doesn't meet validation criteria

**Solutions**:
```bash
# Check Lambda function exists
aws lambda get-function --function-name etl-orchestrator-validate-input

# View Lambda logs
aws logs tail /aws/lambda/etl-orchestrator-validate-input --follow
```

#### 2. Glue Job Fails with ConcurrentRunsExceeded

**Symptoms**: RunGlueETLJob state retries multiple times
**Causes**: Multiple files uploaded simultaneously

**Solutions**:
- Increase MaxConcurrentRuns in Glue job configuration
- Implement queuing mechanism
- Wait for retry logic to handle (5 attempts with backoff)

#### 3. Crawler Never Completes

**Symptoms**: Workflow stuck in WaitForCrawler loop
**Causes**:
- Crawler stuck in RUNNING state
- Crawler failed but status not updated

**Solutions**:
```bash
# Check crawler status
aws glue get-crawler --name crawler-taxi-raw

# Stop crawler if stuck
aws glue stop-crawler --name crawler-taxi-raw

# Restart workflow
```

#### 4. SNS Notifications Not Received

**Symptoms**: No email notifications for successes/failures
**Causes**:
- Email not subscribed to SNS topic
- Subscription not confirmed

**Solutions**:
```bash
# List subscriptions
aws sns list-subscriptions-by-topic \
    --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:etl-pipeline-notifications

# Create new subscription
aws sns subscribe \
    --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:etl-pipeline-notifications \
    --protocol email \
    --notification-endpoint your-email@example.com
```

### Debug Execution

```bash
# Get execution details
aws stepfunctions describe-execution \
    --execution-arn EXECUTION_ARN

# Get execution history
aws stepfunctions get-execution-history \
    --execution-arn EXECUTION_ARN \
    --max-results 100

# View CloudWatch logs
aws logs tail /aws/vendedlogs/states/etl-pipeline-workflow --follow
```

## Cost Optimization

### Estimated Costs

Per execution (assuming 1 MB file, 2-minute Glue job):
- Step Functions: ~$0.00025 (25 state transitions)
- Lambda (orchestrator): ~$0.00001
- Lambda (validation): ~$0.00001
- Glue Job: ~$0.0133 (2 minutes at $0.44/DPU-hour, 2 DPUs)
- Glue Crawler: ~$0.0073 (2 minutes at $0.44/DPU-hour, 1 DPU)
- SNS: ~$0.000001
- **Total per execution: ~$0.021**

### Optimization Tips

1. **Use Glue Job Bookmarks**: Prevent reprocessing of data
2. **Reduce Crawler Frequency**: Only run when schema changes expected
3. **Right-size Glue Workers**: Use appropriate worker type and count
4. **Express Workflows**: Consider Express workflow for high-frequency small files
5. **Batch Processing**: Process multiple files in single execution

## Maintenance

### Update State Machine Definition

```bash
# Make changes to infra/stepfunctions/etl_workflow.json
# Then update deployed state machine
python src/stepfunctions/deploy_workflow.py --update
```

### Update IAM Policies

```bash
# Policies are inline and updated automatically during deployment
python src/stepfunctions/deploy_workflow.py --update
```

### Cleanup Old Executions

Step Functions execution history is retained. To reduce clutter:

```bash
# Note: There's no bulk delete API, use AWS Console to manage old executions
# Or implement custom cleanup script using list_executions + stop_execution
```

## References

- [AWS Step Functions Documentation](https://docs.aws.amazon.com/step-functions/)
- [Amazon States Language Specification](https://states-language.net/spec.html)
- [AWS Glue Developer Guide](https://docs.aws.amazon.com/glue/)
- [Step Functions Best Practices](https://docs.aws.amazon.com/step-functions/latest/dg/bp-express-standard.html)

## Support

For issues or questions:
1. Check CloudWatch logs
2. Review execution history
3. Check AWS service health dashboard
4. Consult AWS Support (if applicable)
