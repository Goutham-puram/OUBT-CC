# Step Functions ETL Workflow - Quick Start Guide

## Overview

This implementation provides a comprehensive AWS Step Functions workflow for orchestrating ETL pipelines. It automates the complete data processing pipeline from S3 file upload through Glue ETL job execution to Data Catalog updates.

## What's Included

### 1. State Machine Definition
**Location**: `infra/stepfunctions/etl_workflow.json`

A complete Step Functions state machine with:
- Input validation
- Glue ETL job execution (with sync wait)
- Job status checking
- Crawler updates
- Success/failure notifications
- Comprehensive error handling and retry logic

### 2. Lambda Orchestrator
**Location**: `src/lambda/etl_orchestrator/lambda_function.py`

Updated from Week 2 S3 notification Lambda to:
- Receive S3 ObjectCreated events
- Validate S3 event data
- Trigger Step Functions workflow
- Handle errors gracefully
- Provide backward compatibility (notification-only mode)

### 3. Input Validation Lambda
**Location**: `src/lambda/validate_input/lambda_function.py`

Validates S3 event input before processing:
- File existence check
- File type validation (.parquet, .csv, .csv.gz)
- File size validation (100 bytes - 10 GB)
- Prefix validation (raw/taxi/)
- Detailed validation reporting

### 4. Deployment Script
**Location**: `src/stepfunctions/deploy_workflow.py`

Automated deployment tool that:
- Creates IAM execution role with required policies
- Deploys SNS topic for notifications
- Creates/updates Step Functions state machine
- Configures CloudWatch alarms
- Enables X-Ray tracing
- Provides deployment verification

### 5. Test Utilities
**Location**: `src/stepfunctions/test_workflow.py`

Command-line tool for:
- Verifying deployment
- Triggering test executions
- Monitoring execution progress
- Viewing execution history
- Managing running executions

### 6. Integration Tests
**Location**: `tests/integration/test_stepfunctions_workflow.py`

Comprehensive test suite covering:
- State machine deployment
- State definition validation
- IAM role permissions
- Execution testing (valid/invalid inputs)
- SNS topic configuration
- CloudWatch alarms
- Retry and error handling

### 7. Documentation
**Location**: `docs/STEPFUNCTIONS_WORKFLOW.md`

Complete documentation including:
- Architecture diagrams
- State descriptions
- Deployment instructions
- Testing procedures
- Monitoring and troubleshooting
- Cost optimization tips

## Quick Start

### Prerequisites

1. AWS CLI configured with appropriate credentials
2. Python 3.8 or higher
3. boto3 library installed
4. Existing AWS Glue job: `job-process-taxi-data`
5. Existing AWS Glue crawler: `crawler-taxi-raw`
6. S3 bucket for data lake

### Step 1: Deploy the Workflow

```bash
# Navigate to project directory
cd de-intern-2024-project

# Deploy Step Functions workflow
python src/stepfunctions/deploy_workflow.py --update

# Output will show:
# - IAM role ARN
# - SNS topic ARN
# - State machine ARN
# - CloudWatch alarms created
```

### Step 2: Subscribe to Notifications

```bash
# Get the SNS topic ARN from deployment output
export SNS_TOPIC_ARN="arn:aws:sns:us-east-1:ACCOUNT_ID:etl-pipeline-notifications"

# Subscribe with your email
aws sns subscribe \
    --topic-arn $SNS_TOPIC_ARN \
    --protocol email \
    --notification-endpoint your-email@example.com

# Check your email and confirm the subscription
```

### Step 3: Deploy Lambda Functions

```bash
# Deploy validation Lambda
cd src/lambda/validate_input
zip -r validate_input.zip lambda_function.py

aws lambda create-function \
    --function-name etl-orchestrator-validate-input \
    --runtime python3.11 \
    --role arn:aws:iam::ACCOUNT_ID:role/LambdaExecutionRole \
    --handler lambda_function.lambda_handler \
    --zip-file fileb://validate_input.zip \
    --timeout 60 \
    --memory-size 256

# Deploy orchestrator Lambda
cd ../etl_orchestrator
zip -r etl_orchestrator.zip lambda_function.py

# Update existing Lambda or create new one
aws lambda update-function-code \
    --function-name etl-orchestrator \
    --zip-file fileb://etl_orchestrator.zip

# Configure environment variables
aws lambda update-function-configuration \
    --function-name etl-orchestrator \
    --environment "Variables={STATE_MACHINE_ARN=arn:aws:states:us-east-1:ACCOUNT_ID:stateMachine:etl-pipeline-workflow,ENABLE_ORCHESTRATION=true}"
```

### Step 4: Verify Deployment

```bash
# Verify workflow is deployed correctly
python src/stepfunctions/test_workflow.py --action verify

# Expected output:
# ✓ State Machine: etl-pipeline-workflow
# ✓ All required states present
# ✓ Workflow deployment verified successfully
```

### Step 5: Run a Test Execution

```bash
# Get your account ID
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Start a test execution
python src/stepfunctions/test_workflow.py \
    --action start \
    --bucket ${ACCOUNT_ID}-oubt-datalake \
    --key raw/taxi/test_data.parquet \
    --wait

# Monitor in AWS Console:
# https://console.aws.amazon.com/states/home?region=us-east-1#/statemachines/view/arn:aws:states:us-east-1:ACCOUNT_ID:stateMachine:etl-pipeline-workflow
```

## Architecture Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                         S3 Upload Event                              │
│                    s3://bucket/raw/taxi/*.parquet                    │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Lambda: etl_orchestrator                          │
│  - Validates S3 event                                                │
│  - Extracts metadata                                                 │
│  - Triggers Step Functions                                           │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│              Step Functions: etl-pipeline-workflow                   │
│                                                                       │
│  ┌──────────────────┐         ┌──────────────────┐                 │
│  │ ValidateInput    │────────▶│ CheckValidation  │                 │
│  │ (Lambda)         │         │ (Choice)         │                 │
│  └──────────────────┘         └─────────┬────────┘                 │
│                                          │                           │
│                           Valid          │ Invalid                  │
│                                          │                           │
│                    ┌─────────────────────┴─────────┐                │
│                    ▼                               ▼                │
│          ┌──────────────────┐          ┌──────────────────┐        │
│          │ RunGlueETLJob    │          │ NotifyValidation │        │
│          │ (Glue Sync)      │          │ Failure (SNS)    │        │
│          └────────┬─────────┘          └──────────────────┘        │
│                   │                                                 │
│                   ▼                                                 │
│          ┌──────────────────┐                                       │
│          │ CheckJobStatus   │───────Failed────▶ NotifyFailure      │
│          │ (Choice)         │                   (SNS)               │
│          └────────┬─────────┘                                       │
│                   │                                                 │
│              Succeeded                                               │
│                   │                                                 │
│                   ▼                                                 │
│          ┌──────────────────┐                                       │
│          │ RunCrawlerUpdate │                                       │
│          │ (Glue Async)     │                                       │
│          └────────┬─────────┘                                       │
│                   │                                                 │
│                   ▼                                                 │
│          ┌──────────────────┐                                       │
│          │ WaitForCrawler   │◀─────┐                               │
│          │ (Wait 30s)       │      │                               │
│          └────────┬─────────┘      │                               │
│                   │                │                               │
│                   ▼                │                               │
│          ┌──────────────────┐      │                               │
│          │ CheckCrawler     │──Running                             │
│          │ Status           │                                       │
│          └────────┬─────────┘                                       │
│                   │                                                 │
│                Ready                                                 │
│                   │                                                 │
│                   ▼                                                 │
│          ┌──────────────────┐                                       │
│          │ NotifySuccess    │                                       │
│          │ (SNS)            │                                       │
│          └──────────────────┘                                       │
└─────────────────────────────────────────────────────────────────────┘
```

## Features

### Error Handling

- **Retry Logic**: Automatic retries with exponential backoff for transient failures
- **Catch Blocks**: Graceful error handling with detailed notifications
- **Partial Success**: Non-critical failures (like crawler) don't fail entire workflow

### Monitoring

- **CloudWatch Logs**: Comprehensive logging for all states
- **CloudWatch Alarms**: Automated alerts for failures, throttling, and long executions
- **X-Ray Tracing**: Distributed tracing for performance analysis
- **SNS Notifications**: Real-time alerts for success and failure

### Scalability

- **Concurrent Execution Control**: Prevents overwhelming downstream services
- **Queue Management**: Built-in retry logic handles concurrent runs
- **Sync Integration**: Glue job runs synchronously to prevent data races

## Cost Estimate

For a typical execution processing a 1 MB file:

| Service | Cost |
|---------|------|
| Step Functions | $0.00025 |
| Lambda (Orchestrator) | $0.00001 |
| Lambda (Validation) | $0.00001 |
| Glue Job (2 min) | $0.0133 |
| Glue Crawler (2 min) | $0.0073 |
| SNS | $0.000001 |
| **Total** | **~$0.021** |

Monthly cost for 1,000 executions: ~$21

## File Structure

```
de-intern-2024-project/
├── infra/
│   └── stepfunctions/
│       └── etl_workflow.json          # State machine definition
├── src/
│   ├── lambda/
│   │   ├── etl_orchestrator/
│   │   │   ├── lambda_function.py    # Main orchestrator
│   │   │   ├── requirements.txt
│   │   │   └── __init__.py
│   │   └── validate_input/
│   │       └── lambda_function.py    # Input validation
│   └── stepfunctions/
│       ├── deploy_workflow.py        # Deployment script
│       ├── test_workflow.py          # Test utilities
│       └── __init__.py
├── tests/
│   └── integration/
│       └── test_stepfunctions_workflow.py  # Integration tests
├── docs/
│   └── STEPFUNCTIONS_WORKFLOW.md    # Detailed documentation
└── README_STEPFUNCTIONS.md          # This file
```

## Common Commands

```bash
# Deploy/update workflow
python src/stepfunctions/deploy_workflow.py --update

# Verify deployment
python src/stepfunctions/test_workflow.py --action verify

# Start test execution
python src/stepfunctions/test_workflow.py --action start --bucket BUCKET --key KEY

# List recent executions
python src/stepfunctions/test_workflow.py --action list

# View execution history
python src/stepfunctions/test_workflow.py --action history --execution-arn ARN

# Stop execution
python src/stepfunctions/test_workflow.py --action stop --execution-arn ARN

# Run integration tests
cd tests/integration
pytest test_stepfunctions_workflow.py -v

# View CloudWatch logs
aws logs tail /aws/vendedlogs/states/etl-pipeline-workflow --follow
```

## Troubleshooting

### State Machine Not Found

```bash
# Check if state machine exists
aws stepfunctions list-state-machines \
    --query "stateMachines[?name=='etl-pipeline-workflow']"

# If not found, deploy it
python src/stepfunctions/deploy_workflow.py
```

### Lambda Function Errors

```bash
# Check Lambda function logs
aws logs tail /aws/lambda/etl-orchestrator --follow
aws logs tail /aws/lambda/etl-orchestrator-validate-input --follow
```

### Execution Failures

```bash
# Get execution details
aws stepfunctions describe-execution --execution-arn <ARN>

# Get execution history with full details
aws stepfunctions get-execution-history --execution-arn <ARN> --max-results 100
```

## Next Steps

1. **Production Deployment**:
   - Review and adjust retry configurations
   - Configure production SNS subscriptions
   - Set up alerting and monitoring dashboards
   - Implement cost monitoring

2. **Enhanced Features**:
   - Add support for multiple file types
   - Implement batch processing
   - Add data quality checks
   - Integrate with other AWS services

3. **Optimization**:
   - Fine-tune Glue job resources
   - Implement caching strategies
   - Optimize crawler schedules
   - Consider Express workflows for high-frequency events

## Support

For detailed documentation, see: `docs/STEPFUNCTIONS_WORKFLOW.md`

For issues:
1. Check CloudWatch logs
2. Review execution history
3. Verify IAM permissions
4. Check AWS service quotas

## References

- [AWS Step Functions Documentation](https://docs.aws.amazon.com/step-functions/)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Amazon States Language](https://states-language.net/spec.html)
