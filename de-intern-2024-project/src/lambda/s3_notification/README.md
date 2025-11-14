# S3 File Notification Lambda Function

Week 2 scope: S3 event notification handler that logs structured file metadata to CloudWatch.

## Overview

This Lambda function is triggered when new files are uploaded to the S3 data lake with the `raw/taxi/` prefix and `.parquet` suffix. It extracts metadata and logs structured notifications to CloudWatch for monitoring and audit purposes.

**Note**: This is notification-only functionality. ETL processing will be added in Week 3.

## Features

- ✅ Triggered by S3 ObjectCreated events
- ✅ Filters for `raw/taxi/*.parquet` files
- ✅ Logs structured JSON to CloudWatch
- ✅ Captures: bucket, key, size, upload time
- ✅ Handles multiple files in single event
- ✅ Error handling and logging

## Architecture

```
S3 Bucket (raw/taxi/*.parquet)
    ↓ (ObjectCreated:Put)
Lambda Function (s3-file-notification)
    ↓ (structured log)
CloudWatch Logs
```

## Deployment

### Prerequisites

- AWS account with appropriate permissions
- Python 3.12
- AWS CLI configured
- S3 bucket created (e.g., `de-intern-2024-datalake-dev`)

### Step 1: Deploy Lambda Function

```bash
cd src/lambda
python deploy_lambda.py
```

This will:
1. Create deployment package from `s3_notification/`
2. Create/update IAM role with necessary permissions
3. Deploy Lambda function with:
   - Runtime: Python 3.12
   - Memory: 128 MB
   - Timeout: 3 seconds
   - Handler: `lambda_function.lambda_handler`

### Step 2: Configure S3 Trigger

```bash
python configure_s3_trigger.py YOUR_BUCKET_NAME
```

Example:
```bash
python configure_s3_trigger.py de-intern-2024-datalake-dev
```

This configures S3 to trigger the Lambda function for:
- Event: `s3:ObjectCreated:*`
- Prefix: `raw/taxi/`
- Suffix: `.parquet`

### Step 3: Test the Function

Upload a test file to S3:

```bash
# Create a test file
echo "test data" > test.parquet

# Upload to S3
aws s3 cp test.parquet s3://YOUR_BUCKET_NAME/raw/taxi/test.parquet

# Check CloudWatch Logs
aws logs tail /aws/lambda/s3-file-notification --follow
```

## Testing

Run the test suite:

```bash
# From project root
pytest tests/test_lambda_notification.py -v

# With coverage
pytest tests/test_lambda_notification.py --cov=src/lambda/s3_notification -v

# Run specific test class
pytest tests/test_lambda_notification.py::TestLambdaNotificationBasic -v
```

## Log Format

The Lambda function logs structured JSON to CloudWatch:

```json
{
  "event": "S3_FILE_UPLOADED",
  "timestamp": "2024-11-14T10:30:00.000Z",
  "bucket": "de-intern-2024-datalake-dev",
  "key": "raw/taxi/yellow_tripdata_2024_01.parquet",
  "size_bytes": 52428800,
  "size_mb": 50.0,
  "event_type": "ObjectCreated:Put",
  "request_id": "abc123-def456",
  "function_name": "s3-file-notification"
}
```

## Monitoring

### CloudWatch Logs

View logs in CloudWatch:
```bash
aws logs tail /aws/lambda/s3-file-notification --follow
```

### CloudWatch Metrics

Monitor Lambda metrics:
- Invocations
- Duration
- Errors
- Throttles

### Alarms (Optional)

Create CloudWatch alarms for:
- Error rate > 5%
- Duration > 2 seconds
- Throttles > 0

## Configuration

### Environment Variables

None required for Week 2 scope.

### IAM Permissions

The Lambda execution role has:
- `AWSLambdaBasicExecutionRole` - CloudWatch Logs
- `AmazonS3ReadOnlyAccess` - S3 metadata access

### Resource Limits

- Memory: 128 MB
- Timeout: 3 seconds
- Concurrent executions: Account default

## Troubleshooting

### Lambda Not Triggered

1. Check S3 event configuration:
   ```bash
   python configure_s3_trigger.py YOUR_BUCKET --verify-only
   ```

2. Verify Lambda permissions:
   ```bash
   aws lambda get-policy --function-name s3-file-notification
   ```

3. Check CloudWatch Logs for errors

### Permission Denied Errors

Ensure the Lambda execution role has:
- CloudWatch Logs write permissions
- S3 bucket read permissions

### Logs Not Appearing

1. Check CloudWatch log group exists: `/aws/lambda/s3-file-notification`
2. Verify IAM role has `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`

## Cost Estimation

### Lambda Costs
- Requests: $0.20 per 1M requests
- Duration: $0.0000166667 per GB-second
- Free Tier: 1M requests + 400,000 GB-seconds per month

### Example
- 1,000 files/month
- 128 MB memory
- 0.5 second duration
- **Cost**: ~$0.001/month (within free tier)

## Next Steps (Week 3)

Week 3 will add ETL processing:
1. Read file from S3
2. Validate data quality
3. Transform to processed zone
4. Update Glue Data Catalog
5. Trigger downstream workflows

## References

- [AWS Lambda Developer Guide](https://docs.aws.amazon.com/lambda/)
- [S3 Event Notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html)
- [CloudWatch Logs](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/)

## Support

For issues or questions:
1. Check CloudWatch Logs
2. Review IAM permissions
3. Verify S3 event configuration
4. Open GitHub issue with logs and error messages
