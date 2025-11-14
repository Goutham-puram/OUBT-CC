# CloudWatch Monitoring and Dashboards

Comprehensive monitoring solution for the DE Intern Pipeline using AWS CloudWatch.

## Overview

This monitoring package provides:
- **CloudWatch Dashboard**: Visual monitoring of all pipeline components
- **CloudWatch Alarms**: Proactive alerts for failures and anomalies
- **Log Insights Queries**: Pre-built queries for troubleshooting
- **SNS Notifications**: Email, SMS, and Slack alert integration
- **Runbook**: Step-by-step troubleshooting guides

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    CloudWatch Dashboard                      │
│  ┌──────────┬──────────┬──────────┬──────────┬──────────┐  │
│  │    S3    │  Lambda  │   Glue   │  Step    │ Redshift │  │
│  │ Metrics  │ Metrics  │ Metrics  │Functions │ Metrics  │  │
│  └──────────┴──────────┴──────────┴──────────┴──────────┘  │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   CloudWatch Alarms                          │
│  • Glue Job Failures                                        │
│  • Lambda Errors (> 5 in 5 min)                            │
│  • Data Freshness (> 24 hours)                             │
│  • Step Functions Failures                                  │
│  • Cost Anomalies (> $10/day)                              │
│  • Redshift Slow Queries                                    │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    SNS Notifications                         │
│  ├─ Email: your-email@example.com                          │
│  ├─ SMS: +1234567890                                        │
│  └─ Slack: #de-intern-alerts                               │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Setup SNS Notifications

First, create the SNS topic and subscribe your email:

```bash
python src/monitoring/setup_notifications.py \
  --emails your-email@example.com \
  --phone-numbers +1234567890
```

**Note:** Check your email and confirm the subscription.

### 2. Create CloudWatch Dashboard

Create a comprehensive dashboard with all pipeline metrics:

```bash
python src/monitoring/create_dashboard.py
```

View the dashboard at:
```
https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=DE-Intern-Pipeline-Monitor
```

### 3. Create CloudWatch Alarms

Set up all monitoring alarms:

```bash
python src/monitoring/create_alarms.py \
  --email your-email@example.com
```

### 4. Test the Setup

Send a test notification:

```bash
python src/monitoring/setup_notifications.py --test-message
```

## Components

### 1. Dashboard (`create_dashboard.py`)

Creates a CloudWatch dashboard with the following widgets:

**S3 Metrics:**
- Total objects in datalake
- Bucket storage size

**Lambda Metrics:**
- Invocations count
- Error count
- Average duration

**Glue Metrics:**
- Job tasks (success/failure)
- Active executors
- Data processing volume

**Step Functions Metrics:**
- Execution status (started/succeeded/failed)
- Execution duration
- Throttling events

**Redshift Metrics:**
- Compute capacity (RPUs)
- Query performance
- Data scanned
- Query count

**Usage:**

```bash
# Create dashboard
python src/monitoring/create_dashboard.py

# Delete dashboard
python src/monitoring/create_dashboard.py --delete

# Get dashboard info
python src/monitoring/create_dashboard.py --info

# Custom dashboard name
python src/monitoring/create_dashboard.py \
  --dashboard-name My-Custom-Dashboard
```

### 2. Alarms (`create_alarms.py`)

Creates the following CloudWatch alarms:

| Alarm Name | Metric | Threshold | Description |
|------------|--------|-----------|-------------|
| `alarm-glue-job-failure` | Glue failed tasks | ≥ 1 | Glue job has failed tasks |
| `alarm-lambda-errors` | Lambda errors | > 5 in 5 min | Lambda function errors |
| `alarm-data-freshness` | Custom metric | > 24 hours | Data not updated |
| `alarm-stepfunctions-failure` | Step Functions failed | ≥ 1 | Workflow execution failed |
| `alarm-cost-anomaly` | Custom metric | > $10/day | Daily cost exceeds budget |
| `alarm-redshift-slow-queries` | Query duration | > 5 minutes | Slow query performance |
| `alarm-s3-bucket-size` | Bucket size | > 100 GB | Storage threshold exceeded |

**Usage:**

```bash
# Create all alarms with email subscription
python src/monitoring/create_alarms.py \
  --email your-email@example.com

# List all alarms
python src/monitoring/create_alarms.py --list

# Delete all alarms
python src/monitoring/create_alarms.py --delete-all

# Custom SNS topic
python src/monitoring/create_alarms.py \
  --sns-topic-name my-custom-topic \
  --email your-email@example.com
```

### 3. Log Insights Queries (`log_queries.py`)

Pre-built CloudWatch Log Insights queries for troubleshooting:

**Available Queries:**

1. **failed_records** - Analyze failed records and error patterns
2. **processing_time_trends** - Processing time trends over time
3. **error_patterns** - Detect common error patterns
4. **lambda_performance** - Lambda function performance metrics
5. **lambda_errors_detailed** - Detailed Lambda error analysis
6. **glue_job_metrics** - Glue job execution metrics
7. **glue_job_failures** - Glue job failure analysis
8. **s3_upload_tracking** - Track S3 file uploads
9. **data_quality_issues** - Identify data quality issues
10. **stepfunctions_execution** - Step Functions execution analysis
11. **cost_optimization** - Cost optimization insights
12. **hourly_activity** - Hourly activity summary

**Usage:**

```bash
# List available queries
python src/monitoring/log_queries.py --list-queries

# Run a specific query (last 1 hour)
python src/monitoring/log_queries.py \
  --query failed_records

# Run query for last 24 hours
python src/monitoring/log_queries.py \
  --query glue_job_failures \
  --hours 24

# Output as JSON
python src/monitoring/log_queries.py \
  --query error_patterns \
  --hours 12 \
  --output json

# Limit results
python src/monitoring/log_queries.py \
  --query lambda_errors_detailed \
  --limit 50
```

**Example Output:**

```
Running query: Failed Records Analysis
Time range: Last 24 hour(s)

Query completed successfully
Results: 15 rows

================================================================================
Query Results
================================================================================
{
  "@timestamp": "2024-11-14 10:23:45.123",
  "error_type": "ERROR",
  "error_message": "Schema validation failed: missing column 'pickup_datetime'",
  "count": "23"
}
...
```

### 4. SNS Notifications (`setup_notifications.py`)

Manages SNS topics and subscriptions for alarm notifications.

**Supported Notification Types:**
- Email
- SMS
- HTTPS endpoints
- Slack webhooks (requires Lambda integration)

**Usage:**

```bash
# Setup with email only
python src/monitoring/setup_notifications.py \
  --emails email1@example.com email2@example.com

# Setup with email and SMS
python src/monitoring/setup_notifications.py \
  --emails your-email@example.com \
  --phone-numbers +1234567890 +10987654321

# Setup with Slack webhook
python src/monitoring/setup_notifications.py \
  --emails your-email@example.com \
  --slack-webhook https://hooks.slack.com/services/YOUR/WEBHOOK/URL

# Send test message
python src/monitoring/setup_notifications.py --test-message

# Custom topic name
python src/monitoring/setup_notifications.py \
  --topic-name my-alerts-topic \
  --emails your-email@example.com
```

**Email Subscription:**
After running the setup, you'll receive a confirmation email. Click the "Confirm subscription" link to start receiving alerts.

**SMS Requirements:**
- Phone numbers must be in E.164 format (e.g., +1234567890)
- SMS may incur additional charges

**Slack Integration:**
For Slack integration, you need to:
1. Create a Lambda function to transform SNS to Slack format
2. Subscribe the Lambda to the SNS topic
3. Configure the Lambda with your webhook URL

See `RUNBOOK.md` for detailed setup instructions.

## Monitoring Workflow

### Daily Operations

```bash
# 1. Check the dashboard
# Visit: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=DE-Intern-Pipeline-Monitor

# 2. Review any alarms that fired
python src/monitoring/create_alarms.py --list

# 3. Investigate issues using log queries
python src/monitoring/log_queries.py --query failed_records --hours 24

# 4. Follow runbook for resolution
# See RUNBOOK.md for step-by-step troubleshooting
```

### When an Alarm Fires

1. **Receive notification** via email/SMS/Slack
2. **Check CloudWatch dashboard** for context
3. **Run relevant log query** to investigate
4. **Follow runbook** for resolution steps
5. **Document incident** and update runbook if needed

### Example: Glue Job Failure

```bash
# 1. Alarm fires: alarm-glue-job-failure

# 2. Check Glue job failures
python src/monitoring/log_queries.py \
  --query glue_job_failures \
  --hours 24

# 3. Get detailed error information
aws glue get-job-runs \
  --job-name job-process-taxi-data \
  --max-results 5

# 4. Check data quality issues
python src/monitoring/log_queries.py \
  --query data_quality_issues \
  --hours 12

# 5. Follow RUNBOOK.md section "Glue Job Failures"
```

## Customization

### Adding Custom Widgets to Dashboard

Edit `create_dashboard.py` and add your widget configuration:

```python
def create_custom_widgets(self) -> List[Dict[str, Any]]:
    widgets = [
        {
            "type": "metric",
            "properties": {
                "metrics": [
                    ["AWS/Service", "MetricName", "Dimension", "Value"]
                ],
                "title": "My Custom Widget",
                ...
            }
        }
    ]
    return widgets
```

### Adding Custom Alarms

Edit `create_alarms.py` and add a new alarm method:

```python
def create_custom_alarm(self, topic_arn: str) -> bool:
    self.cloudwatch_client.put_metric_alarm(
        AlarmName='alarm-custom',
        MetricName='MyMetric',
        Namespace='MyNamespace',
        Threshold=100.0,
        ComparisonOperator='GreaterThanThreshold',
        AlarmActions=[topic_arn],
        ...
    )
```

### Adding Custom Log Queries

Edit `log_queries.py` and add to the `QUERIES` dictionary:

```python
'my_custom_query': {
    'name': 'My Custom Query',
    'description': 'Description of what this query does',
    'query': """
fields @timestamp, @message
| filter @message like /pattern/
| stats count() by bin(5m)
""",
    'log_groups': ['/aws/lambda/*']
}
```

## Best Practices

### Alarm Tuning

1. **Start conservative** - Set initial thresholds higher to avoid false alarms
2. **Monitor patterns** - Track alarm frequency over 2-4 weeks
3. **Adjust thresholds** - Fine-tune based on observed patterns
4. **Document changes** - Keep record of threshold changes and reasons

### Log Retention

Set appropriate log retention periods to balance cost and troubleshooting needs:

```bash
# Set log retention to 30 days
aws logs put-retention-policy \
  --log-group-name /aws/lambda/s3-notification-handler \
  --retention-in-days 30
```

### Cost Optimization

1. **Use metric filters** instead of Log Insights for regular monitoring
2. **Set log retention** policies to avoid indefinite storage
3. **Archive old logs** to S3 Glacier
4. **Review dashboard refresh rate** (default: 1 minute)

### Security

1. **Restrict SNS topic access** to necessary services only
2. **Encrypt SNS messages** for sensitive data
3. **Use VPC endpoints** for CloudWatch API calls where possible
4. **Audit alarm changes** using CloudTrail

## Troubleshooting

### Dashboard Not Showing Data

```bash
# Check if metrics exist
aws cloudwatch list-metrics --namespace AWS/Lambda

# Verify time range in dashboard
# Adjust the time range in CloudWatch console

# Check IAM permissions
aws iam get-user-policy --user-name your-user --policy-name CloudWatchReadOnly
```

### Alarms Not Firing

```bash
# Check alarm state
aws cloudwatch describe-alarms --alarm-names alarm-glue-job-failure

# Verify SNS topic permissions
aws sns get-topic-attributes --topic-arn arn:aws:sns:us-east-1:ACCOUNT:topic

# Test alarm manually
aws cloudwatch set-alarm-state \
  --alarm-name alarm-glue-job-failure \
  --state-value ALARM \
  --state-reason "Testing alarm"
```

### Not Receiving Notifications

```bash
# Check SNS subscriptions
aws sns list-subscriptions-by-topic \
  --topic-arn arn:aws:sns:us-east-1:ACCOUNT:etl-pipeline-alarms

# Check email spam folder
# Check subscription confirmation in email

# Send test message
python src/monitoring/setup_notifications.py --test-message
```

### Log Queries Timing Out

```bash
# Reduce time range
python src/monitoring/log_queries.py \
  --query failed_records \
  --hours 6  # Instead of 24

# Reduce result limit
python src/monitoring/log_queries.py \
  --query error_patterns \
  --limit 20  # Instead of 100

# Optimize query filter
# Add more specific filters to reduce data scanned
```

## Maintenance

### Regular Tasks

**Daily:**
- Review CloudWatch dashboard
- Check alarm state
- Investigate any anomalies

**Weekly:**
- Run cost optimization query
- Review alarm threshold effectiveness
- Clean up old log streams

**Monthly:**
- Update runbook with new issues
- Review and optimize log retention
- Analyze cost trends
- Update alarm thresholds based on patterns

### Updating Monitoring

```bash
# Update dashboard with new widgets
python src/monitoring/create_dashboard.py

# Add new alarms
python src/monitoring/create_alarms.py

# Test all components
python src/monitoring/setup_notifications.py --test-message
python src/monitoring/log_queries.py --list-queries
```

## Integration with CI/CD

Include monitoring setup in your deployment pipeline:

```bash
#!/bin/bash
# deploy-monitoring.sh

set -e

echo "Setting up CloudWatch monitoring..."

# 1. Setup SNS notifications
python src/monitoring/setup_notifications.py \
  --emails devops@example.com

# 2. Create dashboard
python src/monitoring/create_dashboard.py

# 3. Create alarms
python src/monitoring/create_alarms.py \
  --email devops@example.com

echo "Monitoring setup complete!"
```

## Resources

- [CloudWatch Dashboard Documentation](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Dashboards.html)
- [CloudWatch Alarms Documentation](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/AlarmThatSendsEmail.html)
- [CloudWatch Log Insights Query Syntax](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html)
- [SNS Documentation](https://docs.aws.amazon.com/sns/latest/dg/welcome.html)
- [Runbook Best Practices](./RUNBOOK.md)

## Support

For issues or questions:
- Check the [RUNBOOK.md](./RUNBOOK.md) for troubleshooting guides
- Review CloudWatch Logs for detailed error messages
- Contact: de-intern-team@example.com

---

*Last Updated: 2024-11-14*
