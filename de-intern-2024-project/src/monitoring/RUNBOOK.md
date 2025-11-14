# DE Intern Pipeline Monitoring Runbook

## Overview

This runbook provides troubleshooting guides for common issues in the DE Intern Pipeline. Use this guide to quickly diagnose and resolve problems.

## Table of Contents

1. [CloudWatch Alarm Response](#cloudwatch-alarm-response)
2. [Glue Job Failures](#glue-job-failures)
3. [Lambda Errors](#lambda-errors)
4. [Step Functions Failures](#step-functions-failures)
5. [Data Freshness Issues](#data-freshness-issues)
6. [Redshift Query Performance](#redshift-query-performance)
7. [S3 Issues](#s3-issues)
8. [Cost Anomalies](#cost-anomalies)
9. [Quick Reference Commands](#quick-reference-commands)

---

## CloudWatch Alarm Response

### Alarm: alarm-glue-job-failure

**Symptom:** Glue ETL job has failed

**Impact:** Data processing pipeline is halted, data may not be up-to-date

**Investigation Steps:**

1. **Check Glue Job Status**
   ```bash
   aws glue get-job-run \
     --job-name job-process-taxi-data \
     --run-id <RUN_ID> \
     --region us-east-1
   ```

2. **View CloudWatch Logs**
   ```bash
   # Run the log query to see recent failures
   python src/monitoring/log_queries.py \
     --query glue_job_failures \
     --hours 24
   ```

3. **Check Error Messages**
   - Look for common errors:
     - Out of memory errors
     - Data format issues
     - Permission errors
     - Timeout errors

**Resolution:**

| Error Type | Solution |
|------------|----------|
| Out of Memory | Increase worker count or worker type in `src/glue/create_etl_job.py` |
| Data Format Issues | Check source data schema, update ETL logic |
| Permission Errors | Verify IAM role has S3/Glue permissions |
| Timeout | Increase timeout value or optimize ETL code |

**Commands to Fix:**

```bash
# Restart Glue job
aws glue start-job-run \
  --job-name job-process-taxi-data \
  --region us-east-1

# Check job status
python src/glue/create_etl_job.py --info-only
```

---

## Glue Job Failures

### Common Issues

#### 1. Data Quality Issues

**Symptoms:**
- Job fails during validation
- Logs show "ValidationError" or "SchemaException"

**Investigation:**
```bash
# Check data quality issues
python src/monitoring/log_queries.py \
  --query data_quality_issues \
  --hours 12
```

**Resolution:**
1. Review source data schema
2. Update data validation rules in `src/transform/glue_etl_job.py`
3. Add error handling for malformed records
4. Re-run job with fixed logic

#### 2. Resource Constraints

**Symptoms:**
- "Out of memory" errors
- Very slow processing
- Executor failures

**Resolution:**
```python
# Update worker configuration in create_etl_job.py
'NumberOfWorkers': 5,  # Increase from 2
'WorkerType': 'G.2X',  # Upgrade from G.1X
```

#### 3. S3 Access Issues

**Symptoms:**
- "Access Denied" errors
- Cannot read/write to S3

**Resolution:**
```bash
# Check IAM role permissions
aws iam get-role-policy \
  --role-name AWSGlueServiceRole-intern \
  --policy-name GlueS3AccessPolicy

# Verify bucket permissions
aws s3 ls s3://<ACCOUNT_ID>-oubt-datalake/raw/taxi/
```

---

## Lambda Errors

### Alarm: alarm-lambda-errors

**Symptom:** Lambda function has > 5 errors in 5 minutes

**Impact:** S3 file uploads may not be processed, notifications may be lost

**Investigation Steps:**

1. **View Recent Lambda Errors**
   ```bash
   python src/monitoring/log_queries.py \
     --query lambda_errors_detailed \
     --hours 1
   ```

2. **Check Lambda Metrics**
   ```bash
   aws cloudwatch get-metric-statistics \
     --namespace AWS/Lambda \
     --metric-name Errors \
     --dimensions Name=FunctionName,Value=s3-notification-handler \
     --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
     --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
     --period 300 \
     --statistics Sum \
     --region us-east-1
   ```

3. **Test Lambda Function**
   ```bash
   # Invoke with test event
   aws lambda invoke \
     --function-name s3-notification-handler \
     --payload file://test-event.json \
     --region us-east-1 \
     response.json
   ```

**Common Lambda Issues:**

| Error | Cause | Solution |
|-------|-------|----------|
| Timeout | Function exceeds time limit | Increase timeout, optimize code |
| Memory Error | Insufficient memory | Increase memory allocation |
| Permission Error | Missing IAM permissions | Update Lambda execution role |
| Throttling | Too many concurrent executions | Increase reserved concurrency |

**Resolution:**
```bash
# Update Lambda configuration
aws lambda update-function-configuration \
  --function-name s3-notification-handler \
  --timeout 60 \
  --memory-size 256 \
  --region us-east-1
```

---

## Step Functions Failures

### Alarm: alarm-stepfunctions-failure

**Symptom:** Step Functions workflow execution failed

**Impact:** Entire ETL orchestration is halted

**Investigation Steps:**

1. **List Recent Executions**
   ```bash
   aws stepfunctions list-executions \
     --state-machine-arn arn:aws:states:us-east-1:<ACCOUNT_ID>:stateMachine:etl-pipeline-workflow \
     --status-filter FAILED \
     --max-results 10 \
     --region us-east-1
   ```

2. **Get Execution Details**
   ```bash
   aws stepfunctions describe-execution \
     --execution-arn <EXECUTION_ARN> \
     --region us-east-1
   ```

3. **View Execution History**
   ```bash
   aws stepfunctions get-execution-history \
     --execution-arn <EXECUTION_ARN> \
     --region us-east-1 \
     --max-results 100
   ```

**Common Failure Points:**

1. **Glue Job State Failure**
   - Check Glue job logs
   - Verify job completed successfully
   - Ensure job name is correct

2. **Lambda Invocation Failure**
   - Check Lambda function errors
   - Verify Lambda has permissions
   - Check input/output mapping

3. **SNS Notification Failure**
   - Verify SNS topic exists
   - Check Step Functions has publish permissions

**Resolution:**
```bash
# Restart failed execution with same input
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:<ACCOUNT_ID>:stateMachine:etl-pipeline-workflow \
  --input file://execution-input.json \
  --region us-east-1
```

---

## Data Freshness Issues

### Alarm: alarm-data-freshness

**Symptom:** Data has not been updated for > 24 hours

**Impact:** Dashboards and reports show stale data

**Investigation Steps:**

1. **Check Last S3 Upload**
   ```bash
   aws s3 ls s3://<ACCOUNT_ID>-oubt-datalake/raw/taxi/ \
     --recursive \
     --human-readable \
     --summarize | tail -20
   ```

2. **Check Last Glue Job Run**
   ```bash
   aws glue get-job-runs \
     --job-name job-process-taxi-data \
     --max-results 5 \
     --region us-east-1
   ```

3. **Check Step Functions Executions**
   ```bash
   aws stepfunctions list-executions \
     --state-machine-arn arn:aws:states:us-east-1:<ACCOUNT_ID>:stateMachine:etl-pipeline-workflow \
     --max-results 5 \
     --region us-east-1
   ```

**Possible Causes:**

1. **No New Data Uploaded**
   - Check source data pipeline
   - Verify data collection is working

2. **S3 Trigger Not Working**
   - Check Lambda S3 event notification configuration
   - Verify S3 bucket event notifications are enabled

3. **Workflow Not Triggered**
   - Check if Step Functions execution is scheduled
   - Verify EventBridge rules if using scheduled execution

**Resolution:**
```bash
# Manually trigger ETL workflow
python src/stepfunctions/test_workflow.py --execute

# Or trigger Glue job directly
aws glue start-job-run \
  --job-name job-process-taxi-data \
  --region us-east-1
```

---

## Redshift Query Performance

### Alarm: alarm-redshift-slow-queries

**Symptom:** Redshift queries taking > 5 minutes

**Impact:** Slow dashboard loading, delayed analytics

**Investigation Steps:**

1. **Check Running Queries**
   ```sql
   -- Connect to Redshift and run:
   SELECT
     query,
     pid,
     user_name,
     starttime,
     duration/1000000 as duration_seconds,
     substring(querytxt, 1, 100) as query_text
   FROM svl_qlog
   WHERE duration > 300000000  -- 5 minutes in microseconds
   ORDER BY starttime DESC
   LIMIT 10;
   ```

2. **Check Query Plan**
   ```sql
   EXPLAIN <your_slow_query>;
   ```

3. **Check Table Statistics**
   ```sql
   ANALYZE <table_name>;
   ```

**Common Performance Issues:**

| Issue | Cause | Solution |
|-------|-------|----------|
| Full table scan | Missing distribution/sort keys | Add DISTKEY and SORTKEY |
| High disk I/O | Data not compressed | Use columnar compression |
| Skewed data | Poor distribution key choice | Redesign distribution strategy |
| Missing stats | ANALYZE not run | Run ANALYZE on tables |

**Resolution:**
```sql
-- Vacuum and analyze tables
VACUUM DELETE ONLY taxi_data;
ANALYZE taxi_data;

-- Add distribution and sort keys if missing
ALTER TABLE taxi_data
  ALTER DISTKEY pickup_datetime
  ALTER SORTKEY (pickup_datetime);
```

---

## S3 Issues

### Alarm: alarm-s3-bucket-size

**Symptom:** S3 bucket size exceeds 100 GB

**Impact:** Increased storage costs, potential quota issues

**Investigation Steps:**

1. **Check Bucket Size by Prefix**
   ```bash
   aws s3 ls s3://<ACCOUNT_ID>-oubt-datalake/ \
     --recursive \
     --human-readable \
     --summarize
   ```

2. **Find Large Files**
   ```bash
   aws s3 ls s3://<ACCOUNT_ID>-oubt-datalake/ \
     --recursive \
     --human-readable | \
     sort -k3 -h -r | head -20
   ```

**Resolution:**

1. **Implement Lifecycle Policies**
   ```json
   {
     "Rules": [
       {
         "Id": "Archive old raw data",
         "Status": "Enabled",
         "Prefix": "raw/",
         "Transitions": [
           {
             "Days": 90,
             "StorageClass": "GLACIER"
           }
         ]
       },
       {
         "Id": "Delete temp data",
         "Status": "Enabled",
         "Prefix": "temp/",
         "Expiration": {
           "Days": 7
         }
       }
     ]
   }
   ```

2. **Clean Up Old Data**
   ```bash
   # Delete files older than 90 days in temp/
   aws s3 ls s3://<ACCOUNT_ID>-oubt-datalake/temp/ --recursive | \
     awk '{if ($1 < "2024-08-15") print $4}' | \
     xargs -I {} aws s3 rm s3://<ACCOUNT_ID>-oubt-datalake/{}
   ```

---

## Cost Anomalies

### Alarm: alarm-cost-anomaly

**Symptom:** Daily AWS costs exceed $10

**Impact:** Budget overrun, unexpected charges

**Investigation Steps:**

1. **Check Cost Explorer**
   ```bash
   aws ce get-cost-and-usage \
     --time-period Start=$(date -u -d '7 days ago' +%Y-%m-%d),End=$(date -u +%Y-%m-%d) \
     --granularity DAILY \
     --metrics BlendedCost \
     --group-by Type=SERVICE \
     --region us-east-1
   ```

2. **Identify Top Cost Drivers**
   - Check Glue DPU hours
   - Check Redshift Serverless RPU hours
   - Check S3 storage and requests
   - Check Lambda invocations

**Common Cost Issues:**

| Service | Cause | Solution |
|---------|-------|----------|
| Glue | Long-running jobs, many workers | Optimize job, reduce workers |
| Redshift | High RPU usage | Optimize queries, reduce workload |
| S3 | Many API calls | Batch operations, use lifecycle policies |
| Lambda | High invocation count | Optimize triggers, batch processing |

**Resolution:**

1. **Optimize Glue Jobs**
   - Reduce worker count
   - Enable job bookmarks to process only new data
   - Use Glue job timeout to prevent runaway jobs

2. **Optimize Redshift**
   - Reduce RPU capacity if over-provisioned
   - Optimize query patterns
   - Schedule heavy queries for off-peak hours

3. **Set Up AWS Budgets**
   ```bash
   aws budgets create-budget \
     --account-id <ACCOUNT_ID> \
     --budget file://budget.json \
     --notifications-with-subscribers file://notifications.json
   ```

---

## Quick Reference Commands

### Dashboard and Monitoring

```bash
# Create CloudWatch Dashboard
python src/monitoring/create_dashboard.py

# Create CloudWatch Alarms
python src/monitoring/create_alarms.py --email your-email@example.com

# Setup SNS Notifications
python src/monitoring/setup_notifications.py --emails your-email@example.com

# Run Log Insights Query
python src/monitoring/log_queries.py --query failed_records --hours 24

# List available queries
python src/monitoring/log_queries.py --list-queries
```

### Glue Operations

```bash
# Check Glue job info
python src/glue/create_etl_job.py --info-only

# Start Glue job
aws glue start-job-run --job-name job-process-taxi-data

# List recent job runs
aws glue get-job-runs --job-name job-process-taxi-data --max-results 10

# Stop running job
aws glue batch-stop-job-run \
  --job-name job-process-taxi-data \
  --job-run-ids <RUN_ID>
```

### Lambda Operations

```bash
# View Lambda logs
aws logs tail /aws/lambda/s3-notification-handler --follow

# Update Lambda code
aws lambda update-function-code \
  --function-name s3-notification-handler \
  --zip-file fileb://function.zip

# Get Lambda metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Errors \
  --dimensions Name=FunctionName,Value=s3-notification-handler \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Sum
```

### Step Functions Operations

```bash
# List executions
aws stepfunctions list-executions \
  --state-machine-arn arn:aws:states:us-east-1:<ACCOUNT_ID>:stateMachine:etl-pipeline-workflow

# Start execution
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:<ACCOUNT_ID>:stateMachine:etl-pipeline-workflow \
  --input '{}'

# Stop execution
aws stepfunctions stop-execution \
  --execution-arn <EXECUTION_ARN>
```

### S3 Operations

```bash
# List bucket contents
aws s3 ls s3://<ACCOUNT_ID>-oubt-datalake/ --recursive --human-readable

# Check bucket size
aws s3 ls s3://<ACCOUNT_ID>-oubt-datalake/ --recursive | \
  awk '{sum+=$3} END {print "Total: " sum/1024/1024/1024 " GB"}'

# Sync local to S3
aws s3 sync ./local-data/ s3://<ACCOUNT_ID>-oubt-datalake/raw/taxi/

# Copy with metadata
aws s3 cp file.parquet s3://<ACCOUNT_ID>-oubt-datalake/raw/taxi/ \
  --metadata pipeline=etl,timestamp=$(date -u +%Y-%m-%d)
```

### Redshift Operations

```bash
# Connect to Redshift
psql -h <workgroup-endpoint> -U admin -d dev -p 5439

# Check table sizes
SELECT
  "table",
  size,
  tbl_rows
FROM svv_table_info
ORDER BY size DESC
LIMIT 10;

# Check running queries
SELECT * FROM stv_recents WHERE status = 'Running';
```

---

## Escalation Procedures

### Severity Levels

| Severity | Description | Response Time | Example |
|----------|-------------|---------------|---------|
| P1 - Critical | Pipeline completely down | 15 minutes | All Glue jobs failing |
| P2 - High | Major functionality impaired | 1 hour | Data > 24 hours old |
| P3 - Medium | Minor functionality impaired | 4 hours | Slow queries |
| P4 - Low | Cosmetic issues | Next business day | Dashboard widget missing |

### Contact Information

```
Primary On-Call: [Your Team Lead]
Secondary On-Call: [Backup Engineer]
Manager: [Engineering Manager]

Slack Channels:
- #de-intern-alerts (monitoring alerts)
- #de-intern-support (general support)

Email: de-intern-team@example.com
```

### When to Escalate

1. **Immediate Escalation (P1)**
   - Multiple critical alarms firing
   - Data loss risk
   - Security incident
   - Unable to resolve within 30 minutes

2. **Scheduled Escalation (P2-P4)**
   - Issue persists after initial troubleshooting
   - Require architectural changes
   - Need additional AWS service quotas

---

## Additional Resources

- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [AWS Step Functions Documentation](https://docs.aws.amazon.com/step-functions/)
- [AWS Lambda Documentation](https://docs.aws.amazon.com/lambda/)
- [Amazon Redshift Documentation](https://docs.aws.amazon.com/redshift/)
- [CloudWatch Logs Insights Query Syntax](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html)

---

## Maintenance Tasks

### Daily
- Check CloudWatch dashboard for anomalies
- Review failed alarms
- Monitor cost trends

### Weekly
- Review Glue job performance metrics
- Analyze query performance trends
- Check S3 storage growth

### Monthly
- Review and update alarm thresholds
- Optimize cost allocation
- Update runbook with new issues/solutions
- Review and clean up old logs

---

*Last Updated: 2024-11-14*
*Maintained by: DE Intern Team*
