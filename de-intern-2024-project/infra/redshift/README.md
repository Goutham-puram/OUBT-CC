# Redshift Serverless Data Warehouse Setup

This directory contains scripts and documentation for setting up a Redshift Serverless data warehouse with external schema integration for NYC taxi data analytics.

## Overview

The setup creates:
- **Redshift Serverless Namespace**: `oubt-analytics`
- **Redshift Serverless Workgroup**: `intern-workgroup`
- **Base Capacity**: 32 RPU (minimum for Redshift Serverless)
- **IAM Role**: For Redshift Spectrum to access AWS Glue Data Catalog and S3
- **Cost Monitoring**: CloudWatch billing alarms

## Prerequisites

1. AWS CLI configured with appropriate credentials
2. Python 3.8+ with boto3 installed
3. AWS Glue Data Catalog database (`nyc_taxi_catalog`) with taxi data tables
4. S3 bucket with NYC taxi data in Parquet format
5. Appropriate IAM permissions to create:
   - Redshift Serverless resources
   - IAM roles and policies
   - CloudWatch alarms
   - SNS topics

## Installation

### 1. Create Redshift Serverless Infrastructure

```bash
cd /path/to/de-intern-2024-project
python infra/redshift/create_redshift_serverless.py \
  --namespace oubt-analytics \
  --workgroup intern-workgroup \
  --database analytics_db \
  --username admin \
  --base-capacity 32 \
  --cost-threshold 100.0
```

**Parameters:**
- `--namespace`: Namespace name (default: oubt-analytics)
- `--workgroup`: Workgroup name (default: intern-workgroup)
- `--database`: Database name (default: analytics_db)
- `--username`: Admin username (default: admin)
- `--password`: Admin password (will prompt if not provided)
- `--base-capacity`: Base RPU capacity (default: 32, minimum: 32)
- `--region`: AWS region (default: us-east-1)
- `--cost-threshold`: Cost alarm threshold in USD (default: 100)

**What it creates:**
1. IAM role for Redshift Spectrum with:
   - Trust policy for redshift.amazonaws.com
   - AWS managed policy: AWSGlueConsoleFullAccess
   - Inline policy for S3 and Glue Data Catalog access
2. Redshift Serverless namespace with the specified database
3. Redshift Serverless workgroup with 32 RPU base capacity
4. CloudWatch billing alarm for cost monitoring
5. SNS topic for cost alerts
6. Connection info saved to `config/redshift_serverless_connection.json`

**Expected Runtime:** 5-10 minutes

### 2. Create External Schema

After the Redshift Serverless setup completes:

```bash
# Connect to Redshift using query editor or psql
psql -h <endpoint> -p 5439 -U admin -d analytics_db

# Get the Spectrum role ARN from the output
# Replace <SPECTRUM_ROLE_ARN> in the SQL file with your actual role ARN
```

Then run:
```sql
-- Edit sql/redshift/001_create_external_schema.sql
-- Replace <SPECTRUM_ROLE_ARN> with your IAM role ARN
-- Then execute the script
\i sql/redshift/001_create_external_schema.sql
```

**What it creates:**
- External schema `spectrum_schema` pointing to Glue Data Catalog
- Grants usage permissions
- Provides verification queries

### 3. Create Analytics Views

```sql
-- Create analytics views for common queries
\i sql/redshift/002_create_analytics_views.sql
```

**Views created:**
- `v_daily_revenue`: Daily revenue aggregations and trends
- `v_hourly_trips`: Hourly trip patterns by day of week
- `v_payment_analysis`: Payment type distribution and tipping analysis
- `v_location_performance`: Location-based performance metrics

### 4. Run Analysis Queries

```sql
-- Execute analysis queries
\i sql/redshift/003_analysis_queries.sql
```

**Query categories:**
- Revenue analysis by month and year
- Popular routes identification
- Peak hours analysis
- Tip patterns by payment type

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Redshift Serverless                        │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Namespace: oubt-analytics                           │   │
│  │  Database: analytics_db                              │   │
│  │  ┌────────────────────────────────────────────────┐  │   │
│  │  │  Workgroup: intern-workgroup                   │  │   │
│  │  │  Base Capacity: 32 RPU                         │  │   │
│  │  │  ┌──────────────────────────────────────────┐  │  │   │
│  │  │  │  External Schema: spectrum_schema        │  │  │   │
│  │  │  │  ┌────────────────────────────────────┐  │  │  │   │
│  │  │  │  │  Analytics Views:                  │  │  │  │   │
│  │  │  │  │  - v_daily_revenue                 │  │  │  │   │
│  │  │  │  │  - v_hourly_trips                  │  │  │  │   │
│  │  │  │  │  - v_payment_analysis              │  │  │  │   │
│  │  │  │  │  - v_location_performance          │  │  │  │   │
│  │  │  │  └────────────────────────────────────┘  │  │  │   │
│  │  │  └──────────────────────────────────────────┘  │  │   │
│  │  └────────────────────────────────────────────────┘  │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ Spectrum IAM Role
                              ↓
┌─────────────────────────────────────────────────────────────┐
│              AWS Glue Data Catalog                           │
│  Database: nyc_taxi_catalog                                  │
│  Tables:                                                     │
│  - taxi_trips_curated (Parquet, partitioned)                │
└─────────────────────────────────────────────────────────────┘
                              │
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                    S3 Data Lake                              │
│  Bucket: <account-id>-oubt-datalake                         │
│  Path: s3://bucket/curated/taxi_trips/                      │
│  Format: Parquet (columnar, compressed)                     │
└─────────────────────────────────────────────────────────────┘
```

## Cost Monitoring

### CloudWatch Billing Alarm

The setup automatically creates a CloudWatch billing alarm that triggers when Redshift costs exceed the threshold (default: $100).

**Subscribe to alerts:**
```bash
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:redshift-serverless-cost-alerts-oubt-analytics \
  --protocol email \
  --notification-endpoint your-email@example.com
```

### Cost Optimization Tips

1. **Redshift Serverless Pricing:**
   - Charged per RPU-hour
   - 32 RPU base capacity = ~$0.36/hour = ~$259/month (if running 24/7)
   - Auto-scaling can increase costs during peak usage

2. **Spectrum Pricing:**
   - $5 per TB of data scanned from S3
   - Use Parquet format for 87% less data scanned
   - Partition pruning can reduce scans by 90%+

3. **Best Practices:**
   - Pause workgroup when not in use (manual or scheduled)
   - Use COPY to load frequently accessed data into Redshift tables
   - Implement partitioning in S3 (by year/month/day)
   - Use WHERE clauses to minimize Spectrum scans
   - Monitor query costs using `SVL_S3QUERY_SUMMARY`

4. **Query Cost Monitoring:**
   ```sql
   -- Check data scanned for last query
   SELECT
     query,
     s3_scanned_bytes / 1024.0 / 1024.0 / 1024.0 AS gb_scanned,
     (s3_scanned_bytes / 1024.0 / 1024.0 / 1024.0 / 1024.0) * 5 AS estimated_cost_usd
   FROM svl_s3query_summary
   WHERE query = pg_last_query_id();
   ```

## Performance Tips

### 1. Partitioning Strategy

Ensure your S3 data is partitioned:
```
s3://bucket/curated/taxi_trips/
  year=2024/
    month=01/
      day=01/
        part-00000.parquet
        part-00001.parquet
```

Query with partition pruning:
```sql
SELECT COUNT(*), AVG(fare_amount)
FROM spectrum_schema.taxi_trips_curated
WHERE year = 2024 AND month = 1;  -- Scans only Jan 2024 data
```

### 2. Columnar Format Benefits

Parquet advantages:
- Read only required columns (vs reading entire row)
- Better compression (50-75% size reduction)
- Predicate pushdown (filter at storage level)
- Statistics for query optimization

### 3. Materialized Views

For frequently accessed data:
```sql
CREATE MATERIALIZED VIEW mv_daily_revenue AS
SELECT * FROM v_daily_revenue;

-- Refresh periodically
REFRESH MATERIALIZED VIEW mv_daily_revenue;
```

### 4. Query Optimization

- Use specific columns instead of SELECT *
- Add WHERE clauses for date ranges
- Use LIMIT for exploratory queries
- Join on distribution keys
- Analyze query plans with EXPLAIN

## Troubleshooting

### Connection Issues

```bash
# Test connection
psql -h <endpoint> -p 5439 -U admin -d analytics_db

# Check workgroup status
aws redshift-serverless get-workgroup --workgroup-name intern-workgroup

# Verify security groups allow inbound traffic on port 5439
```

### External Schema Issues

```sql
-- Verify external schema
SELECT * FROM svv_external_schemas WHERE schemaname = 'spectrum_schema';

-- Check IAM role
SELECT * FROM svv_iam_roles;

-- View external tables
SELECT * FROM svv_external_tables WHERE schemaname = 'spectrum_schema';
```

### Permission Errors

Ensure IAM role has:
- `glue:GetDatabase`, `glue:GetTable`, `glue:GetPartitions`
- `s3:GetObject`, `s3:ListBucket` on data lake bucket
- Trust relationship with `redshift.amazonaws.com`

### Query Performance Issues

```sql
-- Check Spectrum query performance
SELECT
  query,
  segment,
  s3_scanned_rows,
  s3_scanned_bytes,
  files,
  avg_request_parallelism
FROM svl_s3query_summary
WHERE query = pg_last_query_id();

-- Check for load errors
SELECT * FROM stl_load_errors ORDER BY query DESC LIMIT 10;
```

## Cleanup

To delete all resources:

```bash
# Delete Redshift Serverless resources
python infra/redshift/create_redshift_serverless.py --delete

# Delete IAM role (if needed)
aws iam delete-role-policy --role-name RedshiftSpectrumRole-oubt-analytics --policy-name RedshiftSpectrumRole-oubt-analytics-S3GlueAccess
aws iam detach-role-policy --role-name RedshiftSpectrumRole-oubt-analytics --policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess
aws iam delete-role --role-name RedshiftSpectrumRole-oubt-analytics

# Delete CloudWatch alarm
aws cloudwatch delete-alarms --alarm-names RedshiftServerless-Cost-oubt-analytics

# Delete SNS topic
aws sns delete-topic --topic-arn <topic-arn>
```

## Security Best Practices

1. **Network Security:**
   - Use VPC endpoints for S3 and Glue access
   - Restrict workgroup to private subnets in production
   - Use security groups to limit access

2. **IAM Security:**
   - Use least privilege IAM policies
   - Separate roles for different data access levels
   - Enable CloudTrail logging for audit

3. **Data Security:**
   - Enable encryption at rest and in transit
   - Use AWS KMS for encryption key management
   - Implement row-level security if needed

4. **Access Control:**
   - Use database users/groups for access control
   - Grant minimal permissions on schemas/views
   - Regular access audits

## References

- [Redshift Serverless Documentation](https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-whatis.html)
- [Redshift Spectrum Documentation](https://docs.aws.amazon.com/redshift/latest/dg/c-using-spectrum.html)
- [AWS Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html)
- [Redshift Best Practices](https://docs.aws.amazon.com/redshift/latest/dg/best-practices.html)

## Support

For issues or questions:
1. Check CloudWatch Logs for Redshift query logs
2. Review AWS CloudTrail for API call history
3. Consult AWS support or documentation
4. Review system views (SVV_*, STL_*, STV_*)
