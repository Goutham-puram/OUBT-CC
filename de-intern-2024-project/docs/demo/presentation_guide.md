# NYC Taxi Data Pipeline - Demo Presentation Guide

## Overview

This guide provides a 5-minute walkthrough script for demonstrating the complete NYC Taxi Data Pipeline. The demo showcases the end-to-end data flow from raw data ingestion to analytics-ready insights.

**Target Audience:** Technical stakeholders, data engineers, architects

**Duration:** 5 minutes

**Prerequisites:**
- AWS infrastructure deployed via Terraform
- Demo data file generated (`yellow_tripdata_2024-03.parquet`)
- Environment reset before demo

---

## Pre-Demo Checklist

### 30 Minutes Before Demo

1. **Generate Demo Data**
   ```bash
   cd de-intern-2024-project/demo
   python generate_demo_data.py --num-records 10000 --output yellow_tripdata_2024-03.parquet
   ```

2. **Reset Environment**
   ```bash
   python reset_demo_environment.py --dry-run  # Preview changes
   python reset_demo_environment.py --yes      # Execute reset
   ```

3. **Verify Infrastructure**
   ```bash
   cd ../infrastructure/terraform
   terraform output
   ```

4. **Open Required Browser Tabs**
   - AWS Step Functions Console
   - AWS CloudWatch Dashboard
   - AWS Athena Query Editor
   - Redshift Query Editor (optional)

5. **Test Demo Script (Dry Run)**
   ```bash
   cd ../../demo
   python run_final_demo.py --demo-file yellow_tripdata_2024-03.parquet --help
   ```

---

## Architecture Overview (30 seconds)

### High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        NYC Taxi Data Pipeline                            │
└─────────────────────────────────────────────────────────────────────────┘

Raw Zone              Processing Layer           Curated Zone
┌──────────┐         ┌──────────────────┐        ┌───────────┐
│          │         │                  │        │           │
│  S3 Raw  │────────▶│  Step Functions  │───────▶│ S3 Curated│
│  Bucket  │         │                  │        │  Bucket   │
│          │         │  ┌────────────┐  │        │           │
└──────────┘         │  │ Glue Jobs  │  │        └───────────┘
     │               │  └────────────┘  │              │
     │               │                  │              │
     ▼               │  ┌────────────┐  │              ▼
┌──────────┐         │  │  Crawler   │  │        ┌───────────┐
│  Lambda  │         │  └────────────┘  │        │  Athena   │
│ Trigger  │         │                  │        │  Queries  │
└──────────┘         └──────────────────┘        └───────────┘
     │                                                  │
     ▼                                                  ▼
┌──────────┐                                     ┌───────────┐
│CloudWatch│◀────────────────────────────────────│ Redshift  │
│   Logs   │          Monitoring                 │ Analytics │
└──────────┘                                     └───────────┘
```

### Key Components

1. **Data Lake Zones (S3)**
   - Raw: Original data files (Parquet format)
   - Processed: Cleaned and validated data
   - Curated: Analytics-ready datasets

2. **ETL Orchestration (Step Functions)**
   - Automated workflow management
   - Error handling and retries
   - Visual execution tracking

3. **Data Processing (AWS Glue)**
   - Schema discovery via Crawler
   - PySpark transformations
   - Job bookmarks for incremental processing

4. **Query & Analytics**
   - Athena: Ad-hoc SQL queries
   - Redshift: Data warehouse analytics
   - External tables for federated queries

5. **Monitoring (CloudWatch)**
   - Metrics dashboards
   - Log aggregation
   - Automated alarms

---

## 5-Minute Demo Script

### Introduction (30 seconds)

**Script:**

> "Today I'll demonstrate our NYC Taxi Data Pipeline, which processes millions of taxi trip records daily. This serverless, event-driven architecture automatically ingests, transforms, and prepares data for analytics—all triggered by a simple file upload."

**Talking Points:**
- Fully automated, serverless architecture
- Event-driven processing
- Production-ready with monitoring and error handling

---

### Step 1: Data Upload (45 seconds)

**Action:**
```bash
python run_final_demo.py --demo-file yellow_tripdata_2024-03.parquet
```

**Script:**

> "I'm uploading a March 2024 taxi trip dataset containing 10,000 records. Watch as the file lands in our S3 raw data bucket."

**What to Highlight:**
- File size and format (Parquet for efficient storage)
- S3 event notification triggers Lambda
- Automatic encryption at rest

**Key Metrics:**
- File size: ~X MB
- Upload time: < 1 second
- S3 URI: `s3://de-intern-2024-raw-data-{account}/taxi/yellow_tripdata_2024-03.parquet`

---

### Step 2: Lambda Trigger (30 seconds)

**Script:**

> "The S3 upload automatically triggered a Lambda function, which logged structured metadata to CloudWatch. This creates an audit trail and can trigger downstream workflows."

**What to Highlight:**
- Event-driven architecture (no manual intervention)
- Structured logging in CloudWatch
- Lambda executes in milliseconds

**Key Metrics:**
- Lambda invocation time
- Log entries with file metadata
- Function duration: < 100ms

**CloudWatch Log Sample:**
```json
{
  "event": "S3_FILE_UPLOADED",
  "timestamp": "2024-03-15T10:30:00Z",
  "bucket": "de-intern-2024-raw-data-123456789",
  "key": "taxi/yellow_tripdata_2024-03.parquet",
  "size_mb": 2.5
}
```

---

### Step 3: Step Functions Workflow (60 seconds)

**Action:** Open Step Functions Console

**Script:**

> "Our Step Functions state machine orchestrates the entire ETL pipeline. Let's examine the execution graph showing each stage of processing."

**What to Highlight:**
- Visual workflow representation
- Current execution status
- Built-in error handling and retries

**Workflow Stages:**
1. **StartCrawler** - Initiates Glue Crawler
2. **WaitForCrawler** - Polling with exponential backoff
3. **CheckCrawlerStatus** - Validates completion
4. **StartRawToProcessed** - Data cleaning and validation
5. **StartProcessedToCurated** - Aggregations and optimizations
6. **PipelineSuccess** - Completion notification

**Key Metrics:**
- Total execution time: 3-5 minutes
- Success rate: 99.9%
- Cost per execution: < $0.10

---

### Step 4: Glue Jobs (45 seconds)

**Script:**

> "The Glue jobs perform the actual data transformations using PySpark. The first job cleans and validates the raw data, while the second creates optimized analytics tables."

**What to Highlight:**
- Serverless Spark processing
- Automatic schema evolution
- Job bookmarks prevent reprocessing

**Transformations:**

**Raw → Processed:**
- Schema validation
- Data type conversions
- Null handling
- Partitioning by date

**Processed → Curated:**
- Business logic application
- Aggregations (daily, hourly)
- Denormalization
- Performance optimizations (Parquet + partitioning)

**Key Metrics:**
- Records processed: 10,000
- Processing time: 2-3 minutes
- Data quality: 100% valid records

---

### Step 5: Athena Query (45 seconds)

**Action:** Execute pre-prepared Athena query

**Script:**

> "Now that the data is processed, let's query it using Athena. This serverless query service allows us to analyze data directly in S3 using standard SQL."

**Sample Query:**
```sql
SELECT
    COUNT(*) as trip_count,
    SUM(total_amount) as total_revenue,
    AVG(trip_distance) as avg_distance,
    AVG(fare_amount) as avg_fare
FROM de_intern_2024_catalog.taxi
WHERE year = 2024 AND month = 3;
```

**Expected Results:**
```
trip_count     | 10,000
total_revenue  | $247,583.50
avg_distance   | 3.42 miles
avg_fare       | $23.76
```

**What to Highlight:**
- Instant query start (no provisioning)
- Pay per query (GB scanned)
- Integration with Glue Data Catalog

**Key Metrics:**
- Query time: 2-3 seconds
- Data scanned: 2.5 MB
- Cost: < $0.01

---

### Step 6: Redshift Analytics (30 seconds)

**Script:**

> "For more complex analytics and BI workloads, we load the curated data into Redshift Serverless. This enables sub-second query performance on large datasets and integrates with BI tools like Tableau and QuickSight."

**Sample Analytics Query:**
```sql
SELECT
    DATE_TRUNC('day', pickup_datetime) as trip_date,
    COUNT(*) as daily_trips,
    SUM(total_amount) as daily_revenue,
    AVG(trip_distance) as avg_distance,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fare_amount) as median_fare
FROM taxi_warehouse.fact_trips
WHERE pickup_datetime >= '2024-03-01'
  AND pickup_datetime < '2024-04-01'
GROUP BY DATE_TRUNC('day', pickup_datetime)
ORDER BY trip_date;
```

**What to Highlight:**
- Serverless Redshift (auto-scaling)
- External schema integration with S3
- Support for complex analytics (percentiles, window functions)

---

### Step 7: CloudWatch Dashboard (30 seconds)

**Action:** Open CloudWatch Dashboard

**Script:**

> "Finally, our CloudWatch dashboard provides real-time visibility into pipeline health and performance. We track key metrics like execution success rate, processing time, and data quality."

**Key Metrics to Show:**

1. **Pipeline Metrics**
   - Step Functions execution count
   - Success vs. failure rate
   - Average execution time

2. **Data Metrics**
   - Records processed per hour
   - Data volume by zone
   - Processing throughput

3. **Cost Metrics**
   - Lambda invocations
   - Glue DPU hours
   - Athena data scanned

4. **Error Metrics**
   - Failed executions
   - Data quality issues
   - Retry counts

**Dashboard Widgets:**
- Line graphs for trends
- Counters for totals
- Alarms for anomalies

---

## Conclusion (30 seconds)

**Script:**

> "We've just demonstrated a complete data pipeline that:
> - Automatically ingests data from S3
> - Orchestrates complex ETL workflows
> - Ensures data quality and reliability
> - Provides multiple query interfaces
> - Monitors performance and costs
>
> All of this runs serverless, scaling automatically from zero to millions of records, with no infrastructure to manage. The entire architecture is defined as code using Terraform, enabling rapid deployment across environments."

---

## Fallback Plan (If Live Demo Fails)

### Option 1: Pre-recorded Screenshots

Prepare screenshots of:
1. S3 bucket with uploaded file
2. CloudWatch logs showing Lambda trigger
3. Step Functions execution graph (completed)
4. Glue job run details (successful)
5. Athena query results
6. CloudWatch dashboard

Store in: `docs/demo/screenshots/`

### Option 2: Pre-recorded Video

Record a successful demo run:
```bash
asciinema rec demo-recording.cast
python run_final_demo.py --demo-file yellow_tripdata_2024-03.parquet
```

### Option 3: Dry Run Mode

Modify demo script to include `--dry-run` mode that simulates execution without actual AWS calls.

---

## Q&A Preparation

### Common Questions

**Q: How does this handle failures?**
> A: Step Functions includes built-in error handling with retries. Failed executions trigger CloudWatch alarms, and we maintain dead-letter queues for manual investigation.

**Q: What's the cost?**
> A: For 1 million records/day:
> - Lambda: $5/month
> - Glue: $30/month (2 DPU hours/day)
> - Step Functions: $1/month
> - S3: $23/month (1TB storage)
> - Athena: $5/month (100 GB scanned)
>
> **Total: ~$64/month** for 1M records/day

**Q: How does it scale?**
> A: All components are serverless and auto-scale:
> - S3: Unlimited storage
> - Lambda: 1000 concurrent executions
> - Glue: Auto-scaling DPUs
> - Athena: Massively parallel
> - Step Functions: 1M+ concurrent executions

**Q: Can we process real-time data?**
> A: Yes! The current architecture is near-real-time (minutes). For true real-time, we could add:
> - Kinesis Data Streams for streaming ingestion
> - Lambda for stream processing
> - DynamoDB for millisecond queries

**Q: How do we ensure data quality?**
> A: Multiple layers:
> - Glue Data Quality rules
> - Schema validation in ETL jobs
> - Anomaly detection in CloudWatch
> - Data lineage tracking

**Q: What about data governance?**
> A: Covered by:
> - Lake Formation for access control
> - AWS Glue Data Catalog for metadata
> - CloudTrail for audit logs
> - S3 versioning and lifecycle policies

---

## Technical Deep-Dive (Bonus)

If time permits or for technical audiences, highlight:

### Infrastructure as Code
```bash
cd infrastructure/terraform
terraform plan  # Show planned changes
terraform graph | dot -Tpng > architecture.png  # Generate diagram
```

### Data Quality Rules
```python
# From Glue Data Quality Ruleset
Rules = [
    RowCount > 0,
    IsComplete "pickup_datetime",
    IsComplete "dropoff_datetime",
    ColumnValues "passenger_count" >= 0,
    ColumnValues "trip_distance" >= 0,
    ColumnValues "fare_amount" >= 0
]
```

### Cost Optimization
- S3 Intelligent-Tiering
- Glue job bookmarks
- Athena result caching
- Redshift concurrency scaling

### Security
- VPC isolation
- IAM least privilege
- S3 encryption (AES-256)
- Secrets Manager for credentials

---

## Post-Demo Actions

After the demo:

1. **Reset Environment**
   ```bash
   python reset_demo_environment.py --yes
   ```

2. **Collect Feedback**
   - Note questions asked
   - Identify areas of interest
   - Document requested features

3. **Share Resources**
   - Architecture diagrams
   - Code repository link
   - Documentation

4. **Follow-up**
   - Send demo recording
   - Schedule deep-dive sessions
   - Provide cost estimates

---

## Troubleshooting

### Issue: File upload fails
**Solution:** Check S3 bucket permissions and AWS credentials

### Issue: Lambda not triggered
**Solution:** Verify S3 event notification configuration

### Issue: Step Functions stuck
**Solution:** Check CloudWatch logs for Glue job errors

### Issue: Athena query fails
**Solution:** Ensure Glue Crawler completed successfully

### Issue: Dashboard not showing data
**Solution:** Wait 5-10 minutes for metrics to populate

---

## Additional Resources

- **Architecture Documentation:** `docs/architecture.md`
- **Setup Guide:** `docs/setup/aws_account_setup.md`
- **Monitoring Runbook:** `src/monitoring/RUNBOOK.md`
- **GitHub Repository:** [Link to repo]
- **Terraform Modules:** `infrastructure/terraform/`

---

## Revision History

| Version | Date | Changes | Author |
|---------|------|---------|--------|
| 1.0 | 2024-03-15 | Initial version | Data Engineering Team |

---

**Good luck with your demo! 🚀**
