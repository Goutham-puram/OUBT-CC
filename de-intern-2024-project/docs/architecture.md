# Architecture Overview

## System Architecture

The NYC Taxi Data Pipeline follows a modern data lake architecture on AWS with the following layers:

### 1. Data Ingestion Layer
- **Source**: NYC TLC Trip Record Data (Public S3 Bucket)
- **Storage**: Amazon S3 Raw Zone
- **Processing**: Python scripts for data download and initial validation

### 2. Data Storage Layer (Data Lake)

#### Three-Tier Architecture:
```
┌─────────────────┐
│   Raw Zone      │  Original, immutable data
│   (Bronze)      │  Format: Parquet
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Processed Zone  │  Cleaned, validated data
│   (Silver)      │  Format: Partitioned Parquet
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Curated Zone   │  Aggregated, analytics-ready
│   (Gold)        │  Format: Optimized Parquet
└─────────────────┘
```

### 3. Data Catalog Layer
- **AWS Glue Data Catalog**: Schema registry and metadata store
- **Glue Crawlers**: Automatic schema discovery
- **Tables**: Logical views of S3 data

### 4. ETL Processing Layer

#### Week 1: Initial Data Processing
- Python scripts with Pandas
- PostgreSQL RDS for metadata and exploration
- Local data validation

#### Week 2-3: Scalable ETL
- **AWS Glue Jobs**: PySpark-based transformations
- **Lambda Functions**: Event-driven processing
- **Step Functions**: Workflow orchestration
- **EventBridge**: Scheduling and triggering

#### Transformation Pipeline:
```
Raw Data → Glue Job 1 → Processed Data → Glue Job 2 → Curated Data
                ↓                              ↓
           Validation                    Aggregations
```

### 5. Data Warehouse Layer (Week 4)
- **Amazon Redshift**: MPP data warehouse
- **Schema**: Star schema (dimensional model)
- **Loading**: COPY command from S3
- **Optimization**: Distribution keys, sort keys, compression

### 6. Analytics & Reporting Layer
- SQL queries on Redshift
- Business intelligence integration (future)
- Data visualization tools (future)

## Data Flow

```
┌──────────────┐
│ NYC TLC Data │
│  (Internet)  │
└──────┬───────┘
       │
       ▼
┌──────────────────┐
│  Download Script │
│   (Python/Week1) │
└──────┬───────────┘
       │
       ▼
┌──────────────────┐      ┌─────────────┐
│   S3 Raw Bucket  │─────→│   Lambda    │ (Event Handler)
└──────┬───────────┘      │  Function   │
       │                  └──────┬──────┘
       │                         │
       ▼                         ▼
┌──────────────────┐      ┌─────────────┐
│  Glue Crawler    │      │    Glue     │
│ (Schema Discovery│      │  Catalog    │
└──────────────────┘      └─────────────┘
       │
       ▼
┌──────────────────────────────────┐
│    Step Functions Workflow       │
│  ┌────────────────────────────┐  │
│  │ 1. Start Crawler           │  │
│  │ 2. Glue Job: Raw→Processed │  │
│  │ 3. Glue Job: Proc→Curated  │  │
│  │ 4. Data Quality Checks     │  │
│  └────────────────────────────┘  │
└──────────────┬───────────────────┘
               │
               ▼
        ┌──────────────┐
        │ S3 Processed │
        │   & Curated  │
        └──────┬───────┘
               │
               ▼
        ┌──────────────┐
        │   Redshift   │
        │  COPY Load   │
        └──────┬───────┘
               │
               ▼
        ┌──────────────┐
        │  Analytics   │
        │   Queries    │
        └──────────────┘
```

## Component Details

### Amazon S3 Buckets
1. **Raw Data Bucket**
   - Purpose: Store original data
   - Lifecycle: Archive to Glacier after 90 days
   - Versioning: Enabled

2. **Processed Data Bucket**
   - Purpose: Store cleaned data
   - Partitioning: By year/month
   - Format: Parquet with Snappy compression

3. **Curated Data Bucket**
   - Purpose: Analytics-ready aggregations
   - Contents: Daily stats, hourly patterns, fact tables

4. **Scripts Bucket**
   - Purpose: Store Glue scripts and configs
   - Contents: ETL scripts, temporary files

### AWS Glue Components

#### Glue Database
- Name: `de_intern_2024_catalog`
- Tables: Mapped to S3 paths
- Schema: Auto-discovered and managed

#### Glue Jobs
1. **raw_to_processed**
   - Input: Raw taxi data
   - Output: Cleaned, validated data
   - Transformations: Filtering, derived columns
   - Worker Type: G.1X
   - Workers: 2

2. **processed_to_curated**
   - Input: Processed data
   - Output: Aggregated datasets
   - Transformations: GROUP BY, aggregations
   - Worker Type: G.1X
   - Workers: 2

### Amazon Redshift

#### Cluster Configuration
- Node Type: dc2.large (single-node for dev)
- Distribution: KEY (location_id)
- Sort Keys: date_key, time_key
- Compression: Automatic

#### Schema Design (Star Schema)
- **Fact Table**: fact_taxi_trips (150M+ rows)
- **Dimensions**:
  - dim_date: Calendar dimension
  - dim_time: Time of day dimension
  - dim_location: NYC zones and boroughs
  - dim_payment_type: Payment methods
  - dim_rate_code: Rate codes

### Step Functions Workflow

State Machine: `de-intern-2024-etl-pipeline`

```json
{
  "StartAt": "StartCrawler",
  "States": {
    "StartCrawler": "Start Glue Crawler",
    "WaitForCrawler": "Poll crawler status",
    "StartRawToProcessed": "Run cleaning job",
    "StartProcessedToCurated": "Run aggregation job",
    "PipelineSuccess": "Complete"
  }
}
```

## Security Architecture

### Network Security
- **VPC**: Isolated network for RDS and Redshift
- **Subnets**: Public (NAT) and Private (databases)
- **Security Groups**: Least-privilege access rules
- **VPC Endpoints**: Direct S3 access (no internet)

### Identity & Access Management
- **Service Roles**:
  - Glue service role (S3, Catalog access)
  - Lambda execution role (S3, Glue triggers)
  - Redshift role (S3 read access)
  - Step Functions role (Glue, Lambda)

### Data Security
- **Encryption at Rest**:
  - S3: AES-256
  - RDS: KMS encryption
  - Redshift: KMS encryption
- **Encryption in Transit**: TLS/SSL for all connections
- **Access Control**: IAM policies, bucket policies

## Scalability Considerations

### Current Scale
- Data Volume: ~2-5 million trips/month
- File Size: ~100-200 MB per monthly file
- Processing Time: 5-10 minutes per month

### Scale-Out Strategy
1. **S3**: Unlimited storage, automatic scaling
2. **Glue**: Add more workers or larger worker types
3. **Redshift**: Switch to multi-node cluster
4. **Lambda**: Concurrent execution (up to 1000)

## Cost Optimization

### Current Monthly Estimate (Dev)
- S3: ~$10-20 (100 GB storage)
- RDS: ~$15 (db.t3.micro)
- Glue: ~$20 (10 DPU-hours/month)
- Redshift: ~$180 (dc2.large, single-node)
- Lambda: <$1 (within free tier)
- **Total**: ~$226/month

### Cost Reduction Tips
1. Use S3 lifecycle policies
2. Stop Redshift cluster when not in use
3. Use Glue bookmarks to avoid reprocessing
4. Leverage AWS Free Tier
5. Delete resources after project completion

## Monitoring & Logging

### CloudWatch Logs
- Lambda function logs
- Glue job logs
- Step Functions execution history

### CloudWatch Metrics
- S3 bucket metrics
- RDS performance metrics
- Redshift query performance
- Glue job metrics

### Alarms
- Glue job failures
- Step Function errors
- Redshift disk usage
- RDS CPU utilization

## Disaster Recovery

### Backup Strategy
- **RDS**: Automated backups (7-day retention)
- **Redshift**: Automated snapshots (7-day retention)
- **S3**: Versioning enabled
- **Infrastructure**: Terraform state backup

### Recovery Procedures
1. **Data Loss**: Restore from S3 versioning
2. **Database Failure**: Restore from RDS snapshot
3. **Complete Failure**: Terraform re-deployment
4. **Data Corruption**: Reprocess from raw zone

## Future Enhancements

1. **Real-time Processing**: Kinesis Data Streams
2. **ML Integration**: SageMaker for predictive analytics
3. **Data Quality**: Great Expectations framework
4. **Visualization**: QuickSight dashboards
5. **API Layer**: API Gateway + Lambda
6. **Data Governance**: Lake Formation permissions
