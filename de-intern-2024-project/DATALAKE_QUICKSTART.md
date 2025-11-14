# S3 Data Lake - Quick Start Guide

This guide will help you set up a production-grade S3 Data Lake in minutes.

## Prerequisites

- AWS CLI configured with appropriate credentials
- Python 3.8+ installed
- Required Python packages: `boto3`, `pydantic`, `python-dotenv`

## Quick Setup (5 minutes)

### Step 1: Install Dependencies

```bash
cd de-intern-2024-project
pip install -r requirements.txt
```

### Step 2: Set Environment Variables (Optional)

```bash
export AWS_REGION=us-east-1
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
```

### Step 3: Create Data Lake

```bash
# This single command creates the complete data lake infrastructure
python src/datalake/create_s3_datalake.py
```

This will:
- ✓ Create S3 bucket: `{account-id}-oubt-datalake`
- ✓ Enable versioning
- ✓ Enable AES-256 encryption
- ✓ Block public access
- ✓ Create zones: raw/, processed/, curated/
- ✓ Enable CloudWatch metrics
- ✓ Add governance tags

### Step 4: Configure Lifecycle Policies

```bash
python src/datalake/configure_lifecycle.py \
    --bucket-name ${AWS_ACCOUNT_ID}-oubt-datalake
```

This configures:
- ✓ Raw zone: Standard → Standard-IA (30d) → Glacier (90d)
- ✓ Processed zone: Standard → Intelligent-Tiering (7d)
- ✓ Curated zone: Stays in Standard
- ✓ Cleanup incomplete multipart uploads (7d)

### Step 5: Apply Bucket Policy

```bash
python src/datalake/apply_bucket_policy.py \
    --bucket-name ${AWS_ACCOUNT_ID}-oubt-datalake
```

This enforces:
- ✓ HTTPS/TLS 1.2+ only
- ✓ AES-256 encryption required
- ✓ IAM role-based access control
- ✓ MFA for curated zone deletions

### Step 6: Upload NYC Taxi Data

```bash
# Upload a single file
python src/datalake/upload_taxi_data.py \
    --bucket-name ${AWS_ACCOUNT_ID}-oubt-datalake \
    --file-path data/raw/yellow_tripdata_2024-01.parquet \
    --year 2024 \
    --month 1

# Or upload entire directory
python src/datalake/upload_taxi_data.py \
    --bucket-name ${AWS_ACCOUNT_ID}-oubt-datalake \
    --directory data/raw \
    --year 2024 \
    --month 1
```

Features:
- ✓ Automatic multipart upload for files >100MB
- ✓ Progress tracking
- ✓ Upload verification
- ✓ Partitioned structure (year=YYYY/month=MM)

## Verify Setup

```bash
# Check bucket configuration
python src/datalake/create_s3_datalake.py --info-only

# Check lifecycle policies
python src/datalake/configure_lifecycle.py \
    --bucket-name ${AWS_ACCOUNT_ID}-oubt-datalake \
    --show-current

# Check bucket policy
python src/datalake/apply_bucket_policy.py \
    --bucket-name ${AWS_ACCOUNT_ID}-oubt-datalake \
    --show-current

# List uploaded files
python src/datalake/upload_taxi_data.py \
    --bucket-name ${AWS_ACCOUNT_ID}-oubt-datalake \
    --list \
    --year 2024 \
    --month 1
```

## View in AWS Console

1. **S3 Console**: https://s3.console.aws.amazon.com/s3/buckets/{account-id}-oubt-datalake
2. **CloudWatch Metrics**: CloudWatch → Metrics → S3 → Request Metrics
3. **Cost Explorer**: Billing → Cost Explorer → Filter by S3

## Common Commands

### Check Bucket Exists

```bash
aws s3 ls s3://${AWS_ACCOUNT_ID}-oubt-datalake/
```

### View Bucket Details

```bash
aws s3api get-bucket-versioning --bucket ${AWS_ACCOUNT_ID}-oubt-datalake
aws s3api get-bucket-encryption --bucket ${AWS_ACCOUNT_ID}-oubt-datalake
aws s3api get-bucket-lifecycle-configuration --bucket ${AWS_ACCOUNT_ID}-oubt-datalake
```

### Download Data

```bash
aws s3 cp s3://${AWS_ACCOUNT_ID}-oubt-datalake/raw/taxi/year=2024/month=01/ . --recursive
```

## Cost Estimation

For **1TB** of data:

| Without Lifecycle Policies | With Lifecycle Policies | Savings |
|---------------------------|------------------------|---------|
| $276/year (Standard) | ~$50/year (Multi-tier) | **82%** |

## Monitoring

### CloudWatch Alarms

```bash
# Create alarm for high error rates
aws cloudwatch put-metric-alarm \
    --alarm-name datalake-errors \
    --metric-name 4xxErrors \
    --namespace AWS/S3 \
    --dimensions Name=BucketName,Value=${AWS_ACCOUNT_ID}-oubt-datalake \
    --statistic Sum \
    --period 300 \
    --evaluation-periods 2 \
    --threshold 100 \
    --comparison-operator GreaterThanThreshold \
    --alarm-description "Alert when S3 errors are high"
```

### View Metrics

```bash
# S3 storage metrics
aws cloudwatch get-metric-statistics \
    --namespace AWS/S3 \
    --metric-name BucketSizeBytes \
    --dimensions Name=BucketName,Value=${AWS_ACCOUNT_ID}-oubt-datalake Name=StorageType,Value=StandardStorage \
    --start-time $(date -u -d '7 days ago' +%Y-%m-%dT%H:%M:%S) \
    --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
    --period 86400 \
    --statistics Average
```

## Troubleshooting

### Issue: "NoSuchBucket" error

```bash
# Verify bucket name
echo ${AWS_ACCOUNT_ID}-oubt-datalake

# List all buckets
aws s3 ls
```

### Issue: "Access Denied"

```bash
# Check your IAM permissions
aws sts get-caller-identity

# Verify you can access the bucket
aws s3api head-bucket --bucket ${AWS_ACCOUNT_ID}-oubt-datalake
```

### Issue: Upload fails

```bash
# Check bucket policy allows your role
python src/datalake/apply_bucket_policy.py \
    --bucket-name ${AWS_ACCOUNT_ID}-oubt-datalake \
    --show-current

# Try with verbose logging
export LOG_LEVEL=DEBUG
python src/datalake/upload_taxi_data.py --file-path your-file.parquet ...
```

## Next Steps

1. **Set up AWS Glue**
   - Create Glue crawler for data catalog
   - Set up ETL jobs for raw → processed → curated

2. **Configure Athena**
   - Create external tables
   - Set up query result location
   - Run SQL queries on data lake

3. **Implement Data Quality**
   - Add Lambda functions for validation
   - Set up S3 event notifications
   - Create data quality dashboards

4. **Enable Replication**
   - Set up cross-region replication for DR
   - Configure S3 Batch Replication

5. **Optimize Costs**
   - Review storage class distribution
   - Analyze access patterns
   - Adjust lifecycle policies

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    S3 Data Lake Architecture                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────┐      ┌──────────────┐      ┌───────────────┐  │
│  │   Raw Zone  │ ───► │ Processed    │ ───► │ Curated Zone  │  │
│  │             │      │ Zone         │      │               │  │
│  │ Standard    │      │ Intelligent  │      │ Standard      │  │
│  │ → Std-IA    │      │ Tiering      │      │ (Hot Data)    │  │
│  │ → Glacier   │      │ (Auto-opt)   │      │               │  │
│  │ → Deep Arch │      │              │      │               │  │
│  └─────────────┘      └──────────────┘      └───────────────┘  │
│        ▲                      ▲                      ▲           │
│        │                      │                      │           │
│        └──────────────────────┴──────────────────────┘           │
│                               │                                  │
│                    ┌──────────▼──────────┐                      │
│                    │  Bucket Policies    │                      │
│                    │  - HTTPS/TLS 1.2+   │                      │
│                    │  - AES-256 Encrypt  │                      │
│                    │  - IAM Roles        │                      │
│                    │  - MFA for Delete   │                      │
│                    └─────────────────────┘                      │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              CloudWatch Metrics & Alarms                 │   │
│  │  - Request metrics per zone                              │   │
│  │  - Storage metrics                                       │   │
│  │  - Error rate monitoring                                 │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Security Checklist

- [x] Encryption at rest (AES-256)
- [x] Encryption in transit (HTTPS/TLS 1.2+)
- [x] Public access blocked
- [x] Versioning enabled
- [x] IAM role-based access
- [x] MFA for destructive operations
- [x] Lifecycle policies configured
- [x] CloudWatch monitoring enabled
- [x] Bucket policies applied
- [x] Proper tagging for governance

## Resources

- **Documentation**: [src/datalake/README.md](src/datalake/README.md)
- **Bucket Policy**: [infra/policies/s3_bucket_policy.json](infra/policies/s3_bucket_policy.json)
- **AWS S3 Best Practices**: https://docs.aws.amazon.com/AmazonS3/latest/userguide/best-practices.html
- **AWS S3 Security**: https://docs.aws.amazon.com/AmazonS3/latest/userguide/security-best-practices.html

## Support

For issues:
1. Check the troubleshooting section above
2. Review logs in CloudWatch
3. Verify IAM permissions
4. Check AWS service health dashboard

---

**Time to complete**: ~5 minutes
**Estimated monthly cost**: $5-50 (depending on data volume)
**Difficulty**: Beginner-friendly
**Maintenance**: Minimal (automated lifecycle policies)
