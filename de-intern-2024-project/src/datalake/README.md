# S3 Data Lake Management

Production-grade S3 Data Lake implementation with multiple zones and comprehensive governance.

## Overview

This module provides tools for creating and managing a multi-zone S3 Data Lake with:

- **Three-tier zone architecture**: raw, processed, curated
- **Encryption**: Server-side AES-256 encryption
- **Versioning**: Full object versioning enabled
- **Lifecycle policies**: Automatic cost optimization through storage class transitions
- **Access controls**: Fine-grained IAM-based bucket policies
- **Monitoring**: CloudWatch metrics and request metrics
- **Multipart uploads**: Intelligent handling of large files (>100MB)

## Architecture

```
{account-id}-oubt-datalake/
├── raw/                    # Landing zone for raw data
│   └── taxi/
│       └── year=2024/
│           └── month=01/
├── processed/              # Cleaned and transformed data
│   └── taxi/
│       └── year=2024/
│           └── month=01/
└── curated/                # Business-ready data
    └── taxi/
        └── year=2024/
            └── month=01/
```

## Storage Lifecycle Policies

| Zone | Transition Timeline | Purpose |
|------|-------------------|---------|
| **raw/** | 30d → Standard-IA<br>90d → Glacier IR<br>180d → Deep Archive | Long-term retention with cost optimization |
| **processed/** | 7d → Intelligent-Tiering | Automatic optimization based on access patterns |
| **curated/** | Stays in Standard | High-performance access for business users |

## Scripts

### 1. create_s3_datalake.py

Creates and configures the S3 Data Lake bucket with all required settings.

**Features:**
- Auto-detects AWS account ID
- Creates bucket with proper region configuration
- Enables versioning and encryption
- Sets up public access blocks
- Creates zone folder structure
- Enables CloudWatch metrics
- Adds governance tags

**Usage:**

```bash
# Create data lake (auto-detects account ID)
python src/datalake/create_s3_datalake.py

# Specify account ID and region
python src/datalake/create_s3_datalake.py \
    --account-id 123456789012 \
    --region us-east-1

# Display bucket information only
python src/datalake/create_s3_datalake.py --info-only
```

**Example Output:**
```
================================================================================
Starting S3 Data Lake Setup
================================================================================

[STEP] Create S3 bucket
  Successfully created bucket: 123456789012-oubt-datalake

[STEP] Enable versioning
  Successfully enabled versioning

[STEP] Enable encryption
  Successfully enabled AES-256 encryption

...

SUCCESS: Data lake setup completed successfully!
================================================================================

Bucket Configuration:
  Bucket Name: 123456789012-oubt-datalake
  Region: us-east-1
  Account ID: 123456789012
  Versioning: Enabled
  Encryption: Enabled
  Zones: raw/, processed/, curated/
```

### 2. configure_lifecycle.py

Configures lifecycle policies for automatic storage class transitions.

**Features:**
- Zone-specific lifecycle rules
- Noncurrent version management
- Multipart upload cleanup
- Delete marker cleanup
- Cost savings estimation

**Usage:**

```bash
# Apply lifecycle policies
python src/datalake/configure_lifecycle.py \
    --bucket-name 123456789012-oubt-datalake

# Show current lifecycle configuration
python src/datalake/configure_lifecycle.py \
    --bucket-name 123456789012-oubt-datalake \
    --show-current

# Estimate cost savings
python src/datalake/configure_lifecycle.py \
    --bucket-name 123456789012-oubt-datalake \
    --estimate-savings

# Delete lifecycle configuration
python src/datalake/configure_lifecycle.py \
    --bucket-name 123456789012-oubt-datalake \
    --delete
```

**Example Output:**
```
================================================================================
Starting Lifecycle Configuration
================================================================================

Lifecycle Rules Summary:
================================================================================

Rule ID: raw-zone-lifecycle
  Status: Enabled
  Filter: {'Prefix': 'raw/'}
  Transitions:
    - After 30 days -> STANDARD_IA
    - After 90 days -> GLACIER_IR
    - After 180 days -> DEEP_ARCHIVE

Rule ID: processed-zone-lifecycle
  Status: Enabled
  Filter: {'Prefix': 'processed/'}
  Transitions:
    - After 7 days -> INTELLIGENT_TIERING

...

Estimated Cost Savings:
================================================================================
Raw Zone:
  - Savings after 30 days: $0.0105 per GB/month
  - Savings after 90 days: $0.0190 per GB/month
```

### 3. upload_taxi_data.py

Uploads NYC taxi data with intelligent multipart upload support.

**Features:**
- Multipart upload for files >100MB
- Progress tracking
- Upload verification
- Partitioned structure (year/month)
- Automatic encryption
- Batch directory uploads

**Usage:**

```bash
# Upload single file
python src/datalake/upload_taxi_data.py \
    --bucket-name 123456789012-oubt-datalake \
    --file-path data/raw/yellow_tripdata_2024-01.parquet \
    --year 2024 \
    --month 1

# Upload entire directory
python src/datalake/upload_taxi_data.py \
    --bucket-name 123456789012-oubt-datalake \
    --directory data/raw \
    --year 2024 \
    --month 1 \
    --pattern "*.parquet"

# List uploaded files
python src/datalake/upload_taxi_data.py \
    --bucket-name 123456789012-oubt-datalake \
    --list \
    --year 2024 \
    --month 1

# Disable multipart upload
python src/datalake/upload_taxi_data.py \
    --bucket-name 123456789012-oubt-datalake \
    --file-path data.parquet \
    --year 2024 \
    --month 1 \
    --no-multipart
```

**Example Output:**
```
Initialized TaxiDataUploader for bucket: 123456789012-oubt-datalake
Multipart upload: Enabled
Multipart threshold: 100MB

Uploading: yellow_tripdata_2024-01.parquet
  Size: 245.67MB
  Destination: s3://123456789012-oubt-datalake/raw/taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet
  Using multipart upload (file > 100MB)
  yellow_tripdata_2024-01.parquet: 104857600/257543680 bytes (40.71%)
  yellow_tripdata_2024-01.parquet: 209715200/257543680 bytes (81.42%)
  yellow_tripdata_2024-01.parquet: 257543680/257543680 bytes (100.00%)
✓ Successfully uploaded: yellow_tripdata_2024-01.parquet
✓ Upload verified successfully
```

### 4. apply_bucket_policy.py

Applies IAM-based bucket policies for access control and security.

**Features:**
- Auto-replaces bucket name and account ID placeholders
- Policy validation before application
- Dry-run mode
- Current policy inspection
- Policy deletion

**Usage:**

```bash
# Apply bucket policy
python src/datalake/apply_bucket_policy.py \
    --bucket-name 123456789012-oubt-datalake

# Use custom policy file
python src/datalake/apply_bucket_policy.py \
    --bucket-name 123456789012-oubt-datalake \
    --policy-file custom_policy.json

# Dry run (validate only, don't apply)
python src/datalake/apply_bucket_policy.py \
    --bucket-name 123456789012-oubt-datalake \
    --dry-run

# Show current policy
python src/datalake/apply_bucket_policy.py \
    --bucket-name 123456789012-oubt-datalake \
    --show-current

# Delete bucket policy
python src/datalake/apply_bucket_policy.py \
    --bucket-name 123456789012-oubt-datalake \
    --delete
```

## Bucket Policy

The bucket policy (`infra/policies/s3_bucket_policy.json`) enforces:

### Security Controls

1. **Encryption in Transit**: Denies all requests not using HTTPS/TLS 1.2+
2. **Encryption at Rest**: Requires AES-256 encryption for all uploads
3. **TLS Version**: Enforces minimum TLS 1.2
4. **Public Access**: Blocks all public access

### Access Control

1. **Data Engineering Role**: Full access to all zones
2. **Glue Service Role**: Full access for ETL operations
3. **Lambda Role**: Full access for serverless functions
4. **Data Analyst Role**: Read-only access to processed/ and curated/
5. **Athena Role**: Read-only access to curated/

### Additional Protections

- **MFA for Deletions**: Requires MFA for deleting objects in curated/
- **Version Protection**: Separate permissions for versioned objects

## Complete Setup Example

```bash
# 1. Create the data lake
python src/datalake/create_s3_datalake.py

# 2. Apply lifecycle policies
python src/datalake/configure_lifecycle.py \
    --bucket-name $(aws sts get-caller-identity --query Account --output text)-oubt-datalake

# 3. Apply bucket policy
python src/datalake/apply_bucket_policy.py \
    --bucket-name $(aws sts get-caller-identity --query Account --output text)-oubt-datalake

# 4. Upload taxi data
python src/datalake/upload_taxi_data.py \
    --bucket-name $(aws sts get-caller-identity --query Account --output text)-oubt-datalake \
    --directory data/raw \
    --year 2024 \
    --month 1
```

## CloudWatch Metrics

The following metrics are automatically enabled:

### Request Metrics
- Per-zone metrics (Raw, Processed, Curated)
- Entire bucket metrics
- Available in CloudWatch S3 Request Metrics

### Available Metrics
- **AllRequests**: Total number of requests
- **GetRequests**: GET request count
- **PutRequests**: PUT request count
- **DeleteRequests**: DELETE request count
- **ListRequests**: LIST request count
- **BytesDownloaded**: Total bytes downloaded
- **BytesUploaded**: Total bytes uploaded
- **4xxErrors**: Client error count
- **5xxErrors**: Server error count
- **FirstByteLatency**: Time to first byte

### Accessing Metrics

```bash
# View metrics in AWS Console
# CloudWatch → Metrics → S3 → Storage Metrics / Request Metrics

# Or use AWS CLI
aws cloudwatch get-metric-statistics \
    --namespace AWS/S3 \
    --metric-name NumberOfObjects \
    --dimensions Name=BucketName,Value=123456789012-oubt-datalake Name=StorageType,Value=AllStorageTypes \
    --start-time 2024-01-01T00:00:00Z \
    --end-time 2024-01-31T23:59:59Z \
    --period 86400 \
    --statistics Average
```

## Cost Optimization

### Storage Costs (us-east-1, per GB/month)

| Storage Class | Cost | Use Case |
|--------------|------|----------|
| Standard | $0.023 | Curated zone, frequently accessed |
| Standard-IA | $0.0125 | Raw zone after 30 days |
| Intelligent-Tiering | $0.023* | Processed zone, auto-optimizes |
| Glacier IR | $0.004 | Raw zone after 90 days |
| Deep Archive | $0.00099 | Raw zone after 180 days |

*Base price; automatically moves to lower tiers based on access patterns

### Example Savings

For 1TB of raw data over 1 year:
- **Without lifecycle policies**: $276/year (all in Standard)
- **With lifecycle policies**:
  - 30 days Standard: $23
  - 60 days Standard-IA: $15
  - 90 days Glacier IR: $7.20
  - 185 days Deep Archive: $4.55
  - **Total**: ~$50/year
  - **Savings**: ~$226/year (~82%)

## Monitoring & Alerts

### Recommended CloudWatch Alarms

```bash
# High 4xx error rate
aws cloudwatch put-metric-alarm \
    --alarm-name datalake-high-4xx-errors \
    --metric-name 4xxErrors \
    --namespace AWS/S3 \
    --statistic Sum \
    --period 300 \
    --evaluation-periods 2 \
    --threshold 100 \
    --comparison-operator GreaterThanThreshold

# High 5xx error rate
aws cloudwatch put-metric-alarm \
    --alarm-name datalake-high-5xx-errors \
    --metric-name 5xxErrors \
    --namespace AWS/S3 \
    --statistic Sum \
    --period 300 \
    --evaluation-periods 2 \
    --threshold 10 \
    --comparison-operator GreaterThanThreshold
```

## IAM Roles Required

Before applying the bucket policy, ensure these IAM roles exist:

- `DataEngineeringRole`: For data engineers
- `GlueServiceRole`: For AWS Glue ETL jobs
- `LambdaDataLakeRole`: For Lambda functions
- `DataAnalystRole`: For analysts (read-only)
- `AthenaQueryRole`: For Athena queries

## Troubleshooting

### Issue: "Access Denied" when uploading

**Solution**: Ensure your IAM role is listed in the bucket policy and has the required permissions.

### Issue: Multipart upload fails

**Solution**: Check that incomplete multipart uploads are being cleaned up (7-day lifecycle rule).

### Issue: Can't delete from curated/

**Solution**: This is by design - curated zone requires MFA for deletions. Use MFA or temporarily modify the policy.

### Issue: High S3 costs

**Solution**:
1. Check lifecycle policies are applied correctly
2. Review storage class distribution
3. Check for failed multipart uploads consuming storage

## Best Practices

1. **Always use encryption**: The scripts enforce AES-256, don't disable it
2. **Tag everything**: Use consistent tagging for cost allocation
3. **Monitor metrics**: Set up CloudWatch alarms for anomalies
4. **Version control**: Keep versioning enabled for data protection
5. **Test uploads**: Use `--dry-run` or small files first
6. **Regular audits**: Review access logs and lifecycle transitions
7. **MFA for production**: Enable MFA for destructive operations
8. **Least privilege**: Grant minimum required permissions

## Security Considerations

- All data is encrypted at rest (AES-256)
- All transfers require HTTPS/TLS 1.2+
- Public access is completely blocked
- MFA required for deleting curated data
- IAM role-based access only
- Versioning protects against accidental deletion
- CloudWatch logging for audit trail

## Next Steps

After setting up the data lake:

1. Configure AWS Glue crawlers for data catalog
2. Set up Athena for SQL queries
3. Create EMR clusters for processing
4. Implement data quality checks
5. Set up automated ETL pipelines
6. Configure cross-region replication for DR

## Support

For issues or questions:
- Check CloudWatch logs for detailed error messages
- Review IAM policies and roles
- Verify AWS credentials are configured correctly
- Check bucket exists and you have access
