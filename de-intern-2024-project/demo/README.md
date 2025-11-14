# NYC Taxi Data Pipeline - Demo Scripts

This directory contains scripts for demonstrating the complete end-to-end data pipeline.

## Overview

The demo scripts showcase the full data pipeline workflow:

1. **Data Generation** - Create synthetic taxi trip data
2. **Environment Reset** - Clean up previous demo runs
3. **Demo Orchestration** - Run the complete pipeline demonstration
4. **Presentation Guide** - Step-by-step walkthrough for presenters

## Prerequisites

### AWS Infrastructure

Ensure the following infrastructure is deployed:

- S3 buckets (raw, processed, curated)
- Lambda functions (s3-notification)
- Step Functions state machine
- AWS Glue (crawler, jobs)
- CloudWatch dashboards and alarms
- Athena workgroup
- Redshift Serverless (optional)

Deploy using Terraform:
```bash
cd ../infrastructure/terraform
terraform init
terraform apply
```

### Python Dependencies

Install required Python packages:

```bash
pip install -r requirements.txt
```

Required packages:
- boto3 (AWS SDK)
- pandas (data manipulation)
- pyarrow (Parquet file handling)
- numpy (numerical operations)

### AWS Credentials

Ensure AWS credentials are configured:

```bash
export AWS_REGION=us-east-1
export AWS_PROFILE=your-profile  # Optional

# Or configure via AWS CLI
aws configure
```

## Scripts

### 1. Generate Demo Data

**Purpose:** Create synthetic NYC taxi trip data for demonstration purposes.

**Usage:**
```bash
python generate_demo_data.py [OPTIONS]
```

**Options:**
- `--num-records`: Number of records to generate (default: 10,000)
- `--year`: Year for the data (default: 2024)
- `--month`: Month for the data (default: 3 for March)
- `--output`: Output file path (default: yellow_tripdata_2024-03.parquet)

**Examples:**
```bash
# Generate 10,000 records for March 2024
python generate_demo_data.py

# Generate 50,000 records for April 2024
python generate_demo_data.py --num-records 50000 --month 4

# Custom output location
python generate_demo_data.py --output /tmp/demo_data.parquet
```

**Output:**
- Parquet file with synthetic taxi trip data
- Realistic values for fares, distances, timestamps
- Compatible with the pipeline's expected schema

### 2. Reset Demo Environment

**Purpose:** Clean up AWS resources to prepare for a fresh demo run.

**Usage:**
```bash
python reset_demo_environment.py [OPTIONS]
```

**Options:**
- `--region`: AWS region (default: us-east-1)
- `--project-name`: Project name prefix (default: de-intern-2024)
- `--dry-run`: Show what would be deleted without actually deleting
- `--yes`: Skip confirmation prompt

**Examples:**
```bash
# Dry run - see what would be deleted
python reset_demo_environment.py --dry-run

# Interactive reset (with confirmation)
python reset_demo_environment.py

# Non-interactive reset
python reset_demo_environment.py --yes
```

**Actions Performed:**
1. Stop running Step Functions executions
2. Clear processed S3 zone
3. Clear curated S3 zone
4. Reset Glue job bookmarks
5. Clear CloudWatch log streams
6. Clear Athena query results

**Warning:** This script deletes data! Always run with `--dry-run` first.

### 3. Run Demo

**Purpose:** Orchestrate the complete end-to-end pipeline demonstration.

**Usage:**
```bash
python run_final_demo.py [OPTIONS]
```

**Options:**
- `--region`: AWS region (default: us-east-1)
- `--project-name`: Project name prefix (default: de-intern-2024)
- `--demo-file`: Path to demo data file (required)
- `--record`: Record outputs for fallback mode

**Examples:**
```bash
# Run complete demo
python run_final_demo.py --demo-file yellow_tripdata_2024-03.parquet

# Run with custom region
python run_final_demo.py \
  --demo-file yellow_tripdata_2024-03.parquet \
  --region us-west-2
```

**Demo Steps:**

1. **Upload File** - Upload demo data to S3 raw bucket
2. **Lambda Trigger** - Verify Lambda function triggered by S3 event
3. **Step Functions** - Show workflow execution graph
4. **Glue Jobs** - Monitor ETL job completion
5. **Athena Query** - Query processed data
6. **Redshift Analytics** - Run analytics queries
7. **CloudWatch Dashboard** - Display monitoring metrics

**Output:**
- Colored terminal output with step-by-step progress
- AWS Console URLs for manual inspection
- Summary report of all steps
- Exit code (0=success, 1=failure)

## Quick Start Guide

### Complete Demo Workflow

Follow these steps for a successful demo:

```bash
# 1. Generate demo data
python generate_demo_data.py --num-records 10000

# 2. Reset environment (preview)
python reset_demo_environment.py --dry-run

# 3. Reset environment (execute)
python reset_demo_environment.py --yes

# 4. Run demo
python run_final_demo.py --demo-file yellow_tripdata_2024-03.parquet
```

### Expected Timeline

- **Data Generation:** 30 seconds
- **Environment Reset:** 2-3 minutes
- **Demo Execution:** 5-7 minutes (including ETL processing)
- **Total:** ~10 minutes

## Demo Presentation

See the detailed presentation guide:

```bash
cat ../docs/demo/presentation_guide.md
```

The presentation guide includes:
- 5-minute demo script
- Architecture overview
- Key talking points
- Q&A preparation
- Troubleshooting tips
- Fallback plans

## Troubleshooting

### Common Issues

#### Issue: Import errors
**Solution:**
```bash
pip install -r requirements.txt
```

#### Issue: AWS credentials not found
**Solution:**
```bash
aws configure
# Or set environment variables
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
export AWS_REGION=us-east-1
```

#### Issue: S3 bucket not found
**Solution:**
```bash
# Verify infrastructure is deployed
cd ../infrastructure/terraform
terraform output

# Check bucket names
aws s3 ls | grep de-intern-2024
```

#### Issue: Permission denied errors
**Solution:**
```bash
# Verify IAM permissions
aws sts get-caller-identity

# Check if you have necessary permissions for:
# - S3 (read/write)
# - Lambda (read logs)
# - Step Functions (start/stop executions)
# - Glue (start jobs, reset bookmarks)
# - CloudWatch (read metrics/logs)
```

#### Issue: Step Functions execution not starting
**Solution:**
- Check if S3 event notification is configured
- Verify Lambda function has correct IAM role
- Check Step Functions state machine exists

#### Issue: Glue jobs failing
**Solution:**
```bash
# Check Glue job logs in CloudWatch
aws logs tail /aws-glue/jobs/output --follow

# Verify Glue job scripts exist in S3
aws s3 ls s3://de-intern-2024-scripts-{account}/glue/
```

### Debug Mode

Enable debug logging:

```python
# Add to top of script
import logging
logging.basicConfig(level=logging.DEBUG)
```

## File Descriptions

```
demo/
├── README.md                       # This file
├── generate_demo_data.py          # Synthetic data generator
├── run_final_demo.py              # Demo orchestrator
├── reset_demo_environment.py      # Environment cleanup
└── requirements.txt               # Python dependencies
```

## Architecture Context

The demo scripts interact with the following AWS services:

```
┌─────────────────────────────────────────────────────────────┐
│                    Demo Scripts (Local)                      │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────┐   │
│  │  Generate   │  │    Reset    │  │   Run Demo       │   │
│  │    Data     │  │ Environment │  │  Orchestrator    │   │
│  └─────────────┘  └─────────────┘  └──────────────────┘   │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
        ┌───────────────────────────────────────┐
        │           AWS Services                 │
        │  ┌──────┐  ┌─────────┐  ┌──────────┐ │
        │  │  S3  │  │ Lambda  │  │   Step   │ │
        │  │      │  │         │  │ Functions│ │
        │  └──────┘  └─────────┘  └──────────┘ │
        │  ┌──────┐  ┌─────────┐  ┌──────────┐ │
        │  │ Glue │  │ Athena  │  │CloudWatch│ │
        │  │      │  │         │  │          │ │
        │  └──────┘  └─────────┘  └──────────┘ │
        └───────────────────────────────────────┘
```

## Cost Considerations

Running the demo incurs minimal AWS costs:

- **S3 Storage:** $0.023/GB (~$0.06 for demo data)
- **Lambda:** $0.20/1M requests (~$0.0002 per demo)
- **Step Functions:** $0.025/1K transitions (~$0.001 per demo)
- **Glue:** $0.44/DPU-hour (~$0.15 per demo)
- **Athena:** $5/TB scanned (~$0.01 per demo)

**Total Cost per Demo Run:** < $0.20

## Security Notes

- Demo scripts require AWS credentials with appropriate permissions
- Sensitive data should not be used in demo files
- Reset script permanently deletes data (use with caution)
- CloudWatch logs may contain sensitive information
- Use IAM roles with least privilege principles

## Contributing

When adding new demo features:

1. Update this README
2. Add appropriate error handling
3. Include dry-run mode for destructive operations
4. Update presentation guide
5. Test in isolated AWS account

## Support

For issues or questions:

1. Check troubleshooting section above
2. Review CloudWatch logs
3. Consult main project documentation
4. Contact the data engineering team

## License

Internal use only - Company confidential

---

**Last Updated:** 2024-03-15
