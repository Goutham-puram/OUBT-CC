# Quick Start - Demo in 3 Steps

This is the fastest way to run the NYC Taxi Data Pipeline demo.

## Prerequisites

- AWS infrastructure deployed (Terraform)
- AWS credentials configured
- Python 3.8+ installed

## Setup (One-Time)

```bash
# Install dependencies
pip install boto3 pandas pyarrow numpy

# Verify AWS access
aws sts get-caller-identity
```

## Run Demo

### Step 1: Generate Data (30 seconds)

```bash
cd demo
python generate_demo_data.py
```

**Output:** `yellow_tripdata_2024-03.parquet` (10,000 records)

### Step 2: Reset Environment (2 minutes)

```bash
python reset_demo_environment.py --yes
```

**Actions:** Clears processed/curated zones, resets Glue bookmarks, clears logs

### Step 3: Run Demo (5 minutes)

```bash
python run_final_demo.py --demo-file yellow_tripdata_2024-03.parquet
```

**Output:** Complete pipeline execution with visual progress

## Expected Output

```
================================================================================
STEP 1: Upload Demo File to S3
================================================================================

ℹ Uploading yellow_tripdata_2024-03.parquet to s3://de-intern-2024-raw-data-{account}/taxi/yellow_tripdata_2024-03.parquet
✓ Successfully uploaded yellow_tripdata_2024-03.parquet (2.50 MB)
ℹ S3 URI: s3://de-intern-2024-raw-data-{account}/taxi/yellow_tripdata_2024-03.parquet

================================================================================
STEP 2: Check Lambda Trigger in CloudWatch
================================================================================

ℹ Checking CloudWatch logs in /aws/lambda/de-intern-2024-s3-notification
ℹ Latest log stream: 2024/03/15/[$LATEST]abc123...
✓ Lambda function triggered successfully!

[... continues through all 7 steps ...]

================================================================================
DEMO SUMMARY
================================================================================

  Upload File: ✓ PASSED
  Lambda Trigger: ✓ PASSED
  Step Functions: ✓ PASSED
  Glue Jobs: ✓ PASSED
  Athena Query: ✓ PASSED
  Redshift Analytics: ✓ PASSED
  CloudWatch Dashboard: ✓ PASSED

Results: 7/7 steps completed successfully

✓ Demo completed successfully! 🎉
```

## Troubleshooting

### Common Issues

**Error: "Demo file not found"**
```bash
# Ensure you're in the demo directory
cd de-intern-2024-project/demo
ls -la yellow_tripdata_2024-03.parquet
```

**Error: "S3 bucket not found"**
```bash
# Verify Terraform infrastructure is deployed
cd ../infrastructure/terraform
terraform output s3_raw_bucket
```

**Error: "Access Denied"**
```bash
# Check AWS credentials
aws sts get-caller-identity

# Verify IAM permissions (need S3, Lambda, StepFunctions, Glue, CloudWatch)
```

## Next Steps

- Review `docs/demo/presentation_guide.md` for detailed walkthrough
- Open AWS Console to see resources
- Customize demo data with `--num-records` flag
- Schedule a presentation!

## Quick Commands Reference

```bash
# Generate more data
python generate_demo_data.py --num-records 50000

# Dry-run reset (see what would be deleted)
python reset_demo_environment.py --dry-run

# Run demo with custom region
python run_final_demo.py --demo-file yellow_tripdata_2024-03.parquet --region us-west-2

# Clean up after demo
python reset_demo_environment.py --yes
```

## Demo Timeline

| Step | Duration | Description |
|------|----------|-------------|
| Generate Data | 30s | Create synthetic taxi trip data |
| Reset Env | 2m | Clean previous demo artifacts |
| Upload File | 1s | Upload to S3 raw bucket |
| Lambda Trigger | 5s | S3 event notification |
| Step Functions | 3-5m | ETL workflow execution |
| Glue Jobs | 2-3m | Data processing |
| Athena Query | 2-3s | SQL query on processed data |
| Redshift Analytics | 30s | Analytics query preparation |
| Dashboard | 5s | CloudWatch metrics display |
| **Total** | **~10m** | Complete end-to-end demo |

## Architecture at a Glance

```
Upload File → Lambda → Step Functions → Glue (Crawler + Jobs) → Athena/Redshift
     ↓           ↓            ↓                    ↓                    ↓
   S3 Raw   CloudWatch   Workflow           Data Processing      Analytics
```

---

**Ready? Let's go!** 🚀

```bash
python generate_demo_data.py && \
python reset_demo_environment.py --yes && \
python run_final_demo.py --demo-file yellow_tripdata_2024-03.parquet
```
