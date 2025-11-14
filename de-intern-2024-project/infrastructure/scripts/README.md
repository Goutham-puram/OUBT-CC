# Infrastructure Scripts

This directory contains utility scripts for managing and validating AWS infrastructure.

## Scripts

### validate_aws_setup.py

**Purpose**: Validates AWS account configuration and setup for the Data Engineering Internship project.

**Usage**:
```bash
python infrastructure/scripts/validate_aws_setup.py
```

**What it checks**:
- AWS CLI installation and version
- Boto3 Python library installation
- AWS credentials configuration
- Account ID verification (expects: 052162193815)
- Region validation (expects: us-east-2)
- IAM permissions for required services:
  - S3
  - IAM
  - RDS
  - AWS Glue
  - Lambda
  - Step Functions
- Boto3 connectivity to AWS

**Requirements**:
- Python 3.9+
- boto3 library (`pip install boto3`)
- Configured AWS credentials

**Environment Variables** (optional overrides):
- `AWS_ACCOUNT_ID`: Override expected account ID
- `AWS_DEFAULT_REGION`: Override expected region

**Exit Codes**:
- `0`: All checks passed
- `1`: One or more checks failed

**Example Output**:
```
======================================================================
              AWS Account Setup Validation
======================================================================

Expected Configuration:
  Account ID: 052162193815
  Region:     us-east-2

✓ PASS | AWS CLI Installation
✓ PASS | Boto3 Installation
✓ PASS | AWS Credentials
✓ PASS | Account ID Verification
✓ PASS | Region Verification
✓ PASS | IAM Permissions Check
✓ PASS | Boto3 Connectivity

======================================================================
Summary
======================================================================

✓ All checks passed! (7/7)
```

## Adding New Scripts

When adding new infrastructure scripts:

1. Add a clear docstring explaining the script's purpose
2. Include usage instructions in this README
3. Make scripts executable: `chmod +x script_name.py`
4. Add appropriate error handling
5. Use environment variables for configuration
6. Include helpful error messages

## Related Documentation

- [AWS Account Setup Guide](../../docs/setup/aws_account_setup.md)
- [Week 1 Guide](../../docs/week1-guide.md)
