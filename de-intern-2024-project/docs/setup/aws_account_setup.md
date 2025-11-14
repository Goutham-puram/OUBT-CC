# AWS Account Setup Guide - Day 1

This guide walks you through setting up your AWS account and validating your environment for the Data Engineering Internship project.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [AWS Account Setup](#aws-account-setup)
3. [AWS CLI Installation](#aws-cli-installation)
4. [Configure AWS Credentials](#configure-aws-credentials)
5. [Python Environment Setup](#python-environment-setup)
6. [Environment Variables Configuration](#environment-variables-configuration)
7. [Validation](#validation)
8. [Troubleshooting](#troubleshooting)

---

## Prerequisites

Before starting, ensure you have:

- An AWS account (Account ID: `052162193815`)
- IAM user with programmatic access
- Admin or appropriate permissions for the following services:
  - S3
  - RDS
  - AWS Glue
  - Lambda
  - Step Functions
  - IAM
  - CloudWatch
- Python 3.9 or higher installed
- Git installed
- Terminal/Command line access

---

## AWS Account Setup

### 1. Verify Your AWS Account

Your AWS account details:
- **Account ID**: `052162193815`
- **Region**: `us-east-2` (US East - Ohio)

### 2. Create IAM User (if not already done)

If you don't have an IAM user with programmatic access:

1. Log into AWS Console: https://console.aws.amazon.com/
2. Navigate to **IAM** service
3. Click **Users** → **Add users**
4. Enter username (e.g., `de-intern-user`)
5. Select **Access key - Programmatic access**
6. Click **Next: Permissions**
7. Attach policies:
   - `AdministratorAccess` (for learning purposes)
   - Or create a custom policy with specific permissions (see [Required Permissions](#required-permissions))
8. Click through to **Create user**
9. **IMPORTANT**: Download and save the credentials:
   - Access Key ID
   - Secret Access Key
   - You won't be able to see the secret key again!

### 3. Required Permissions

Minimum required IAM permissions for this project:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:*",
        "rds:*",
        "glue:*",
        "lambda:*",
        "states:*",
        "iam:GetUser",
        "iam:ListRoles",
        "iam:PassRole",
        "iam:CreateRole",
        "iam:AttachRolePolicy",
        "logs:*",
        "cloudwatch:*",
        "sts:GetCallerIdentity"
      ],
      "Resource": "*"
    }
  ]
}
```

For production environments, follow the principle of least privilege and restrict permissions further.

---

## AWS CLI Installation

The AWS Command Line Interface (CLI) is required for interacting with AWS services.

### Installation Methods

#### Linux
```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

#### macOS
```bash
curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"
sudo installer -pkg AWSCLIV2.pkg -target /
```

#### Windows
Download and run the installer:
https://awscli.amazonaws.com/AWSCLIV2.msi

### Verify Installation

```bash
aws --version
```

Expected output:
```
aws-cli/2.x.x Python/3.x.x ...
```

---

## Configure AWS Credentials

### Method 1: Using AWS CLI (Recommended)

Run the AWS configure command:

```bash
aws configure
```

You will be prompted to enter:

```
AWS Access Key ID [None]: <your-access-key-id>
AWS Secret Access Key [None]: <your-secret-access-key>
Default region name [None]: us-east-2
Default output format [None]: json
```

### Method 2: Manual Configuration

Create/edit the credentials file:

**Linux/macOS**: `~/.aws/credentials`
**Windows**: `C:\Users\USERNAME\.aws\credentials`

```ini
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
```

Create/edit the config file:

**Linux/macOS**: `~/.aws/config`
**Windows**: `C:\Users\USERNAME\.aws\config`

```ini
[default]
region = us-east-2
output = json
```

### Method 3: Environment Variables

Set the following environment variables:

**Linux/macOS**:
```bash
export AWS_ACCESS_KEY_ID="your-access-key-id"
export AWS_SECRET_ACCESS_KEY="your-secret-access-key"
export AWS_DEFAULT_REGION="us-east-2"
```

**Windows (PowerShell)**:
```powershell
$env:AWS_ACCESS_KEY_ID="your-access-key-id"
$env:AWS_SECRET_ACCESS_KEY="your-secret-access-key"
$env:AWS_DEFAULT_REGION="us-east-2"
```

### Verify AWS Configuration

Test your credentials:

```bash
aws sts get-caller-identity
```

Expected output:
```json
{
    "UserId": "AIDAXXXXXXXXXXXXXXXXX",
    "Account": "052162193815",
    "Arn": "arn:aws:iam::052162193815:user/your-username"
}
```

**Important**: Verify that the `Account` matches `052162193815`

---

## Python Environment Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd de-intern-2024-project
```

### 2. Create Virtual Environment

**Linux/macOS**:
```bash
python3 -m venv venv
source venv/bin/activate
```

**Windows**:
```bash
python -m venv venv
venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Install Development Dependencies (Optional)

```bash
pip install -r requirements-dev.txt
```

### 5. Verify Boto3 Installation

```bash
python -c "import boto3; print(f'Boto3 version: {boto3.__version__}')"
```

---

## Environment Variables Configuration

### 1. Create .env File

Copy the template and configure your environment:

```bash
cp .env.template .env
```

### 2. Edit .env File

Open `.env` in your text editor and configure the required variables:

```bash
# Core AWS Configuration
AWS_ACCOUNT_ID=052162193815
AWS_DEFAULT_REGION=us-east-2
AWS_REGION=us-east-2

# S3 Configuration
S3_BUCKET_PREFIX=de-intern-2024

# Project Configuration
PROJECT_NAME=de-intern-2024-project

# Application Settings
ENVIRONMENT=dev
LOG_LEVEL=INFO
```

### 3. Load Environment Variables

**Linux/macOS**:
```bash
source .env
# or
export $(cat .env | xargs)
```

**Windows (PowerShell)**:
```powershell
Get-Content .env | ForEach-Object {
    $name, $value = $_.split('=')
    Set-Content env:\$name $value
}
```

**Python (using python-dotenv)**:
```python
from dotenv import load_dotenv
load_dotenv()
```

---

## Validation

### Run the Validation Script

We've created a comprehensive validation script to check your AWS setup:

```bash
python infrastructure/scripts/validate_aws_setup.py
```

### What the Script Checks

The validation script verifies:

1. ✓ **AWS CLI Installation** - Checks if AWS CLI is installed and accessible
2. ✓ **Boto3 Installation** - Verifies boto3 Python library is installed
3. ✓ **AWS Credentials** - Confirms credentials are configured correctly
4. ✓ **Account ID** - Validates you're using account `052162193815`
5. ✓ **Region** - Confirms region is set to `us-east-2`
6. ✓ **IAM Permissions** - Tests access to required AWS services:
   - S3
   - IAM
   - RDS
   - Glue
   - Lambda
   - Step Functions
7. ✓ **Boto3 Connectivity** - Tests boto3 can connect to AWS

### Expected Output

If everything is configured correctly, you should see:

```
======================================================================
              AWS Account Setup Validation
======================================================================

Expected Configuration:
  Account ID: 052162193815
  Region:     us-east-2

✓ PASS | AWS CLI Installation
       Version: aws-cli/2.x.x ...

✓ PASS | Boto3 Installation
       Version: 1.x.x

✓ PASS | AWS Credentials
       Access Key: AKIA...XXXX

✓ PASS | Account ID Verification
       Current: 052162193815
       ARN: arn:aws:iam::052162193815:user/your-username

✓ PASS | Region Verification
       Current: us-east-2

✓ PASS | IAM Permissions Check
       S3: ✓
       IAM: ✓
       RDS: ✓
       Glue: ✓
       Lambda: ✓
       Step Functions: ✓

✓ PASS | Boto3 Connectivity
       Successfully connected as: your-username

======================================================================
Summary
======================================================================

✓ All checks passed! (7/7)

Your AWS environment is correctly configured.
You can proceed with the Data Engineering Internship project.
```

### Manual Verification Commands

You can also verify individual components manually:

#### Check AWS CLI
```bash
aws --version
```

#### Check Account Identity
```bash
aws sts get-caller-identity
```

#### Check Region
```bash
aws configure get region
```

#### Test S3 Access
```bash
aws s3 ls
```

#### Test Boto3 Connection
```python
import boto3

# Create session
session = boto3.Session()

# Get account ID
sts = session.client('sts')
identity = sts.get_caller_identity()

print(f"Account ID: {identity['Account']}")
print(f"User ARN: {identity['Arn']}")
print(f"Region: {session.region_name}")
```

---

## Troubleshooting

### Common Issues and Solutions

#### 1. AWS CLI Not Found

**Error**: `aws: command not found`

**Solution**:
- Reinstall AWS CLI following the [installation guide](#aws-cli-installation)
- On Linux/macOS, ensure the installation directory is in your PATH:
  ```bash
  export PATH=$PATH:/usr/local/bin
  ```

#### 2. No Credentials Found

**Error**: `Unable to locate credentials`

**Solution**:
- Run `aws configure` to set up credentials
- Verify credentials file exists: `cat ~/.aws/credentials`
- Check environment variables: `echo $AWS_ACCESS_KEY_ID`

#### 3. Wrong Account ID

**Error**: Account ID mismatch (expected 052162193815)

**Solution**:
- Verify you're logged into the correct AWS account
- Check credentials are for the right account:
  ```bash
  aws sts get-caller-identity
  ```
- Update credentials if needed using `aws configure`

#### 4. Wrong Region

**Error**: Region mismatch (expected us-east-2)

**Solution**:
- Update region configuration:
  ```bash
  aws configure set region us-east-2
  ```
- Or set environment variable:
  ```bash
  export AWS_DEFAULT_REGION=us-east-2
  ```

#### 5. Access Denied Errors

**Error**: `AccessDenied` or `UnauthorizedOperation`

**Solution**:
- Verify IAM user has required permissions
- Check IAM policies attached to your user
- Contact AWS administrator to grant necessary permissions
- Review [Required Permissions](#required-permissions)

#### 6. Boto3 Import Error

**Error**: `ModuleNotFoundError: No module named 'boto3'`

**Solution**:
- Ensure virtual environment is activated
- Install boto3:
  ```bash
  pip install boto3
  ```

#### 7. SSL Certificate Errors

**Error**: `SSL: CERTIFICATE_VERIFY_FAILED`

**Solution**:
- Update certificates:
  ```bash
  pip install --upgrade certifi
  ```
- On macOS, run: `/Applications/Python 3.x/Install Certificates.command`

#### 8. Connection Timeout

**Error**: Connection timeouts when calling AWS services

**Solution**:
- Check internet connection
- Verify firewall/proxy settings
- Try using a different network
- Check AWS service status: https://status.aws.amazon.com/

### Getting Help

If you continue to experience issues:

1. Review AWS documentation: https://docs.aws.amazon.com/
2. Check boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
3. Review project documentation in `docs/`
4. Open an issue on the project repository
5. Contact your project mentor or instructor

---

## Next Steps

Once your AWS environment is validated:

1. ✓ AWS account setup complete
2. → Proceed to Week 1 tasks: [Week 1 Guide](../week1-guide.md)
3. → Set up RDS PostgreSQL database
4. → Create your first data ingestion script
5. → Download NYC Taxi dataset

---

## Security Best Practices

### Protecting Your Credentials

1. **Never commit credentials to Git**
   - `.env` is in `.gitignore`
   - Always use `.env.template` for examples

2. **Rotate credentials regularly**
   - Change access keys every 90 days
   - Use IAM password policy for console access

3. **Use MFA (Multi-Factor Authentication)**
   - Enable MFA for console access
   - Use MFA for sensitive operations

4. **Principle of Least Privilege**
   - Grant only necessary permissions
   - Use IAM roles when possible
   - Avoid using root account

5. **Monitor AWS Activity**
   - Enable CloudTrail
   - Review CloudWatch logs
   - Set up billing alerts

### Environment File Security

```bash
# Verify .env is not tracked by Git
git status

# If .env appears, add it to .gitignore
echo ".env" >> .gitignore
git add .gitignore
git commit -m "Add .env to gitignore"
```

---

## Quick Reference

### Important Account Details
- **Account ID**: `052162193815`
- **Region**: `us-east-2` (US East - Ohio)
- **Project**: Data Engineering Internship 2024

### Key Files
- **Validation Script**: `infrastructure/scripts/validate_aws_setup.py`
- **Environment Template**: `.env.template`
- **Environment File**: `.env` (create from template)

### Essential Commands

```bash
# Validate AWS setup
python infrastructure/scripts/validate_aws_setup.py

# Check AWS identity
aws sts get-caller-identity

# List S3 buckets
aws s3 ls

# Check AWS CLI configuration
aws configure list

# Test boto3
python -c "import boto3; print(boto3.Session().region_name)"
```

---

## Appendix

### A. AWS Services Used in This Project

| Service | Purpose | Week |
|---------|---------|------|
| S3 | Data lake storage | 1-4 |
| RDS (PostgreSQL) | Metadata and exploration database | 1 |
| IAM | Identity and access management | 1-4 |
| AWS Glue | Data catalog and ETL | 2-3 |
| Lambda | Event-driven data processing | 2 |
| Step Functions | Workflow orchestration | 3 |
| Redshift | Data warehouse | 4 |
| CloudWatch | Logging and monitoring | 1-4 |

### B. Helpful AWS CLI Commands

```bash
# List all regions
aws ec2 describe-regions --output table

# Get current region
aws configure get region

# List IAM users
aws iam list-users

# List S3 buckets
aws s3 ls

# List RDS instances
aws rds describe-db-instances

# List Glue databases
aws glue get-databases

# Get account ID
aws sts get-caller-identity --query Account --output text
```

### C. Boto3 Quick Start

```python
import boto3
from botocore.exceptions import ClientError

# Create session with explicit configuration
session = boto3.Session(
    region_name='us-east-2'
)

# Create S3 client
s3 = session.client('s3')

# List buckets
try:
    response = s3.list_buckets()
    for bucket in response['Buckets']:
        print(f"Bucket: {bucket['Name']}")
except ClientError as e:
    print(f"Error: {e}")
```

---

**Document Version**: 1.0
**Last Updated**: 2024-11-14
**Project**: Data Engineering Internship 2024
