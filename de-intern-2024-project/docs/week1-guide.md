# Week 1 Guide: AWS Setup and RDS PostgreSQL

## Overview

Week 1 focuses on setting up your AWS environment, understanding Python/Boto3 basics, and working with RDS PostgreSQL for initial data exploration.

## Learning Objectives

By the end of Week 1, you will:
1. Set up and configure AWS account and IAM
2. Understand Python virtual environments and dependencies
3. Use Boto3 SDK for AWS service interaction
4. Connect to and query RDS PostgreSQL databases
5. Download and ingest NYC Taxi data
6. Perform basic data exploration with SQL and Pandas

## Prerequisites

- AWS Account (with appropriate permissions)
- Python 3.9+ installed locally
- Basic SQL knowledge
- Basic Python knowledge
- Git installed

## Day 1: AWS Account Setup

### Tasks

1. **Create AWS Account**
   - Sign up at https://aws.amazon.com
   - Set up billing alerts
   - Enable MFA for root account

2. **Create IAM User**
   ```bash
   # Create a user with programmatic access
   # Attach policies:
   # - AmazonS3FullAccess
   # - AmazonRDSFullAccess
   # - IAMReadOnlyAccess
   # - AWSGlueConsoleFullAccess
   ```

3. **Configure AWS CLI**
   ```bash
   # Install AWS CLI
   pip install awscli

   # Configure credentials
   aws configure
   # Enter: Access Key ID, Secret Access Key, Region (us-east-1), Output (json)

   # Test configuration
   aws sts get-caller-identity
   ```

4. **Set Up Project**
   ```bash
   # Clone repository
   git clone <your-repo-url>
   cd de-intern-2024-project

   # Create virtual environment
   make venv
   source venv/bin/activate

   # Install dependencies
   make install-dev
   ```

### Deliverables
- [ ] AWS account created and secured
- [ ] IAM user configured with CLI access
- [ ] Project environment set up locally

## Day 2: Infrastructure Deployment

### Tasks

1. **Configure Terraform Variables**
   ```bash
   cd infrastructure/terraform
   cp example.tfvars terraform.tfvars
   # Edit terraform.tfvars with your values
   ```

2. **Deploy Infrastructure**
   ```bash
   # Initialize Terraform
   terraform init

   # Review planned changes
   terraform plan

   # Deploy (only RDS for now)
   # Comment out Redshift in main.tf to save costs
   terraform apply
   ```

3. **Get RDS Endpoint**
   ```bash
   # Get RDS endpoint from Terraform output
   terraform output rds_endpoint

   # Update .env file
   cp .env.example .env
   # Add RDS connection details
   ```

4. **Test Database Connection**
   ```bash
   # Using psql (if installed)
   psql -h <rds-endpoint> -U admin -d taxi_data

   # Or using Python
   python -c "from de_intern_2024.week1 import RDSConnector; \
              conn = RDSConnector(); \
              print(conn.test_connection())"
   ```

### Deliverables
- [ ] Terraform infrastructure deployed
- [ ] RDS PostgreSQL running
- [ ] Database connection verified

## Day 3: Data Ingestion

### Tasks

1. **Download NYC Taxi Data**
   ```bash
   # Download January 2023 data
   python data/scripts/download_taxi_data.py --year 2023 --month 1

   # Verify download
   ls -lh data/raw/
   ```

2. **Explore Data Locally**
   ```python
   # In Python shell or Jupyter notebook
   from de_intern_2024.week1.data_ingestion import load_taxi_parquet

   # Load data
   df = load_taxi_parquet("data/raw/yellow_tripdata_2023-01.parquet", nrows=10000)

   # Explore
   print(df.head())
   print(df.info())
   print(df.describe())
   ```

3. **Run Data Cleaning**
   ```python
   from de_intern_2024.week1.data_ingestion import clean_taxi_data

   df_clean = clean_taxi_data(df)
   print(f"Original rows: {len(df)}")
   print(f"Clean rows: {len(df_clean)}")
   ```

4. **Create Database Schema**
   ```bash
   # Run migration script
   psql -h <rds-endpoint> -U admin -d taxi_data \
        -f sql/migrations/001_create_taxi_trips_table.sql
   ```

5. **Ingest Data to PostgreSQL**
   ```bash
   # Run Week 1 main script
   python -m de_intern_2024.week1.main \
       --year 2023 \
       --month 1 \
       --sample 100000  # Start with 100k rows for testing
   ```

### Deliverables
- [ ] Sample data downloaded
- [ ] Data cleaning pipeline working
- [ ] Database table created
- [ ] Data ingested into PostgreSQL

## Day 4: SQL Exploration

### Tasks

1. **Connect to Database**
   ```bash
   # Using psql
   psql -h <rds-endpoint> -U admin -d taxi_data

   # Or using DBeaver, pgAdmin, etc.
   ```

2. **Run Exploratory Queries**
   ```sql
   -- Count trips
   SELECT COUNT(*) FROM taxi_trips;

   -- Daily trip counts
   SELECT
       DATE(tpep_pickup_datetime) as trip_date,
       COUNT(*) as trip_count,
       AVG(trip_distance) as avg_distance,
       AVG(fare_amount) as avg_fare
   FROM taxi_trips
   GROUP BY trip_date
   ORDER BY trip_date;

   -- Payment type distribution
   SELECT
       payment_type,
       COUNT(*) as count,
       AVG(total_amount) as avg_amount
   FROM taxi_trips
   GROUP BY payment_type;

   -- Peak hours
   SELECT
       EXTRACT(HOUR FROM tpep_pickup_datetime) as hour,
       COUNT(*) as trip_count
   FROM taxi_trips
   GROUP BY hour
   ORDER BY hour;
   ```

3. **Create Views**
   ```sql
   -- Daily summary view
   CREATE VIEW daily_summary AS
   SELECT
       DATE(tpep_pickup_datetime) as trip_date,
       COUNT(*) as total_trips,
       SUM(fare_amount) as total_revenue,
       AVG(trip_distance) as avg_distance,
       AVG(passenger_count) as avg_passengers
   FROM taxi_trips
   GROUP BY trip_date;

   -- Query the view
   SELECT * FROM daily_summary ORDER BY trip_date DESC LIMIT 7;
   ```

### Deliverables
- [ ] Basic SQL queries executed
- [ ] Data patterns identified
- [ ] Summary views created

## Day 5: Python Data Analysis

### Tasks

1. **Load Data from PostgreSQL**
   ```python
   from de_intern_2024.week1 import RDSConnector

   connector = RDSConnector()

   # Load data into DataFrame
   query = """
   SELECT *
   FROM taxi_trips
   WHERE tpep_pickup_datetime >= '2023-01-01'
     AND tpep_pickup_datetime < '2023-02-01'
   LIMIT 50000
   """
   df = connector.read_sql(query)
   ```

2. **Analyze with Pandas**
   ```python
   import pandas as pd
   import matplotlib.pyplot as plt

   # Time series analysis
   df['pickup_date'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.date
   daily_trips = df.groupby('pickup_date').size()

   # Visualize (if matplotlib available)
   daily_trips.plot(kind='bar', figsize=(15, 5))
   plt.title('Daily Trip Counts - January 2023')
   plt.show()

   # Fare analysis
   print("\nFare Statistics:")
   print(df['fare_amount'].describe())

   # Distance vs Fare correlation
   correlation = df[['trip_distance', 'fare_amount']].corr()
   print("\nDistance-Fare Correlation:")
   print(correlation)
   ```

3. **Create Summary Report**
   ```python
   from de_intern_2024.week1.data_ingestion import get_sample_statistics

   stats = get_sample_statistics(df)
   print("\nJanuary 2023 Taxi Data Summary:")
   print("=" * 50)
   for key, value in stats.items():
       print(f"{key}: {value}")
   ```

### Deliverables
- [ ] Data analysis with Pandas completed
- [ ] Key insights documented
- [ ] Summary statistics generated

## Key Concepts

### Boto3 Basics
```python
import boto3

# Create client
s3_client = boto3.client('s3', region_name='us-east-1')

# List buckets
response = s3_client.list_buckets()
print([bucket['Name'] for bucket in response['Buckets']])

# Upload file
s3_client.upload_file('local_file.txt', 'my-bucket', 'remote_file.txt')

# Download file
s3_client.download_file('my-bucket', 'remote_file.txt', 'downloaded.txt')
```

### SQLAlchemy Basics
```python
from sqlalchemy import create_engine, text

# Create engine
engine = create_engine('postgresql://user:pass@host:5432/dbname')

# Execute query
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM table LIMIT 10"))
    for row in result:
        print(row)

# With Pandas
import pandas as pd
df = pd.read_sql("SELECT * FROM table", engine)
```

## Common Issues and Solutions

### Issue 1: Cannot connect to RDS
```bash
# Check security group allows your IP
# In AWS Console: EC2 > Security Groups > RDS SG > Inbound Rules
# Add rule: PostgreSQL (5432) from My IP

# Test connectivity
telnet <rds-endpoint> 5432
# or
nc -zv <rds-endpoint> 5432
```

### Issue 2: Terraform apply fails
```bash
# Check AWS credentials
aws sts get-caller-identity

# Check Terraform syntax
terraform validate

# Initialize again
terraform init -upgrade
```

### Issue 3: Data download fails
```bash
# Check internet connectivity
ping google.com

# Try manual download
curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet

# Check disk space
df -h
```

## Resources

- [AWS RDS Documentation](https://docs.aws.amazon.com/rds/)
- [Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [NYC TLC Trip Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Next Week Preview

Week 2 will cover:
- S3 data lake architecture
- AWS Glue Data Catalog
- Lambda functions for event processing
- Data quality validation

## Assessment Checklist

- [ ] AWS environment properly configured
- [ ] RDS PostgreSQL database running
- [ ] Data successfully downloaded and ingested
- [ ] SQL queries working correctly
- [ ] Python data analysis completed
- [ ] Understanding of Boto3 basics
- [ ] Familiarity with Pandas and SQLAlchemy
