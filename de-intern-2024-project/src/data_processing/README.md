# NYC Taxi Data Processing Scripts

Python scripts for processing NYC Yellow Taxi trip data using pandas and boto3.

## Overview

This module provides four scripts to download, explore, clean, and sample NYC taxi data:

1. **download_taxi_data.py** - Download January 2024 NYC Yellow Taxi parquet file
2. **explore_taxi_data.py** - Load with pandas, show schema, basic statistics
3. **clean_taxi_data.py** - Handle nulls, add calculated columns (trip_duration, tip_percentage)
4. **sample_taxi_data.py** - Create 10,000 record sample for RDS testing

## Prerequisites

Install required dependencies:

```bash
pip install pandas boto3 requests pyarrow
```

Or install from the project requirements:

```bash
pip install -r requirements.txt
```

## Usage

### 1. Download Data

Download January 2024 NYC Yellow Taxi trip data:

```bash
python src/data_processing/download_taxi_data.py
```

**Features:**
- Downloads from official NYC TLC data repository
- Progress tracking during download
- Optional S3 upload (set `S3_BUCKET` environment variable)
- Skips download if file already exists

**Output:** `data/raw/yellow_tripdata_2024-01.parquet`

### 2. Explore Data

Analyze the downloaded data:

```bash
python src/data_processing/explore_taxi_data.py
```

**Features:**
- Dataset schema with column types and null counts
- Sample data preview
- Numerical statistics (mean, std, min, max, percentiles)
- Categorical value distributions
- Datetime range analysis
- Duplicate detection
- Missing data summary

**Output:** Console report with comprehensive data analysis

### 3. Clean Data

Clean and transform the data:

```bash
python src/data_processing/clean_taxi_data.py
```

**Features:**
- **Missing value handling:**
  - Remove rows with missing critical fields (pickup/dropoff times, locations)
  - Fill numeric fields with appropriate defaults

- **Calculated columns:**
  - `trip_duration` - Duration in minutes
  - `tip_percentage` - Tip as percentage of fare
  - `avg_speed_mph` - Average speed in miles per hour
  - `pickup_hour` - Hour of day (0-23)
  - `pickup_day_of_week` - Day of week (0=Monday, 6=Sunday)
  - `pickup_date` - Date only
  - `cost_per_mile` - Total cost divided by distance

- **Data validation:**
  - Remove invalid trip durations (≤0 or >24 hours)
  - Remove unrealistic speeds (>100 mph)
  - Cap extreme tip percentages
  - Flag negative values and other quality issues

**Output:** `data/processed/yellow_tripdata_2024-01_cleaned.parquet`

### 4. Create Sample

Generate a 10,000 record sample for testing:

```bash
python src/data_processing/sample_taxi_data.py
```

**Features:**
- Random sampling with reproducible seed
- Sample validation (compare distributions)
- Multiple output formats:
  - Parquet for data analysis
  - CSV optimized for RDS import
- Stratified and time-based sampling options

**Output:**
- `data/processed/yellow_tripdata_2024-01_sample.parquet`
- `data/processed/yellow_tripdata_2024-01_sample_rds.csv`

## Data Pipeline

Complete workflow:

```bash
# Step 1: Download data
python src/data_processing/download_taxi_data.py

# Step 2: Explore raw data
python src/data_processing/explore_taxi_data.py

# Step 3: Clean and transform
python src/data_processing/clean_taxi_data.py

# Step 4: Create test sample
python src/data_processing/sample_taxi_data.py
```

## AWS S3 Integration

All scripts support S3 operations via boto3:

### Upload to S3

Set environment variable before running:

```bash
export S3_BUCKET=your-bucket-name
python src/data_processing/download_taxi_data.py
```

### AWS Configuration

Configure AWS credentials:

```bash
aws configure
```

Or set environment variables:

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

## Script Details

### download_taxi_data.py

**Class:** `TaxiDataDownloader`

**Methods:**
- `download_from_url(url, filename)` - Download file from URL
- `upload_to_s3(filepath, bucket, key)` - Upload to S3
- `download_yellow_taxi_data(year, month)` - Download specific month

### explore_taxi_data.py

**Class:** `TaxiDataExplorer`

**Methods:**
- `load_from_local(filepath)` - Load from local file
- `load_from_s3(bucket, key)` - Load from S3
- `show_schema()` - Display schema information
- `show_statistics()` - Display statistical analysis
- `show_sample_data(n)` - Display sample records
- `generate_report()` - Generate comprehensive report

### clean_taxi_data.py

**Class:** `TaxiDataCleaner`

**Methods:**
- `load_data(filepath)` - Load parquet file
- `handle_missing_values()` - Handle null values
- `add_calculated_columns()` - Add derived columns
- `validate_data()` - Validate data quality
- `save_cleaned_data(output_path)` - Save cleaned data
- `print_cleaning_summary()` - Display cleaning statistics

### sample_taxi_data.py

**Class:** `TaxiDataSampler`

**Methods:**
- `load_data(filepath)` - Load parquet file
- `create_random_sample(n, random_state)` - Random sampling
- `create_stratified_sample(n, stratify_column)` - Stratified sampling
- `create_time_based_sample(n, date_column, start_date, end_date)` - Time-based sampling
- `validate_sample()` - Validate sample representativeness
- `save_sample(output_path, format)` - Save sample data
- `export_to_csv_for_rds(output_path)` - Export for RDS import

## Error Handling

All scripts include:
- Comprehensive logging with timestamps
- Try-except blocks for error handling
- Input validation
- Graceful failure with informative error messages

## Logging

Logging is configured to INFO level by default. Modify in each script:

```python
logging.basicConfig(
    level=logging.DEBUG,  # Change to DEBUG for verbose output
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

## Data Schema

NYC Yellow Taxi trip data includes:

**Temporal:**
- `tpep_pickup_datetime` - Pickup timestamp
- `tpep_dropoff_datetime` - Dropoff timestamp

**Location:**
- `PULocationID` - Pickup location ID (TLC Taxi Zone)
- `DOLocationID` - Dropoff location ID

**Trip Details:**
- `trip_distance` - Distance in miles
- `passenger_count` - Number of passengers

**Fare Information:**
- `fare_amount` - Base fare
- `extra` - Miscellaneous extras
- `mta_tax` - MTA tax
- `tip_amount` - Tip amount
- `tolls_amount` - Tolls
- `improvement_surcharge` - Improvement surcharge
- `total_amount` - Total fare
- `congestion_surcharge` - Congestion surcharge

**Payment:**
- `payment_type` - Payment method (1=Credit card, 2=Cash, etc.)
- `RatecodeID` - Rate code

**Calculated Columns (after cleaning):**
- `trip_duration` - Trip duration in minutes
- `tip_percentage` - Tip as % of fare
- `avg_speed_mph` - Average speed
- `pickup_hour` - Hour of pickup
- `pickup_day_of_week` - Day of week
- `cost_per_mile` - Cost per mile

## Troubleshooting

**File not found:**
```
FileNotFoundError: data/raw/yellow_tripdata_2024-01.parquet
```
Solution: Run `download_taxi_data.py` first

**S3 upload fails:**
```
ClientError: Access Denied
```
Solution: Check AWS credentials and S3 bucket permissions

**Memory issues:**
```
MemoryError: Unable to allocate array
```
Solution: Process data in chunks or use a machine with more RAM

## References

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [TLC Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)

## License

MIT License - See project LICENSE file for details.
