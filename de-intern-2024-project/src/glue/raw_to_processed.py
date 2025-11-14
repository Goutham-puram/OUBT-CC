"""
AWS Glue ETL Job: Raw to Processed

This job:
1. Reads raw taxi data from S3 (raw zone)
2. Performs data cleaning and validation
3. Adds derived columns
4. Writes processed data to S3 (processed zone) in partitioned Parquet format
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'target_bucket'])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f"Starting job: {args['JOB_NAME']}")
print(f"Source bucket: {args['source_bucket']}")
print(f"Target bucket: {args['target_bucket']}")

# Read raw data from S3
source_path = f"s3://{args['source_bucket']}/taxi/"
print(f"Reading data from: {source_path}")

df = spark.read.parquet(source_path)
initial_count = df.count()
print(f"Initial record count: {initial_count}")

# Data Cleaning and Transformation
print("Starting data cleaning...")

# 1. Remove null values in critical columns
df = df.dropna(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

# 2. Filter invalid values
df = df.filter(
    (F.col('trip_distance') > 0) &
    (F.col('fare_amount') > 0) &
    (F.col('passenger_count') > 0) &
    (F.col('passenger_count') <= 6) &
    (F.col('trip_distance') < 100) &
    (F.col('fare_amount') < 500) &
    (F.col('tpep_dropoff_datetime') > F.col('tpep_pickup_datetime'))
)

# 3. Add derived columns
print("Adding derived columns...")

# Trip duration in minutes
df = df.withColumn(
    'trip_duration_minutes',
    (F.unix_timestamp('tpep_dropoff_datetime') -
     F.unix_timestamp('tpep_pickup_datetime')) / 60
)

# Average speed (mph)
df = df.withColumn(
    'avg_speed_mph',
    F.when(F.col('trip_duration_minutes') > 0,
           F.col('trip_distance') / (F.col('trip_duration_minutes') / 60))
    .otherwise(0)
)

# Hour of day
df = df.withColumn('pickup_hour', F.hour('tpep_pickup_datetime'))

# Day of week (1 = Monday, 7 = Sunday)
df = df.withColumn('pickup_day_of_week', F.dayofweek('tpep_pickup_datetime'))

# Year and month for partitioning
df = df.withColumn('year', F.year('tpep_pickup_datetime'))
df = df.withColumn('month', F.month('tpep_pickup_datetime'))

# 4. Filter outliers based on trip duration
df = df.filter(
    (F.col('trip_duration_minutes') >= 1) &
    (F.col('trip_duration_minutes') <= 300)  # 5 hours max
)

# 5. Filter unrealistic speeds (likely data errors)
df = df.filter(F.col('avg_speed_mph') < 100)  # 100 mph max

# 6. Add data quality flags
df = df.withColumn(
    'quality_flag',
    F.when(
        (F.col('trip_distance') < 0.5) & (F.col('fare_amount') > 20), 'suspicious_fare'
    ).when(
        F.col('avg_speed_mph') > 60, 'high_speed'
    ).otherwise('normal')
)

# 7. Add processing metadata
df = df.withColumn('processed_timestamp', F.current_timestamp())

final_count = df.count()
records_removed = initial_count - final_count
print(f"Records after cleaning: {final_count}")
print(f"Records removed: {records_removed} ({records_removed/initial_count*100:.2f}%)")

# Write to processed zone (partitioned by year and month)
target_path = f"s3://{args['target_bucket']}/taxi/"
print(f"Writing data to: {target_path}")

df.write \
    .mode('append') \
    .partitionBy('year', 'month') \
    .parquet(target_path)

print("Data written successfully!")

# Print sample statistics
print("\n=== Sample Statistics ===")
df.select(
    F.avg('trip_distance').alias('avg_trip_distance'),
    F.avg('fare_amount').alias('avg_fare'),
    F.avg('trip_duration_minutes').alias('avg_duration'),
    F.count('*').alias('total_records')
).show()

# Commit job
job.commit()
print("Job completed successfully!")
