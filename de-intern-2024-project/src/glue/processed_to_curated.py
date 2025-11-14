"""
AWS Glue ETL Job: Processed to Curated

This job:
1. Reads processed taxi data from S3 (processed zone)
2. Performs aggregations and creates analytical datasets
3. Writes curated data to S3 (curated zone) for analytics
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

# Read processed data from S3
source_path = f"s3://{args['source_bucket']}/taxi/"
print(f"Reading data from: {source_path}")

df = spark.read.parquet(source_path)
record_count = df.count()
print(f"Record count: {record_count}")

# === Aggregation 1: Daily Statistics ===
print("Creating daily statistics...")

daily_stats = df.groupBy(
    F.to_date('tpep_pickup_datetime').alias('date'),
    'year',
    'month'
).agg(
    F.count('*').alias('total_trips'),
    F.sum('fare_amount').alias('total_revenue'),
    F.avg('fare_amount').alias('avg_fare'),
    F.avg('trip_distance').alias('avg_distance'),
    F.avg('trip_duration_minutes').alias('avg_duration'),
    F.sum('passenger_count').alias('total_passengers'),
    F.avg('passenger_count').alias('avg_passengers_per_trip'),
    F.countDistinct('VendorID').alias('active_vendors')
)

# Write daily statistics
daily_path = f"s3://{args['target_bucket']}/daily_statistics/"
print(f"Writing daily statistics to: {daily_path}")
daily_stats.write.mode('overwrite').partitionBy('year', 'month').parquet(daily_path)

# === Aggregation 2: Hourly Patterns ===
print("Creating hourly patterns...")

hourly_stats = df.groupBy('pickup_hour').agg(
    F.count('*').alias('total_trips'),
    F.avg('fare_amount').alias('avg_fare'),
    F.avg('trip_distance').alias('avg_distance'),
    F.avg('trip_duration_minutes').alias('avg_duration'),
    F.avg('passenger_count').alias('avg_passengers')
).orderBy('pickup_hour')

# Write hourly patterns
hourly_path = f"s3://{args['target_bucket']}/hourly_patterns/"
print(f"Writing hourly patterns to: {hourly_path}")
hourly_stats.write.mode('overwrite').parquet(hourly_path)

# === Aggregation 3: Day of Week Patterns ===
print("Creating day of week patterns...")

dow_stats = df.groupBy('pickup_day_of_week').agg(
    F.count('*').alias('total_trips'),
    F.avg('fare_amount').alias('avg_fare'),
    F.avg('trip_distance').alias('avg_distance'),
    F.avg('trip_duration_minutes').alias('avg_duration')
).orderBy('pickup_day_of_week')

# Write day of week patterns
dow_path = f"s3://{args['target_bucket']}/day_of_week_patterns/"
print(f"Writing day of week patterns to: {dow_path}")
dow_stats.write.mode('overwrite').parquet(dow_path)

# === Aggregation 4: Payment Type Analysis ===
print("Creating payment type analysis...")

payment_stats = df.groupBy(
    'payment_type',
    F.to_date('tpep_pickup_datetime').alias('date')
).agg(
    F.count('*').alias('total_trips'),
    F.sum('fare_amount').alias('total_revenue'),
    F.avg('fare_amount').alias('avg_fare'),
    F.avg('tip_amount').alias('avg_tip')
)

# Write payment type analysis
payment_path = f"s3://{args['target_bucket']}/payment_analysis/"
print(f"Writing payment analysis to: {payment_path}")
payment_stats.write.mode('overwrite').parquet(payment_path)

# === Aggregation 5: Location Analysis (Top Pickup/Dropoff) ===
print("Creating location analysis...")

pickup_stats = df.groupBy('PULocationID').agg(
    F.count('*').alias('total_pickups'),
    F.avg('fare_amount').alias('avg_fare_from_location'),
    F.avg('trip_distance').alias('avg_distance_from_location')
).orderBy(F.desc('total_pickups')).limit(100)  # Top 100 locations

# Write location analysis
location_path = f"s3://{args['target_bucket']}/location_analysis/"
print(f"Writing location analysis to: {location_path}")
pickup_stats.write.mode('overwrite').parquet(location_path)

# === Create a denormalized fact table for Redshift ===
print("Creating denormalized fact table...")

fact_table = df.select(
    F.col('tpep_pickup_datetime').alias('pickup_datetime'),
    F.col('tpep_dropoff_datetime').alias('dropoff_datetime'),
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'total_amount',
    'payment_type',
    'trip_duration_minutes',
    'avg_speed_mph',
    'pickup_hour',
    'pickup_day_of_week',
    'quality_flag',
    'year',
    'month'
)

# Write fact table
fact_path = f"s3://{args['target_bucket']}/fact_taxi_trips/"
print(f"Writing fact table to: {fact_path}")
fact_table.write.mode('overwrite').partitionBy('year', 'month').parquet(fact_path)

print("\n=== Curated Datasets Summary ===")
print(f"Daily statistics records: {daily_stats.count()}")
print(f"Hourly patterns records: {hourly_stats.count()}")
print(f"Day of week patterns records: {dow_stats.count()}")
print(f"Payment analysis records: {payment_stats.count()}")
print(f"Location analysis records: {pickup_stats.count()}")
print(f"Fact table records: {fact_table.count()}")

# Commit job
job.commit()
print("Job completed successfully!")
