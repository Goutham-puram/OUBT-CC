# AWS Glue Resources

# Glue Catalog Database
resource "aws_glue_catalog_database" "main" {
  name        = "${var.project_name}_catalog"
  description = "Glue catalog database for NYC taxi data"

  tags = local.common_tags
}

# Glue Crawler for Raw Data
resource "aws_glue_crawler" "raw_taxi_data" {
  name          = "${var.project_name}-raw-taxi-crawler"
  role          = aws_iam_role.glue_service_role.arn
  database_name = aws_glue_catalog_database.main.name

  s3_target {
    path = "s3://${aws_s3_bucket.raw_data.id}/taxi/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  tags = local.common_tags
}

# Glue Job: Raw to Processed
resource "aws_glue_job" "raw_to_processed" {
  name     = "${var.project_name}-raw-to-processed"
  role_arn = aws_iam_role.glue_service_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.id}/glue/raw_to_processed.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.scripts.id}/temp/"
    "--enable-glue-datacatalog"          = "true"
    "--source_bucket"                    = aws_s3_bucket.raw_data.id
    "--target_bucket"                    = aws_s3_bucket.processed_data.id
  }

  glue_version      = "4.0"
  worker_type       = var.glue_job_worker_type
  number_of_workers = var.glue_job_number_of_workers

  timeout = 60

  tags = local.common_tags
}

# Glue Job: Processed to Curated
resource "aws_glue_job" "processed_to_curated" {
  name     = "${var.project_name}-processed-to-curated"
  role_arn = aws_iam_role.glue_service_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.id}/glue/processed_to_curated.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.scripts.id}/temp/"
    "--enable-glue-datacatalog"          = "true"
    "--source_bucket"                    = aws_s3_bucket.processed_data.id
    "--target_bucket"                    = aws_s3_bucket.curated_data.id
  }

  glue_version      = "4.0"
  worker_type       = var.glue_job_worker_type
  number_of_workers = var.glue_job_number_of_workers

  timeout = 60

  tags = local.common_tags
}

# Glue Connection to RDS
resource "aws_glue_connection" "postgres" {
  name = "${var.project_name}-postgres-connection"

  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:postgresql://${aws_db_instance.postgres.endpoint}/${var.rds_database_name}"
    USERNAME            = var.rds_username
    PASSWORD            = var.rds_password
  }

  physical_connection_requirements {
    availability_zone      = aws_subnet.private[0].availability_zone
    security_group_id_list = [aws_security_group.rds.id]
    subnet_id              = aws_subnet.private[0].id
  }
}

# Glue Data Quality Ruleset
resource "aws_glue_data_quality_ruleset" "taxi_data_quality" {
  name        = "${var.project_name}-taxi-data-quality"
  description = "Data quality rules for taxi dataset"

  ruleset = <<-RULESET
    Rules = [
      RowCount > 0,
      IsComplete "pickup_datetime",
      IsComplete "dropoff_datetime",
      ColumnValues "passenger_count" >= 0,
      ColumnValues "trip_distance" >= 0,
      ColumnValues "fare_amount" >= 0
    ]
  RULESET

  target_table {
    database_name = aws_glue_catalog_database.main.name
    table_name    = "raw_taxi_trips"
  }

  tags = local.common_tags
}
