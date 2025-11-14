output "account_id" {
  description = "AWS Account ID"
  value       = local.account_id
}

output "region" {
  description = "AWS Region"
  value       = var.aws_region
}

# S3 Outputs
output "s3_raw_bucket" {
  description = "S3 bucket for raw data"
  value       = aws_s3_bucket.raw_data.id
}

output "s3_processed_bucket" {
  description = "S3 bucket for processed data"
  value       = aws_s3_bucket.processed_data.id
}

output "s3_curated_bucket" {
  description = "S3 bucket for curated data"
  value       = aws_s3_bucket.curated_data.id
}

output "s3_scripts_bucket" {
  description = "S3 bucket for scripts and configs"
  value       = aws_s3_bucket.scripts.id
}

# RDS Outputs
output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint"
  value       = aws_db_instance.postgres.endpoint
}

output "rds_database_name" {
  description = "RDS database name"
  value       = aws_db_instance.postgres.db_name
}

# Redshift Outputs
output "redshift_cluster_endpoint" {
  description = "Redshift cluster endpoint"
  value       = aws_redshift_cluster.main.endpoint
}

output "redshift_cluster_id" {
  description = "Redshift cluster identifier"
  value       = aws_redshift_cluster.main.id
}

# Glue Outputs
output "glue_catalog_database" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.main.name
}

output "glue_job_names" {
  description = "List of Glue job names"
  value = {
    raw_to_processed = aws_glue_job.raw_to_processed.name
    processed_to_curated = aws_glue_job.processed_to_curated.name
  }
}

# IAM Outputs
output "glue_role_arn" {
  description = "Glue service role ARN"
  value       = aws_iam_role.glue_service_role.arn
}

output "lambda_role_arn" {
  description = "Lambda execution role ARN"
  value       = aws_iam_role.lambda_execution_role.arn
}

# Step Functions Outputs
output "step_function_arn" {
  description = "Step Functions state machine ARN"
  value       = aws_sfn_state_machine.etl_pipeline.arn
}
