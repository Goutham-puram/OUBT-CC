# Example Terraform Variables
# Copy this file to terraform.tfvars and update with your values
# WARNING: Never commit terraform.tfvars to version control

aws_region   = "us-east-1"
environment  = "dev"
project_name = "de-intern-2024"

# VPC Configuration
vpc_cidr = "10.0.0.0/16"

# RDS Configuration
rds_instance_class    = "db.t3.micro"
rds_allocated_storage = 20
rds_engine_version    = "15.4"
rds_database_name     = "taxi_data"
rds_username          = "admin"
rds_password          = "ChangeMe123!" # Change this!

# Redshift Configuration
redshift_node_type          = "dc2.large"
redshift_cluster_type       = "single-node"
redshift_number_of_nodes    = 1
redshift_database_name      = "taxi_warehouse"
redshift_master_username    = "admin"
redshift_master_password    = "ChangeMe123!" # Change this!

# S3 Configuration
enable_s3_versioning         = true
s3_lifecycle_glacier_days    = 90
s3_lifecycle_expiration_days = 365

# Glue Configuration
glue_job_worker_type       = "G.1X"
glue_job_number_of_workers = 2

# Additional Tags
additional_tags = {
  Owner = "Data Engineering Team"
  Week  = "All"
}
