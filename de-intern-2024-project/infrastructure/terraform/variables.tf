variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "de-intern-2024"
}

# VPC Configuration
variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

# RDS Configuration
variable "rds_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.micro"
}

variable "rds_allocated_storage" {
  description = "Allocated storage for RDS in GB"
  type        = number
  default     = 20
}

variable "rds_engine_version" {
  description = "PostgreSQL engine version"
  type        = string
  default     = "15.4"
}

variable "rds_database_name" {
  description = "Name of the initial database"
  type        = string
  default     = "taxi_data"
}

variable "rds_username" {
  description = "Master username for RDS"
  type        = string
  default     = "admin"
  sensitive   = true
}

variable "rds_password" {
  description = "Master password for RDS"
  type        = string
  sensitive   = true
}

# Redshift Configuration
variable "redshift_node_type" {
  description = "Redshift node type"
  type        = string
  default     = "dc2.large"
}

variable "redshift_cluster_type" {
  description = "Redshift cluster type (single-node or multi-node)"
  type        = string
  default     = "single-node"
}

variable "redshift_number_of_nodes" {
  description = "Number of Redshift nodes"
  type        = number
  default     = 1
}

variable "redshift_database_name" {
  description = "Name of the Redshift database"
  type        = string
  default     = "taxi_warehouse"
}

variable "redshift_master_username" {
  description = "Master username for Redshift"
  type        = string
  default     = "admin"
  sensitive   = true
}

variable "redshift_master_password" {
  description = "Master password for Redshift"
  type        = string
  sensitive   = true
}

# S3 Configuration
variable "enable_s3_versioning" {
  description = "Enable versioning for S3 buckets"
  type        = bool
  default     = true
}

variable "s3_lifecycle_glacier_days" {
  description = "Days before moving to Glacier"
  type        = number
  default     = 90
}

variable "s3_lifecycle_expiration_days" {
  description = "Days before expiration"
  type        = number
  default     = 365
}

# Glue Configuration
variable "glue_job_worker_type" {
  description = "Glue job worker type (G.1X, G.2X, etc.)"
  type        = string
  default     = "G.1X"
}

variable "glue_job_number_of_workers" {
  description = "Number of Glue job workers"
  type        = number
  default     = 2
}

# Tags
variable "additional_tags" {
  description = "Additional tags for resources"
  type        = map(string)
  default     = {}
}
