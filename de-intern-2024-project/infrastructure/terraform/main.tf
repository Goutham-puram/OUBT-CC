terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Uncomment for remote state storage
  # backend "s3" {
  #   bucket = "de-intern-2024-terraform-state"
  #   key    = "state/terraform.tfstate"
  #   region = "us-east-1"
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "de-intern-2024"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# Data sources
data "aws_caller_identity" "current" {}
data "aws_availability_zones" "available" {
  state = "available"
}

# Local variables
locals {
  account_id = data.aws_caller_identity.current.account_id
  common_tags = {
    Project     = "de-intern-2024"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}
