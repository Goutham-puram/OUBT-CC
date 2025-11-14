# S3 Buckets for Data Lake Architecture

# Raw Data Bucket
resource "aws_s3_bucket" "raw_data" {
  bucket = "${var.project_name}-raw-data-${local.account_id}"

  tags = merge(
    local.common_tags,
    {
      Name = "Raw Data Lake"
      Zone = "raw"
    }
  )
}

resource "aws_s3_bucket_versioning" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  versioning_configuration {
    status = var.enable_s3_versioning ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    id     = "archive-old-data"
    status = "Enabled"

    transition {
      days          = var.s3_lifecycle_glacier_days
      storage_class = "GLACIER"
    }

    expiration {
      days = var.s3_lifecycle_expiration_days
    }
  }
}

# Processed Data Bucket
resource "aws_s3_bucket" "processed_data" {
  bucket = "${var.project_name}-processed-data-${local.account_id}"

  tags = merge(
    local.common_tags,
    {
      Name = "Processed Data Lake"
      Zone = "processed"
    }
  )
}

resource "aws_s3_bucket_versioning" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id

  versioning_configuration {
    status = var.enable_s3_versioning ? "Enabled" : "Disabled"
  }
}

# Curated Data Bucket
resource "aws_s3_bucket" "curated_data" {
  bucket = "${var.project_name}-curated-data-${local.account_id}"

  tags = merge(
    local.common_tags,
    {
      Name = "Curated Data Lake"
      Zone = "curated"
    }
  )
}

resource "aws_s3_bucket_versioning" "curated_data" {
  bucket = aws_s3_bucket.curated_data.id

  versioning_configuration {
    status = var.enable_s3_versioning ? "Enabled" : "Disabled"
  }
}

# Scripts and Configuration Bucket
resource "aws_s3_bucket" "scripts" {
  bucket = "${var.project_name}-scripts-${local.account_id}"

  tags = merge(
    local.common_tags,
    {
      Name = "Scripts and Configs"
    }
  )
}

resource "aws_s3_bucket_versioning" "scripts" {
  bucket = aws_s3_bucket.scripts.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Block public access for all buckets
resource "aws_s3_bucket_public_access_block" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "curated_data" {
  bucket = aws_s3_bucket.curated_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "scripts" {
  bucket = aws_s3_bucket.scripts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "curated_data" {
  bucket = aws_s3_bucket.curated_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
