# Amazon Redshift Data Warehouse

# Security Group for Redshift
resource "aws_security_group" "redshift" {
  name        = "${var.project_name}-redshift-sg"
  description = "Security group for Redshift cluster"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Redshift from VPC"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-redshift-sg"
    }
  )
}

# Redshift Subnet Group
resource "aws_redshift_subnet_group" "main" {
  name       = "${var.project_name}-redshift-subnet-group"
  subnet_ids = aws_subnet.private[*].id

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-redshift-subnet-group"
    }
  )
}

# Redshift Cluster
resource "aws_redshift_cluster" "main" {
  cluster_identifier = "${var.project_name}-cluster"
  database_name      = var.redshift_database_name
  master_username    = var.redshift_master_username
  master_password    = var.redshift_master_password

  node_type       = var.redshift_node_type
  cluster_type    = var.redshift_cluster_type
  number_of_nodes = var.redshift_cluster_type == "multi-node" ? var.redshift_number_of_nodes : null

  # Network configuration
  vpc_security_group_ids    = [aws_security_group.redshift.id]
  cluster_subnet_group_name = aws_redshift_subnet_group.main.name
  publicly_accessible       = false

  # IAM Role for Redshift to access S3
  iam_roles = [aws_iam_role.redshift_s3_access.arn]

  # Encryption
  encrypted   = true
  kms_key_id  = aws_kms_key.redshift.arn

  # Enhanced VPC Routing
  enhanced_vpc_routing = true

  # Snapshot configuration
  automated_snapshot_retention_period = 7
  skip_final_snapshot                = true
  final_snapshot_identifier          = null

  # Maintenance window
  preferred_maintenance_window = "sun:05:00-sun:06:00"

  # Logging
  logging {
    enable        = true
    bucket_name   = aws_s3_bucket.scripts.id
    s3_key_prefix = "redshift-logs/"
  }

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-redshift-cluster"
    }
  )

  depends_on = [
    aws_iam_role_policy_attachment.redshift_s3_access
  ]
}

# KMS Key for Redshift Encryption
resource "aws_kms_key" "redshift" {
  description             = "KMS key for Redshift encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = merge(
    local.common_tags,
    {
      Name = "${var.project_name}-redshift-kms-key"
    }
  )
}

resource "aws_kms_alias" "redshift" {
  name          = "alias/${var.project_name}-redshift"
  target_key_id = aws_kms_key.redshift.key_id
}

# IAM Role for Redshift to access S3
resource "aws_iam_role" "redshift_s3_access" {
  name = "${var.project_name}-redshift-s3-access-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "redshift_s3_access" {
  name = "${var.project_name}-redshift-s3-access-policy"
  role = aws_iam_role.redshift_s3_access.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.raw_data.arn,
          "${aws_s3_bucket.raw_data.arn}/*",
          aws_s3_bucket.processed_data.arn,
          "${aws_s3_bucket.processed_data.arn}/*",
          aws_s3_bucket.curated_data.arn,
          "${aws_s3_bucket.curated_data.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "redshift_s3_access" {
  role       = aws_iam_role.redshift_s3_access.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}
