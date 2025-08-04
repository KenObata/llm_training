
provider "aws" {
  region = var.aws_region
}

# Generate random suffix for unique naming
resource "random_id" "suffix" {
  byte_length = 4
}

# Data source to get current AWS account ID
data "aws_caller_identity" "current" {}

# Data source to get current AWS region
data "aws_region" "current" {}

# S3 bucket for SageMaker training
resource "aws_s3_bucket" "sagemaker_training" {
  bucket = "sagemaker-training-${data.aws_caller_identity.current.account_id}-${random_id.suffix.hex}"

  tags = {
    Name        = "SageMaker Training Bucket"
    Environment = var.environment
    Project     = "ML Infrastructure Learning"
    CreatedBy   = "Terraform"
  }
}

# S3 bucket versioning
resource "aws_s3_bucket_versioning" "sagemaker_training" {
  bucket = aws_s3_bucket.sagemaker_training.id
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 bucket encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "sagemaker_training" {
  bucket = aws_s3_bucket.sagemaker_training.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Block public access
resource "aws_s3_bucket_public_access_block" "sagemaker_training" {
  bucket = aws_s3_bucket.sagemaker_training.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# IAM trust policy for SageMaker
data "aws_iam_policy_document" "sagemaker_trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["sagemaker.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

# SageMaker execution role
resource "aws_iam_role" "sagemaker_execution_role" {
  name               = "SageMakerExecutionRole-${random_id.suffix.hex}"
  assume_role_policy = data.aws_iam_policy_document.sagemaker_trust_policy.json

  tags = {
    Name        = "SageMaker Execution Role"
    Environment = var.environment
    Project     = "ML Infrastructure Learning"
    CreatedBy   = "Terraform"
  }
}

# Attach AWS managed SageMaker policy
resource "aws_iam_role_policy_attachment" "sagemaker_execution_role_policy" {
  role       = aws_iam_role.sagemaker_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"
}

# Custom policy for S3 access to our specific bucket
data "aws_iam_policy_document" "sagemaker_s3_policy" {
  statement {
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket"
    ]

    resources = [
      aws_s3_bucket.sagemaker_training.arn,
      "${aws_s3_bucket.sagemaker_training.arn}/*"
    ]
  }
}

# Create and attach custom S3 policy
resource "aws_iam_policy" "sagemaker_s3_policy" {
  name        = "SageMakerS3Access-${random_id.suffix.hex}"
  description = "Policy for SageMaker to access S3 training bucket"
  policy      = data.aws_iam_policy_document.sagemaker_s3_policy.json
}

resource "aws_iam_role_policy_attachment" "sagemaker_s3_policy_attachment" {
  role       = aws_iam_role.sagemaker_execution_role.name
  policy_arn = aws_iam_policy.sagemaker_s3_policy.arn
}

# CloudWatch Logs policy for SageMaker training jobs
data "aws_iam_policy_document" "sagemaker_logs_policy" {
  statement {
    effect = "Allow"

    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams"
    ]

    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
  }
}

resource "aws_iam_policy" "sagemaker_logs_policy" {
  name        = "SageMakerLogsAccess-${random_id.suffix.hex}"
  description = "Policy for SageMaker to write CloudWatch logs"
  policy      = data.aws_iam_policy_document.sagemaker_logs_policy.json
}

resource "aws_iam_role_policy_attachment" "sagemaker_logs_policy_attachment" {
  role       = aws_iam_role.sagemaker_execution_role.name
  policy_arn = aws_iam_policy.sagemaker_logs_policy.arn
}