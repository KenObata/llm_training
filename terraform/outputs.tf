output "sagemaker_role_arn" {
  description = "ARN of the SageMaker execution role"
  value       = aws_iam_role.sagemaker_execution_role.arn
}

output "s3_bucket_name" {
  description = "Name of the S3 bucket for training data"
  value       = aws_s3_bucket.sagemaker_training.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.sagemaker_training.arn
}

output "account_id" {
  description = "AWS Account ID"
  value       = data.aws_caller_identity.current.account_id
}

output "region" {
  description = "AWS Region"
  value       = data.aws_region.current.name
}

# Export configuration for Python scripts
resource "local_file" "sagemaker_config" {
  content = jsonencode({
    sagemaker_role_arn = aws_iam_role.sagemaker_execution_role.arn
    s3_bucket_name     = aws_s3_bucket.sagemaker_training.bucket
    aws_region         = data.aws_region.current.name
    aws_account_id     = data.aws_caller_identity.current.account_id
  })
  filename = "${path.module}/../sagemaker_config.json"
}
