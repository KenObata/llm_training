import json
import os

def load_terraform_config():
    """Load configuration created by Terraform"""
    config_path = 'sagemaker_config.json'
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Configuration file {config_path} not found. "
            f"Make sure you've run 'terraform apply' first."
        )
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    return config

def get_sagemaker_role():
    """Get SageMaker execution role ARN"""
    config = load_terraform_config()
    return config['sagemaker_role_arn']

def get_s3_bucket():
    """Get S3 bucket name"""
    config = load_terraform_config()
    return config['s3_bucket_name']

def get_aws_region():
    """Get AWS region"""
    config = load_terraform_config()
    return config['aws_region']

if __name__ == "__main__":
    try:
        config = load_terraform_config()
        print("✅ Terraform configuration loaded successfully!")
        print(f"   Role ARN: {config['sagemaker_role_arn']}")
        print(f"   S3 Bucket: {config['s3_bucket_name']}")
        print(f"   Region: {config['aws_region']}")
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
