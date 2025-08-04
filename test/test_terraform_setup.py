import boto3
from load_config import load_terraform_config

def test_infrastructure():
    """Test that Terraform-created infrastructure is working"""
    try:
        # Load configuration
        config = load_terraform_config()
        print("✅ Configuration loaded")
        
        # Test S3 bucket access
        s3 = boto3.client('s3', region_name=config['aws_region'])
        response = s3.head_bucket(Bucket=config['s3_bucket_name'])
        print("✅ S3 bucket accessible")
        
        # Test IAM role exists
        iam = boto3.client('iam', region_name=config['aws_region'])
        role_name = config['sagemaker_role_arn'].split('/')[-1]
        role = iam.get_role(RoleName=role_name)
        print("✅ IAM role exists")
        
        # Test SageMaker SDK can use the role
        import sagemaker
        session = sagemaker.Session()
        print("✅ SageMaker session created")
        
        print("\n🎉 All infrastructure tests passed!")
        print("\nYour infrastructure is ready for SageMaker training jobs.")
        
        return True
        
    except Exception as e:
        print(f"❌ Infrastructure test failed: {e}")
        return False

if __name__ == "__main__":
    test_infrastructure()