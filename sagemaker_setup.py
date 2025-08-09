import sagemaker
from sagemaker.pytorch import PyTorch
from load_config import load_terraform_config
from src import const
from src.const import HYPERPARAMETERS

def setup_sagemaker_training():
    """Set up SageMaker training using Terraform-created resources"""
    
    # Load Terraform configuration
    config = load_terraform_config()
    
    # Create SageMaker session
    sagemaker_session = sagemaker.Session()
    
    print(f"Using Terraform-created resources:")
    print(f"  Role ARN: {config['sagemaker_role_arn']}")
    print(f"  S3 Bucket: {config['s3_bucket_name']}")
    print(f"  Region: {config['aws_region']}")
    
    # Define training job parameters
    hyperparameters = HYPERPARAMETERS
    
    # Create PyTorch estimator
    estimator = PyTorch(
        entry_point='train_sagemaker.py',
        source_dir='src',
        role=config['sagemaker_role_arn'],  # From Terraform
        instance_type=const.INSTANCE_TYPE,
        instance_count=const.INSTANCE_COUNT,
        framework_version=const.FRAMEWORK_VERSION,
        py_version=const.PY_VERSION,
        hyperparameters=hyperparameters,
        
        # Distributed training configuration
        distribution={
            'torch_distributed': {
                'enabled': True
            }
        },
        
        # Cost optimization
        use_spot_instances=const.USE_SPOT_INSTANCES,
        max_wait=4*60*60,
        max_run=2*60*60,
        
        # Output configuration
        output_path=f's3://{config["s3_bucket_name"]}/models',
        
        # Enable metrics
        enable_sagemaker_metrics=True
    )
    
    return estimator, sagemaker_session

if __name__ == "__main__":
    try:
        estimator, session = setup_sagemaker_training()
        print("✅ SageMaker estimator created successfully!")
        print(f"   Instance type: {estimator.instance_type}")
        print(f"   Framework: PyTorch {estimator.framework_version}")
        print(f"   Spot instances: {estimator.use_spot_instances}")
    except Exception as e:
        print(f"❌ Failed to create estimator: {e}")