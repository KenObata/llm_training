import sagemaker
from sagemaker.pytorch import PyTorch
from load_config import load_terraform_config

def launch_training():
    # Load Terraform configuration
    config = load_terraform_config()
    
    # Create estimator
    estimator = PyTorch(
        entry_point='train_sagemaker.py',      # Your script name
        source_dir='src',                      # Directory to upload
        role=config['sagemaker_role_arn'],
        instance_type='ml.p3.2xlarge',
        instance_count=1,
        framework_version='1.12.0',
        py_version='py38',
        hyperparameters={
            'epochs': 5,
            'batch_size': 16,
            'learning_rate': 1e-4,
            'model_dim': 512,
            'num_heads': 8,
            'num_layers': 4
        },
        distribution={'torch_distributed': {'enabled': True}},
        use_spot_instances=True,
        max_run=2*60*60,
        output_path=f's3://{config["s3_bucket_name"]}/models',
    )
    
    print("🚀 Starting SageMaker training job...")
    
    # This uploads your code and starts training
    estimator.fit(wait=True, logs=True)
    
    print("✅ Training completed!")
    return estimator

if __name__ == '__main__':
    launch_training()