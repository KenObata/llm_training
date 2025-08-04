# launch_sagemaker_training.py - Import and launch
from sagemaker_setup import setup_sagemaker_training
from load_config import load_terraform_config

def launch_training():
    """Launch SageMaker training job using existing setup"""
    
    print("🚀 Launching SageMaker Training Job")
    
    # Use existing setup function
    estimator, sagemaker_session = setup_sagemaker_training()
    
    # Load config for S3 paths
    config = load_terraform_config()
    
    print("\n📋 Training Job Configuration:")
    print(f"   Instance: {estimator.instance_type}")
    print(f"   Count: {estimator.instance_count}")
    print(f"   Spot instances: {estimator.use_spot_instances}")
    print(f"   Max runtime: {estimator.max_run // 3600} hours")
    
    # Start training (this is the only new code!)
    print("\n🎬 Starting training job...")

    # the model creates synthetic data during runtime, so we skip inputs data.
    estimator.fit(
        # inputs={
        #    'train': f's3://{config["s3_bucket_name"]}/data/train'
        #},
        wait=True,    # Wait for completion
        logs=True     # Show logs in real-time
    )
    
    print(f"\n🎉 Training completed!")
    print(f"   Model artifacts: {estimator.model_data}")
    
    return estimator

if __name__ == '__main__':
    try:
        estimator = launch_training()
    except Exception as e:
        print(f"❌ Training failed: {e}")
        import traceback
        traceback.print_exc()