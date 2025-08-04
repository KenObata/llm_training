# test_sagemaker_locally.py
import os
import subprocess
import sys

def setup_local_sagemaker_env():
    """Simulate SageMaker environment variables for local testing"""
    
    # Set SageMaker environment variables
    sagemaker_env = {
        # Paths
        'SM_MODEL_DIR': './models',
        'SM_CHANNEL_TRAIN': './data/train',
        'SM_OUTPUT_DATA_DIR': './output',
        
        # Cluster info (simulating single machine)
        'SM_HOSTS': '["algo-1"]',
        'SM_CURRENT_HOST': 'algo-1',
        'SM_NUM_GPUS': '1',
        'LOCAL_RANK': '0',
        
        # Instance info
        'SM_CURRENT_INSTANCE_TYPE': 'ml.p3.2xlarge',
        'SM_NUM_CPUS': '8',
        'SM_LOG_LEVEL': '20',
        
        # Training job info
        'SM_TRAINING_JOB_NAME': 'local-test-job',
        'SM_REGION': 'us-east-1'
    }
    
    return sagemaker_env

def run_local_test():
    """Run the SageMaker script locally for testing"""
    
    env_vars = setup_local_sagemaker_env()
    
    # Command to run your training script
    cmd = [
        sys.executable, 'src/train_sagemaker.py',
        '--epochs', '2',           # Short test
        '--batch_size', '8',       # Small batch
        '--learning_rate', '1e-4',
        '--model_dim', '256',      # Smaller model for testing
        '--num_heads', '4',
        '--num_layers', '2',
        '--num_workers', '2'
    ]
    
    print("🧪 Running SageMaker script locally...")
    print(f"Command: {' '.join(cmd)}")
    print(f"Environment variables: {list(env_vars.keys())}")
    
    # Run with SageMaker environment
    result = subprocess.run(
        cmd, 
        env={**os.environ, **env_vars},
        capture_output=False  # Show output in real-time
    )
    
    if result.returncode == 0:
        print("✅ Local test PASSED!")
        print("🚀 Ready to deploy to SageMaker")
        return True
    else:
        print("❌ Local test FAILED!")
        print("🔧 Fix the issues before deploying to SageMaker")
        return False

if __name__ == "__main__":
    success = run_local_test()