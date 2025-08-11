# PyTorch
FRAMEWORK_VERSION = "2.0.1"
INSTANCE_COUNT = 2
PY_VERSION = "py310"
USE_SPOT_INSTANCES = True
INSTANCE_TYPE = "ml.g4dn.xlarge"

# For sagemaker setup which is called from the launch script
HYPERPARAMETERS = {
        'epochs': 5,
        'batch_size': 16,
        'learning_rate': 1e-4,
        'model_dim': 256,
        'num_heads': 8,
        'num_layers': 4,
        'accumulation_steps': 4,
    }
MODEL_TRAIN_FILE_NAME = "train_sagemaker_with_gradient_accumulation.py"
