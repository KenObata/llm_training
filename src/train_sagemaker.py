import argparse
import os
import torch
import torch.nn as nn
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data.distributed import DistributedSampler
import json

class SimpleTransformer(nn.Module):
    def __init__(self, vocab_size, dimension_model, nhead, num_layers):
        super().__init__()
        # TODO: Implement a simple transformer model
        self.embedding = nn.Embedding(vocab_size, dimension_model)  # vocab_size, d_model
        self.transformer = nn.Transformer(
            d_model=dimension_model,      # model dimension
            nhead=nhead,        # number of attention heads
            num_encoder_layers=num_layers,  # number of layers
            batch_first=True
        )
        self.fc_out = nn.Linear(dimension_model, vocab_size)  # d_model, vocab_size

    def forward(self, x):
        # TODO: Implement forward pass
        # 1. Apply embedding
        embedded = self.embedding(x)
        # 2. Pass through transformer (using same input as src and tgt for simplicity)
        transformer_out = self.transformer(embedded, embedded)
        # 3. Apply output linear layer

        output = self.fc_out(transformer_out)
        return output
        
def parse_args():
    parser = argparse.ArgumentParser()
    
    # TODO: Add hyperparameters (these come from your launch script)
    parser.add_argument('--epochs', type=int, default=10)
    parser.add_argument('--batch_size', type=int, default=32)
    parser.add_argument('--learning_rate', type=float, default=1e-4)
    parser.add_argument('--model_dim', type=int, default=512)
    parser.add_argument('--num_heads', type=int, default=8)
    parser.add_argument('--num_layers', type=int, default=6)
    
    # TODO: Add SageMaker environment arguments (SageMaker sets these automatically)
    parser.add_argument('--model_dir', type=str, default=os.environ.get('SM_MODEL_DIR'))
    parser.add_argument('--train_dir', type=str, default=os.environ.get('SM_CHANNEL_TRAIN'))
    parser.add_argument('--hosts', type=list, default=json.loads(os.environ.get('SM_HOSTS')))
    parser.add_argument('--current_host', type=str, default=os.environ.get('SM_CURRENT_HOST'))
    parser.add_argument('--num_gpus', type=int, default=os.environ.get('SM_NUM_GPUS'))
    
    return parser.parse_args()

def setup_distributed(args):
    """Set up distributed training for SageMaker
    This is like your `setup()` function,
     but calculates rank across multiple machines.
     ex) With 2 machines with 4 GPUs each:
	 - Machine 0: ranks 0,1,2,3 (local_ranks 0,1,2,3)
	 - Machine 1: ranks 4,5,6,7 (local_ranks 0,1,2,3)
    """
    # TODO: Get world size and rank from SageMaker environment
    world_size = len(args.hosts) * args.num_gpus
    host_rank = args.hosts.index(args.current_host)
    local_rank = int(os.environ.get('LOCAL_RANK', 0)) # this is new as multi machine
    rank = host_rank * args.num_gpus + local_rank
    
    # TODO: Set up process group
    dist.init_process_group(
        backend='nccl',           # 'nccl'
        world_size=world_size,          # world_size
        rank=rank                # rank
    )
    
    return rank, local_rank, world_size

class CustomDataset(torch.utils.data.Dataset):
    """Custom dataset for demonstration"""
    def __init__(self, data_dir, vocab_size=10000, seq_len=128, size=10000):
        # TODO: Implement dataset loading
        # For now, generate synthetic data
        self.data = torch.randint(0, vocab_size, (size, seq_len))
        self.targets = torch.randint(0, vocab_size, (size, seq_len))
    
    def __len__(self):
        return len(self.data)
    
    def __getitem__(self, idx):
        return self.data[idx], self.targets[idx]

def create_data_loader(args, rank, world_size):
    """Create distributed data loader"""
    # TODO: Create dataset
    dataset = CustomDataset(args.train_dir)
    
    # TODO: Create distributed sampler
    sampler = DistributedSampler(
        dataset,
        num_replicas=world_size,    # world_size
        rank=rank,            # rank
        shuffle=True
    )
    
    # TODO: Create data loader
    dataloader = torch.utils.data.DataLoader(
        dataset,
        batch_size=args.batch_size,      # args.batch_size
        sampler=sampler,
        num_workers=4,
        pin_memory=True
    )
    
    return dataloader, sampler

def save_model(model, args, epoch):
    """Save model checkpoint"""
    # TODO: Save model state dict
    if dist.get_rank() == 0:  # Only save on rank 0
        checkpoint = {
            'epoch': epoch,
            'model_state_dict': model.module.state_dict(),
            'hyperparameters': {
                'model_dim': args.model_dim,
                'num_heads': args.num_heads,
                'num_layers': args.num_layers
            }
        }
        torch.save(checkpoint, os.path.join(args.model_dir, f'model_epoch_{epoch}.pth'))

def main():
    args = parse_args()
    
    # TODO: Set up distributed training
    rank, local_rank, world_size = setup_distributed(args)
    
    # TODO: Set device
    device = torch.device(f'cuda:{local_rank}')
    torch.cuda.set_device(device)
    
    # TODO: Create model (reuse SimpleTransformer from before)
    model = SimpleTransformer(
        vocab_size=10000,
        dimension_model=args.model_dim,
        nhead=args.num_heads,
        num_layers=args.num_layers
    )
    
    # TODO: Move model to device and wrap with DDP
    model = model.to(device)
    model = DDP(model, device_ids=[local_rank])
    
    # TODO: Set up optimizer and loss
    optimizer = torch.optim.Adam(model.parameters(), lr=args.learning_rate)
    criterion = nn.CrossEntropyLoss()
    
    # TODO: Create data loader
    dataloader, sampler = create_data_loader(args, rank, world_size)
    
    # TODO: Training loop
    for epoch in range(args.epochs):
        sampler.set_epoch(epoch)
        
        total_loss = 0
        num_batches = 0
        
        for batch_idx, (data, target) in enumerate(dataloader):
            # TODO: Move data to device
            data, target = data.to(device), target.to(device)
            
            # TODO: Forward pass
            optimizer.zero_grad()
            output = model(data)
            
            # TODO: Calculate loss
            loss = criterion(output.view(-1, output.size(-1)), target.view(-1))
            
            # TODO: Backward pass
            loss.backward()
            
            # TODO: Gradient clipping (optional)
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            
            optimizer.step()
            
            total_loss += loss.item()
            num_batches += 1
            
            if batch_idx % 10 == 0 and rank == 0:
                print(f'Epoch: {epoch}, Batch: {batch_idx}, Loss: {loss.item():.4f}')
        
        # TODO: Save model checkpoint
        if epoch % 5 == 0:
            save_model(model, args, epoch)
        
        if rank == 0:
            avg_loss = total_loss / num_batches
            print(f'Epoch {epoch} completed. Average Loss: {avg_loss:.4f}')
    
    # TODO: Final model save
    save_model(model, args, args.epochs)

if __name__ == '__main__':
    main()