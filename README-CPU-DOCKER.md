# CPU-Only Docker Deployment for Linux

This guide explains how to deploy the Rules Backend FastAPI application on Linux using CPU-only dependencies, avoiding GPU requirements.

## Files Created for CPU-Only Deployment

### 1. `requirements-cpu.txt`
CPU-only version of requirements with the following key changes:
- `opencv-python` → `opencv-python-headless` (no GUI dependencies)
- Added CPU-only PyTorch: `torch==2.1.0+cpu` and `torchvision==0.16.0+cpu`
- Added PyTorch CPU index URL for installation
- Includes `ultralytics` for YOLO but configured to use CPU

### 2. `Dockerfile.cpu`
Optimized Dockerfile for CPU-only deployment:
- Uses `requirements-cpu.txt` instead of `requirements.txt`
- Sets environment variables to force CPU usage:
  - `CUDA_VISIBLE_DEVICES=""` - Hides GPU devices
  - `TORCH_DEVICE=cpu` - Forces PyTorch to use CPU
  - `YOLO_DEVICE=cpu` - Forces YOLO to use CPU
- Removes GPU-related system dependencies

### 3. Code Modifications
Modified `DEV/project/prepare_data_model/ladder_data_modelling.py`:
- Added environment variables to force CPU usage
- Updated model loading function to explicitly use CPU device

## Usage Instructions

### Building the CPU-Only Docker Image
```bash
# Build using the CPU-only Dockerfile
docker build -f Dockerfile.cpu -t rules-backend-cpu .
```

### Running the Container
```bash
# Run the container
docker run -p 8000:8000 rules-backend-cpu

# Or with volume mounts for data persistence
docker run -p 8000:8000 \
  -v $(pwd)/input_files:/app/input_files \
  -v $(pwd)/output_db:/app/output_db \
  rules-backend-cpu
```

### Using Docker Compose (CPU-only)
Create a `docker-compose.cpu.yml`:
```yaml
version: '3.8'
services:
  rules-backend:
    build:
      context: .
      dockerfile: Dockerfile.cpu
    ports:
      - "8000:8000"
    volumes:
      - ./input_files:/app/input_files
      - ./output_db:/app/output_db
    environment:
      - CUDA_VISIBLE_DEVICES=""
      - TORCH_DEVICE=cpu
      - YOLO_DEVICE=cpu
```

Then run:
```bash
docker-compose -f docker-compose.cpu.yml up
```

## Key Benefits

1. **No GPU Dependencies**: Eliminates CUDA and GPU driver requirements
2. **Smaller Image Size**: CPU-only PyTorch is significantly smaller
3. **Better Compatibility**: Works on any Linux system without GPU setup
4. **Cost Effective**: Can run on cheaper CPU-only cloud instances
5. **Consistent Performance**: Predictable performance across different environments

## Performance Considerations

- YOLO model inference will be slower on CPU compared to GPU
- For production workloads with high throughput, consider:
  - Using smaller YOLO models (YOLOv8n instead of YOLOv8x)
  - Implementing batch processing
  - Adding caching mechanisms
  - Using multiple worker processes

## Troubleshooting

### If you encounter PyTorch installation issues:
```bash
# Clear pip cache and reinstall
pip cache purge
pip install --no-cache-dir torch==2.1.0+cpu torchvision==0.16.0+cpu --extra-index-url https://download.pytorch.org/whl/cpu
```

### If YOLO still tries to use GPU:
Ensure these environment variables are set:
```bash
export CUDA_VISIBLE_DEVICES=""
export TORCH_DEVICE=cpu
```

### Memory optimization for CPU:
```python
# In your YOLO model loading code
model = YOLO('model.pt')
model.to('cpu')
# Reduce batch size for CPU inference
results = model(image, batch_size=1)
```

## Migration from GPU to CPU

To migrate existing GPU-based deployment:
1. Stop current containers
2. Rebuild using `Dockerfile.cpu`
3. Update any model loading code to specify CPU device
4. Test inference performance and adjust batch sizes if needed

This setup ensures your FastAPI application runs reliably on Linux without any GPU dependencies while maintaining full functionality.