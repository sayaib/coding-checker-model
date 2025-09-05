#!/bin/bash

# Build and test script for Rules Backend FastAPI

set -e

IMAGE_NAME="rules-backend-fastapi"
CONTAINER_NAME="rules-backend-test"
PORT="8000"

echo "🚀 Building and testing Rules Backend FastAPI Docker container..."

# Clean up any existing container
echo "🧹 Cleaning up existing containers..."
docker stop $CONTAINER_NAME 2>/dev/null || true
docker rm $CONTAINER_NAME 2>/dev/null || true

# Build the Docker image
echo "🔨 Building Docker image..."
docker build -t $IMAGE_NAME .

# Run the container
echo "🏃 Starting container..."
docker run -d \
  --name $CONTAINER_NAME \
  -p $PORT:8000 \
  -v $(pwd)/input_files:/app/input_files \
  -v $(pwd)/output_db:/app/output_db \
  -v $(pwd)/logs:/app/logs \
  $IMAGE_NAME

# Wait for the container to start
echo "⏳ Waiting for container to start..."
sleep 10

# Check if container is running
if docker ps | grep -q $CONTAINER_NAME; then
    echo "✅ Container is running successfully!"
else
    echo "❌ Container failed to start. Checking logs..."
    docker logs $CONTAINER_NAME
    exit 1
fi

# Test the health endpoint
echo "🔍 Testing health endpoint..."
if curl -f http://localhost:$PORT/testing > /dev/null 2>&1; then
    echo "✅ Health check passed!"
else
    echo "❌ Health check failed. Checking logs..."
    docker logs $CONTAINER_NAME
    exit 1
fi

# Show container info
echo "📊 Container information:"
docker ps | grep $CONTAINER_NAME

echo ""
echo "🎉 Build and test completed successfully!"
echo "📖 API Documentation: http://localhost:$PORT/testing"
echo "🔌 WebSocket endpoint: ws://localhost:$PORT/ws"
echo ""
echo "To view logs: docker logs $CONTAINER_NAME"
echo "To stop container: docker stop $CONTAINER_NAME"
echo "To remove container: docker rm $CONTAINER_NAME"
echo "To run with docker-compose: docker-compose up -d"