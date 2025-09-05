# Build and test script for Rules Backend FastAPI (PowerShell)

$IMAGE_NAME = "rules-backend-fastapi"
$CONTAINER_NAME = "rules-backend-test"
$PORT = "8000"

Write-Host "🚀 Building and testing Rules Backend FastAPI Docker container..." -ForegroundColor Green

try {
    # Clean up any existing container
    Write-Host "🧹 Cleaning up existing containers..." -ForegroundColor Yellow
    docker stop $CONTAINER_NAME 2>$null
    docker rm $CONTAINER_NAME 2>$null

    # Build the Docker image
    Write-Host "🔨 Building Docker image..." -ForegroundColor Yellow
    docker build -t $IMAGE_NAME .
    if ($LASTEXITCODE -ne 0) {
        throw "Docker build failed"
    }

    # Run the container
    Write-Host "🏃 Starting container..." -ForegroundColor Yellow
    docker run -d `
        --name $CONTAINER_NAME `
        -p "${PORT}:8000" `
        -v "${PWD}/input_files:/app/input_files" `
        -v "${PWD}/output_db:/app/output_db" `
        -v "${PWD}/logs:/app/logs" `
        $IMAGE_NAME

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to start container"
    }

    # Wait for the container to start
    Write-Host "⏳ Waiting for container to start..." -ForegroundColor Yellow
    Start-Sleep -Seconds 10

    # Check if container is running
    $containerStatus = docker ps --filter "name=$CONTAINER_NAME" --format "table {{.Names}}"
    if ($containerStatus -match $CONTAINER_NAME) {
        Write-Host "✅ Container is running successfully!" -ForegroundColor Green
    } else {
        Write-Host "❌ Container failed to start. Checking logs..." -ForegroundColor Red
        docker logs $CONTAINER_NAME
        throw "Container not running"
    }

    # Test the health endpoint
    Write-Host "🔍 Testing health endpoint..." -ForegroundColor Yellow
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:$PORT/testing" -TimeoutSec 10 -UseBasicParsing
        if ($response.StatusCode -eq 200) {
            Write-Host "✅ Health check passed!" -ForegroundColor Green
        } else {
            throw "Health check returned status code: $($response.StatusCode)"
        }
    } catch {
        Write-Host "❌ Health check failed. Checking logs..." -ForegroundColor Red
        docker logs $CONTAINER_NAME
        throw "Health check failed: $($_.Exception.Message)"
    }

    # Show container info
    Write-Host "📊 Container information:" -ForegroundColor Cyan
    docker ps --filter "name=$CONTAINER_NAME"

    Write-Host ""
    Write-Host "🎉 Build and test completed successfully!" -ForegroundColor Green
    Write-Host "📖 API Documentation: http://localhost:$PORT/testing" -ForegroundColor Cyan
    Write-Host "🔌 WebSocket endpoint: ws://localhost:$PORT/ws" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "To view logs: docker logs $CONTAINER_NAME" -ForegroundColor White
    Write-Host "To stop container: docker stop $CONTAINER_NAME" -ForegroundColor White
    Write-Host "To remove container: docker rm $CONTAINER_NAME" -ForegroundColor White
    Write-Host "To run with docker-compose: docker-compose up -d" -ForegroundColor White

} catch {
    Write-Host "❌ Build and test failed: $($_.Exception.Message)" -ForegroundColor Red
    
    # Show container logs if container exists
    $containerExists = docker ps -a --filter "name=$CONTAINER_NAME" --format "table {{.Names}}"
    if ($containerExists -match $CONTAINER_NAME) {
        Write-Host "Container logs:" -ForegroundColor Yellow
        docker logs $CONTAINER_NAME
    }
    
    exit 1
}