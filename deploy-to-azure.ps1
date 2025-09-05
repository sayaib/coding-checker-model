# Azure deployment script for Rules Backend FastAPI (PowerShell)
# Make sure you're logged in to Azure CLI: az login

# Configuration variables
$RESOURCE_GROUP = "rules-backend-rg"
$LOCATION = "eastus"
$ACR_NAME = "rulesbackendacr"
$CONTAINER_NAME = "rules-backend-fastapi"
$IMAGE_TAG = "latest"
$STORAGE_ACCOUNT = "rulesbackendstorage"
$CONTAINER_GROUP = "rules-backend-aci"

Write-Host "Starting Azure deployment for Rules Backend FastAPI..." -ForegroundColor Green

try {
    # Create resource group
    Write-Host "Creating resource group..." -ForegroundColor Yellow
    az group create --name $RESOURCE_GROUP --location $LOCATION

    # Create Azure Container Registry
    Write-Host "Creating Azure Container Registry..." -ForegroundColor Yellow
    az acr create --resource-group $RESOURCE_GROUP --name $ACR_NAME --sku Basic --admin-enabled true

    # Get ACR login server
    $ACR_LOGIN_SERVER = az acr show --name $ACR_NAME --resource-group $RESOURCE_GROUP --query "loginServer" --output tsv

    # Build and push Docker image
    Write-Host "Building and pushing Docker image..." -ForegroundColor Yellow
    az acr build --registry $ACR_NAME --image "${CONTAINER_NAME}:${IMAGE_TAG}" .

    # Create storage account for persistent volumes
    Write-Host "Creating storage account..." -ForegroundColor Yellow
    az storage account create `
        --resource-group $RESOURCE_GROUP `
        --name $STORAGE_ACCOUNT `
        --location $LOCATION `
        --sku Standard_LRS

    # Get storage account key
    $STORAGE_KEY = az storage account keys list --resource-group $RESOURCE_GROUP --account-name $STORAGE_ACCOUNT --query "[0].value" --output tsv

    # Create Azure Storage connection string
    $STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=$STORAGE_ACCOUNT;AccountKey=$STORAGE_KEY;EndpointSuffix=core.windows.net"

    # Create blob containers
    Write-Host "Creating blob containers..." -ForegroundColor Yellow
    az storage container create --name input-files --account-name $STORAGE_ACCOUNT --account-key $STORAGE_KEY --public-access off
    az storage container create --name output-db --account-name $STORAGE_ACCOUNT --account-key $STORAGE_KEY --public-access off

    # Create file share for logs
    Write-Host "Creating file share for logs..." -ForegroundColor Yellow
    az storage share create --name logs --account-name $STORAGE_ACCOUNT --account-key $STORAGE_KEY

    # Get ACR password
    $ACR_PASSWORD = az acr credential show --name $ACR_NAME --query "passwords[0].value" --output tsv

    # Deploy container instance
    Write-Host "Deploying container instance..." -ForegroundColor Yellow
    az container create `
        --resource-group $RESOURCE_GROUP `
        --name $CONTAINER_GROUP `
        --image "${ACR_LOGIN_SERVER}/${CONTAINER_NAME}:${IMAGE_TAG}" `
        --registry-login-server $ACR_LOGIN_SERVER `
        --registry-username $ACR_NAME `
        --registry-password $ACR_PASSWORD `
        --dns-name-label rules-backend-fastapi `
        --ports 8000 `
        --cpu 1 `
        --memory 2 `
        --environment-variables PYTHONPATH=/app:/app/DEV PYTHONUNBUFFERED=1 AZURE_STORAGE_CONNECTION_STRING="$STORAGE_CONNECTION_STRING" AZURE_STORAGE_CONTAINER_NAME=input-files FASTAPI_HOST=0.0.0.0 FASTAPI_PORT=8000 LOG_LEVEL=INFO ENVIRONMENT=production CORS_ORIGINS=* CORS_ALLOW_CREDENTIALS=true CORS_ALLOW_METHODS=GET,POST,PUT,DELETE,OPTIONS CORS_ALLOW_HEADERS=* `
        --azure-file-volume-account-name $STORAGE_ACCOUNT `
        --azure-file-volume-account-key $STORAGE_KEY `
        --azure-file-volume-share-name logs `
        --azure-file-volume-mount-path /app/logs

    # Get the FQDN
    $FQDN = az container show --resource-group $RESOURCE_GROUP --name $CONTAINER_GROUP --query "ipAddress.fqdn" --output tsv

    Write-Host "Deployment completed successfully!" -ForegroundColor Green
    Write-Host "Application URL: http://$FQDN:8000" -ForegroundColor Cyan
    Write-Host "API Documentation: http://$FQDN:8000/testing" -ForegroundColor Cyan
    Write-Host "WebSocket endpoint: ws://$FQDN:8000/ws" -ForegroundColor Cyan

    Write-Host "`nTo check container logs, run:" -ForegroundColor Yellow
    Write-Host "az container logs --resource-group $RESOURCE_GROUP --name $CONTAINER_GROUP" -ForegroundColor White

    Write-Host "`nTo delete all resources, run:" -ForegroundColor Yellow
    Write-Host "az group delete --name $RESOURCE_GROUP --yes --no-wait" -ForegroundColor White

} catch {
    Write-Host "Deployment failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}