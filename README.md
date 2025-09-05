# Rules Backend FastAPI - Docker & Azure Deployment

A FastAPI application for rule checking and data modeling with WebSocket support, containerized with Docker and ready for Azure deployment.

## 🚀 Features

- FastAPI web framework with WebSocket support
- Rule checking engine with 60+ rules
- Data modeling capabilities
- Real-time communication via WebSockets
- Containerized with Docker
- Azure Container Instances ready
- Persistent storage support

## 📋 Prerequisites

- Docker Desktop installed
- Azure CLI installed (for Azure deployment)
- Python 3.12.9 (for local development)

## 🐳 Docker Setup

### Local Development with Docker

1. **Build the Docker image:**
   ```bash
   docker build -t rules-backend-fastapi .
   ```

2. **Run with Docker Compose:**
   ```bash
   docker-compose up -d
   ```

3. **Access the application:**
   - API Documentation: http://localhost:8000/testing
   - WebSocket endpoint: ws://localhost:8000/ws

### Manual Docker Run

```bash
docker run -d \
  --name rules-backend \
  -p 8000:8000 \
  -v $(pwd)/output_db:/app/output_db \
  -v $(pwd)/logs:/app/logs \
  --env-file .env \
  rules-backend-fastapi
```

**Note:** Input files are now stored in Azure Blob Storage, so no local `input_files` volume is needed.

## ☁️ Azure Deployment

### Option 1: Automated Deployment Script

**For Linux/macOS:**
```bash
chmod +x deploy-to-azure.sh
./deploy-to-azure.sh
```

**For Windows (PowerShell):**
```powershell
.\deploy-to-azure.ps1
```

### Option 2: Manual Azure Deployment

1. **Login to Azure:**
   ```bash
   az login
   ```

2. **Create Resource Group:**
   ```bash
   az group create --name rules-backend-rg --location eastus
   ```

3. **Create Azure Container Registry:**
   ```bash
   az acr create --resource-group rules-backend-rg --name rulesbackendacr --sku Basic --admin-enabled true
   ```

4. **Build and Push Image:**
   ```bash
   az acr build --registry rulesbackendacr --image rules-backend-fastapi:latest .
   ```

5. **Deploy Container Instance:**
   ```bash
   az container create \
     --resource-group rules-backend-rg \
     --name rules-backend-aci \
     --image rulesbackendacr.azurecr.io/rules-backend-fastapi:latest \
     --registry-login-server rulesbackendacr.azurecr.io \
     --registry-username rulesbackendacr \
     --registry-password $(az acr credential show --name rulesbackendacr --query "passwords[0].value" --output tsv) \
     --dns-name-label rules-backend-fastapi \
     --ports 8000 \
     --cpu 1 \
     --memory 2
   ```

### Option 3: Azure Resource Manager Template

Use the provided `azure-deployment.yml` template:

```bash
az deployment group create \
  --resource-group rules-backend-rg \
  --template-file azure-deployment.yml
```

## 🔧 Configuration

### Environment Variables

**Required:**
- `AZURE_STORAGE_CONNECTION_STRING`: Azure Storage account connection string
- `AZURE_STORAGE_CONTAINER_NAME`: Blob container name (default: `input-files`)

**Optional:**
- `PYTHONPATH`: Set to `/app`
- `PYTHONUNBUFFERED`: Set to `1` for real-time logging
- `FASTAPI_HOST`: FastAPI host (default: `0.0.0.0`)
- `FASTAPI_PORT`: FastAPI port (default: `8000`)
- `LOG_LEVEL`: Logging level (default: `INFO`)
- `ENVIRONMENT`: Environment mode (default: `development`)

**CORS Configuration:**
- `CORS_ORIGINS`: Comma-separated list of allowed origins (default: `*`)
- `CORS_ALLOW_CREDENTIALS`: Allow credentials in CORS requests (default: `true`)
- `CORS_ALLOW_METHODS`: Comma-separated list of allowed HTTP methods (default: `GET,POST,PUT,DELETE,OPTIONS`)
- `CORS_ALLOW_HEADERS`: Comma-separated list of allowed headers (default: `*`)

**Note:** For production, replace `CORS_ORIGINS=*` with specific frontend URLs for security.

### Azure Blob Storage Setup

1. **Create Azure Storage Account:**
   ```bash
   az storage account create \
     --resource-group your-resource-group \
     --name yourstorageaccount \
     --location eastus \
     --sku Standard_LRS
   ```

2. **Create Blob Container:**
   ```bash
   az storage container create \
     --name input-files \
     --account-name yourstorageaccount \
     --public-access off
   ```

3. **Get Connection String:**
   ```bash
   az storage account show-connection-string \
     --resource-group your-resource-group \
     --name yourstorageaccount \
     --output tsv
   ```

4. **Set Environment Variables:**
   Create a `.env` file based on `.env.example`:
   ```bash
   cp .env.example .env
   # Edit .env with your Azure Storage credentials
   ```

### Volumes

- `/app/output_db`: Output database files (local storage)
- `/app/logs`: Application logs (local storage)
- **Input files**: Now stored in Azure Blob Storage (`input-files` container)

## 📁 Project Structure

```
Rules_backend_fastapi/
├── DEV/
│   ├── main.py                 # FastAPI application
│   └── project/
│       ├── azure_storage.py   # Azure Blob Storage utility
│       ├── model.py           # Data models
│       ├── prepare_data_model/ # Data modeling modules
│       └── rule_checker/      # Rule checking modules
├── input_files/               # Local input data (for development only)
├── output_db/                 # Output database directory
├── .env.example              # Environment variables template
├── Dockerfile                 # Docker configuration
├── docker-compose.yml         # Docker Compose configuration
├── requirements.txt           # Python dependencies
├── azure-deployment.yml       # Azure ARM template
├── deploy-to-azure.sh        # Linux/macOS deployment script
├── deploy-to-azure.ps1       # Windows deployment script
└── README.md                 # This file
```

**Note:** In production, input files are stored in Azure Blob Storage (`input-files` container), not in the local `input_files/` directory.

## 🔌 API Endpoints

- `GET /testing` - API documentation (Swagger UI)
- `POST /get_task_name` - Get task name
- `POST /data_modelling_api` - Data modeling endpoint
- `POST /rule_checker_api` - Rule checking endpoint
- `WebSocket /ws` - WebSocket connection for real-time updates

## 🧪 Testing

### Test WebSocket Connection

Use the provided test client:
```bash
python run_test.py
```

### Health Check

```bash
curl -f http://localhost:8000/testing
```

## 📊 Monitoring

### Check Container Logs

**Docker:**
```bash
docker logs rules-backend
```

**Azure:**
```bash
az container logs --resource-group rules-backend-rg --name rules-backend-aci
```

### Container Status

**Docker:**
```bash
docker ps
```

**Azure:**
```bash
az container show --resource-group rules-backend-rg --name rules-backend-aci
```

## 🛠️ Troubleshooting

### Common Issues

1. **Port already in use:**
   ```bash
   docker stop rules-backend
   docker rm rules-backend
   ```

2. **Permission denied on scripts:**
   ```bash
   chmod +x deploy-to-azure.sh
   ```

3. **Azure CLI not logged in:**
   ```bash
   az login
   ```

### Logs Location

- Container logs: `/app/logs/`
- Application logs: `execute_rule_data_modelling.log`

## 🔒 Security Considerations

- Update the default storage account names and keys in deployment scripts
- Use Azure Key Vault for sensitive configuration
- Enable HTTPS in production
- Configure proper network security groups

## 💰 Cost Optimization

- Use Azure Container Instances for development/testing
- Consider Azure Container Apps for production workloads
- Monitor resource usage and scale accordingly
- Use Azure Storage lifecycle policies for log retention

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with Docker
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 📞 Support

For issues and questions:
1. Check the troubleshooting section
2. Review container logs
3. Create an issue in the repository

---
docker build -t rules-backend-fastapi . 
docker login
docker tag rules-backend-fastapi:latest sayaibosl/coding-checker-model:latest
docker push sayaibosl/coding-checker-model:latest
**Happy Coding! 🚀**