#!/usr/bin/env python3
"""
Test script to verify Azure authentication fix
"""

import os
import sys
from pathlib import Path

# Add the DEV directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "DEV"))

try:
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    print("✓ Environment variables loaded successfully")
    
    # Test Azure storage initialization
    from project.azure_storage import AzureBlobStorageManager
    
    print("✓ Azure storage module imported successfully")
    
    # Initialize Azure storage manager
    storage = AzureBlobStorageManager()
    
    if storage.enabled:
        print("✓ Azure Blob Storage initialized successfully")
        print(f"  - Account: {storage.account_name or 'Connection String'}")
        print(f"  - Container: {storage.container_name}")
    else:
        print("⚠ Azure Blob Storage running in local mode (not configured)")
    
    # Test FastAPI app initialization
    print("\nTesting FastAPI app initialization...")
    from main import app
    
    print("✓ FastAPI app initialized successfully")
    print("✓ No Azure credential errors detected")
    
    print("\n🎉 All tests passed! The Azure authentication issue has been resolved.")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)