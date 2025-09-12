import os
import asyncio
from typing import List, Optional, BinaryIO
from pathlib import Path
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import DefaultAzureCredential
from loguru import logger
import tempfile
import shutil

class AzureBlobStorageManager:
    """
    Azure Blob Storage manager for handling file operations
    """
    
    def __init__(self):
        self.account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
        self.account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
        self.connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        self.container_name = os.getenv('AZURE_CONTAINER_NAME', 'coding-checker')
        self.blob_service_client = None
        self.container_client = None
        self.enabled = False
        
        # Initialize blob service client
        try:
            if self.connection_string:
                self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
                logger.info("Azure Blob Storage initialized with connection string")
            elif self.account_name and self.account_key:
                account_url = f"https://{self.account_name}.blob.core.windows.net"
                self.blob_service_client = BlobServiceClient(account_url=account_url, credential=self.account_key)
                logger.info("Azure Blob Storage initialized with account name and key")
            elif self.account_name:
                # Use default Azure credentials (managed identity, service principal, etc.)
                account_url = f"https://{self.account_name}.blob.core.windows.net"
                credential = DefaultAzureCredential()
                self.blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
                logger.info("Azure Blob Storage initialized with DefaultAzureCredential")
            else:
                logger.warning("Azure Blob Storage not configured - running in local mode")
                return
            
            self.container_client = self.blob_service_client.get_container_client(self.container_name)
            
            # Ensure container exists
            self._ensure_container_exists()
            self.enabled = True
            
        except Exception as e:
            logger.error(f"Failed to initialize Azure Blob Storage: {e}")
            logger.warning("Azure Blob Storage disabled - running in local mode")
            self.blob_service_client = None
            self.container_client = None
            self.enabled = False
    
    def _ensure_container_exists(self):
        """Ensure the container exists, create if it doesn't"""
        if not self.enabled or not self.container_client:
            return
            
        try:
            self.container_client.get_container_properties()
            logger.info(f"Container '{self.container_name}' exists")
        except Exception:
            try:
                self.container_client.create_container()
                logger.info(f"Created container '{self.container_name}'")
            except Exception as e:
                logger.error(f"Failed to create container '{self.container_name}': {e}")
                raise
    
    async def upload_file(self, local_file_path: str, blob_name: str, overwrite: bool = True) -> bool:
        """
        Upload a file to Azure Blob Storage
        
        Args:
            local_file_path: Path to the local file
            blob_name: Name of the blob in storage
            overwrite: Whether to overwrite existing blob
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.enabled:
            logger.warning(f"Azure Blob Storage not enabled - cannot upload {local_file_path}")
            return False
            
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            
            with open(local_file_path, 'rb') as data:
                await asyncio.to_thread(blob_client.upload_blob, data, overwrite=overwrite)
            
            logger.info(f"Successfully uploaded {local_file_path} to {blob_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload {local_file_path} to {blob_name}: {e}")
            return False
    
    async def download_file(self, blob_name: str, local_file_path: str, progress_callback=None) -> bool:
        """
        Download a file from Azure Blob Storage
        
        Args:
            blob_name: Name of the blob in storage
            local_file_path: Path where to save the file locally
            progress_callback: Optional callback function for progress updates (current_bytes, total_bytes, filename)
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.enabled:
            logger.warning(f"Azure Blob Storage not enabled - cannot download {blob_name}")
            return False
            
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            
            # Ensure local directory exists
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            
            # Get blob properties to determine file size
            blob_properties = await asyncio.to_thread(blob_client.get_blob_properties)
            total_size = blob_properties.size
            
            with open(local_file_path, 'wb') as download_file:
                download_stream = await asyncio.to_thread(blob_client.download_blob)
                
                if progress_callback and total_size > 0:
                    # Download in chunks to track progress
                    chunk_size = 8192  # 8KB chunks
                    downloaded_bytes = 0
                    last_update_time = 0
                    last_update_bytes = 0
                    update_interval = 5.0  # Update every 5 seconds
                    min_progress_threshold = 0.05  # Update when 5% progress is made
                    
                    # Send initial progress
                    await progress_callback(0, total_size, os.path.basename(blob_name))
                    
                    # Read and write in chunks
                    while True:
                        chunk = await asyncio.to_thread(download_stream.read, chunk_size)
                        if not chunk:
                            break
                        
                        await asyncio.to_thread(download_file.write, chunk)
                        downloaded_bytes += len(chunk)
                        
                        # Throttle progress updates
                        current_time = asyncio.get_event_loop().time()
                        progress_since_last = (downloaded_bytes - last_update_bytes) / total_size
                        time_since_last = current_time - last_update_time
                        
                        # Send update if enough time passed OR significant progress made
                        if (time_since_last >= update_interval or 
                            progress_since_last >= min_progress_threshold or 
                            downloaded_bytes == total_size):  # Always send final update
                            await progress_callback(downloaded_bytes, total_size, os.path.basename(blob_name))
                            last_update_time = current_time
                            last_update_bytes = downloaded_bytes
                else:
                    # Fallback to original method if no progress callback
                    await asyncio.to_thread(lambda: download_file.write(download_stream.readall()))
            
            logger.info(f"Successfully downloaded {blob_name} to {local_file_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to download {blob_name} to {local_file_path}: {e}")
            return False
    
    async def list_blobs(self, prefix: str = "") -> List[str]:
        """
        List all blobs in the container with optional prefix filter
        
        Args:
            prefix: Optional prefix to filter blobs
        
        Returns:
            List[str]: List of blob names
        """
        try:
            blob_list = await asyncio.to_thread(
                lambda: list(self.container_client.list_blobs(name_starts_with=prefix))
            )
            return [blob.name for blob in blob_list]
        except Exception as e:
            logger.error(f"Failed to list blobs with prefix '{prefix}': {e}")
            return []
    
    async def delete_blob(self, blob_name: str) -> bool:
        """
        Delete a blob from Azure Blob Storage
        
        Args:
            blob_name: Name of the blob to delete
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            await asyncio.to_thread(blob_client.delete_blob)
            logger.info(f"Successfully deleted blob {blob_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete blob {blob_name}: {e}")
            return False
    
    async def blob_exists(self, blob_name: str) -> bool:
        """
        Check if a blob exists in the container
        
        Args:
            blob_name: Name of the blob to check
        
        Returns:
            bool: True if blob exists, False otherwise
        """
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            await asyncio.to_thread(blob_client.get_blob_properties)
            return True
        except Exception:
            return False
    
    async def validate_folder_contents(self, blob_prefix: str) -> dict:
        """
        Validate if a folder contains required XML and PDF files before downloading
        
        Args:
            blob_prefix: Prefix of blobs to validate (acts as directory path)
        
        Returns:
            dict: Validation result with status, xml_files, pdf_files, and message
        """
        if not self.enabled:
            return {
                "valid": False,
                "message": "Azure Blob Storage not enabled",
                "xml_files": [],
                "pdf_files": []
            }
        
        try:
            blobs = await self.list_blobs(prefix=blob_prefix)
            
            if not blobs:
                return {
                    "valid": False,
                    "message": f"No files found in folder '{blob_prefix}'",
                    "xml_files": [],
                    "pdf_files": []
                }
            
            # Filter XML and PDF files
            xml_files = [blob for blob in blobs if blob.lower().endswith('.xml')]
            pdf_files = [blob for blob in blobs if blob.lower().endswith('.pdf')]
            
            if not xml_files:
                return {
                    "valid": False,
                    "message": f"No XML files found in folder '{blob_prefix}'",
                    "xml_files": [],
                    "pdf_files": pdf_files
                }
            
            if not pdf_files:
                return {
                    "valid": False,
                    "message": f"No PDF files found in folder '{blob_prefix}'",
                    "xml_files": xml_files,
                    "pdf_files": []
                }
            
            # Check if XML and PDF file names match
            xml_name = os.path.basename(xml_files[0]).split('.')[0]
            pdf_name = os.path.basename(pdf_files[0]).split('.')[0]
            
            if xml_name != pdf_name:
                return {
                    "valid": False,
                    "message": f"XML and PDF file names do not match in folder '{blob_prefix}'",
                    "xml_files": xml_files,
                    "pdf_files": pdf_files
                }
            
            return {
                "valid": True,
                "message": f"Folder '{blob_prefix}' contains valid XML and PDF files",
                "xml_files": xml_files,
                "pdf_files": pdf_files
            }
            
        except Exception as e:
            logger.error(f"Failed to validate folder contents for '{blob_prefix}': {e}")
            return {
                "valid": False,
                "message": f"Error validating folder contents: {str(e)}",
                "xml_files": [],
                "pdf_files": []
            }
    
    async def download_directory(self, blob_prefix: str, local_directory: str, progress_callback=None) -> bool:
        """
        Download all blobs with a specific prefix to a local directory
        
        Args:
            blob_prefix: Prefix of blobs to download (acts as directory path)
            local_directory: Local directory to download files to
            progress_callback: Optional callback function for progress updates
        
        Returns:
            bool: True if all downloads successful, False otherwise
        """
        try:
            blobs = await self.list_blobs(prefix=blob_prefix)
            
            if not blobs:
                logger.warning(f"No blobs found with prefix '{blob_prefix}'")
                return False
            
            # Get total size of all blobs for overall progress tracking
            total_size = 0
            blob_sizes = {}
            
            if progress_callback:
                for blob_name in blobs:
                    try:
                        blob_client = self.container_client.get_blob_client(blob_name)
                        blob_properties = await asyncio.to_thread(blob_client.get_blob_properties)
                        blob_sizes[blob_name] = blob_properties.size
                        total_size += blob_properties.size
                    except Exception as e:
                        logger.warning(f"Could not get size for blob {blob_name}: {e}")
                        blob_sizes[blob_name] = 0
            
            success_count = 0
            downloaded_bytes = 0
            last_progress_update_time = 0
            last_progress_bytes = 0
            progress_update_interval = 10.0  # Update every 5 seconds
            min_progress_change = 0.10  # Update when 5% overall progress is made
            
            # Create a wrapper progress callback for individual files
            async def file_progress_callback(current_bytes, file_total_bytes, filename):
                nonlocal last_progress_update_time, last_progress_bytes
                
                if progress_callback:
                    # Calculate overall progress
                    file_downloaded = current_bytes
                    overall_downloaded = downloaded_bytes + file_downloaded
                    
                    # Throttle progress updates
                    current_time = asyncio.get_event_loop().time()
                    progress_since_last = (overall_downloaded - last_progress_bytes) / total_size if total_size > 0 else 0
                    time_since_last = current_time - last_progress_update_time
                    
                    # Send update if enough time passed OR significant progress made OR file completed
                    if (time_since_last >= progress_update_interval or 
                        progress_since_last >= min_progress_change or 
                        current_bytes == file_total_bytes):  # Always send when file completes
                        
                        await progress_callback({
                            'type': 'file_progress',
                            'current_file': filename,
                            'file_progress': current_bytes,
                            'file_total': file_total_bytes,
                            'overall_progress': overall_downloaded,
                            'overall_total': total_size,
                            'files_completed': success_count,
                            'total_files': len(blobs)
                        })
                        
                        last_progress_update_time = current_time
                        last_progress_bytes = overall_downloaded
            
            for i, blob_name in enumerate(blobs):
                # Create relative path by removing prefix
                relative_path = blob_name[len(blob_prefix):].lstrip('/')
                local_file_path = os.path.join(local_directory, relative_path)
                
                # Send file start notification
                if progress_callback:
                    await progress_callback({
                        'type': 'file_start',
                        'current_file': os.path.basename(blob_name),
                        'file_index': i + 1,
                        'total_files': len(blobs)
                    })
                
                if await self.download_file(blob_name, local_file_path, file_progress_callback):
                    success_count += 1
                    # Add this file's size to downloaded bytes
                    downloaded_bytes += blob_sizes.get(blob_name, 0)
                    
                    # Send file completion notification
                    if progress_callback:
                        await progress_callback({
                            'type': 'file_complete',
                            'current_file': os.path.basename(blob_name),
                            'files_completed': success_count,
                            'total_files': len(blobs)
                        })
            
            # Send final completion notification
            if progress_callback:
                await progress_callback({
                    'type': 'download_complete',
                    'files_completed': success_count,
                    'total_files': len(blobs),
                    'success': success_count == len(blobs)
                })
            
            logger.info(f"Downloaded {success_count}/{len(blobs)} files from '{blob_prefix}'")
            return success_count == len(blobs)
        except Exception as e:
            logger.error(f"Failed to download directory '{blob_prefix}': {e}")
            return False
    
    async def upload_directory(self, local_directory: str, blob_prefix: str = "", container_name: str = None) -> bool:
        """
        Upload all files from a local directory to blob storage
        
        Args:
            local_directory: Local directory to upload
            blob_prefix: Prefix to add to blob names (acts as directory path)
            container_name: Optional container name (defaults to self.container_name)
        
        Returns:
            bool: True if all uploads successful, False otherwise
        """
        try:
            # Use specified container or default
            target_container = container_name or self.container_name
            
            # Get container client for target container
            if container_name and container_name != self.container_name:
                target_container_client = self.blob_service_client.get_container_client(container_name)
                # Ensure target container exists
                try:
                    target_container_client.create_container()
                    logger.info(f"Created container '{container_name}'")
                except Exception as e:
                    if "ContainerAlreadyExists" not in str(e):
                        logger.warning(f"Could not create container '{container_name}': {e}")
            else:
                target_container_client = self.container_client
            
            local_path = Path(local_directory)
            if not local_path.exists():
                logger.error(f"Local directory '{local_directory}' does not exist")
                return False
            
            files_to_upload = []
            for file_path in local_path.rglob('*'):
                if file_path.is_file():
                    relative_path = file_path.relative_to(local_path)
                    blob_name = f"{blob_prefix}/{relative_path}".replace('\\', '/').lstrip('/')
                    files_to_upload.append((str(file_path), blob_name))
            
            if not files_to_upload:
                logger.warning(f"No files found in directory '{local_directory}'")
                return False
            
            success_count = 0
            for local_file_path, blob_name in files_to_upload:
                # Upload to specific container
                if await self._upload_file_to_container(local_file_path, blob_name, target_container_client):
                    success_count += 1
            
            logger.info(f"Uploaded {success_count}/{len(files_to_upload)} files to '{target_container}/{blob_prefix}'")
            return success_count == len(files_to_upload)
        except Exception as e:
            logger.error(f"Failed to upload directory '{local_directory}': {e}")
            return False
    
    async def _upload_file_to_container(self, local_file_path: str, blob_name: str, container_client: ContainerClient, overwrite: bool = True) -> bool:
        """
        Upload a file to a specific container
        
        Args:
            local_file_path: Path to local file
            blob_name: Name for the blob in storage
            container_client: Container client to upload to
            overwrite: Whether to overwrite existing blob
        
        Returns:
            bool: True if upload successful, False otherwise
        """
        try:
            if not self.enabled:
                logger.warning("Azure Blob Storage not enabled - skipping upload")
                return False
            
            local_path = Path(local_file_path)
            if not local_path.exists():
                logger.error(f"Local file '{local_file_path}' does not exist")
                return False
            
            def upload_sync():
                with open(local_file_path, 'rb') as data:
                    container_client.upload_blob(
                        name=blob_name,
                        data=data,
                        overwrite=overwrite
                    )
            
            await asyncio.to_thread(upload_sync)
            logger.debug(f"Uploaded '{local_file_path}' to blob '{blob_name}'")
            return True
        except Exception as e:
            logger.error(f"Failed to upload '{local_file_path}' to blob '{blob_name}': {e}")
            return False
    
    async def get_temp_directory_with_blobs(self, blob_prefix: str) -> Optional[str]:
        """
        Download blobs to a temporary directory and return the path
        This is useful for processing files that need to be accessed locally
        
        Args:
            blob_prefix: Prefix of blobs to download
        
        Returns:
            Optional[str]: Path to temporary directory, or None if failed
        """
        try:
            temp_dir = tempfile.mkdtemp()
            
            if await self.download_directory(blob_prefix, temp_dir):
                logger.info(f"Created temporary directory with blobs: {temp_dir}")
                return temp_dir
            else:
                # Clean up if download failed
                shutil.rmtree(temp_dir, ignore_errors=True)
                return None
        except Exception as e:
            logger.error(f"Failed to create temp directory with blobs: {e}")
            return None
    
    def cleanup_temp_directory(self, temp_dir: str):
        """
        Clean up a temporary directory
        
        Args:
            temp_dir: Path to temporary directory to clean up
        """
        try:
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                logger.info(f"Cleaned up temporary directory: {temp_dir}")
        except Exception as e:
            logger.error(f"Failed to clean up temporary directory {temp_dir}: {e}")

# Global instance
azure_storage = AzureBlobStorageManager()