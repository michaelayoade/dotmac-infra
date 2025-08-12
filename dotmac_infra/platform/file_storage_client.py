"""
Platform SDK File Storage Client (Python)
Provides file storage operations with tenant isolation
"""

from typing import List, Optional, Union
from pathlib import Path

from dotmac_infra.utils.logging import logger


class FileStorageClient:
    """Platform SDK File Storage Client with tenant isolation"""
    
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.logger = logger
        self.base_path = Path("/tmp/file_storage")  # In production, use proper storage
        self.tenant_path = self.base_path / f"tenant_{tenant_id}"
        
        # Ensure tenant directory exists
        self.tenant_path.mkdir(parents=True, exist_ok=True)
    
    async def store_file(self, file_path: str, content: Union[str, bytes]) -> bool:
        """Store file content"""
        try:
            full_path = self.tenant_path / file_path.lstrip('/')
            
            # Ensure parent directories exist
            full_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write content
            if isinstance(content, str):
                full_path.write_text(content, encoding='utf-8')
            else:
                full_path.write_bytes(content)
            
            self.logger.info(f"File stored: {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"File storage failed: {str(e)}")
            return False
    
    async def get_file(self, file_path: str) -> Optional[bytes]:
        """Get file content"""
        try:
            full_path = self.tenant_path / file_path.lstrip('/')
            
            if full_path.exists():
                return full_path.read_bytes()
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"File retrieval failed: {str(e)}")
            return None
    
    async def delete_file(self, file_path: str) -> bool:
        """Delete file"""
        try:
            full_path = self.tenant_path / file_path.lstrip('/')
            
            if full_path.exists():
                full_path.unlink()
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"File deletion failed: {str(e)}")
            return False
    
    async def list_files(self, directory: str = "") -> List[str]:
        """List files in directory"""
        try:
            dir_path = self.tenant_path / directory.lstrip('/')
            
            if dir_path.exists() and dir_path.is_dir():
                files = []
                for item in dir_path.rglob('*'):
                    if item.is_file():
                        relative_path = item.relative_to(self.tenant_path)
                        files.append(str(relative_path))
                return files
            else:
                return []
                
        except Exception as e:
            self.logger.error(f"File listing failed: {str(e)}")
            return []
