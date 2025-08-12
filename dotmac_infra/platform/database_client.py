"""
Platform SDK Database Client (Python)
Provides database operations with tenant isolation and observability
"""

from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import uuid

# Database dependency removed - use external injection
from dotmac_infra.utils.logging import logger
from sqlalchemy.orm import Session
from sqlalchemy import text


class DatabaseClient:
    """Platform SDK Database Client with tenant isolation"""
    
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.logger = logger
    
    def get_session(self) -> Session:
        """Get database session"""
        return next(get_db())
    
    async def execute(self, query: str, params: tuple = None) -> bool:
        """Execute a database query"""
        try:
            db = self.get_session()
            try:
                db.execute(text(query), params or ())
                db.commit()
                return True
            finally:
                db.close()
        except Exception as e:
            self.logger.error(f"Database execute failed: {str(e)}")
            return False
    
    async def query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Execute a query and return results"""
        try:
            db = self.get_session()
            try:
                result = db.execute(text(query), params or ())
                columns = result.keys()
                rows = result.fetchall()
                return [dict(zip(columns, row)) for row in rows]
            finally:
                db.close()
        except Exception as e:
            self.logger.error(f"Database query failed: {str(e)}")
            return []
    
    async def query_one(self, query: str, params: tuple = None) -> Optional[Dict[str, Any]]:
        """Execute a query and return first result"""
        results = await self.query(query, params)
        return results[0] if results else None
    
    async def insert(self, table: str, data: Dict[str, Any]) -> Optional[str]:
        """Insert data into table and return ID"""
        try:
            # Add tenant_id and audit fields
            data['tenant_id'] = self.tenant_id
            data['created_at'] = datetime.utcnow()
            
            # Build insert query
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['%s'] * len(data))
            query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) RETURNING id"
            
            result = await self.query_one(query, tuple(data.values()))
            return result['id'] if result else None
            
        except Exception as e:
            self.logger.error(f"Database insert failed: {str(e)}")
            return None
    
    async def update(self, table: str, id_value: str, data: Dict[str, Any]) -> bool:
        """Update data in table"""
        try:
            # Add audit fields
            data['updated_at'] = datetime.utcnow()
            
            # Build update query
            set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
            query = f"UPDATE {table} SET {set_clause} WHERE id = %s AND tenant_id = %s"
            params = tuple(list(data.values()) + [id_value, self.tenant_id])
            
            return await self.execute(query, params)
            
        except Exception as e:
            self.logger.error(f"Database update failed: {str(e)}")
            return False
    
    async def delete(self, table: str, id_value: str) -> bool:
        """Delete data from table"""
        try:
            query = f"DELETE FROM {table} WHERE id = %s AND tenant_id = %s"
            return await self.execute(query, (id_value, self.tenant_id))
            
        except Exception as e:
            self.logger.error(f"Database delete failed: {str(e)}")
            return False
