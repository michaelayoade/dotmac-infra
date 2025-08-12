"""
Platform SDK Cache Client (Python)
Provides caching operations with tenant isolation and observability
"""

from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import json
import uuid

# Redis dependency removed - use external injection
from dotmac_infra.utils.logging import logger
import redis


class CacheClient:
    """Platform SDK Cache Client with tenant isolation"""
    
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.logger = logger
        self.redis_client = None
        self._initialize_redis()
    
    def _initialize_redis(self):
        """Initialize Redis connection with fallback"""
        try:
            self.redis_client = get_redis()
            # Test connection
            self.redis_client.ping()
        except Exception as e:
            self.logger.warning(f"Redis connection failed, using in-memory fallback: {str(e)}")
            self.redis_client = None
            # Initialize in-memory cache as fallback
            self._memory_cache = {}
    
    def _get_key(self, key: str) -> str:
        """Get tenant-isolated cache key"""
        return f"tenant:{self.tenant_id}:{key}"
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        try:
            cache_key = self._get_key(key)
            
            if self.redis_client:
                # Use Redis
                value = self.redis_client.get(cache_key)
                if value:
                    return json.loads(value.decode('utf-8'))
            else:
                # Use in-memory fallback
                cache_entry = self._memory_cache.get(cache_key)
                if cache_entry:
                    # Check expiration
                    if cache_entry['expires_at'] is None or datetime.utcnow() < cache_entry['expires_at']:
                        return cache_entry['value']
                    else:
                        # Remove expired entry
                        del self._memory_cache[cache_key]
            
            return None
            
        except Exception as e:
            self.logger.error(f"Cache get failed: {str(e)}")
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in cache with optional TTL (seconds)"""
        try:
            cache_key = self._get_key(key)
            serialized_value = json.dumps(value, default=str)
            
            if self.redis_client:
                # Use Redis
                if ttl:
                    self.redis_client.setex(cache_key, ttl, serialized_value)
                else:
                    self.redis_client.set(cache_key, serialized_value)
            else:
                # Use in-memory fallback
                expires_at = None
                if ttl:
                    expires_at = datetime.utcnow() + timedelta(seconds=ttl)
                
                self._memory_cache[cache_key] = {
                    'value': value,
                    'expires_at': expires_at,
                    'created_at': datetime.utcnow()
                }
            
            return True
            
        except Exception as e:
            self.logger.error(f"Cache set failed: {str(e)}")
            return False
    
    async def delete(self, key: str) -> bool:
        """Delete value from cache"""
        try:
            cache_key = self._get_key(key)
            
            if self.redis_client:
                # Use Redis
                self.redis_client.delete(cache_key)
            else:
                # Use in-memory fallback
                self._memory_cache.pop(cache_key, None)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Cache delete failed: {str(e)}")
            return False
    
    async def delete_pattern(self, pattern: str) -> bool:
        """Delete keys matching pattern"""
        try:
            cache_pattern = self._get_key(pattern)
            
            if self.redis_client:
                # Use Redis
                keys = self.redis_client.keys(cache_pattern)
                if keys:
                    self.redis_client.delete(*keys)
            else:
                # Use in-memory fallback
                keys_to_delete = [k for k in self._memory_cache.keys() if pattern in k]
                for key in keys_to_delete:
                    del self._memory_cache[key]
            
            return True
            
        except Exception as e:
            self.logger.error(f"Cache delete pattern failed: {str(e)}")
            return False
    
    async def exists(self, key: str) -> bool:
        """Check if key exists in cache"""
        try:
            cache_key = self._get_key(key)
            
            if self.redis_client:
                # Use Redis
                return bool(self.redis_client.exists(cache_key))
            else:
                # Use in-memory fallback
                cache_entry = self._memory_cache.get(cache_key)
                if cache_entry:
                    # Check expiration
                    if cache_entry['expires_at'] is None or datetime.utcnow() < cache_entry['expires_at']:
                        return True
                    else:
                        # Remove expired entry
                        del self._memory_cache[cache_key]
                return False
            
        except Exception as e:
            self.logger.error(f"Cache exists failed: {str(e)}")
            return False
    
    async def increment(self, key: str, amount: int = 1) -> Optional[int]:
        """Increment numeric value in cache"""
        try:
            cache_key = self._get_key(key)
            
            if self.redis_client:
                # Use Redis
                return self.redis_client.incrby(cache_key, amount)
            else:
                # Use in-memory fallback
                current_value = await self.get(key) or 0
                new_value = int(current_value) + amount
                await self.set(key, new_value)
                return new_value
            
        except Exception as e:
            self.logger.error(f"Cache increment failed: {str(e)}")
            return None
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        try:
            if self.redis_client:
                info = self.redis_client.info()
                return {
                    "type": "redis",
                    "connected_clients": info.get("connected_clients", 0),
                    "used_memory": info.get("used_memory", 0),
                    "keyspace_hits": info.get("keyspace_hits", 0),
                    "keyspace_misses": info.get("keyspace_misses", 0)
                }
            else:
                return {
                    "type": "in_memory",
                    "total_keys": len(self._memory_cache),
                    "tenant_keys": len([k for k in self._memory_cache.keys() if f"tenant:{self.tenant_id}" in k])
                }
                
        except Exception as e:
            self.logger.error(f"Cache stats failed: {str(e)}")
            return {"type": "error", "error": str(e)}
