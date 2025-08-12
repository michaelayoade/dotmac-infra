"""
Base Operations SDK

Foundation class for all Operations Plane SDKs with strict DRY principles
and SDK-to-SDK composition patterns.

Features:
- Cross-cutting concerns (caching, audit, observability, events)
- Common operations patterns (retry, validation, error handling)
- SDK composition with Platform, Data, Event, Analytics planes
- Standardized operation context and security validation
- Shared utilities for workflow orchestration and automation
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union
from uuid import uuid4
from enum import Enum

from ..base_sdk import BaseSDK, OperationContext, SecurityContext
from ..platform.database_sdk import DatabaseSDK
from ..platform.cache_sdk import CacheSDK
from ..platform.event_bus_sdk import EventBusSDK
from ..platform.observability_sdk import ObservabilitySDK
from ..data.queue_sdk import QueueSDK
from ...core.enums import Permission, ResourceType, OperationType, EventType

logger = logging.getLogger(__name__)


class OperationStatus(Enum):
    """Standard operation status across all Operations Plane SDKs"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"
    SUSPENDED = "suspended"


class OperationPriority(Enum):
    """Standard operation priority levels"""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4
    URGENT = 5


class OperationResult:
    """Standardized operation result across all Operations Plane SDKs"""
    
    def __init__(
        self,
        operation_id: str,
        status: OperationStatus,
        result: Optional[Any] = None,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None
    ):
        self.operation_id = operation_id
        self.status = status
        self.result = result
        self.error = error
        self.metadata = metadata or {}
        self.started_at = started_at or datetime.now(timezone.utc)
        self.completed_at = completed_at
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "operation_id": self.operation_id,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
            "metadata": self.metadata,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None
        }


class BaseOperationsSDK(BaseSDK, ABC):
    """
    Base class for all Operations Plane SDKs
    
    Provides:
    - DRY cross-cutting concerns (caching, audit, observability, events)
    - SDK-to-SDK composition with Platform, Data, Event planes
    - Common operation patterns and utilities
    - Standardized error handling and validation
    - Shared operation context and security validation
    """
    
    def __init__(self, tenant_id: str, **kwargs):
        super().__init__(tenant_id, **kwargs)
        
        # Compose with Platform SDKs (DRY pattern)
        self.db_sdk = DatabaseSDK(tenant_id)
        self.cache_sdk = CacheSDK(tenant_id)
        self.event_sdk = EventBusSDK(tenant_id)
        self.observability_sdk = ObservabilitySDK(tenant_id)
        self.queue_sdk = QueueSDK(tenant_id)
        
        # Operations-specific configuration
        self.operation_timeout = kwargs.get('operation_timeout', 300)  # 5 minutes default
        self.max_retries = kwargs.get('max_retries', 3)
        self.retry_delay = kwargs.get('retry_delay', 5)  # seconds
        
        logger.info(f"Initialized {self.__class__.__name__} for tenant {tenant_id}")
    
    async def create_operation_context(
        self,
        operation_type: str,
        operation_data: Dict[str, Any],
        security_context: SecurityContext,
        priority: OperationPriority = OperationPriority.NORMAL,
        metadata: Optional[Dict[str, Any]] = None
    ) -> OperationContext:
        """
        Create standardized operation context (DRY pattern)
        Used by all Operations Plane SDKs
        """
        operation_id = str(uuid4())
        
        context = OperationContext(
            operation_id=operation_id,
            tenant_id=self.tenant_id,
            user_id=security_context.user_id,
            operation_type=operation_type,
            resource_type=ResourceType.OPERATION,
            operation_data=operation_data,
            security_context=security_context,
            metadata={
                **(metadata or {}),
                "priority": priority.value,
                "sdk_name": self.__class__.__name__,
                "created_at": datetime.now(timezone.utc).isoformat()
            }
        )
        
        # Log operation creation (DRY observability)
        await self.observability_sdk.log_operation_start(
            operation_id, operation_type, context
        )
        
        return context
    
    async def execute_with_retry(
        self,
        operation_func,
        context: OperationContext,
        max_retries: Optional[int] = None,
        retry_delay: Optional[int] = None
    ) -> OperationResult:
        """
        Execute operation with retry logic (DRY pattern)
        Used by all Operations Plane SDKs for resilience
        """
        max_retries = max_retries or self.max_retries
        retry_delay = retry_delay or self.retry_delay
        operation_id = context.operation_id
        
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                # Update operation status
                await self._update_operation_status(
                    operation_id, 
                    OperationStatus.RETRYING if attempt > 0 else OperationStatus.RUNNING,
                    context
                )
                
                # Execute the operation
                result = await operation_func(context)
                
                # Success - update status and return
                await self._update_operation_status(
                    operation_id, OperationStatus.COMPLETED, context
                )
                
                return OperationResult(
                    operation_id=operation_id,
                    status=OperationStatus.COMPLETED,
                    result=result,
                    completed_at=datetime.now(timezone.utc)
                )
                
            except Exception as e:
                last_error = str(e)
                logger.warning(
                    f"Operation {operation_id} attempt {attempt + 1} failed: {e}"
                )
                
                if attempt < max_retries:
                    await asyncio.sleep(retry_delay)
                else:
                    # Final failure
                    await self._update_operation_status(
                        operation_id, OperationStatus.FAILED, context
                    )
                    
                    return OperationResult(
                        operation_id=operation_id,
                        status=OperationStatus.FAILED,
                        error=last_error,
                        completed_at=datetime.now(timezone.utc)
                    )
        
        # Should not reach here, but handle edge case
        return OperationResult(
            operation_id=operation_id,
            status=OperationStatus.FAILED,
            error="Maximum retries exceeded",
            completed_at=datetime.now(timezone.utc)
        )
    
    async def _update_operation_status(
        self,
        operation_id: str,
        status: OperationStatus,
        context: OperationContext,
        result_data: Optional[Dict[str, Any]] = None
    ):
        """Update operation status with caching and events (DRY pattern)"""
        
        status_data = {
            "operation_id": operation_id,
            "status": status.value,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "result_data": result_data
        }
        
        # Update in cache for fast access
        cache_key = f"operation_status:{self.tenant_id}:{operation_id}"
        await self.cache_sdk.set(cache_key, status_data, ttl=3600)  # 1 hour
        
        # Persist to database
        await self.db_sdk.execute(
            """
            INSERT INTO operations_status (
                operation_id, tenant_id, status, updated_at, result_data
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (operation_id) DO UPDATE SET
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at,
                result_data = EXCLUDED.result_data
            """,
            (operation_id, self.tenant_id, status.value, 
             datetime.now(timezone.utc), result_data)
        )
        
        # Publish status change event
        await self.event_sdk.publish(
            f"operation.status.{status.value}",
            {
                "operation_id": operation_id,
                "tenant_id": self.tenant_id,
                "status": status.value,
                "sdk_name": self.__class__.__name__,
                "context": context.to_dict()
            }
        )
    
    async def get_operation_status(
        self,
        operation_id: str,
        context: SecurityContext
    ) -> Optional[Dict[str, Any]]:
        """Get operation status with caching (DRY pattern)"""
        
        # Check cache first
        cache_key = f"operation_status:{self.tenant_id}:{operation_id}"
        cached_status = await self.cache_sdk.get(cache_key)
        
        if cached_status:
            return cached_status
        
        # Fallback to database
        status_record = await self.db_sdk.query_one(
            """
            SELECT operation_id, status, updated_at, result_data
            FROM operations_status
            WHERE operation_id = %s AND tenant_id = %s
            """,
            (operation_id, self.tenant_id)
        )
        
        if status_record:
            # Update cache
            await self.cache_sdk.set(cache_key, dict(status_record), ttl=3600)
            return dict(status_record)
        
        return None
    
    async def validate_operation_permissions(
        self,
        operation_type: str,
        required_permissions: List[Permission],
        context: SecurityContext
    ) -> bool:
        """Validate operation permissions (DRY pattern)"""
        
        # Check if user has required permissions
        user_permissions = set(context.permissions)
        required_permissions_set = set(required_permissions)
        
        if not required_permissions_set.issubset(user_permissions):
            missing_permissions = required_permissions_set - user_permissions
            logger.warning(
                f"User {context.user_id} missing permissions for {operation_type}: "
                f"{[p.value for p in missing_permissions]}"
            )
            return False
        
        return True
    
    async def publish_operation_event(
        self,
        event_type: str,
        operation_data: Dict[str, Any],
        context: OperationContext
    ):
        """Publish operation event (DRY pattern)"""
        
        event_data = {
            "operation_id": context.operation_id,
            "tenant_id": self.tenant_id,
            "operation_type": context.operation_type,
            "sdk_name": self.__class__.__name__,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": operation_data
        }
        
        await self.event_sdk.publish(f"operations.{event_type}", event_data)
    
    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """Health check implementation required by all Operations SDKs"""
        pass
    
    def get_sdk_info(self) -> Dict[str, Any]:
        """Get SDK information (DRY pattern)"""
        return {
            "sdk_name": self.__class__.__name__,
            "tenant_id": self.tenant_id,
            "version": "1.0.0",
            "plane": "operations",
            "capabilities": self._get_sdk_capabilities()
        }
    
    @abstractmethod
    def _get_sdk_capabilities(self) -> List[str]:
        """Get SDK-specific capabilities"""
        pass
