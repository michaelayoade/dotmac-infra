"""
Base SDK with DRY cross-cutting concerns
Provides common functionality and decorators for all SDKs
"""

import logging
import functools
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable, TypeVar, Awaitable, cast
from uuid import UUID

from sqlalchemy.orm import Session
from pydantic import BaseModel

# Platform SDKs
from dotmac_infra.platform.database_client import DatabaseClient
from dotmac_infra.platform.cache_client import CacheClient
from dotmac_infra.platform.event_bus_client import EventBusClient
from dotmac_infra.platform.observability_client import ObservabilityClient
from dotmac_infra.platform.file_storage_client import FileStorageClient
from dotmac_infra.platform.search_client import SearchClient

# Enums
from dotmac_infra.utils.enums import OperationType, EventType, Permission

logger = logging.getLogger(__name__)

# Generic type variables for helpers and decorators
T = TypeVar("T")
R = TypeVar("R")


class SecurityContext(BaseModel):
    """Security context for operations"""

    user_id: UUID
    tenant_id: str
    roles: List[str]
    permissions: List[str]
    customer_access: Dict[str, List[str]] = {}
    reseller_access: Dict[str, List[str]] = {}
    cached_at: datetime
    expires_at: datetime


class OperationContext(BaseModel):
    """Context for SDK operations"""

    operation_id: str
    user_context: Optional[SecurityContext] = None
    trace_id: Optional[str] = None
    correlation_id: Optional[str] = None
    metadata: Dict[str, Any] = {}


class BaseSDK:
    """
    Base SDK providing common functionality for all SDKs

    Features:
    - Platform SDK composition (database, cache, events, observability, file storage, search)
    - Security context management
    - Audit logging
    - Error handling
    - Caching patterns
    - Event emission
    """

    def __init__(self, tenant_id: str) -> None:
        self.tenant_id = tenant_id

        # Platform SDKs - Cross-cutting concerns
        self.database = DatabaseClient(tenant_id)
        self.cache = CacheClient(tenant_id)
        self.events = EventBusClient(tenant_id)
        self.observability = ObservabilityClient(tenant_id)
        self.file_storage = FileStorageClient(tenant_id)
        self.search = SearchClient(tenant_id)

        # SDK metadata
        self.sdk_name = self.__class__.__name__
        self.sdk_version = "1.0.0"

    # --- Backward-compatible client aliases expected by tests ---
    @property
    def db_client(self) -> DatabaseClient:
        return self.database

    @property
    def cache_client(self) -> CacheClient:
        return self.cache

    @property
    def event_bus_client(self) -> EventBusClient:
        return self.events

    @property
    def observability_client(self) -> ObservabilityClient:
        return self.observability

    @property
    def file_storage_client(self) -> FileStorageClient:
        return self.file_storage

    @property
    def search_client(self) -> SearchClient:
        return self.search

    async def _check_permissions(
        self,
        security_context: SecurityContext,
        required_permission: str,
        resource_type: str,
        resource_id: Optional[str] = None,
    ) -> bool:
        """
        DRY permission checking
        """
        # Check if user has the required permission
        if required_permission in security_context.permissions:
            return True

        # Check role-based permissions
        role_permissions = await self._get_role_permissions(security_context.roles)
        if required_permission in role_permissions:
            return True

        # Check resource-specific access
        if resource_id and resource_type in security_context.customer_access:
            resource_permissions = security_context.customer_access.get(
                resource_type, []
            )
            if required_permission in resource_permissions:
                return True

        return False

    async def _audit_operation(
        self,
        operation_context: OperationContext,
        operation_type: OperationType,
        resource_type: str,
        resource_id: str,
        data: Dict[str, Any],
        success: bool = True,
        error: Optional[str] = None,
    ) -> None:
        """
        DRY audit logging
        """
        audit_data = {
            "operation_id": operation_context.operation_id,
            "user_id": operation_context.user_context.user_id
            if operation_context.user_context
            else None,
            "tenant_id": self.tenant_id,
            "sdk_name": self.sdk_name,
            "operation_type": operation_type.value,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "data": data,
            "success": success,
            "error": error,
            "timestamp": datetime.utcnow(),
            "trace_id": operation_context.trace_id,
            "correlation_id": operation_context.correlation_id,
            "metadata": operation_context.metadata,
        }

        # Log to observability
        await self.observability.log_operation(
            operation=f"{self.sdk_name}.{operation_type.value}",
            data=audit_data,
            level="INFO" if success else "ERROR",
        )

        # Store audit record
        await self.database.create("audit_logs", audit_data)

    async def _emit_event(
        self,
        event_type: EventType,
        resource_type: str,
        resource_id: str,
        data: Dict[str, Any],
        operation_context: OperationContext,
    ) -> None:
        """
        DRY event emission
        """
        event_data = {
            "event_type": event_type.value,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "tenant_id": self.tenant_id,
            "sdk_name": self.sdk_name,
            "data": data,
            "operation_id": operation_context.operation_id,
            "user_id": operation_context.user_context.user_id
            if operation_context.user_context
            else None,
            "timestamp": datetime.utcnow(),
            "trace_id": operation_context.trace_id,
            "correlation_id": operation_context.correlation_id,
        }

        await self.events.publish(event_type.value, event_data)

    async def _cache_get_or_set(
        self,
        cache_key: str,
        factory_func: Callable[..., Awaitable[T]],
        ttl: int = 300,
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """
        DRY caching pattern
        """
        # Try to get from cache
        cached_value = await self.cache.get(cache_key)
        if cached_value is not None:
            return cast(T, cached_value)

        # Generate value using factory function
        value: T = await factory_func(*args, **kwargs)

        # Cache the value
        await self.cache.set(cache_key, value, ttl=ttl)

        return value

    async def _index_for_search(
        self,
        entity_type: str,
        entity_id: str,
        searchable_data: Dict[str, Any],
        db: Session,
    ) -> None:
        """
        DRY search indexing
        """
        try:
            await self.search.index_entity(db, entity_type, entity_id, searchable_data)
        except Exception as e:
            logger.warning(
                f"Failed to index {entity_type}:{entity_id} for search: {str(e)}"
            )

    async def _get_role_permissions(self, roles: List[str]) -> List[str]:
        """Get permissions for roles (cached)"""
        cache_key = f"role_permissions:{':'.join(sorted(roles))}"

        async def fetch_permissions() -> List[str]:
            # This would typically query the database
            # For now, return basic permissions based on roles
            permissions: List[str] = []
            for role in roles:
                if role == "admin":
                    permissions.extend([p.value for p in Permission])
                else:
                    # For non-admin roles in this mock, return no predefined permissions
                    # to avoid referencing enum members that may not exist.
                    permissions.extend([])
            return list(set(permissions))

        return await self._cache_get_or_set(cache_key, fetch_permissions, ttl=900)


# Decorators for DRY cross-cutting concerns


def require_permission(
    required_permission: str, resource_type: str = "generic"
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator to check permissions before executing operation
    """

    def _require_permission_decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        async def _require_permission_wrapper(
            self: "BaseSDK", operation_context: OperationContext, *args: Any, **kwargs: Any
        ) -> Any:
            if not operation_context.user_context:
                raise PermissionError("No security context provided")

            has_permission = await self._check_permissions(
                operation_context.user_context, required_permission, resource_type
            )

            if not has_permission:
                raise PermissionError(f"Missing permission: {required_permission}")

            return await func(self, operation_context, *args, **kwargs)

        # Ensure unique function name for DRY validator clarity
        _require_permission_wrapper.__name__ = f"{func.__name__}__require_permission"
        _require_permission_wrapper.__qualname__ = (
            f"{getattr(func, '__qualname__', func.__name__)}__require_permission"
        )

        return _require_permission_wrapper

    return _require_permission_decorator


def audit_operation(
    operation_type: OperationType, resource_type: str
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator to audit operations
    """

    def _audit_operation_decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        async def _audit_operation_wrapper(
            self: "BaseSDK", operation_context: OperationContext, *args: Any, **kwargs: Any
        ) -> Any:
            resource_id = None
            success = True
            error = None

            try:
                result = await func(self, operation_context, *args, **kwargs)

                # Try to extract resource_id from result
                if hasattr(result, "id"):
                    resource_id = str(result.id)
                elif isinstance(result, dict) and "id" in result:
                    resource_id = str(result["id"])

                return result

            except Exception as e:
                success = False
                error = str(e)
                raise

            finally:
                # Audit the operation
                await self._audit_operation(
                    operation_context,
                    operation_type,
                    resource_type,
                    resource_id or "unknown",
                    {"args": str(args)[:500], "kwargs": str(kwargs)[:500]},
                    success,
                    error,
                )

        # Unique name/qualname for wrapper to avoid duplicate signature issues
        _audit_operation_wrapper.__name__ = f"{func.__name__}__audit_operation"
        _audit_operation_wrapper.__qualname__ = (
            f"{getattr(func, '__qualname__', func.__name__)}__audit_operation"
        )

        return _audit_operation_wrapper

    return _audit_operation_decorator


def emit_event(
    event_type: EventType, resource_type: str
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator to emit events after successful operations
    """

    def _emit_event_decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        async def _emit_event_wrapper(
            self: "BaseSDK", operation_context: OperationContext, *args: Any, **kwargs: Any
        ) -> Any:
            result = await func(self, operation_context, *args, **kwargs)

            # Extract resource_id from result
            resource_id = "unknown"
            if hasattr(result, "id"):
                resource_id = str(result.id)
            elif isinstance(result, dict) and "id" in result:
                resource_id = str(result["id"])

            # Emit event
            await self._emit_event(
                event_type,
                resource_type,
                resource_id,
                {"result": str(result)[:1000]},
                operation_context,
            )

            return result

        # Unique name/qualname
        _emit_event_wrapper.__name__ = f"{func.__name__}__emit_event"
        _emit_event_wrapper.__qualname__ = (
            f"{getattr(func, '__qualname__', func.__name__)}__emit_event"
        )

        return _emit_event_wrapper
