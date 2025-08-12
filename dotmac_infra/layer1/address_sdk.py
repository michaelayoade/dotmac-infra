"""
Layer 1 Address SDK - Contract-first implementation using BaseSDK and decorators
Demonstrates DRY principles, compositional architecture, and cross-cutting concerns
"""

from typing import Dict, List, Optional, Any
from uuid import UUID, uuid4
from datetime import datetime
from sqlalchemy.orm import Session

from dotmac_infra.utils.base_sdk import (
    BaseSDK, OperationContext, SecurityContext,
    require_permission, audit_operation, emit_event, cache_result, search_indexable, trace_operation
)
from dotmac_infra.utils.enums import (
    AddressType, AddressStatus, Permission, 
    OperationType, EventType, ResourceType
)


class AddressSDK(BaseSDK):
    """
    Layer 1 Address SDK - Pure compositional architecture
    
    Features:
    - Contract-first implementation (OpenAPI 3.1.0 compliant)
    - BaseSDK composition for cross-cutting concerns
    - DRY decorators for permissions, audit, events, caching, search
    - Platform SDK integration (database, cache, events, observability, search)
    - Standardized enums for type safety
    - Multi-layered security with cached contexts
    - Address validation and geocoding support
    """

    def __init__(self, tenant_id: str):
        super().__init__(tenant_id)
        self.resource_type = ResourceType.ADDRESS.value

    @require_permission(Permission.ADDRESS_CREATE.value, ResourceType.ADDRESS.value)
    @audit_operation(OperationType.CREATE, ResourceType.ADDRESS.value)
    @emit_event(EventType.ENTITY_CREATED, ResourceType.ADDRESS.value)
    @search_indexable("address", ["street_address", "city", "state", "postal_code", "country"])
    @trace_operation("address.create")
    async def create_address(
        self, 
        operation_context: OperationContext,
        address_data: Dict[str, Any],
        db: Session
    ) -> Dict[str, Any]:
        """
        Create a new address with full cross-cutting concerns
        
        Args:
            operation_context: Security and operation context
            address_data: Address information (contract-compliant)
            db: Database session
            
        Returns:
            Created address data with ID
        """
        # Generate unique ID
        address_id = str(uuid4())
        
        # Validate and normalize data using enums
        normalized_data = {
            "id": address_id,
            "tenant_id": self.tenant_id,
            "address_type": AddressType(address_data.get("address_type", AddressType.BILLING.value)),
            "status": AddressStatus(address_data.get("status", AddressStatus.ACTIVE.value)),
            "street_address": address_data["street_address"],
            "street_address_2": address_data.get("street_address_2"),
            "city": address_data["city"],
            "state": address_data.get("state"),
            "postal_code": address_data["postal_code"],
            "country": address_data.get("country", "US"),
            "latitude": address_data.get("latitude"),
            "longitude": address_data.get("longitude"),
            "is_verified": address_data.get("is_verified", False),
            "verification_date": address_data.get("verification_date"),
            "verification_source": address_data.get("verification_source"),
            "is_primary": address_data.get("is_primary", False),
            "delivery_instructions": address_data.get("delivery_instructions"),
            "timezone": address_data.get("timezone"),
            "metadata": address_data.get("metadata", {}),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "created_by": operation_context.user_context.user_id if operation_context.user_context else None
        }
        
        # Create address using Platform Database SDK
        address = await self.database.create("addresses", normalized_data, db)
        
        # Cache the address for fast retrieval
        cache_key = f"address:{address_id}"
        await self.cache.set(cache_key, address, ttl=600)
        
        # Index for search
        await self._index_for_search(
            "address", 
            address_id, 
            {
                "street_address": normalized_data["street_address"],
                "city": normalized_data["city"],
                "state": normalized_data.get("state", ""),
                "postal_code": normalized_data["postal_code"],
                "country": normalized_data["country"]
            },
            db
        )
        
        return address

    @require_permission(Permission.ADDRESS_READ.value, ResourceType.ADDRESS.value)
    @cache_result(ttl=300, key_func=lambda self, ctx, address_id, db: f"address:{address_id}")
    @trace_operation("address.get")
    async def get_address(
        self, 
        operation_context: OperationContext,
        address_id: str,
        db: Session
    ) -> Optional[Dict[str, Any]]:
        """
        Get address by ID with caching and security
        """
        address = await self.database.get_by_id("addresses", address_id, db)
        
        if not address:
            return None
            
        # Check tenant isolation
        if address.get("tenant_id") != self.tenant_id:
            raise PermissionError("Address not accessible in current tenant")
            
        return address

    @require_permission(Permission.ADDRESS_UPDATE.value, ResourceType.ADDRESS.value)
    @audit_operation(OperationType.UPDATE, ResourceType.ADDRESS.value)
    @emit_event(EventType.ENTITY_UPDATED, ResourceType.ADDRESS.value)
    @search_indexable("address", ["street_address", "city", "state", "postal_code", "country"])
    @trace_operation("address.update")
    async def update_address(
        self, 
        operation_context: OperationContext,
        address_id: str,
        update_data: Dict[str, Any],
        db: Session
    ) -> Dict[str, Any]:
        """
        Update address with full cross-cutting concerns
        """
        # Validate address exists and is accessible
        existing_address = await self.get_address(operation_context, address_id, db)
        if not existing_address:
            raise ValueError(f"Address {address_id} not found")
        
        # Normalize update data using enums
        normalized_updates = {}
        for key, value in update_data.items():
            if key == "address_type" and value:
                normalized_updates[key] = AddressType(value).value
            elif key == "status" and value:
                normalized_updates[key] = AddressStatus(value).value
            else:
                normalized_updates[key] = value
        
        normalized_updates["updated_at"] = datetime.utcnow()
        normalized_updates["updated_by"] = operation_context.user_context.user_id if operation_context.user_context else None
        
        # Update using Platform Database SDK
        updated_address = await self.database.update("addresses", address_id, normalized_updates, db)
        
        # Invalidate cache
        cache_key = f"address:{address_id}"
        await self.cache.delete(cache_key)
        
        # Re-index for search
        await self._index_for_search(
            "address", 
            address_id, 
            {
                "street_address": updated_address.get("street_address", ""),
                "city": updated_address.get("city", ""),
                "state": updated_address.get("state", ""),
                "postal_code": updated_address.get("postal_code", ""),
                "country": updated_address.get("country", "")
            },
            db
        )
        
        return updated_address

    @require_permission(Permission.ADDRESS_DELETE.value, ResourceType.ADDRESS.value)
    @audit_operation(OperationType.DELETE, ResourceType.ADDRESS.value)
    @emit_event(EventType.ENTITY_DELETED, ResourceType.ADDRESS.value)
    @trace_operation("address.delete")
    async def delete_address(
        self, 
        operation_context: OperationContext,
        address_id: str,
        db: Session
    ) -> bool:
        """
        Delete address with full cross-cutting concerns
        """
        # Validate address exists and is accessible
        existing_address = await self.get_address(operation_context, address_id, db)
        if not existing_address:
            raise ValueError(f"Address {address_id} not found")
        
        # Soft delete using Platform Database SDK
        await self.database.update("addresses", address_id, {
            "status": AddressStatus.DELETED.value,
            "deleted_at": datetime.utcnow(),
            "deleted_by": operation_context.user_context.user_id if operation_context.user_context else None
        }, db)
        
        # Remove from cache
        cache_key = f"address:{address_id}"
        await self.cache.delete(cache_key)
        
        # Remove from search index
        await self.search.delete_entity(db, "address", address_id)
        
        return True

    @require_permission(Permission.ADDRESS_READ.value, ResourceType.ADDRESS.value)
    @cache_result(ttl=180)
    @trace_operation("address.list")
    async def list_addresses(
        self, 
        operation_context: OperationContext,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 50,
        offset: int = 0,
        db: Session = None
    ) -> Dict[str, Any]:
        """
        List addresses with filtering, pagination, and caching
        """
        # Build query filters
        query_filters = {"tenant_id": self.tenant_id}
        if filters:
            # Apply enum validation for filter values
            if "address_type" in filters:
                query_filters["address_type"] = AddressType(filters["address_type"]).value
            if "status" in filters:
                query_filters["status"] = AddressStatus(filters["status"]).value
            else:
                query_filters["status"] = AddressStatus.ACTIVE.value  # Default to active
            
            # Geographic filters
            if "country" in filters:
                query_filters["country"] = filters["country"]
            if "state" in filters:
                query_filters["state"] = filters["state"]
            if "city" in filters:
                query_filters["city"] = filters["city"]
        
        # Get addresses using Platform Database SDK
        addresses = await self.database.list("addresses", query_filters, limit, offset, db)
        
        return {
            "addresses": addresses,
            "total": len(addresses),
            "limit": limit,
            "offset": offset
        }

    @require_permission(Permission.ADDRESS_READ.value, ResourceType.ADDRESS.value)
    @trace_operation("address.search")
    async def search_addresses(
        self, 
        operation_context: OperationContext,
        search_query: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 20,
        db: Session = None
    ) -> Dict[str, Any]:
        """
        Search addresses using Platform Search SDK
        """
        from dotmac_infra.platform.search_client import SearchQuery
        
        # Build search query
        search_filters = {"tenant_id": self.tenant_id}
        if filters:
            search_filters.update(filters)
        
        query = SearchQuery(
            query=search_query,
            entity_types=["address"],
            filters=search_filters,
            limit=limit,
            fuzzy=True
        )
        
        # Search using Platform Search SDK
        results = await self.search.search(db, query)
        
        return {
            "results": results.results,
            "total": results.total_count,
            "took_ms": results.took_ms
        }

    @require_permission(Permission.ADDRESS_UPDATE.value, ResourceType.ADDRESS.value)
    @audit_operation(OperationType.UPDATE, ResourceType.ADDRESS.value)
    @trace_operation("address.verify")
    async def verify_address(
        self, 
        operation_context: OperationContext,
        address_id: str,
        verification_data: Dict[str, Any],
        db: Session
    ) -> Dict[str, Any]:
        """
        Verify address with geocoding and validation services
        """
        # Get existing address
        existing_address = await self.get_address(operation_context, address_id, db)
        if not existing_address:
            raise ValueError(f"Address {address_id} not found")
        
        # Update with verification data
        verification_updates = {
            "is_verified": True,
            "verification_date": datetime.utcnow(),
            "verification_source": verification_data.get("source", "manual"),
            "latitude": verification_data.get("latitude"),
            "longitude": verification_data.get("longitude"),
            "timezone": verification_data.get("timezone"),
            "updated_at": datetime.utcnow(),
            "updated_by": operation_context.user_context.user_id if operation_context.user_context else None
        }
        
        # Apply any address corrections from verification
        if "corrected_address" in verification_data:
            corrected = verification_data["corrected_address"]
            verification_updates.update({
                "street_address": corrected.get("street_address", existing_address["street_address"]),
                "city": corrected.get("city", existing_address["city"]),
                "state": corrected.get("state", existing_address["state"]),
                "postal_code": corrected.get("postal_code", existing_address["postal_code"]),
                "country": corrected.get("country", existing_address["country"])
            })
        
        # Update address
        updated_address = await self.database.update("addresses", address_id, verification_updates, db)
        
        # Invalidate cache
        cache_key = f"address:{address_id}"
        await self.cache.delete(cache_key)
        
        return updated_address

    @require_permission(Permission.ADDRESS_READ.value, ResourceType.ADDRESS.value)
    @trace_operation("address.get_by_entity")
    async def get_addresses_for_entity(
        self, 
        operation_context: OperationContext,
        entity_type: str,
        entity_id: str,
        address_type: Optional[str] = None,
        db: Session = None
    ) -> List[Dict[str, Any]]:
        """
        Get all addresses for a specific entity (customer, contact, organization)
        """
        # Build query filters
        query_filters = {
            "tenant_id": self.tenant_id,
            "status": AddressStatus.ACTIVE.value
        }
        
        if address_type:
            query_filters["address_type"] = AddressType(address_type).value
        
        # Get entity addresses from junction table
        entity_addresses = await self.database.list(f"{entity_type}_addresses", {
            f"{entity_type}_id": entity_id
        }, limit=50, offset=0, db=db)
        
        # Get address details
        addresses = []
        for entity_address in entity_addresses:
            address = await self.get_address(
                operation_context,
                entity_address["address_id"],
                db
            )
            if address:
                address["entity_role"] = entity_address.get("role", "general")
                addresses.append(address)
        
        return addresses
