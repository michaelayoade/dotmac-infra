"""
Layer 1 Contact SDK - Contract-first implementation using BaseSDK and decorators
Demonstrates DRY principles, compositional architecture, and cross-cutting concerns
"""

from typing import Dict, Optional, Any
from uuid import uuid4
from datetime import datetime
from sqlalchemy.orm import Session

from dotmac_infra.utils.base_sdk import (
    BaseSDK, OperationContext, require_permission, audit_operation, emit_event, cache_result, search_indexable, trace_operation
)
from dotmac_infra.utils.enums import (
    ContactType, ContactRole, ContactStatus, Permission, 
    OperationType, EventType, ResourceType
)
# Model imports removed - use external injection or interfaces


class ContactSDK(BaseSDK):
    """
    Layer 1 Contact SDK - Pure compositional architecture
    
    Features:
    - Contract-first implementation (OpenAPI 3.1.0 compliant)
    - BaseSDK composition for cross-cutting concerns
    - DRY decorators for permissions, audit, events, caching, search
    - Platform SDK integration (database, cache, events, observability, search)
    - Standardized enums for type safety
    - Multi-layered security with cached contexts
    """

    def __init__(self, tenant_id: str):
        super().__init__(tenant_id)
        self.resource_type = ResourceType.CONTACT.value

    @require_permission(Permission.CONTACT_CREATE.value, ResourceType.CONTACT.value)
    @audit_operation(OperationType.CREATE, ResourceType.CONTACT.value)
    @emit_event(EventType.ENTITY_CREATED, ResourceType.CONTACT.value)
    @search_indexable("contact", ["first_name", "last_name", "email", "phone"])
    @trace_operation("contact.create")
    async def create_contact(
        self, 
        operation_context: OperationContext,
        contact_data: Dict[str, Any],
        db: Session
    ) -> Dict[str, Any]:
        """
        Create a new contact with full cross-cutting concerns
        
        Args:
            operation_context: Security and operation context
            contact_data: Contact information (contract-compliant)
            db: Database session
            
        Returns:
            Created contact data with ID
        """
        # Generate unique ID
        contact_id = str(uuid4())
        
        # Validate and normalize data using enums
        normalized_data = {
            "id": contact_id,
            "tenant_id": self.tenant_id,
            "contact_type": ContactType(contact_data.get("contact_type", ContactType.INDIVIDUAL.value)),
            "role": ContactRole(contact_data.get("role", ContactRole.PRIMARY.value)),
            "status": ContactStatus(contact_data.get("status", ContactStatus.ACTIVE.value)),
            "first_name": contact_data["first_name"],
            "last_name": contact_data["last_name"],
            "email": contact_data.get("email"),
            "phone": contact_data.get("phone"),
            "title": contact_data.get("title"),
            "company": contact_data.get("company"),
            "department": contact_data.get("department"),
            "notes": contact_data.get("notes"),
            "preferences": contact_data.get("preferences", {}),
            "metadata": contact_data.get("metadata", {}),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "created_by": operation_context.user_context.user_id if operation_context.user_context else None
        }
        
        # Create contact using Platform Database SDK
        contact = await self.database.create("contacts", normalized_data, db)
        
        # Cache the contact for fast retrieval
        cache_key = f"contact:{contact_id}"
        await self.cache.set(cache_key, contact, ttl=600)
        
        # Index for search (handled by decorator)
        await self._index_for_search(
            "contact", 
            contact_id, 
            {
                "first_name": normalized_data["first_name"],
                "last_name": normalized_data["last_name"],
                "email": normalized_data.get("email", ""),
                "phone": normalized_data.get("phone", ""),
                "company": normalized_data.get("company", "")
            },
            db
        )
        
        return contact

    @require_permission(Permission.CONTACT_READ.value, ResourceType.CONTACT.value)
    @cache_result(ttl=300, key_func=lambda self, ctx, contact_id, db: f"contact:{contact_id}")
    @trace_operation("contact.get")
    async def get_contact(
        self, 
        operation_context: OperationContext,
        contact_id: str,
        db: Session
    ) -> Optional[Dict[str, Any]]:
        """
        Get contact by ID with caching and security
        """
        # Try cache first (handled by decorator)
        contact = await self.database.get_by_id("contacts", contact_id, db)
        
        if not contact:
            return None
            
        # Check tenant isolation
        if contact.get("tenant_id") != self.tenant_id:
            raise PermissionError("Contact not accessible in current tenant")
            
        return contact

    @require_permission(Permission.CONTACT_UPDATE.value, ResourceType.CONTACT.value)
    @audit_operation(OperationType.UPDATE, ResourceType.CONTACT.value)
    @emit_event(EventType.ENTITY_UPDATED, ResourceType.CONTACT.value)
    @search_indexable("contact", ["first_name", "last_name", "email", "phone"])
    @trace_operation("contact.update")
    async def update_contact(
        self, 
        operation_context: OperationContext,
        contact_id: str,
        update_data: Dict[str, Any],
        db: Session
    ) -> Dict[str, Any]:
        """
        Update contact with full cross-cutting concerns
        """
        # Validate contact exists and is accessible
        existing_contact = await self.get_contact(operation_context, contact_id, db)
        if not existing_contact:
            raise ValueError(f"Contact {contact_id} not found")
        
        # Normalize update data using enums
        normalized_updates = {}
        for key, value in update_data.items():
            if key == "contact_type" and value:
                normalized_updates[key] = ContactType(value).value
            elif key == "role" and value:
                normalized_updates[key] = ContactRole(value).value
            elif key == "status" and value:
                normalized_updates[key] = ContactStatus(value).value
            else:
                normalized_updates[key] = value
        
        normalized_updates["updated_at"] = datetime.utcnow()
        normalized_updates["updated_by"] = operation_context.user_context.user_id if operation_context.user_context else None
        
        # Update using Platform Database SDK
        updated_contact = await self.database.update("contacts", contact_id, normalized_updates, db)
        
        # Invalidate cache
        cache_key = f"contact:{contact_id}"
        await self.cache.delete(cache_key)
        
        # Re-index for search
        await self._index_for_search(
            "contact", 
            contact_id, 
            {
                "first_name": updated_contact.get("first_name", ""),
                "last_name": updated_contact.get("last_name", ""),
                "email": updated_contact.get("email", ""),
                "phone": updated_contact.get("phone", ""),
                "company": updated_contact.get("company", "")
            },
            db
        )
        
        return updated_contact

    @require_permission(Permission.CONTACT_DELETE.value, ResourceType.CONTACT.value)
    @audit_operation(OperationType.DELETE, ResourceType.CONTACT.value)
    @emit_event(EventType.ENTITY_DELETED, ResourceType.CONTACT.value)
    @trace_operation("contact.delete")
    async def delete_contact(
        self, 
        operation_context: OperationContext,
        contact_id: str,
        db: Session
    ) -> bool:
        """
        Delete contact with full cross-cutting concerns
        """
        # Validate contact exists and is accessible
        existing_contact = await self.get_contact(operation_context, contact_id, db)
        if not existing_contact:
            raise ValueError(f"Contact {contact_id} not found")
        
        # Soft delete using Platform Database SDK
        await self.database.update("contacts", contact_id, {
            "status": ContactStatus.DELETED.value,
            "deleted_at": datetime.utcnow(),
            "deleted_by": operation_context.user_context.user_id if operation_context.user_context else None
        }, db)
        
        # Remove from cache
        cache_key = f"contact:{contact_id}"
        await self.cache.delete(cache_key)
        
        # Remove from search index
        await self.search.delete_entity(db, "contact", contact_id)
        
        return True

    @require_permission(Permission.CONTACT_READ.value, ResourceType.CONTACT.value)
    @cache_result(ttl=180)
    @trace_operation("contact.list")
    async def list_contacts(
        self, 
        operation_context: OperationContext,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 50,
        offset: int = 0,
        db: Session = None
    ) -> Dict[str, Any]:
        """
        List contacts with filtering, pagination, and caching
        """
        # Build query filters
        query_filters = {"tenant_id": self.tenant_id}
        if filters:
            # Apply enum validation for filter values
            if "contact_type" in filters:
                query_filters["contact_type"] = ContactType(filters["contact_type"]).value
            if "role" in filters:
                query_filters["role"] = ContactRole(filters["role"]).value
            if "status" in filters:
                query_filters["status"] = ContactStatus(filters["status"]).value
            else:
                query_filters["status"] = ContactStatus.ACTIVE.value  # Default to active
        
        # Get contacts using Platform Database SDK
        contacts = await self.database.list("contacts", query_filters, limit, offset, db)
        
        return {
            "contacts": contacts,
            "total": len(contacts),
            "limit": limit,
            "offset": offset
        }

    @require_permission(Permission.CONTACT_READ.value, ResourceType.CONTACT.value)
    @trace_operation("contact.search")
    async def search_contacts(
        self, 
        operation_context: OperationContext,
        search_query: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 20,
        db: Session = None
    ) -> Dict[str, Any]:
        """
        Search contacts using Platform Search SDK
        """
        from dotmac_infra.platform.search_client import SearchQuery
        
        # Build search query
        search_filters = {"tenant_id": self.tenant_id}
        if filters:
            search_filters.update(filters)
        
        query = SearchQuery(
            query=search_query,
            entity_types=["contact"],
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
