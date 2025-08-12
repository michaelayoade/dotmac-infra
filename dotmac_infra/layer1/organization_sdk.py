"""
Layer 1 Organization SDK - Contract-first implementation using BaseSDK and decorators
Demonstrates DRY principles, compositional architecture, and cross-cutting concerns
"""

from typing import Dict, List, Optional, Any
from uuid import uuid4
from datetime import datetime
from sqlalchemy.orm import Session

from dotmac_infra.utils.base_sdk import (
    BaseSDK, OperationContext, require_permission, audit_operation, emit_event, cache_result, search_indexable, trace_operation
)
from dotmac_infra.utils.enums import (
    OrganizationType, OrganizationStatus, Permission, 
    OperationType, EventType, ResourceType
)


class OrganizationSDK(BaseSDK):
    """
    Layer 1 Organization SDK - Pure compositional architecture
    
    Features:
    - Contract-first implementation (OpenAPI 3.1.0 compliant)
    - BaseSDK composition for cross-cutting concerns
    - DRY decorators for permissions, audit, events, caching, search
    - Platform SDK integration (database, cache, events, observability, search)
    - Standardized enums for type safety
    - Multi-layered security with cached contexts
    - Business registration and tax ID validation
    - Hierarchical organization support (parent/child relationships)
    """

    def __init__(self, tenant_id: str):
        super().__init__(tenant_id)
        self.resource_type = ResourceType.ORGANIZATION.value

    def _validate_tax_id(self, tax_id: str, country: str = "US") -> Dict[str, Any]:
        """
        Basic tax ID validation (in production, use proper validation libraries)
        """
        import re
        
        # Remove all non-alphanumeric characters
        cleaned = re.sub(r'[^A-Za-z0-9]', '', tax_id)
        
        is_valid = False
        formatted = tax_id
        
        if country == "US":
            # US EIN format: XX-XXXXXXX
            if re.match(r'^\d{9}$', cleaned):
                is_valid = True
                formatted = f"{cleaned[:2]}-{cleaned[2:]}"
        
        return {
            "is_valid": is_valid,
            "original": tax_id,
            "cleaned": cleaned,
            "formatted": formatted,
            "country": country
        }

    @require_permission(Permission.ORGANIZATION_CREATE.value, ResourceType.ORGANIZATION.value)
    @audit_operation(OperationType.CREATE, ResourceType.ORGANIZATION.value)
    @emit_event(EventType.ENTITY_CREATED, ResourceType.ORGANIZATION.value)
    @search_indexable("organization", ["name", "legal_name", "registration_number", "tax_id", "industry"])
    @trace_operation("organization.create")
    async def create_organization(
        self, 
        operation_context: OperationContext,
        organization_data: Dict[str, Any],
        db: Session
    ) -> Dict[str, Any]:
        """
        Create a new organization with full cross-cutting concerns
        
        Args:
            operation_context: Security and operation context
            organization_data: Organization information (contract-compliant)
            db: Database session
            
        Returns:
            Created organization data with ID
        """
        # Generate unique ID
        organization_id = str(uuid4())
        
        # Validate tax ID if provided
        tax_validation = None
        if organization_data.get("tax_id"):
            tax_validation = self._validate_tax_id(
                organization_data["tax_id"], 
                organization_data.get("country", "US")
            )
        
        # Validate and normalize data using enums
        normalized_data = {
            "id": organization_id,
            "tenant_id": self.tenant_id,
            "organization_type": OrganizationType(organization_data.get("organization_type", OrganizationType.CORPORATION.value)),
            "status": OrganizationStatus(organization_data.get("status", OrganizationStatus.ACTIVE.value)),
            "name": organization_data["name"],
            "legal_name": organization_data.get("legal_name", organization_data["name"]),
            "dba_name": organization_data.get("dba_name"),
            "registration_number": organization_data.get("registration_number"),
            "tax_id": tax_validation["formatted"] if tax_validation else organization_data.get("tax_id"),
            "industry": organization_data.get("industry"),
            "industry_code": organization_data.get("industry_code"),
            "website": organization_data.get("website"),
            "description": organization_data.get("description"),
            "founded_date": organization_data.get("founded_date"),
            "employee_count": organization_data.get("employee_count"),
            "annual_revenue": organization_data.get("annual_revenue"),
            "country": organization_data.get("country", "US"),
            "timezone": organization_data.get("timezone"),
            "parent_organization_id": organization_data.get("parent_organization_id"),
            "is_verified": organization_data.get("is_verified", False),
            "verification_date": organization_data.get("verification_date"),
            "verification_source": organization_data.get("verification_source"),
            "preferences": organization_data.get("preferences", {}),
            "metadata": organization_data.get("metadata", {}),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "created_by": operation_context.user_context.user_id if operation_context.user_context else None
        }
        
        # Create organization using Platform Database SDK
        organization = await self.database.create("organizations", normalized_data, db)
        
        # Cache the organization for fast retrieval
        cache_key = f"organization:{organization_id}"
        await self.cache.set(cache_key, organization, ttl=600)
        
        # Index for search
        await self._index_for_search(
            "organization", 
            organization_id, 
            {
                "name": normalized_data["name"],
                "legal_name": normalized_data["legal_name"],
                "registration_number": normalized_data.get("registration_number", ""),
                "tax_id": normalized_data.get("tax_id", ""),
                "industry": normalized_data.get("industry", "")
            },
            db
        )
        
        return organization

    @require_permission(Permission.ORGANIZATION_READ.value, ResourceType.ORGANIZATION.value)
    @cache_result(ttl=300, key_func=lambda self, ctx, org_id, db: f"organization:{org_id}")
    @trace_operation("organization.get")
    async def get_organization(
        self, 
        operation_context: OperationContext,
        organization_id: str,
        db: Session
    ) -> Optional[Dict[str, Any]]:
        """
        Get organization by ID with caching and security
        """
        organization = await self.database.get_by_id("organizations", organization_id, db)
        
        if not organization:
            return None
            
        # Check tenant isolation
        if organization.get("tenant_id") != self.tenant_id:
            raise PermissionError("Organization not accessible in current tenant")
            
        return organization

    @require_permission(Permission.ORGANIZATION_UPDATE.value, ResourceType.ORGANIZATION.value)
    @audit_operation(OperationType.UPDATE, ResourceType.ORGANIZATION.value)
    @emit_event(EventType.ENTITY_UPDATED, ResourceType.ORGANIZATION.value)
    @search_indexable("organization", ["name", "legal_name", "registration_number", "tax_id", "industry"])
    @trace_operation("organization.update")
    async def update_organization(
        self, 
        operation_context: OperationContext,
        organization_id: str,
        update_data: Dict[str, Any],
        db: Session
    ) -> Dict[str, Any]:
        """
        Update organization with full cross-cutting concerns
        """
        # Validate organization exists and is accessible
        existing_organization = await self.get_organization(operation_context, organization_id, db)
        if not existing_organization:
            raise ValueError(f"Organization {organization_id} not found")
        
        # Validate tax ID if being updated
        normalized_updates = {}
        if "tax_id" in update_data and update_data["tax_id"]:
            tax_validation = self._validate_tax_id(
                update_data["tax_id"], 
                update_data.get("country", existing_organization.get("country", "US"))
            )
            normalized_updates["tax_id"] = tax_validation["formatted"]
        
        # Normalize other update data using enums
        for key, value in update_data.items():
            if key == "organization_type" and value:
                normalized_updates[key] = OrganizationType(value).value
            elif key == "status" and value:
                normalized_updates[key] = OrganizationStatus(value).value
            elif key != "tax_id":  # Already handled above
                normalized_updates[key] = value
        
        normalized_updates["updated_at"] = datetime.utcnow()
        normalized_updates["updated_by"] = operation_context.user_context.user_id if operation_context.user_context else None
        
        # Update using Platform Database SDK
        updated_organization = await self.database.update("organizations", organization_id, normalized_updates, db)
        
        # Invalidate cache
        cache_key = f"organization:{organization_id}"
        await self.cache.delete(cache_key)
        
        # Re-index for search
        await self._index_for_search(
            "organization", 
            organization_id, 
            {
                "name": updated_organization.get("name", ""),
                "legal_name": updated_organization.get("legal_name", ""),
                "registration_number": updated_organization.get("registration_number", ""),
                "tax_id": updated_organization.get("tax_id", ""),
                "industry": updated_organization.get("industry", "")
            },
            db
        )
        
        return updated_organization

    @require_permission(Permission.ORGANIZATION_DELETE.value, ResourceType.ORGANIZATION.value)
    @audit_operation(OperationType.DELETE, ResourceType.ORGANIZATION.value)
    @emit_event(EventType.ENTITY_DELETED, ResourceType.ORGANIZATION.value)
    @trace_operation("organization.delete")
    async def delete_organization(
        self, 
        operation_context: OperationContext,
        organization_id: str,
        db: Session
    ) -> bool:
        """
        Delete organization with full cross-cutting concerns
        """
        # Validate organization exists and is accessible
        existing_organization = await self.get_organization(operation_context, organization_id, db)
        if not existing_organization:
            raise ValueError(f"Organization {organization_id} not found")
        
        # Check for child organizations
        child_organizations = await self.database.list("organizations", {
            "parent_organization_id": organization_id,
            "status": OrganizationStatus.ACTIVE.value
        }, limit=1, offset=0, db=db)
        
        if child_organizations:
            raise ValueError("Cannot delete organization with active child organizations")
        
        # Soft delete using Platform Database SDK
        await self.database.update("organizations", organization_id, {
            "status": OrganizationStatus.DELETED.value,
            "deleted_at": datetime.utcnow(),
            "deleted_by": operation_context.user_context.user_id if operation_context.user_context else None
        }, db)
        
        # Remove from cache
        cache_key = f"organization:{organization_id}"
        await self.cache.delete(cache_key)
        
        # Remove from search index
        await self.search.delete_entity(db, "organization", organization_id)
        
        return True

    @require_permission(Permission.ORGANIZATION_READ.value, ResourceType.ORGANIZATION.value)
    @cache_result(ttl=180)
    @trace_operation("organization.list")
    async def list_organizations(
        self, 
        operation_context: OperationContext,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 50,
        offset: int = 0,
        db: Session = None
    ) -> Dict[str, Any]:
        """
        List organizations with filtering, pagination, and caching
        """
        # Build query filters
        query_filters = {"tenant_id": self.tenant_id}
        if filters:
            # Apply enum validation for filter values
            if "organization_type" in filters:
                query_filters["organization_type"] = OrganizationType(filters["organization_type"]).value
            if "status" in filters:
                query_filters["status"] = OrganizationStatus(filters["status"]).value
            else:
                query_filters["status"] = OrganizationStatus.ACTIVE.value  # Default to active
            
            # Business filters
            if "industry" in filters:
                query_filters["industry"] = filters["industry"]
            if "country" in filters:
                query_filters["country"] = filters["country"]
            if "parent_organization_id" in filters:
                query_filters["parent_organization_id"] = filters["parent_organization_id"]
            if "is_verified" in filters:
                query_filters["is_verified"] = filters["is_verified"]
        
        # Get organizations using Platform Database SDK
        organizations = await self.database.list("organizations", query_filters, limit, offset, db)
        
        return {
            "organizations": organizations,
            "total": len(organizations),
            "limit": limit,
            "offset": offset
        }

    @require_permission(Permission.ORGANIZATION_READ.value, ResourceType.ORGANIZATION.value)
    @trace_operation("organization.search")
    async def search_organizations(
        self, 
        operation_context: OperationContext,
        search_query: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 20,
        db: Session = None
    ) -> Dict[str, Any]:
        """
        Search organizations using Platform Search SDK
        """
        from dotmac_infra.platform.search_client import SearchQuery
        
        # Build search query
        search_filters = {"tenant_id": self.tenant_id}
        if filters:
            search_filters.update(filters)
        
        query = SearchQuery(
            query=search_query,
            entity_types=["organization"],
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

    @require_permission(Permission.ORGANIZATION_READ.value, ResourceType.ORGANIZATION.value)
    @trace_operation("organization.get_hierarchy")
    async def get_organization_hierarchy(
        self, 
        operation_context: OperationContext,
        organization_id: str,
        include_children: bool = True,
        include_parent: bool = True,
        db: Session = None
    ) -> Dict[str, Any]:
        """
        Get organization with its hierarchical relationships
        """
        # Get the main organization
        organization = await self.get_organization(operation_context, organization_id, db)
        if not organization:
            raise ValueError(f"Organization {organization_id} not found")
        
        hierarchy = {
            "organization": organization,
            "parent": None,
            "children": []
        }
        
        # Get parent organization
        if include_parent and organization.get("parent_organization_id"):
            parent = await self.get_organization(
                operation_context,
                organization["parent_organization_id"],
                db
            )
            hierarchy["parent"] = parent
        
        # Get child organizations
        if include_children:
            children = await self.database.list("organizations", {
                "parent_organization_id": organization_id,
                "status": OrganizationStatus.ACTIVE.value
            }, limit=100, offset=0, db=db)
            hierarchy["children"] = children
        
        return hierarchy

    @require_permission(Permission.ORGANIZATION_UPDATE.value, ResourceType.ORGANIZATION.value)
    @audit_operation(OperationType.VERIFY, ResourceType.ORGANIZATION.value)
    @emit_event(EventType.ENTITY_UPDATED, ResourceType.ORGANIZATION.value)
    @trace_operation("organization.verify")
    async def verify_organization(
        self, 
        operation_context: OperationContext,
        organization_id: str,
        verification_data: Dict[str, Any],
        db: Session
    ) -> Dict[str, Any]:
        """
        Verify organization with business registration and tax validation
        """
        # Get existing organization
        existing_organization = await self.get_organization(operation_context, organization_id, db)
        if not existing_organization:
            raise ValueError(f"Organization {organization_id} not found")
        
        # Update with verification data
        verification_updates = {
            "is_verified": True,
            "verification_date": datetime.utcnow(),
            "verification_source": verification_data.get("source", "manual"),
            "updated_at": datetime.utcnow(),
            "updated_by": operation_context.user_context.user_id if operation_context.user_context else None
        }
        
        # Apply any corrections from verification
        if "verified_data" in verification_data:
            verified = verification_data["verified_data"]
            verification_updates.update({
                "legal_name": verified.get("legal_name", existing_organization["legal_name"]),
                "registration_number": verified.get("registration_number", existing_organization["registration_number"]),
                "tax_id": verified.get("tax_id", existing_organization["tax_id"]),
                "industry": verified.get("industry", existing_organization["industry"]),
                "industry_code": verified.get("industry_code", existing_organization["industry_code"])
            })
        
        # Update organization
        updated_organization = await self.database.update("organizations", organization_id, verification_updates, db)
        
        # Invalidate cache
        cache_key = f"organization:{organization_id}"
        await self.cache.delete(cache_key)
        
        return updated_organization

    @require_permission(Permission.ORGANIZATION_READ.value, ResourceType.ORGANIZATION.value)
    @trace_operation("organization.get_contacts")
    async def get_organization_contacts(
        self, 
        operation_context: OperationContext,
        organization_id: str,
        contact_role: Optional[str] = None,
        db: Session = None
    ) -> List[Dict[str, Any]]:
        """
        Get all contacts associated with an organization
        """
        # Build query filters
        query_filters = {"organization_id": organization_id}
        if contact_role:
            query_filters["role"] = contact_role
        
        # Get organization contacts from junction table
        organization_contacts = await self.database.list("organization_contacts", 
            query_filters, limit=100, offset=0, db=db)
        
        # Get contact details using Contact SDK
        # Note: This would require importing ContactSDK, demonstrating SDK composition
        contacts = []
        for org_contact in organization_contacts:
            # In production, use ContactSDK to get contact details
            contact_data = {
                "contact_id": org_contact["contact_id"],
                "role": org_contact["role"],
                "is_primary": org_contact.get("is_primary", False),
                "start_date": org_contact.get("start_date"),
                "end_date": org_contact.get("end_date")
            }
            contacts.append(contact_data)
        
        return contacts

    @require_permission(Permission.ORGANIZATION_UPDATE.value, ResourceType.ORGANIZATION.value)
    @audit_operation(OperationType.UPDATE, ResourceType.ORGANIZATION.value)
    @emit_event(EventType.ENTITY_UPDATED, ResourceType.ORGANIZATION.value)
    @trace_operation("organization.add_contact")
    async def add_organization_contact(
        self, 
        operation_context: OperationContext,
        organization_id: str,
        contact_id: str,
        role: str = "employee",
        is_primary: bool = False,
        db: Session = None
    ) -> Dict[str, Any]:
        """
        Add a contact to an organization
        """
        # Validate organization exists
        organization = await self.get_organization(operation_context, organization_id, db)
        if not organization:
            raise ValueError(f"Organization {organization_id} not found")
        
        # Create organization-contact relationship
        relationship = await self.database.create("organization_contacts", {
            "organization_id": organization_id,
            "contact_id": contact_id,
            "role": role,
            "is_primary": is_primary,
            "start_date": datetime.utcnow(),
            "created_at": datetime.utcnow(),
            "created_by": operation_context.user_context.user_id if operation_context.user_context else None
        }, db)
        
        return relationship
