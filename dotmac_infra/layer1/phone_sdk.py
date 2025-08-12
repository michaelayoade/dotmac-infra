"""
Layer 1 Phone SDK - Contract-first implementation using BaseSDK and decorators
Demonstrates DRY principles, compositional architecture, and cross-cutting concerns
"""

from typing import Dict, List, Optional, Any
from uuid import uuid4
from datetime import datetime
import re
from sqlalchemy.orm import Session

from dotmac_infra.utils.base_sdk import (
    BaseSDK, OperationContext, require_permission, audit_operation, emit_event, cache_result, search_indexable, trace_operation
)
from dotmac_infra.utils.enums import (
    PhoneType, PhoneStatus, Permission, 
    OperationType, EventType, ResourceType
)


class PhoneSDK(BaseSDK):
    """
    Layer 1 Phone SDK - Pure compositional architecture
    
    Features:
    - Contract-first implementation (OpenAPI 3.1.0 compliant)
    - BaseSDK composition for cross-cutting concerns
    - DRY decorators for permissions, audit, events, caching, search
    - Platform SDK integration (database, cache, events, observability, search)
    - Standardized enums for type safety
    - Multi-layered security with cached contexts
    - Phone number validation and formatting
    - SMS capability integration
    """

    def __init__(self, tenant_id: str):
        super().__init__(tenant_id)
        self.resource_type = ResourceType.PHONE.value

    def _validate_phone_number(self, phone_number: str) -> Dict[str, Any]:
        """
        Validate and normalize phone number
        Basic validation - in production, use libphonenumber
        """
        # Remove all non-digit characters except +
        cleaned = re.sub(r'[^\d+]', '', phone_number)
        
        # Basic validation patterns
        patterns = {
            'US': r'^\+?1?[2-9]\d{2}[2-9]\d{2}\d{4}$',
            'INTL': r'^\+\d{1,3}\d{4,14}$'
        }
        
        is_valid = False
        country_code = None
        formatted = cleaned
        
        # Check US format
        if re.match(patterns['US'], cleaned):
            is_valid = True
            country_code = 'US'
            # Format as +1-XXX-XXX-XXXX
            digits = re.sub(r'[^\d]', '', cleaned)
            if len(digits) == 10:
                formatted = f"+1-{digits[:3]}-{digits[3:6]}-{digits[6:]}"
            elif len(digits) == 11 and digits[0] == '1':
                formatted = f"+1-{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
        
        # Check international format
        elif re.match(patterns['INTL'], cleaned):
            is_valid = True
            country_code = 'INTL'
            formatted = cleaned
        
        return {
            "is_valid": is_valid,
            "original": phone_number,
            "cleaned": cleaned,
            "formatted": formatted,
            "country_code": country_code
        }

    @require_permission(Permission.PHONE_CREATE.value, ResourceType.PHONE.value)
    @audit_operation(OperationType.CREATE, ResourceType.PHONE.value)
    @emit_event(EventType.ENTITY_CREATED, ResourceType.PHONE.value)
    @search_indexable("phone", ["phone_number", "formatted_number", "extension"])
    @trace_operation("phone.create")
    async def create_phone(
        self, 
        operation_context: OperationContext,
        phone_data: Dict[str, Any],
        db: Session
    ) -> Dict[str, Any]:
        """
        Create a new phone number with full cross-cutting concerns
        
        Args:
            operation_context: Security and operation context
            phone_data: Phone information (contract-compliant)
            db: Database session
            
        Returns:
            Created phone data with ID
        """
        # Generate unique ID
        phone_id = str(uuid4())
        
        # Validate phone number
        validation = self._validate_phone_number(phone_data["phone_number"])
        if not validation["is_valid"]:
            raise ValueError(f"Invalid phone number format: {phone_data['phone_number']}")
        
        # Validate and normalize data using enums
        normalized_data = {
            "id": phone_id,
            "tenant_id": self.tenant_id,
            "phone_type": PhoneType(phone_data.get("phone_type", PhoneType.MOBILE.value)),
            "status": PhoneStatus(phone_data.get("status", PhoneStatus.ACTIVE.value)),
            "phone_number": validation["original"],
            "formatted_number": validation["formatted"],
            "country_code": validation["country_code"],
            "extension": phone_data.get("extension"),
            "is_primary": phone_data.get("is_primary", False),
            "is_verified": phone_data.get("is_verified", False),
            "verification_date": phone_data.get("verification_date"),
            "verification_method": phone_data.get("verification_method"),
            "can_sms": phone_data.get("can_sms", True),
            "can_voice": phone_data.get("can_voice", True),
            "preferred_contact_time": phone_data.get("preferred_contact_time"),
            "notes": phone_data.get("notes"),
            "metadata": phone_data.get("metadata", {}),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "created_by": operation_context.user_context.user_id if operation_context.user_context else None
        }
        
        # Create phone using Platform Database SDK
        phone = await self.database.create("phones", normalized_data, db)
        
        # Cache the phone for fast retrieval
        cache_key = f"phone:{phone_id}"
        await self.cache.set(cache_key, phone, ttl=600)
        
        # Index for search
        await self._index_for_search(
            "phone", 
            phone_id, 
            {
                "phone_number": normalized_data["phone_number"],
                "formatted_number": normalized_data["formatted_number"],
                "extension": normalized_data.get("extension", ""),
                "phone_type": normalized_data["phone_type"]
            },
            db
        )
        
        return phone

    @require_permission(Permission.PHONE_READ.value, ResourceType.PHONE.value)
    @cache_result(ttl=300, key_func=lambda self, ctx, phone_id, db: f"phone:{phone_id}")
    @trace_operation("phone.get")
    async def get_phone(
        self, 
        operation_context: OperationContext,
        phone_id: str,
        db: Session
    ) -> Optional[Dict[str, Any]]:
        """
        Get phone by ID with caching and security
        """
        phone = await self.database.get_by_id("phones", phone_id, db)
        
        if not phone:
            return None
            
        # Check tenant isolation
        if phone.get("tenant_id") != self.tenant_id:
            raise PermissionError("Phone not accessible in current tenant")
            
        return phone

    @require_permission(Permission.PHONE_UPDATE.value, ResourceType.PHONE.value)
    @audit_operation(OperationType.UPDATE, ResourceType.PHONE.value)
    @emit_event(EventType.ENTITY_UPDATED, ResourceType.PHONE.value)
    @search_indexable("phone", ["phone_number", "formatted_number", "extension"])
    @trace_operation("phone.update")
    async def update_phone(
        self, 
        operation_context: OperationContext,
        phone_id: str,
        update_data: Dict[str, Any],
        db: Session
    ) -> Dict[str, Any]:
        """
        Update phone with full cross-cutting concerns
        """
        # Validate phone exists and is accessible
        existing_phone = await self.get_phone(operation_context, phone_id, db)
        if not existing_phone:
            raise ValueError(f"Phone {phone_id} not found")
        
        # Validate phone number if being updated
        normalized_updates = {}
        if "phone_number" in update_data:
            validation = self._validate_phone_number(update_data["phone_number"])
            if not validation["is_valid"]:
                raise ValueError(f"Invalid phone number format: {update_data['phone_number']}")
            
            normalized_updates.update({
                "phone_number": validation["original"],
                "formatted_number": validation["formatted"],
                "country_code": validation["country_code"]
            })
        
        # Normalize other update data using enums
        for key, value in update_data.items():
            if key == "phone_type" and value:
                normalized_updates[key] = PhoneType(value).value
            elif key == "status" and value:
                normalized_updates[key] = PhoneStatus(value).value
            elif key != "phone_number":  # Already handled above
                normalized_updates[key] = value
        
        normalized_updates["updated_at"] = datetime.utcnow()
        normalized_updates["updated_by"] = operation_context.user_context.user_id if operation_context.user_context else None
        
        # Update using Platform Database SDK
        updated_phone = await self.database.update("phones", phone_id, normalized_updates, db)
        
        # Invalidate cache
        cache_key = f"phone:{phone_id}"
        await self.cache.delete(cache_key)
        
        # Re-index for search
        await self._index_for_search(
            "phone", 
            phone_id, 
            {
                "phone_number": updated_phone.get("phone_number", ""),
                "formatted_number": updated_phone.get("formatted_number", ""),
                "extension": updated_phone.get("extension", ""),
                "phone_type": updated_phone.get("phone_type", "")
            },
            db
        )
        
        return updated_phone

    @require_permission(Permission.PHONE_DELETE.value, ResourceType.PHONE.value)
    @audit_operation(OperationType.DELETE, ResourceType.PHONE.value)
    @emit_event(EventType.ENTITY_DELETED, ResourceType.PHONE.value)
    @trace_operation("phone.delete")
    async def delete_phone(
        self, 
        operation_context: OperationContext,
        phone_id: str,
        db: Session
    ) -> bool:
        """
        Delete phone with full cross-cutting concerns
        """
        # Validate phone exists and is accessible
        existing_phone = await self.get_phone(operation_context, phone_id, db)
        if not existing_phone:
            raise ValueError(f"Phone {phone_id} not found")
        
        # Soft delete using Platform Database SDK
        await self.database.update("phones", phone_id, {
            "status": PhoneStatus.DELETED.value,
            "deleted_at": datetime.utcnow(),
            "deleted_by": operation_context.user_context.user_id if operation_context.user_context else None
        }, db)
        
        # Remove from cache
        cache_key = f"phone:{phone_id}"
        await self.cache.delete(cache_key)
        
        # Remove from search index
        await self.search.delete_entity(db, "phone", phone_id)
        
        return True

    @require_permission(Permission.PHONE_READ.value, ResourceType.PHONE.value)
    @cache_result(ttl=180)
    @trace_operation("phone.list")
    async def list_phones(
        self, 
        operation_context: OperationContext,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 50,
        offset: int = 0,
        db: Session = None
    ) -> Dict[str, Any]:
        """
        List phones with filtering, pagination, and caching
        """
        # Build query filters
        query_filters = {"tenant_id": self.tenant_id}
        if filters:
            # Apply enum validation for filter values
            if "phone_type" in filters:
                query_filters["phone_type"] = PhoneType(filters["phone_type"]).value
            if "status" in filters:
                query_filters["status"] = PhoneStatus(filters["status"]).value
            else:
                query_filters["status"] = PhoneStatus.ACTIVE.value  # Default to active
            
            # Capability filters
            if "can_sms" in filters:
                query_filters["can_sms"] = filters["can_sms"]
            if "can_voice" in filters:
                query_filters["can_voice"] = filters["can_voice"]
            if "is_verified" in filters:
                query_filters["is_verified"] = filters["is_verified"]
        
        # Get phones using Platform Database SDK
        phones = await self.database.list("phones", query_filters, limit, offset, db)
        
        return {
            "phones": phones,
            "total": len(phones),
            "limit": limit,
            "offset": offset
        }

    @require_permission(Permission.PHONE_READ.value, ResourceType.PHONE.value)
    @trace_operation("phone.search")
    async def search_phones(
        self, 
        operation_context: OperationContext,
        search_query: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 20,
        db: Session = None
    ) -> Dict[str, Any]:
        """
        Search phones using Platform Search SDK
        """
        from dotmac_infra.platform.search_client import SearchQuery
        
        # Build search query
        search_filters = {"tenant_id": self.tenant_id}
        if filters:
            search_filters.update(filters)
        
        query = SearchQuery(
            query=search_query,
            entity_types=["phone"],
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

    @require_permission(Permission.PHONE_UPDATE.value, ResourceType.PHONE.value)
    @audit_operation(OperationType.VERIFY, ResourceType.PHONE.value)
    @emit_event(EventType.ENTITY_UPDATED, ResourceType.PHONE.value)
    @trace_operation("phone.verify")
    async def verify_phone(
        self, 
        operation_context: OperationContext,
        phone_id: str,
        verification_code: str,
        verification_method: str = "sms",
        db: Session = None
    ) -> Dict[str, Any]:
        """
        Verify phone number with verification code
        """
        # Get existing phone
        existing_phone = await self.get_phone(operation_context, phone_id, db)
        if not existing_phone:
            raise ValueError(f"Phone {phone_id} not found")
        
        # In production, verify the code against stored verification
        # For now, simulate verification
        verification_updates = {
            "is_verified": True,
            "verification_date": datetime.utcnow(),
            "verification_method": verification_method,
            "updated_at": datetime.utcnow(),
            "updated_by": operation_context.user_context.user_id if operation_context.user_context else None
        }
        
        # Update phone
        updated_phone = await self.database.update("phones", phone_id, verification_updates, db)
        
        # Invalidate cache
        cache_key = f"phone:{phone_id}"
        await self.cache.delete(cache_key)
        
        return updated_phone

    @require_permission(Permission.PHONE_READ.value, ResourceType.PHONE.value)
    @trace_operation("phone.get_by_entity")
    async def get_phones_for_entity(
        self, 
        operation_context: OperationContext,
        entity_type: str,
        entity_id: str,
        phone_type: Optional[str] = None,
        db: Session = None
    ) -> List[Dict[str, Any]]:
        """
        Get all phones for a specific entity (customer, contact, organization)
        """
        # Build query filters
        query_filters = {
            "tenant_id": self.tenant_id,
            "status": PhoneStatus.ACTIVE.value
        }
        
        if phone_type:
            query_filters["phone_type"] = PhoneType(phone_type).value
        
        # Get entity phones from junction table
        entity_phones = await self.database.list(f"{entity_type}_phones", {
            f"{entity_type}_id": entity_id
        }, limit=50, offset=0, db=db)
        
        # Get phone details
        phones = []
        for entity_phone in entity_phones:
            phone = await self.get_phone(
                operation_context,
                entity_phone["phone_id"],
                db
            )
            if phone:
                phone["entity_role"] = entity_phone.get("role", "general")
                phones.append(phone)
        
        return phones

    @require_permission(Permission.PHONE_CREATE.value, ResourceType.PHONE.value)
    @audit_operation(OperationType.CREATE, ResourceType.PHONE.value)
    @trace_operation("phone.send_verification")
    async def send_verification_code(
        self, 
        operation_context: OperationContext,
        phone_id: str,
        method: str = "sms",
        db: Session = None
    ) -> Dict[str, Any]:
        """
        Send verification code to phone number
        """
        # Get phone
        phone = await self.get_phone(operation_context, phone_id, db)
        if not phone:
            raise ValueError(f"Phone {phone_id} not found")
        
        if method == "sms" and not phone.get("can_sms", True):
            raise ValueError("SMS not supported for this phone number")
        
        # Generate verification code (in production, store securely)
        import random
        verification_code = f"{random.randint(100000, 999999)}"
        
        # Send via Communications SDK (SMS)
        # In production, integrate with SMS SDK
        # await self.communications.sms.send(...)
        
        # Store verification attempt
        await self.database.create("phone_verifications", {
            "phone_id": phone_id,
            "verification_code": verification_code,  # In production, hash this
            "method": method,
            "expires_at": datetime.utcnow().timestamp() + 300,  # 5 minutes
            "created_at": datetime.utcnow(),
            "created_by": operation_context.user_context.user_id if operation_context.user_context else None
        }, db)
        
        return {
            "success": True,
            "method": method,
            "phone_number": phone["formatted_number"],
            "expires_in": 300
        }
