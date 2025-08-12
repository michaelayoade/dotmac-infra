"""
Layer 1 Email SDK - Contract-first implementation using BaseSDK and decorators
Demonstrates DRY principles, compositional architecture, and cross-cutting concerns
"""

from typing import Dict, List, Optional, Any
from uuid import UUID, uuid4
from datetime import datetime
import re
from sqlalchemy.orm import Session

from dotmac_infra.utils.base_sdk import (
    BaseSDK, OperationContext, SecurityContext,
    require_permission, audit_operation, emit_event, cache_result, search_indexable, trace_operation
)
from dotmac_infra.utils.enums import (
    EmailType, EmailStatus, Permission, 
    OperationType, EventType, ResourceType
)


class EmailSDK(BaseSDK):
    """
    Layer 1 Email SDK - Pure compositional architecture
    
    Features:
    - Contract-first implementation (OpenAPI 3.1.0 compliant)
    - BaseSDK composition for cross-cutting concerns
    - DRY decorators for permissions, audit, events, caching, search
    - Platform SDK integration (database, cache, events, observability, search)
    - Standardized enums for type safety
    - Multi-layered security with cached contexts
    - Email validation and verification
    - Integration with Communications SDK
    """

    def __init__(self, tenant_id: str):
        super().__init__(tenant_id)
        self.resource_type = ResourceType.EMAIL.value

    def _validate_email(self, email_address: str) -> Dict[str, Any]:
        """
        Validate email address format
        """
        # Basic email validation regex
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        is_valid = re.match(email_pattern, email_address) is not None
        
        # Extract domain
        domain = None
        if '@' in email_address:
            domain = email_address.split('@')[1].lower()
        
        # Normalize email (lowercase)
        normalized = email_address.lower().strip()
        
        return {
            "is_valid": is_valid,
            "original": email_address,
            "normalized": normalized,
            "domain": domain
        }

    @require_permission(Permission.EMAIL_CREATE.value, ResourceType.EMAIL.value)
    @audit_operation(OperationType.CREATE, ResourceType.EMAIL.value)
    @emit_event(EventType.ENTITY_CREATED, ResourceType.EMAIL.value)
    @search_indexable("email", ["email_address", "domain"])
    @trace_operation("email.create")
    async def create_email(
        self, 
        operation_context: OperationContext,
        email_data: Dict[str, Any],
        db: Session
    ) -> Dict[str, Any]:
        """
        Create a new email address with full cross-cutting concerns
        
        Args:
            operation_context: Security and operation context
            email_data: Email information (contract-compliant)
            db: Database session
            
        Returns:
            Created email data with ID
        """
        # Generate unique ID
        email_id = str(uuid4())
        
        # Validate email address
        validation = self._validate_email(email_data["email_address"])
        if not validation["is_valid"]:
            raise ValueError(f"Invalid email address format: {email_data['email_address']}")
        
        # Validate and normalize data using enums
        normalized_data = {
            "id": email_id,
            "tenant_id": self.tenant_id,
            "email_type": EmailType(email_data.get("email_type", EmailType.PERSONAL.value)),
            "status": EmailStatus(email_data.get("status", EmailStatus.ACTIVE.value)),
            "email_address": validation["normalized"],
            "domain": validation["domain"],
            "is_primary": email_data.get("is_primary", False),
            "is_verified": email_data.get("is_verified", False),
            "verification_date": email_data.get("verification_date"),
            "verification_method": email_data.get("verification_method"),
            "can_receive_marketing": email_data.get("can_receive_marketing", True),
            "can_receive_notifications": email_data.get("can_receive_notifications", True),
            "bounce_count": 0,
            "last_bounce_date": None,
            "last_sent_date": None,
            "last_opened_date": None,
            "notes": email_data.get("notes"),
            "metadata": email_data.get("metadata", {}),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "created_by": operation_context.user_context.user_id if operation_context.user_context else None
        }
        
        # Create email using Platform Database SDK
        email = await self.database.create("emails", normalized_data, db)
        
        # Cache the email for fast retrieval
        cache_key = f"email:{email_id}"
        await self.cache.set(cache_key, email, ttl=600)
        
        # Index for search
        await self._index_for_search(
            "email", 
            email_id, 
            {
                "email_address": normalized_data["email_address"],
                "domain": normalized_data["domain"],
                "email_type": normalized_data["email_type"]
            },
            db
        )
        
        return email

    @require_permission(Permission.EMAIL_READ.value, ResourceType.EMAIL.value)
    @cache_result(ttl=300, key_func=lambda self, ctx, email_id, db: f"email:{email_id}")
    @trace_operation("email.get")
    async def get_email(
        self, 
        operation_context: OperationContext,
        email_id: str,
        db: Session
    ) -> Optional[Dict[str, Any]]:
        """
        Get email by ID with caching and security
        """
        email = await self.database.get_by_id("emails", email_id, db)
        
        if not email:
            return None
            
        # Check tenant isolation
        if email.get("tenant_id") != self.tenant_id:
            raise PermissionError("Email not accessible in current tenant")
            
        return email

    @require_permission(Permission.EMAIL_READ.value, ResourceType.EMAIL.value)
    @cache_result(ttl=300, key_func=lambda self, ctx, email_address, db: f"email:address:{email_address}")
    @trace_operation("email.get_by_address")
    async def get_email_by_address(
        self, 
        operation_context: OperationContext,
        email_address: str,
        db: Session
    ) -> Optional[Dict[str, Any]]:
        """
        Get email by email address with caching and security
        """
        # Normalize email address
        validation = self._validate_email(email_address)
        normalized_email = validation["normalized"]
        
        email = await self.database.get_by_field("emails", "email_address", normalized_email, db)
        
        if not email:
            return None
            
        # Check tenant isolation
        if email.get("tenant_id") != self.tenant_id:
            raise PermissionError("Email not accessible in current tenant")
            
        return email

    @require_permission(Permission.EMAIL_UPDATE.value, ResourceType.EMAIL.value)
    @audit_operation(OperationType.UPDATE, ResourceType.EMAIL.value)
    @emit_event(EventType.ENTITY_UPDATED, ResourceType.EMAIL.value)
    @search_indexable("email", ["email_address", "domain"])
    @trace_operation("email.update")
    async def update_email(
        self, 
        operation_context: OperationContext,
        email_id: str,
        update_data: Dict[str, Any],
        db: Session
    ) -> Dict[str, Any]:
        """
        Update email with full cross-cutting concerns
        """
        # Validate email exists and is accessible
        existing_email = await self.get_email(operation_context, email_id, db)
        if not existing_email:
            raise ValueError(f"Email {email_id} not found")
        
        # Validate email address if being updated
        normalized_updates = {}
        if "email_address" in update_data:
            validation = self._validate_email(update_data["email_address"])
            if not validation["is_valid"]:
                raise ValueError(f"Invalid email address format: {update_data['email_address']}")
            
            normalized_updates.update({
                "email_address": validation["normalized"],
                "domain": validation["domain"]
            })
        
        # Normalize other update data using enums
        for key, value in update_data.items():
            if key == "email_type" and value:
                normalized_updates[key] = EmailType(value).value
            elif key == "status" and value:
                normalized_updates[key] = EmailStatus(value).value
            elif key != "email_address":  # Already handled above
                normalized_updates[key] = value
        
        normalized_updates["updated_at"] = datetime.utcnow()
        normalized_updates["updated_by"] = operation_context.user_context.user_id if operation_context.user_context else None
        
        # Update using Platform Database SDK
        updated_email = await self.database.update("emails", email_id, normalized_updates, db)
        
        # Invalidate cache
        cache_key = f"email:{email_id}"
        await self.cache.delete(cache_key)
        
        # Also invalidate address-based cache if email changed
        if "email_address" in update_data:
            old_cache_key = f"email:address:{existing_email['email_address']}"
            await self.cache.delete(old_cache_key)
        
        # Re-index for search
        await self._index_for_search(
            "email", 
            email_id, 
            {
                "email_address": updated_email.get("email_address", ""),
                "domain": updated_email.get("domain", ""),
                "email_type": updated_email.get("email_type", "")
            },
            db
        )
        
        return updated_email

    @require_permission(Permission.EMAIL_DELETE.value, ResourceType.EMAIL.value)
    @audit_operation(OperationType.DELETE, ResourceType.EMAIL.value)
    @emit_event(EventType.ENTITY_DELETED, ResourceType.EMAIL.value)
    @trace_operation("email.delete")
    async def delete_email(
        self, 
        operation_context: OperationContext,
        email_id: str,
        db: Session
    ) -> bool:
        """
        Delete email with full cross-cutting concerns
        """
        # Validate email exists and is accessible
        existing_email = await self.get_email(operation_context, email_id, db)
        if not existing_email:
            raise ValueError(f"Email {email_id} not found")
        
        # Soft delete using Platform Database SDK
        await self.database.update("emails", email_id, {
            "status": EmailStatus.DELETED.value,
            "deleted_at": datetime.utcnow(),
            "deleted_by": operation_context.user_context.user_id if operation_context.user_context else None
        }, db)
        
        # Remove from cache
        cache_key = f"email:{email_id}"
        await self.cache.delete(cache_key)
        
        # Remove address-based cache
        address_cache_key = f"email:address:{existing_email['email_address']}"
        await self.cache.delete(address_cache_key)
        
        # Remove from search index
        await self.search.delete_entity(db, "email", email_id)
        
        return True

    @require_permission(Permission.EMAIL_READ.value, ResourceType.EMAIL.value)
    @cache_result(ttl=180)
    @trace_operation("email.list")
    async def list_emails(
        self, 
        operation_context: OperationContext,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 50,
        offset: int = 0,
        db: Session = None
    ) -> Dict[str, Any]:
        """
        List emails with filtering, pagination, and caching
        """
        # Build query filters
        query_filters = {"tenant_id": self.tenant_id}
        if filters:
            # Apply enum validation for filter values
            if "email_type" in filters:
                query_filters["email_type"] = EmailType(filters["email_type"]).value
            if "status" in filters:
                query_filters["status"] = EmailStatus(filters["status"]).value
            else:
                query_filters["status"] = EmailStatus.ACTIVE.value  # Default to active
            
            # Domain filter
            if "domain" in filters:
                query_filters["domain"] = filters["domain"]
            
            # Capability filters
            if "can_receive_marketing" in filters:
                query_filters["can_receive_marketing"] = filters["can_receive_marketing"]
            if "can_receive_notifications" in filters:
                query_filters["can_receive_notifications"] = filters["can_receive_notifications"]
            if "is_verified" in filters:
                query_filters["is_verified"] = filters["is_verified"]
        
        # Get emails using Platform Database SDK
        emails = await self.database.list("emails", query_filters, limit, offset, db)
        
        return {
            "emails": emails,
            "total": len(emails),
            "limit": limit,
            "offset": offset
        }

    @require_permission(Permission.EMAIL_READ.value, ResourceType.EMAIL.value)
    @trace_operation("email.search")
    async def search_emails(
        self, 
        operation_context: OperationContext,
        search_query: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 20,
        db: Session = None
    ) -> Dict[str, Any]:
        """
        Search emails using Platform Search SDK
        """
        from dotmac_infra.platform.search_client import SearchQuery
        
        # Build search query
        search_filters = {"tenant_id": self.tenant_id}
        if filters:
            search_filters.update(filters)
        
        query = SearchQuery(
            query=search_query,
            entity_types=["email"],
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

    @require_permission(Permission.EMAIL_UPDATE.value, ResourceType.EMAIL.value)
    @audit_operation(OperationType.VERIFY, ResourceType.EMAIL.value)
    @emit_event(EventType.ENTITY_UPDATED, ResourceType.EMAIL.value)
    @trace_operation("email.verify")
    async def verify_email(
        self, 
        operation_context: OperationContext,
        email_id: str,
        verification_token: str,
        db: Session = None
    ) -> Dict[str, Any]:
        """
        Verify email address with verification token
        """
        # Get existing email
        existing_email = await self.get_email(operation_context, email_id, db)
        if not existing_email:
            raise ValueError(f"Email {email_id} not found")
        
        # In production, verify the token against stored verification
        # For now, simulate verification
        verification_updates = {
            "is_verified": True,
            "verification_date": datetime.utcnow(),
            "verification_method": "email_link",
            "updated_at": datetime.utcnow(),
            "updated_by": operation_context.user_context.user_id if operation_context.user_context else None
        }
        
        # Update email
        updated_email = await self.database.update("emails", email_id, verification_updates, db)
        
        # Invalidate cache
        cache_key = f"email:{email_id}"
        await self.cache.delete(cache_key)
        
        address_cache_key = f"email:address:{existing_email['email_address']}"
        await self.cache.delete(address_cache_key)
        
        return updated_email

    @require_permission(Permission.EMAIL_READ.value, ResourceType.EMAIL.value)
    @trace_operation("email.get_by_entity")
    async def get_emails_for_entity(
        self, 
        operation_context: OperationContext,
        entity_type: str,
        entity_id: str,
        email_type: Optional[str] = None,
        db: Session = None
    ) -> List[Dict[str, Any]]:
        """
        Get all emails for a specific entity (customer, contact, organization)
        """
        # Build query filters
        query_filters = {
            "tenant_id": self.tenant_id,
            "status": EmailStatus.ACTIVE.value
        }
        
        if email_type:
            query_filters["email_type"] = EmailType(email_type).value
        
        # Get entity emails from junction table
        entity_emails = await self.database.list(f"{entity_type}_emails", {
            f"{entity_type}_id": entity_id
        }, limit=50, offset=0, db=db)
        
        # Get email details
        emails = []
        for entity_email in entity_emails:
            email = await self.get_email(
                operation_context,
                entity_email["email_id"],
                db
            )
            if email:
                email["entity_role"] = entity_email.get("role", "general")
                emails.append(email)
        
        return emails

    @require_permission(Permission.EMAIL_CREATE.value, ResourceType.EMAIL.value)
    @audit_operation(OperationType.CREATE, ResourceType.EMAIL.value)
    @trace_operation("email.send_verification")
    async def send_verification_email(
        self, 
        operation_context: OperationContext,
        email_id: str,
        verification_url_base: str,
        db: Session = None
    ) -> Dict[str, Any]:
        """
        Send verification email to email address
        """
        # Get email
        email = await self.get_email(operation_context, email_id, db)
        if not email:
            raise ValueError(f"Email {email_id} not found")
        
        # Generate verification token (in production, store securely)
        import secrets
        verification_token = secrets.token_urlsafe(32)
        
        # Create verification URL
        verification_url = f"{verification_url_base}?token={verification_token}&email_id={email_id}"
        
        # Send via Communications SDK (Email)
        # In production, integrate with Email SDK
        # await self.communications.email.send(...)
        
        # Store verification attempt
        await self.database.create("email_verifications", {
            "email_id": email_id,
            "verification_token": verification_token,  # In production, hash this
            "verification_url": verification_url,
            "expires_at": datetime.utcnow().timestamp() + 86400,  # 24 hours
            "created_at": datetime.utcnow(),
            "created_by": operation_context.user_context.user_id if operation_context.user_context else None
        }, db)
        
        return {
            "success": True,
            "email_address": email["email_address"],
            "verification_url": verification_url,
            "expires_in": 86400
        }

    @require_permission(Permission.EMAIL_UPDATE.value, ResourceType.EMAIL.value)
    @audit_operation(OperationType.UPDATE, ResourceType.EMAIL.value)
    @trace_operation("email.record_bounce")
    async def record_email_bounce(
        self, 
        operation_context: OperationContext,
        email_id: str,
        bounce_type: str,
        bounce_reason: str,
        db: Session = None
    ) -> Dict[str, Any]:
        """
        Record email bounce for deliverability tracking
        """
        # Get email
        email = await self.get_email(operation_context, email_id, db)
        if not email:
            raise ValueError(f"Email {email_id} not found")
        
        # Update bounce count and date
        bounce_updates = {
            "bounce_count": email.get("bounce_count", 0) + 1,
            "last_bounce_date": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "updated_by": operation_context.user_context.user_id if operation_context.user_context else None
        }
        
        # If too many bounces, mark as bounced
        if bounce_updates["bounce_count"] >= 5:
            bounce_updates["status"] = EmailStatus.BOUNCED.value
        
        # Update email
        updated_email = await self.database.update("emails", email_id, bounce_updates, db)
        
        # Record bounce event
        await self.database.create("email_bounces", {
            "email_id": email_id,
            "bounce_type": bounce_type,
            "bounce_reason": bounce_reason,
            "bounce_date": datetime.utcnow(),
            "created_at": datetime.utcnow()
        }, db)
        
        # Invalidate cache
        cache_key = f"email:{email_id}"
        await self.cache.delete(cache_key)
        
        return updated_email
