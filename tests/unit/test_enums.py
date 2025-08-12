"""
Unit tests for dotmac_infra enums
"""
import pytest
from dotmac_infra.utils.enums import (
    Permission, ContactType, ContactRole, ContactStatus,
    AddressType, AddressStatus, PhoneType, PhoneStatus,
    EmailType, EmailStatus, OrganizationType, OrganizationStatus,
    AccessLevel, ResourceType, EventType, OperationType,
    Status, Priority, LogLevel, ValidationLevel, CacheStrategy
)


class TestPermissionEnum:
    """Test Permission enum"""
    
    def test_permission_values(self):
        """Test permission enum values"""
        assert Permission.CONTACT_CREATE == "contact.create"
        assert Permission.CONTACT_READ == "contact.read"
        assert Permission.ADDRESS_CREATE == "address.create"
        assert Permission.PLATFORM_ADMIN == "platform.admin"
    
    def test_permission_membership(self):
        """Test permission enum membership"""
        assert Permission.CONTACT_CREATE in Permission
        assert "invalid.permission" not in [p.value for p in Permission]


class TestContactEnums:
    """Test contact-related enums"""
    
    def test_contact_type(self):
        """Test ContactType enum"""
        assert ContactType.INDIVIDUAL == "individual"
        assert ContactType.BUSINESS == "business"
        assert ContactType.ORGANIZATION == "organization"
    
    def test_contact_role(self):
        """Test ContactRole enum"""
        assert ContactRole.PRIMARY == "primary"
        assert ContactRole.BILLING == "billing"
        assert ContactRole.TECHNICAL == "technical"
    
    def test_contact_status(self):
        """Test ContactStatus enum"""
        assert ContactStatus.ACTIVE == "active"
        assert ContactStatus.VERIFIED == "verified"
        assert ContactStatus.DELETED == "deleted"


class TestAddressEnums:
    """Test address-related enums"""
    
    def test_address_type(self):
        """Test AddressType enum"""
        assert AddressType.HOME == "home"
        assert AddressType.WORK == "work"
        assert AddressType.BILLING == "billing"
    
    def test_address_status(self):
        """Test AddressStatus enum"""
        assert AddressStatus.ACTIVE == "active"
        assert AddressStatus.VERIFIED == "verified"
        assert AddressStatus.DELETED == "deleted"


class TestPhoneEnums:
    """Test phone-related enums"""
    
    def test_phone_type(self):
        """Test PhoneType enum"""
        assert PhoneType.MOBILE == "mobile"
        assert PhoneType.HOME == "home"
        assert PhoneType.WORK == "work"
    
    def test_phone_status(self):
        """Test PhoneStatus enum"""
        assert PhoneStatus.ACTIVE == "active"
        assert PhoneStatus.VERIFIED == "verified"
        assert PhoneStatus.DELETED == "deleted"


class TestEmailEnums:
    """Test email-related enums"""
    
    def test_email_type(self):
        """Test EmailType enum"""
        assert EmailType.PERSONAL == "personal"
        assert EmailType.WORK == "work"
        assert EmailType.BILLING == "billing"
    
    def test_email_status(self):
        """Test EmailStatus enum"""
        assert EmailStatus.ACTIVE == "active"
        assert EmailStatus.VERIFIED == "verified"
        assert EmailStatus.BOUNCED == "bounced"


class TestOrganizationEnums:
    """Test organization-related enums"""
    
    def test_organization_type(self):
        """Test OrganizationType enum"""
        assert OrganizationType.CORPORATION == "corporation"
        assert OrganizationType.LLC == "llc"
        assert OrganizationType.NON_PROFIT == "non_profit"
    
    def test_organization_status(self):
        """Test OrganizationStatus enum"""
        assert OrganizationStatus.ACTIVE == "active"
        assert OrganizationStatus.VERIFIED == "verified"
        assert OrganizationStatus.SUSPENDED == "suspended"


class TestSystemEnums:
    """Test system-level enums"""
    
    def test_operation_type(self):
        """Test OperationType enum"""
        assert OperationType.CREATE == "create"
        assert OperationType.READ == "read"
        assert OperationType.VERIFY == "verify"
        assert OperationType.DELETE == "delete"
    
    def test_event_type(self):
        """Test EventType enum"""
        assert EventType.ENTITY_CREATED == "entity.created"
        assert EventType.ENTITY_UPDATED == "entity.updated"
        assert EventType.ERROR_OCCURRED == "error.occurred"
    
    def test_resource_type(self):
        """Test ResourceType enum"""
        assert ResourceType.CONTACT == "contact"
        assert ResourceType.ADDRESS == "address"
        assert ResourceType.DATABASE == "database"
    
    def test_access_level(self):
        """Test AccessLevel enum"""
        assert AccessLevel.PUBLIC == "public"
        assert AccessLevel.CONFIDENTIAL == "confidential"
        assert AccessLevel.TOP_SECRET == "top_secret"


class TestUtilityEnums:
    """Test utility enums"""
    
    def test_status(self):
        """Test generic Status enum"""
        assert Status.ACTIVE == "active"
        assert Status.PENDING == "pending"
        assert Status.DELETED == "deleted"
    
    def test_priority(self):
        """Test Priority enum"""
        assert Priority.LOW == "low"
        assert Priority.HIGH == "high"
        assert Priority.CRITICAL == "critical"
    
    def test_log_level(self):
        """Test LogLevel enum"""
        assert LogLevel.DEBUG == "debug"
        assert LogLevel.ERROR == "error"
        assert LogLevel.CRITICAL == "critical"
    
    def test_validation_level(self):
        """Test ValidationLevel enum"""
        assert ValidationLevel.BASIC == "basic"
        assert ValidationLevel.STRICT == "strict"
        assert ValidationLevel.ENTERPRISE == "enterprise"
    
    def test_cache_strategy(self):
        """Test CacheStrategy enum"""
        assert CacheStrategy.NO_CACHE == "no_cache"
        assert CacheStrategy.SHORT_TERM == "short_term"
        assert CacheStrategy.PERMANENT == "permanent"


class TestEnumConsistency:
    """Test enum consistency and completeness"""
    
    def test_status_enums_consistency(self):
        """Test that status enums have consistent values"""
        status_enums = [ContactStatus, AddressStatus, PhoneStatus, EmailStatus, OrganizationStatus]
        
        # All status enums should have ACTIVE, INACTIVE, DELETED
        for enum_class in status_enums:
            assert hasattr(enum_class, 'ACTIVE')
            assert hasattr(enum_class, 'INACTIVE') 
            assert hasattr(enum_class, 'DELETED')
            assert enum_class.ACTIVE.value == "active"
            assert enum_class.INACTIVE.value == "inactive"
            assert enum_class.DELETED.value == "deleted"
    
    def test_type_enums_have_other(self):
        """Test that type enums have OTHER option"""
        type_enums = [ContactType, AddressType, PhoneType, EmailType, OrganizationType]
        
        for enum_class in type_enums:
            if hasattr(enum_class, 'OTHER'):
                assert enum_class.OTHER.value == "other"
    
    def test_enum_values_are_lowercase(self):
        """Test that enum values follow lowercase convention"""
        from dotmac_infra.utils import enums
        
        for attr_name in dir(enums):
            attr = getattr(enums, attr_name)
            if hasattr(attr, '__members__'):  # It's an enum
                for member in attr:
                    # Should be lowercase with underscores or dots
                    assert member.value.islower() or '.' in member.value
