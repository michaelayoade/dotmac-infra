"""
Internal enums for dotmac_infra package
"""
from enum import Enum


class Permission(str, Enum):
    """Permission enumeration for RBAC system"""
    # Contact permissions
    CONTACT_CREATE = "contact.create"
    CONTACT_READ = "contact.read"
    CONTACT_UPDATE = "contact.update"
    CONTACT_DELETE = "contact.delete"
    
    # Address permissions
    ADDRESS_CREATE = "address.create"
    ADDRESS_READ = "address.read"
    ADDRESS_UPDATE = "address.update"
    ADDRESS_DELETE = "address.delete"
    
    # Phone permissions
    PHONE_CREATE = "phone.create"
    PHONE_READ = "phone.read"
    PHONE_UPDATE = "phone.update"
    PHONE_DELETE = "phone.delete"
    
    # Email permissions
    EMAIL_CREATE = "email.create"
    EMAIL_READ = "email.read"
    EMAIL_UPDATE = "email.update"
    EMAIL_DELETE = "email.delete"
    
    # Organization permissions
    ORGANIZATION_CREATE = "organization.create"
    ORGANIZATION_READ = "organization.read"
    ORGANIZATION_UPDATE = "organization.update"
    ORGANIZATION_DELETE = "organization.delete"
    
    # Platform permissions
    PLATFORM_READ = "platform.read"
    PLATFORM_WRITE = "platform.write"
    PLATFORM_ADMIN = "platform.admin"


class ContactType(str, Enum):
    """Contact type enumeration"""
    INDIVIDUAL = "individual"
    BUSINESS = "business"
    ORGANIZATION = "organization"


class ContactRole(str, Enum):
    """Contact role enumeration for business relationships"""
    PRIMARY = "primary"
    SECONDARY = "secondary"
    BILLING = "billing"
    TECHNICAL = "technical"
    EMERGENCY = "emergency"
    LEGAL = "legal"
    SALES = "sales"
    SUPPORT = "support"


class ContactStatus(str, Enum):
    """Contact status enumeration"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"
    SUSPENDED = "suspended"
    DELETED = "deleted"
    VERIFIED = "verified"
    UNVERIFIED = "unverified"


class AddressType(str, Enum):
    """Address type enumeration"""
    HOME = "home"
    WORK = "work"
    BILLING = "billing"
    SHIPPING = "shipping"
    OTHER = "other"


class AddressStatus(str, Enum):
    """Address status enumeration"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"
    VERIFIED = "verified"
    UNVERIFIED = "unverified"
    DELETED = "deleted"


class PhoneType(str, Enum):
    """Phone type enumeration"""
    MOBILE = "mobile"
    HOME = "home"
    WORK = "work"
    FAX = "fax"
    OTHER = "other"


class PhoneStatus(str, Enum):
    """Phone status enumeration"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"
    VERIFIED = "verified"
    UNVERIFIED = "unverified"
    DELETED = "deleted"


class EmailType(str, Enum):
    """Email type enumeration"""
    PERSONAL = "personal"
    WORK = "work"
    BILLING = "billing"
    SUPPORT = "support"
    OTHER = "other"


class EmailStatus(str, Enum):
    """Email status enumeration"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"
    VERIFIED = "verified"
    UNVERIFIED = "unverified"
    DELETED = "deleted"
    BOUNCED = "bounced"


class OrganizationType(str, Enum):
    """Organization type enumeration"""
    CORPORATION = "corporation"
    LLC = "llc"
    PARTNERSHIP = "partnership"
    SOLE_PROPRIETORSHIP = "sole_proprietorship"
    NON_PROFIT = "non_profit"
    GOVERNMENT = "government"
    OTHER = "other"


class OrganizationStatus(str, Enum):
    """Organization status enumeration"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"
    SUSPENDED = "suspended"
    DELETED = "deleted"
    VERIFIED = "verified"
    UNVERIFIED = "unverified"


class Status(str, Enum):
    """Generic status enumeration"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"
    SUSPENDED = "suspended"
    DELETED = "deleted"


class Priority(str, Enum):
    """Priority enumeration"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class LogLevel(str, Enum):
    """Log level enumeration"""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class OperationType(str, Enum):
    """Operation type enumeration for audit and tracking"""
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    SEARCH = "search"
    VALIDATE = "validate"
    VERIFY = "verify"
    EXPORT = "export"
    IMPORT = "import"


class EventType(str, Enum):
    """Event type enumeration for event bus and messaging"""
    ENTITY_CREATED = "entity.created"
    ENTITY_UPDATED = "entity.updated"
    ENTITY_DELETED = "entity.deleted"
    VALIDATION_FAILED = "validation.failed"
    OPERATION_COMPLETED = "operation.completed"
    ERROR_OCCURRED = "error.occurred"
    NOTIFICATION_SENT = "notification.sent"
    AUDIT_LOG_CREATED = "audit.log.created"


class ResourceType(str, Enum):
    """Resource type enumeration for RBAC and authorization"""
    CONTACT = "contact"
    ADDRESS = "address"
    PHONE = "phone"
    EMAIL = "email"
    ORGANIZATION = "organization"
    CUSTOMER = "customer"
    USER = "user"
    SYSTEM = "system"
    API = "api"
    DATABASE = "database"


class AccessLevel(str, Enum):
    """Access level enumeration for security and permissions"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    TOP_SECRET = "top_secret"


class ValidationLevel(str, Enum):
    """Validation level enumeration for data validation"""
    BASIC = "basic"
    STANDARD = "standard"
    STRICT = "strict"
    ENTERPRISE = "enterprise"


class CacheStrategy(str, Enum):
    """Cache strategy enumeration for caching behavior"""
    NO_CACHE = "no_cache"
    SHORT_TERM = "short_term"
    MEDIUM_TERM = "medium_term"
    LONG_TERM = "long_term"
    PERMANENT = "permanent"
