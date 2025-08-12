"""
Dotmac Infrastructure Plane

A comprehensive, reusable infrastructure package providing foundational SDK components
for modern applications organized into logical planes.
"""

__version__ = "0.1.0"
__author__ = "Dotmac Engineering Team"
__email__ = "engineering@dotmac.ng"

# Import only what actually exists and works
try:
    # Platform Plane - Core utilities and foundational services
    from .platform import (
        DatabaseClient,
        CacheClient,
        EventBusClient,
        ObservabilityClient,
        FileStorageClient,
    )
    _platform_available = True
except ImportError:
    _platform_available = False

try:
    # Layer 1 - Foundational entities (only import the ones that exist)
    from .layer1.contact_sdk import ContactSDK
    from .layer1.address_sdk import AddressSDK
    from .layer1.phone_sdk import PhoneSDK
    from .layer1.email_sdk import EmailSDK
    from .layer1.organization_sdk import OrganizationSDK
    _layer1_available = True
except ImportError:
    _layer1_available = False

try:
    # Utilities - Shared components
    from .utils.base_sdk import BaseSDK
    from .utils.enums import Permission, ContactType, AddressType, PhoneType, EmailType
    from .utils.logging import logger, StructuredLogger
    _utils_available = True
except ImportError:
    _utils_available = False

# Build __all__ dynamically based on what's available
__all__ = []

if _platform_available:
    __all__.extend([
        "DatabaseClient",
        "CacheClient", 
        "EventBusClient",
        "ObservabilityClient",
        "FileStorageClient",
    ])

if _layer1_available:
    __all__.extend([
        "ContactSDK",
        "AddressSDK",
        "PhoneSDK", 
        "EmailSDK",
        "OrganizationSDK",
    ])

if _utils_available:
    __all__.extend([
        "BaseSDK",
        "Permission",
        "ContactType",
        "AddressType",
        "PhoneType",
        "EmailType",
        "logger",
        "StructuredLogger",
    ])
