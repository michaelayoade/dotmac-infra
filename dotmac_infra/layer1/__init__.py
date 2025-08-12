"""
Layer 1 Foundational SDKs - Contract-first implementations
All Layer 1 SDKs use BaseSDK composition and DRY decorators for cross-cutting concerns
"""

from .contact_sdk import ContactSDK
from .address_sdk import AddressSDK
from .phone_sdk import PhoneSDK
from .email_sdk import EmailSDK
from .organization_sdk import OrganizationSDK

__all__ = [
    "ContactSDK",
    "AddressSDK", 
    "PhoneSDK",
    "EmailSDK",
    "OrganizationSDK"
]
