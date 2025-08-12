"""
Utils Module

Core utilities and shared components for the dotmac_infra package.

This module provides:
- BaseSDK: Foundation class for all SDK implementations
- Enums: Centralized enumerations for type safety
- Logging: Structured logging utilities
"""

from .base_sdk import BaseSDK, OperationContext, SecurityContext
from .enums import (
    Permission, UserRole, ContactType, ContactStatus,
    AddressType, PhoneType, EmailType, OrganizationType,
    OperationType, EventType, ResourceType, AccessLevel,
    Status, Priority, LogLevel, ValidationLevel, CacheStrategy
)
from .logging import logger, StructuredLogger

__all__ = [
    "BaseSDK",
    "OperationContext", 
    "SecurityContext",
    "Permission",
    "UserRole",
    "ContactType",
    "ContactStatus",
    "AddressType",
    "PhoneType", 
    "EmailType",
    "OrganizationType",
    "OperationType",
    "EventType",
    "ResourceType",
    "AccessLevel",
    "Status",
    "Priority",
    "LogLevel",
    "ValidationLevel",
    "CacheStrategy",
    "logger",
    "StructuredLogger",
]