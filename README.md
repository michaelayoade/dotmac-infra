# Dotmac Infrastructure Plane

A comprehensive, reusable infrastructure package providing foundational SDK components for modern applications.

## Overview

The Dotmac Infrastructure Plane provides a complete set of composable SDKs organized into logical planes:

- **Platform Plane**: Core utilities (database, cache, secrets, transport, file storage, webhooks)
- **Data Plane**: Data management (database clients, caching, event bus, queues, governance)
- **Event Plane**: Event processing (streaming, orchestration, analytics, gateway)
- **Observability Plane**: Monitoring and alerting (observability, monitoring, alerting)
- **Control Plane**: Orchestration and governance (workflow, policy, operations)
- **Layer 1**: Foundational entities (Contact, Address, Phone, Email, Organization)

## Features

- **Contract-First Design**: All SDKs follow OpenAPI 3.1.0 specifications
- **DRY Architecture**: Centralized utilities and decorators prevent code duplication
- **SDK-to-SDK Composition**: Higher-level SDKs compose lower-level SDKs
- **Cross-Cutting Concerns**: Centralized error handling, logging, validation, and caching
- **Production Ready**: Comprehensive error handling, monitoring, and performance optimization

## Installation

```bash
pip install dotmac-infra
```

For development:
```bash
pip install -e ".[dev]"
```

## Quick Start

```python
from dotmac_infra.platform import DatabaseSDK, CacheSDK
from dotmac_infra.layer1 import ContactSDK
from dotmac_infra.observability import ObservabilitySDK

# Initialize infrastructure components
db_sdk = DatabaseSDK(tenant_id="my-tenant")
cache_sdk = CacheSDK(tenant_id="my-tenant")
observability = ObservabilitySDK(tenant_id="my-tenant")

# Use Layer 1 entities with infrastructure composition
contact_sdk = ContactSDK(tenant_id="my-tenant")
```

## Architecture

The infrastructure follows a layered, compositional architecture:

1. **Platform SDKs** provide foundational services
2. **Data/Event/Observability/Control SDKs** compose Platform SDKs
3. **Layer 1 SDKs** compose Platform SDKs for entity management
4. **Higher layers** compose Layer 1 + Platform SDKs for business logic

## Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black .
isort .

# Type checking
mypy dotmac_infra
```

## License

MIT License - see LICENSE file for details.
