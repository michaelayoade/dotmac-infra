"""
Integration tests for dotmac_infra SDK components
"""
import pytest
from unittest.mock import Mock, patch


class TestSDKIntegration:
    """Test integration between different SDK components"""
    
    def test_layer1_sdk_composition(self):
        """Test that Layer1 SDKs properly compose with platform components"""
        from dotmac_infra.layer1.contact_sdk import ContactSDK
        from dotmac_infra.layer1.address_sdk import AddressSDK
        
        # Test SDK instantiation with tenant isolation
        contact_sdk = ContactSDK(tenant_id="tenant-1")
        address_sdk = AddressSDK(tenant_id="tenant-1")
        
        assert contact_sdk.tenant_id == "tenant-1"
        assert address_sdk.tenant_id == "tenant-1"
        
        # Test that SDKs have required platform dependencies
        assert hasattr(contact_sdk, 'db_client')
        assert hasattr(contact_sdk, 'cache_client')
        assert hasattr(address_sdk, 'db_client')
        assert hasattr(address_sdk, 'cache_client')
    
    def test_cross_sdk_data_consistency(self):
        """Test data consistency across different SDKs"""
        from dotmac_infra.layer1.contact_sdk import ContactSDK
        from dotmac_infra.layer1.address_sdk import AddressSDK
        from dotmac_infra.utils.enums import ContactType, AddressType
        
        contact_sdk = ContactSDK(tenant_id="test-tenant")
        address_sdk = AddressSDK(tenant_id="test-tenant")
        
        # Test that enums are consistent across SDKs
        assert hasattr(contact_sdk, 'validate_contact_type')
        assert hasattr(address_sdk, 'validate_address_type')
        
        # Test enum usage
        assert ContactType.INDIVIDUAL in ContactType
        assert AddressType.HOME in AddressType
    
    @patch('dotmac_infra.platform.database_client.DatabaseClient')
    def test_platform_client_integration(self, mock_db_client):
        """Test integration with platform clients"""
        from dotmac_infra.layer1.contact_sdk import ContactSDK
        
        # Mock database client
        mock_db_instance = Mock()
        mock_db_client.return_value = mock_db_instance
        
        contact_sdk = ContactSDK(tenant_id="test-tenant")
        
        # Test that SDK properly initializes with platform clients
        assert contact_sdk.tenant_id == "test-tenant"
        # Note: In a real test, we'd verify database operations
    
    def test_enum_cross_reference_consistency(self):
        """Test that enums are consistently used across all components"""
        from dotmac_infra.utils.enums import (
            Permission, ResourceType, OperationType, EventType
        )
        
        # Test that permission enums align with resource types
        contact_permissions = [p for p in Permission if 'contact' in p.value]
        assert len(contact_permissions) > 0
        assert ResourceType.CONTACT in ResourceType
        
        # Test that operation types are comprehensive
        required_operations = {'create', 'read', 'update', 'delete', 'verify'}
        operation_values = {op.value for op in OperationType}
        assert required_operations.issubset(operation_values)
    
    def test_logging_integration(self):
        """Test logging integration across components"""
        from dotmac_infra.utils.logging import logger, StructuredLogger
        from dotmac_infra.layer1.contact_sdk import ContactSDK
        
        # Test that logger is accessible
        assert hasattr(logger, 'info')
        assert hasattr(logger, 'error')
        
        # Test structured logger
        struct_logger = StructuredLogger("integration-test")
        bound_logger = struct_logger.bind(test_context="integration")
        
        # Should not raise exceptions
        bound_logger.info("Integration test log message")
        
        # Test SDK logging (should not raise exceptions)
        contact_sdk = ContactSDK(tenant_id="test-tenant")
        # In a real implementation, we'd test that SDK operations log appropriately


class TestPackageIntegrity:
    """Test overall package integrity and consistency"""
    
    def test_all_exports_are_importable(self):
        """Test that all exported components can be imported"""
        import dotmac_infra
        
        # Test that all items in __all__ are actually importable
        for item_name in dotmac_infra.__all__:
            assert hasattr(dotmac_infra, item_name), f"{item_name} not found in package"
            
            # Test that the item is not None
            item = getattr(dotmac_infra, item_name)
            assert item is not None, f"{item_name} is None"
    
    def test_package_version_consistency(self):
        """Test package version consistency"""
        import dotmac_infra
        
        # Test version format
        version = dotmac_infra.__version__
        assert isinstance(version, str)
        assert len(version.split('.')) >= 2  # At least major.minor
        
        # Test that version follows semantic versioning pattern
        import re
        semver_pattern = r'^\d+\.\d+\.\d+.*$'
        assert re.match(semver_pattern, version), f"Version {version} doesn't follow semver"
    
    def test_no_circular_imports(self):
        """Test that there are no circular imports"""
        # This test imports all major components to detect circular imports
        try:
            # Platform imports
            from dotmac_infra.platform import (
                DatabaseClient, CacheClient, EventBusClient,
                ObservabilityClient, FileStorageClient
            )
            
            # Layer1 imports
            from dotmac_infra.layer1.contact_sdk import ContactSDK
            from dotmac_infra.layer1.address_sdk import AddressSDK
            from dotmac_infra.layer1.phone_sdk import PhoneSDK
            from dotmac_infra.layer1.email_sdk import EmailSDK
            from dotmac_infra.layer1.organization_sdk import OrganizationSDK
            
            # Utils imports
            from dotmac_infra.utils.base_sdk import BaseSDK
            from dotmac_infra.utils.enums import Permission
            from dotmac_infra.utils.logging import logger
            
            # If we get here, no circular imports detected
            assert True
            
        except ImportError as e:
            if "circular import" in str(e).lower():
                pytest.fail(f"Circular import detected: {e}")
            else:
                # Re-raise other import errors
                raise
    
    def test_dependency_isolation(self):
        """Test that the package doesn't have external app dependencies"""
        import dotmac_infra
        
        # Test that package can be imported without external app dependencies
        # This is verified by the successful import above
        assert dotmac_infra.__version__ is not None
        
        # Test that enums are self-contained
        from dotmac_infra.utils.enums import Permission, ContactType
        assert Permission.CONTACT_CREATE == "contact.create"
        assert ContactType.INDIVIDUAL == "individual"


class TestErrorHandling:
    """Test error handling across components"""
    
    def test_sdk_initialization_with_invalid_params(self):
        """Test SDK behavior with invalid initialization parameters"""
        from dotmac_infra.layer1.contact_sdk import ContactSDK
        
        # Test with empty tenant_id (should handle gracefully)
        try:
            contact_sdk = ContactSDK(tenant_id="")
            # Should either work or raise a clear error
            assert contact_sdk.tenant_id == ""
        except Exception as e:
            # If it raises an exception, it should be a clear validation error
            assert "tenant" in str(e).lower() or "invalid" in str(e).lower()
    
    def test_enum_invalid_access(self):
        """Test enum behavior with invalid access"""
        from dotmac_infra.utils.enums import ContactType
        
        # Test that invalid enum access raises appropriate error
        with pytest.raises(AttributeError):
            _ = ContactType.INVALID_TYPE
        
        # Test that enum values are properly typed
        assert isinstance(ContactType.INDIVIDUAL, ContactType)
        assert isinstance(ContactType.INDIVIDUAL.value, str)


class TestPerformanceConsiderations:
    """Test performance-related aspects"""
    
    def test_import_performance(self):
        """Test that imports are reasonably fast"""
        import time
        
        start_time = time.time()
        
        # Import major components
        import dotmac_infra
        from dotmac_infra.layer1.contact_sdk import ContactSDK
        from dotmac_infra.utils.enums import Permission
        
        end_time = time.time()
        import_time = end_time - start_time
        
        # Imports should complete in reasonable time (< 1 second)
        assert import_time < 1.0, f"Imports took too long: {import_time:.2f}s"
    
    def test_memory_usage(self):
        """Test that package doesn't consume excessive memory"""
        import sys
        
        # Get initial memory usage
        initial_modules = len(sys.modules)
        
        # Import package
        import dotmac_infra
        from dotmac_infra.layer1.contact_sdk import ContactSDK
        
        # Check module count increase
        final_modules = len(sys.modules)
        module_increase = final_modules - initial_modules
        
        # Should not import an excessive number of modules
        assert module_increase < 50, f"Too many modules imported: {module_increase}"
