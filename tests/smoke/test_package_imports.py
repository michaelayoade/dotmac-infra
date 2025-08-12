"""
Smoke tests for dotmac_infra package imports and basic functionality
"""


class TestPackageImports:
    """Test that all package components can be imported successfully"""
    
    def test_main_package_import(self):
        """Test main package import"""
        import dotmac_infra
        assert hasattr(dotmac_infra, '__version__')
        assert dotmac_infra.__version__ == "0.1.0"
    
    def test_platform_imports(self):
        """Test platform component imports"""
        from dotmac_infra.platform import (
            DatabaseClient,
            CacheClient,
            EventBusClient,
            ObservabilityClient,
            FileStorageClient,
        )
        
        # Verify classes are importable and have expected attributes
        assert hasattr(DatabaseClient, '__init__')
        assert hasattr(CacheClient, '__init__')
        assert hasattr(EventBusClient, '__init__')
        assert hasattr(ObservabilityClient, '__init__')
        assert hasattr(FileStorageClient, '__init__')
    
    def test_layer1_imports(self):
        """Test Layer1 SDK imports"""
        from dotmac_infra.layer1.contact_sdk import ContactSDK
        from dotmac_infra.layer1.address_sdk import AddressSDK
        from dotmac_infra.layer1.phone_sdk import PhoneSDK
        from dotmac_infra.layer1.email_sdk import EmailSDK
        from dotmac_infra.layer1.organization_sdk import OrganizationSDK
        
        # Verify SDKs are importable and have expected methods
        assert hasattr(ContactSDK, 'create')
        assert hasattr(AddressSDK, 'create')
        assert hasattr(PhoneSDK, 'create')
        assert hasattr(EmailSDK, 'create')
        assert hasattr(OrganizationSDK, 'create')
    
    def test_utils_imports(self):
        """Test utils component imports"""
        from dotmac_infra.utils.enums import (
            Permission, ContactType, OperationType
        )
        from dotmac_infra.utils.logging import logger, StructuredLogger
        
        # Verify enums have expected values
        assert Permission.CONTACT_CREATE == "contact.create"
        assert ContactType.INDIVIDUAL == "individual"
        assert OperationType.VERIFY == "verify"
        
        # Verify logging components
        assert hasattr(logger, 'info')
        assert hasattr(StructuredLogger, 'bind')
    
    def test_package_availability_flags(self):
        """Test package availability flags"""
        import dotmac_infra
        
        # Check that availability flags exist and are boolean
        assert hasattr(dotmac_infra, '_platform_available')
        assert hasattr(dotmac_infra, '_layer1_available')
        assert hasattr(dotmac_infra, '_utils_available')
        
        assert isinstance(dotmac_infra._platform_available, bool)
        assert isinstance(dotmac_infra._layer1_available, bool)
        assert isinstance(dotmac_infra._utils_available, bool)


class TestBasicFunctionality:
    """Test basic functionality of key components"""
    
    def test_sdk_instantiation(self):
        """Test that SDKs can be instantiated"""
        from dotmac_infra.layer1.contact_sdk import ContactSDK
        from dotmac_infra.layer1.address_sdk import AddressSDK
        
        # Test instantiation with tenant_id
        contact_sdk = ContactSDK(tenant_id="test-tenant")
        address_sdk = AddressSDK(tenant_id="test-tenant")
        
        assert contact_sdk.tenant_id == "test-tenant"
        assert address_sdk.tenant_id == "test-tenant"
    
    def test_enum_functionality(self):
        """Test enum functionality"""
        from dotmac_infra.utils.enums import ContactType, OperationType
        
        # Test enum values
        assert ContactType.INDIVIDUAL.value == "individual"
        assert OperationType.CREATE.value == "create"
        
        # Test enum membership
        assert ContactType.INDIVIDUAL in ContactType
        assert "invalid_type" not in [ct.value for ct in ContactType]
    
    def test_logging_functionality(self):
        """Test logging functionality"""
        from dotmac_infra.utils.logging import logger, StructuredLogger
        
        # Test basic logging (should not raise exceptions)
        logger.info("Test log message")
        
        # Test structured logger
        struct_logger = StructuredLogger("test")
        struct_logger.info("Test structured log")
        
        # Test logger binding
        bound_logger = struct_logger.bind(test_key="test_value")
        assert isinstance(bound_logger, StructuredLogger)
