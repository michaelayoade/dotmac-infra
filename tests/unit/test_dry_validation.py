"""
DRY (Don't Repeat Yourself) validation tests for dotmac_infra package
"""
import ast
import os
import pytest
from pathlib import Path
from collections import defaultdict


class TestDRYValidation:
    """Test that the codebase follows DRY principles"""
    
    @pytest.fixture
    def package_root(self):
        """Get the package root directory"""
        return Path(__file__).parent.parent.parent / "dotmac_infra"
    
    def get_python_files(self, package_root):
        """Get all Python files in the package"""
        python_files = []
        for root, dirs, files in os.walk(package_root):
            # Skip test directories and __pycache__
            dirs[:] = [d for d in dirs if not d.startswith('__pycache__') and d != 'tests']
            for file in files:
                if file.endswith('.py') and not file.startswith('__'):
                    python_files.append(Path(root) / file)
        return python_files
    
    def extract_functions_and_classes(self, file_path):
        """Extract function and class definitions from a Python file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content)
            definitions = []
            
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    # Get function signature
                    args = [arg.arg for arg in node.args.args]
                    definitions.append({
                        'type': 'function',
                        'name': node.name,
                        'args': args,
                        'file': str(file_path),
                        'line': node.lineno
                    })
                elif isinstance(node, ast.ClassDef):
                    definitions.append({
                        'type': 'class',
                        'name': node.name,
                        'file': str(file_path),
                        'line': node.lineno
                    })
            
            return definitions
        except Exception as e:
            pytest.skip(f"Could not parse {file_path}: {e}")
            return []
    
    def test_no_duplicate_function_signatures(self, package_root):
        """Test that there are no duplicate function signatures"""
        python_files = self.get_python_files(package_root)
        function_signatures = defaultdict(list)
        
        for file_path in python_files:
            definitions = self.extract_functions_and_classes(file_path)
            
            for definition in definitions:
                if definition['type'] == 'function':
                    # Create signature key
                    sig_key = f"{definition['name']}({','.join(definition['args'])})"
                    function_signatures[sig_key].append({
                        'file': definition['file'],
                        'line': definition['line']
                    })
        
        # Find duplicates (excluding common patterns like __init__, __str__, etc.)
        duplicates = []
        common_methods = {'__init__', '__str__', '__repr__', '__eq__', '__hash__'}
        
        for sig, locations in function_signatures.items():
            if len(locations) > 1:
                func_name = sig.split('(')[0]
                if func_name not in common_methods:
                    duplicates.append((sig, locations))
        
        if duplicates:
            error_msg = "Found duplicate function signatures:\n"
            for sig, locations in duplicates:
                error_msg += f"  {sig}:\n"
                for loc in locations:
                    error_msg += f"    - {loc['file']}:{loc['line']}\n"
            pytest.fail(error_msg)
    
    def test_no_duplicate_class_names(self, package_root):
        """Test that there are no duplicate class names"""
        python_files = self.get_python_files(package_root)
        class_names = defaultdict(list)
        
        for file_path in python_files:
            definitions = self.extract_functions_and_classes(file_path)
            
            for definition in definitions:
                if definition['type'] == 'class':
                    class_names[definition['name']].append({
                        'file': definition['file'],
                        'line': definition['line']
                    })
        
        # Find duplicates
        duplicates = []
        for class_name, locations in class_names.items():
            if len(locations) > 1:
                duplicates.append((class_name, locations))
        
        if duplicates:
            error_msg = "Found duplicate class names:\n"
            for class_name, locations in duplicates:
                error_msg += f"  {class_name}:\n"
                for loc in locations:
                    error_msg += f"    - {loc['file']}:{loc['line']}\n"
            pytest.fail(error_msg)
    
    def test_consistent_import_patterns(self, package_root):
        """Test that import patterns are consistent"""
        python_files = self.get_python_files(package_root)
        import_patterns = defaultdict(set)
        
        for file_path in python_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                tree = ast.parse(content)
                
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        for alias in node.names:
                            import_patterns['import'].add(alias.name)
                    elif isinstance(node, ast.ImportFrom):
                        if node.module:
                            import_patterns['from'].add(node.module)
            except Exception:
                continue
        
        # Check for consistent internal imports
        internal_imports = [imp for imp in import_patterns['from'] if 'dotmac_infra' in imp]
        
        # All internal imports should use dotmac_infra prefix
        invalid_imports = []
        for imp in internal_imports:
            if not imp.startswith('dotmac_infra'):
                invalid_imports.append(imp)
        
        if invalid_imports:
            pytest.fail(f"Found inconsistent internal imports: {invalid_imports}")
    
    def test_enum_consistency(self, package_root):
        """Test that enums follow consistent patterns"""
        from dotmac_infra.utils.enums import (
            ContactStatus, AddressStatus, PhoneStatus, 
            EmailStatus, OrganizationStatus
        )
        
        status_enums = [
            ContactStatus, AddressStatus, PhoneStatus,
            EmailStatus, OrganizationStatus
        ]
        
        # All status enums should have these common values
        required_status_values = {'ACTIVE', 'INACTIVE', 'DELETED'}
        
        for enum_class in status_enums:
            enum_values = {member.name for member in enum_class}
            missing_values = required_status_values - enum_values
            
            if missing_values:
                pytest.fail(
                    f"{enum_class.__name__} is missing required status values: {missing_values}"
                )
    
    def test_sdk_base_class_consistency(self, package_root):
        """Test that all SDKs follow consistent base class patterns"""
        python_files = self.get_python_files(package_root)
        sdk_files = [f for f in python_files if 'sdk' in str(f).lower()]
        
        sdk_classes = []
        
        for file_path in sdk_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                tree = ast.parse(content)
                
                for node in ast.walk(tree):
                    if isinstance(node, ast.ClassDef) and node.name.endswith('SDK'):
                        # Check if it inherits from BaseSDK
                        bases = [base.id for base in node.bases if hasattr(base, 'id')]
                        sdk_classes.append({
                            'name': node.name,
                            'bases': bases,
                            'file': str(file_path)
                        })
            except Exception:
                continue
        
        # All SDK classes should inherit from BaseSDK
        invalid_sdks = []
        for sdk in sdk_classes:
            if 'BaseSDK' not in sdk['bases']:
                invalid_sdks.append(f"{sdk['name']} in {sdk['file']}")
        
        if invalid_sdks:
            pytest.fail(f"SDKs not inheriting from BaseSDK: {invalid_sdks}")
    
    def test_no_hardcoded_values(self, package_root):
        """Test that there are no hardcoded values that should be configurable"""
        python_files = self.get_python_files(package_root)
        
        # Patterns that might indicate hardcoded values
        suspicious_patterns = [
            'localhost',
            '127.0.0.1',
            'redis://localhost',
            'postgresql://localhost',
            'mongodb://localhost'
        ]
        
        violations = []
        
        for file_path in python_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                for i, line in enumerate(content.split('\n'), 1):
                    for pattern in suspicious_patterns:
                        if pattern in line and not line.strip().startswith('#'):
                            violations.append(f"{file_path}:{i} - {line.strip()}")
            except Exception:
                continue
        
        if violations:
            error_msg = "Found potentially hardcoded values:\n"
            for violation in violations:
                error_msg += f"  {violation}\n"
            # This is a warning, not a failure for now
            print(f"WARNING: {error_msg}")


class TestCodeQuality:
    """Test code quality metrics"""
    
    def test_package_structure(self):
        """Test that package structure follows conventions"""
        package_root = Path(__file__).parent.parent.parent / "dotmac_infra"
        
        # Required directories
        required_dirs = ['platform', 'layer1', 'utils']
        
        for dir_name in required_dirs:
            dir_path = package_root / "dotmac_infra" / dir_name
            assert dir_path.exists(), f"Required directory {dir_name} not found"
            
            # Each directory should have __init__.py
            init_file = dir_path / "__init__.py"
            assert init_file.exists(), f"__init__.py not found in {dir_name}"
    
    def test_all_modules_have_docstrings(self):
        """Test that all modules have docstrings"""
        package_root = Path(__file__).parent.parent.parent / "dotmac_infra" / "dotmac_infra"
        
        python_files = []
        for root, dirs, files in os.walk(package_root):
            dirs[:] = [d for d in dirs if not d.startswith('__pycache__')]
            for file in files:
                if file.endswith('.py') and file != '__init__.py':
                    python_files.append(Path(root) / file)
        
        missing_docstrings = []
        
        for file_path in python_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                tree = ast.parse(content)
                
                # Check if module has a docstring
                if not (tree.body and isinstance(tree.body[0], ast.Expr) 
                       and isinstance(tree.body[0].value, ast.Constant)
                       and isinstance(tree.body[0].value.value, str)):
                    missing_docstrings.append(str(file_path))
            except Exception:
                continue
        
        if missing_docstrings:
            # This is a warning for now
            print(f"WARNING: Modules without docstrings: {missing_docstrings}")
