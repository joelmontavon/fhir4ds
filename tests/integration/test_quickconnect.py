"""
QuickConnect Integration Tests

Tests for the QuickConnect database connection factory including:
- In-memory database connections
- File-based database connections  
- Connection error handling
- Database initialization
- Resource loading capabilities
"""

import os
import pytest
import tempfile
import json
from typing import Dict, List, Any

# Import test utilities
import sys
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

try:
    from fhir4ds.helpers import QuickConnect
    from tests.helpers.test_data_generator import get_minimal_test_data, get_standard_test_data
    from tests.helpers.assertion_helpers import (
        assert_database_connection_valid,
        assert_valid_query_result,
        assert_roughly_equal
    )
except ImportError as e:
    pytest.skip(f"FHIR4DS modules not available: {e}", allow_module_level=True)


class TestQuickConnect:
    """Test cases for QuickConnect database factory."""
    
    def test_memory_connection(self):
        """Test creating an in-memory database connection."""
        # Create connection
        db = QuickConnect.memory()
        
        # Validate connection
        assert_database_connection_valid(db)
        assert db.datastore.dialect.name == "DUCKDB"
        
        # Test basic functionality
        initial_count = db.get_resource_count()
        assert initial_count == 0
    
    def test_memory_connection_with_data(self):
        """Test in-memory connection with data loading."""
        db = QuickConnect.memory()
        test_data = get_minimal_test_data()
        
        # Load data
        db.load_resources(test_data)
        
        # Verify data loaded
        final_count = db.get_resource_count()
        assert final_count == len(test_data)
        assert final_count > 0
    
    def test_duckdb_file_connection(self):
        """Test creating a file-based DuckDB connection."""
        # Use a non-existent file path that DuckDB can create
        temp_dir = tempfile.mkdtemp()
        temp_path = os.path.join(temp_dir, "test.db")
        
        try:
            # Create connection
            db = QuickConnect.duckdb(temp_path)
            
            # Validate connection
            assert_database_connection_valid(db)
            assert db.datastore.dialect.name == "DUCKDB"
            
            # Verify file exists
            assert os.path.exists(temp_path)
            assert os.path.getsize(temp_path) > 0
            
        finally:
            # Cleanup
            import shutil
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
    
    def test_duckdb_connection_persistence(self):
        """Test that DuckDB file connections persist data."""
        test_data = get_minimal_test_data()
        
        # Use a non-existent file path that DuckDB can create
        temp_dir = tempfile.mkdtemp()
        temp_path = os.path.join(temp_dir, "persistence_test.db")
        
        try:
            # First connection - load data
            db1 = QuickConnect.duckdb(temp_path)
            db1.load_resources(test_data)
            initial_count = db1.get_resource_count()
            assert initial_count == len(test_data)
            
            # Close first connection to ensure data is written
            if hasattr(db1.datastore.dialect, 'connection') and db1.datastore.dialect.connection:
                db1.datastore.dialect.connection.close()
            
            # Second connection - verify persistence
            db2 = QuickConnect.duckdb(temp_path)
            persisted_count = db2.get_resource_count()
            assert persisted_count == initial_count
            
        finally:
            import shutil
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
    
    def test_connection_info(self):
        """Test database connection information."""
        db = QuickConnect.memory()
        
        # Get connection info
        info = db.info()
        
        # Validate info structure
        assert isinstance(info, dict)
        assert 'connection_type' in info
        assert 'dialect' in info
        assert 'queries_executed' in info
        assert 'resources_loaded' in info
        
        # Check initial values
        assert info['dialect'] == 'DUCKDB'
        assert info['queries_executed'] == 0
        assert info['resources_loaded'] == 0
    
    def test_resource_loading_incremental(self):
        """Test incremental resource loading."""
        db = QuickConnect.memory()
        test_data = get_minimal_test_data()
        
        # Load data in chunks
        chunk1 = test_data[:2]
        chunk2 = test_data[2:]
        
        # Load first chunk
        db.load_resources(chunk1)
        count1 = db.get_resource_count()
        assert count1 == len(chunk1)
        
        # Load second chunk
        if chunk2:  # Only if there's a second chunk
            db.load_resources(chunk2)
            count2 = db.get_resource_count()
            assert count2 == len(test_data)
    
    def test_resource_loading_from_json_file(self):
        """Test loading resources from JSON file."""
        db = QuickConnect.memory()
        test_data = get_minimal_test_data()
        
        # Create temporary JSON file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(test_data, temp_file, indent=2)
            temp_path = temp_file.name
        
        try:
            # Load from file
            db.load_from_json_file(temp_path)
            
            # Verify loading
            count = db.get_resource_count()
            assert count == len(test_data)
            
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_connection_statistics_tracking(self):
        """Test that connection tracks usage statistics."""
        db = QuickConnect.memory()
        test_data = get_minimal_test_data()
        
        # Load resources
        db.load_resources(test_data)
        
        # Execute a query
        from fhir4ds.helpers import Templates
        query = Templates.patient_demographics()
        result = db.execute(query)
        
        # Check updated statistics
        info = db.info()
        assert info['queries_executed'] > 0
        assert info['resources_loaded'] > 0
    
    def test_multiple_connection_isolation(self):
        """Test that multiple in-memory connections are isolated."""
        db1 = QuickConnect.memory()
        db2 = QuickConnect.memory()
        
        test_data = get_minimal_test_data()
        
        # Load data only in first connection
        db1.load_resources(test_data)
        
        # Verify isolation
        count1 = db1.get_resource_count()
        count2 = db2.get_resource_count()
        
        assert count1 == len(test_data)
        assert count2 == 0
    
    def test_database_directory_creation(self):
        """Test that database directories are created automatically."""
        # Create path with non-existing directory
        temp_dir = tempfile.mkdtemp()
        nested_path = os.path.join(temp_dir, "nested", "database.db")
        
        try:
            # This should create the nested directory
            db = QuickConnect.duckdb(nested_path)
            
            # Verify directory and file creation
            assert os.path.exists(os.path.dirname(nested_path))
            assert os.path.exists(nested_path)
            
            # Verify connection works
            assert_database_connection_valid(db)
            
        finally:
            # Cleanup
            import shutil
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
    
    def test_test_connection_method(self):
        """Test the test_connection method."""
        db = QuickConnect.memory()
        
        # Test connection should return True
        is_connected = db.test_connection()
        assert is_connected is True
    
    def test_connection_string_representation(self):
        """Test string representation of connection."""
        db = QuickConnect.memory()
        
        # Get string representation
        db_str = str(db)
        
        # Verify it contains useful information
        assert 'ConnectedDatabase' in db_str
        assert 'queries=' in db_str


class TestQuickConnectErrorHandling:
    """Test error handling in QuickConnect."""
    
    def test_invalid_file_path_handling(self):
        """Test handling of invalid file paths."""
        # Try to create database in invalid location
        invalid_path = "/invalid/path/that/should/not/exist/database.db"
        
        # This should handle the error gracefully by creating directories
        # or raise a meaningful error
        try:
            db = QuickConnect.duckdb(invalid_path)
            # If it succeeds, clean up
            if os.path.exists(invalid_path):
                os.unlink(invalid_path)
        except (OSError, PermissionError):
            # Expected for truly invalid paths
            pass
    
    def test_loading_invalid_json_file(self):
        """Test handling of invalid JSON files."""
        db = QuickConnect.memory()
        
        # Create invalid JSON file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            temp_file.write("invalid json content {")
            temp_path = temp_file.name
        
        try:
            # This should raise an appropriate error
            with pytest.raises(Exception):
                db.load_from_json_file(temp_path)
                
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_loading_nonexistent_file(self):
        """Test handling of non-existent files."""
        db = QuickConnect.memory()
        
        # Try to load from non-existent file
        with pytest.raises(Exception):
            db.load_from_json_file("/path/that/does/not/exist.json")


class TestQuickConnectPerformance:
    """Performance-related tests for QuickConnect."""
    
    def test_large_data_loading_performance(self):
        """Test performance with larger datasets."""
        db = QuickConnect.memory()
        
        # Generate larger test dataset
        from tests.helpers.test_data_generator import get_large_test_data
        large_data = get_large_test_data()
        
        # Measure loading time
        import time
        start_time = time.time()
        db.load_resources(large_data)
        end_time = time.time()
        
        # Verify data loaded
        count = db.get_resource_count()
        assert count == len(large_data)
        
        # Performance should be reasonable (adjust threshold as needed)
        loading_time = end_time - start_time
        assert loading_time < 10.0, f"Loading took too long: {loading_time:.2f}s"
    
    def test_memory_vs_file_performance(self):
        """Compare performance between memory and file databases."""
        test_data = get_standard_test_data()
        
        # Test memory database
        import time
        start_time = time.time()
        db_memory = QuickConnect.memory()
        db_memory.load_resources(test_data)
        memory_time = time.time() - start_time
        
        # Test file database
        temp_dir = tempfile.mkdtemp()
        temp_path = os.path.join(temp_dir, "performance_test.db")
        
        try:
            start_time = time.time()
            db_file = QuickConnect.duckdb(temp_path)
            db_file.load_resources(test_data)
            file_time = time.time() - start_time
            
            # Memory should generally be faster or comparable
            # Allow file to be up to 2x slower
            assert file_time < memory_time * 2, f"File database too slow: {file_time:.3f}s vs memory {memory_time:.3f}s"
            
        finally:
            import shutil
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)