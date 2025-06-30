"""
Database Fixtures

Utilities for setting up and managing test databases for Phase 2 integration tests.
"""

import os
import tempfile
import pytest
from typing import Generator, Optional
from contextlib import contextmanager

try:
    import sys
    # Add project root to path
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    
    from fhir4ds.helpers import QuickConnect
    from .test_data_generator import get_standard_test_data, get_minimal_test_data
    
except ImportError as e:
    # Graceful degradation for test discovery
    QuickConnect = None
    get_standard_test_data = None
    get_minimal_test_data = None


class DatabaseFixture:
    """
    Test database fixture that provides clean database instances for testing.
    """
    
    def __init__(self, use_memory: bool = True, load_test_data: bool = True):
        """
        Initialize database fixture.
        
        Args:
            use_memory: Whether to use in-memory database (faster)
            load_test_data: Whether to automatically load test data
        """
        self.use_memory = use_memory
        self.load_test_data = load_test_data
        self.db = None
        self.temp_file = None
    
    def setup(self):
        """Set up the test database."""
        if QuickConnect is None:
            raise ImportError("FHIR4DS modules not available")
        
        if self.use_memory:
            self.db = QuickConnect.memory()
        else:
            # Create temporary file for persistent database
            self.temp_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
            self.temp_file.close()
            self.db = QuickConnect.duckdb(self.temp_file.name)
        
        if self.load_test_data and get_standard_test_data:
            test_data = get_standard_test_data()
            self.db.load_resources(test_data)
        
        return self.db
    
    def teardown(self):
        """Clean up the test database."""
        if self.temp_file and os.path.exists(self.temp_file.name):
            os.unlink(self.temp_file.name)
        self.db = None
        self.temp_file = None
    
    def reload_data(self, data=None):
        """Reload test data into the database."""
        if not self.db:
            raise RuntimeError("Database not set up")
        
        if data is None:
            data = get_standard_test_data() if get_standard_test_data else []
        
        # Clear existing data and reload
        # Note: In-memory databases are automatically cleared on reconnect
        if self.use_memory:
            self.db = QuickConnect.memory()
        
        self.db.load_resources(data)


@contextmanager
def test_database(use_memory: bool = True, load_data: bool = True):
    """
    Context manager for test database.
    
    Args:
        use_memory: Whether to use in-memory database
        load_data: Whether to load test data
        
    Yields:
        Connected database instance
        
    Example:
        with test_database() as db:
            result = db.execute(some_query)
    """
    fixture = DatabaseFixture(use_memory=use_memory, load_test_data=load_data)
    try:
        db = fixture.setup()
        yield db
    finally:
        fixture.teardown()


@contextmanager
def empty_test_database():
    """Get an empty test database without any data loaded."""
    with test_database(load_data=False) as db:
        yield db


@contextmanager
def minimal_test_database():
    """Get a test database with minimal test data."""
    fixture = DatabaseFixture(use_memory=True, load_test_data=False)
    try:
        db = fixture.setup()
        if get_minimal_test_data:
            db.load_resources(get_minimal_test_data())
        yield db
    finally:
        fixture.teardown()


# Pytest fixtures
@pytest.fixture
def db():
    """Pytest fixture for standard test database."""
    with test_database() as database:
        yield database


@pytest.fixture
def empty_db():
    """Pytest fixture for empty test database."""
    with empty_test_database() as database:
        yield database


@pytest.fixture
def minimal_db():
    """Pytest fixture for minimal test database."""
    with minimal_test_database() as database:
        yield database


@pytest.fixture(scope="session")
def shared_db():
    """Pytest fixture for shared test database across test session."""
    fixture = DatabaseFixture(use_memory=True, load_test_data=True)
    try:
        db = fixture.setup()
        yield db
    finally:
        fixture.teardown()


def get_test_database_info(db) -> dict:
    """
    Get information about the test database.
    
    Args:
        db: Database connection
        
    Returns:
        Dictionary with database statistics
    """
    if not db:
        return {"error": "No database connection"}
    
    try:
        total_resources = db.get_resource_count()
        info = db.info()
        
        return {
            "total_resources": total_resources,
            "dialect": info.get("dialect", "unknown"),
            "connection_type": info.get("connection_type", "unknown"),
            "queries_executed": info.get("queries_executed", 0)
        }
    except Exception as e:
        return {"error": str(e)}


def assert_database_ready(db):
    """
    Assert that the database is ready for testing.
    
    Args:
        db: Database connection
        
    Raises:
        AssertionError: If database is not ready
    """
    assert db is not None, "Database connection is None"
    
    try:
        # Try to get basic info
        info = get_test_database_info(db)
        assert "error" not in info, f"Database error: {info.get('error')}"
        
        # Verify we can execute a simple query
        from fhir4ds.helpers import Templates
        simple_query = Templates.patient_demographics()
        result = db.execute(simple_query)
        assert result is not None, "Failed to execute simple query"
        
    except Exception as e:
        raise AssertionError(f"Database not ready: {e}")


def create_test_data_file(file_path: str, patient_count: int = 3) -> int:
    """
    Create a test data file with FHIR resources.
    
    Args:
        file_path: Path where to save the test data
        patient_count: Number of patients to generate
        
    Returns:
        Number of resources created
    """
    if get_standard_test_data is None:
        raise ImportError("Test data generator not available")
    
    from .test_data_generator import save_test_dataset_to_file
    return save_test_dataset_to_file(file_path, patient_count)