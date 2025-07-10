"""
FHIR for Data Science (FHIR4DS)

Production-ready healthcare analytics platform providing:

Core Capabilities:
- 100% SQL-on-FHIR v2.0 compliance (117/117 tests passing)
- Dual database support (DuckDB + PostgreSQL) with identical functionality
- Advanced FHIRPath parsing with 187 choice type mappings
- Database object creation (views, tables, schemas)
- Parallel processing with enterprise-grade performance
- Multi-format export (Pandas, JSON, CSV, Excel, Parquet)
- RESTful API server for analytics as a service

Quick Start - Library Usage:
    from fhir4ds.datastore import QuickConnect
    
    # One-line database setup
    db = QuickConnect.duckdb("./healthcare_data.db")
    
    # High-performance data loading
    db.load_resources(fhir_resources, parallel=True)
    db.load_from_json_file("bundle.json", use_native_json=True)
    
    # Execute analytics with immediate export
    df = db.execute_to_dataframe(view_definition)
    db.execute_to_excel([query1, query2], "report.xlsx", parallel=True)
    
    # Create database objects
    db.create_view(patient_view, "patient_demographics")
    db.create_table(analytics_view, "cohort_table", materialized=True)

Server Usage:
    # Start FHIR analytics API server
    python -m fhir4ds.server
    
    # Or with custom configuration  
    python -m fhir4ds.server --database-type postgresql --port 8080
    
    # API endpoints available at http://localhost:8000/docs
"""

# Core data management and SQL-on-FHIR implementation
from .datastore import FHIRDataStore, QueryResult
from .dialects import DatabaseDialect, DuckDBDialect, PostgreSQLDialect
from .view_runner import ViewRunner

# FHIRPath public API for direct usage
from .fhirpath import FHIRPath, SimpleFHIRPath, parse_fhirpath, fhirpath_to_sql, validate_fhirpath

# User-friendly helper modules for simplified usage
try:
    from .datastore import QuickConnect, ConnectedDatabase
    from .datastore import ResultFormatter
    from .datastore import BatchProcessor
    HELPERS_AVAILABLE = True
except ImportError:
    QuickConnect = None
    ConnectedDatabase = None
    ResultFormatter = None
    BatchProcessor = None
    HELPERS_AVAILABLE = False

# Server module for API functionality
try:
    from .server import create_app, FHIRAnalyticsServer, FHIRAnalyticsServerConfig
    SERVER_AVAILABLE = True
except ImportError:
    create_app = None
    FHIRAnalyticsServer = None
    FHIRAnalyticsServerConfig = None
    SERVER_AVAILABLE = False

# Import view definition components for compatibility
try:
    from .view_definition import ViewDefinition, Column, SelectStructure
except ImportError:
    ViewDefinition = None
    Column = None
    SelectStructure = None

# Utility modules for testing and performance analysis
try:
    from .utils import PerformanceTester, PerformanceMetrics, DatasetStats
    from .utils import quick_performance_test, comprehensive_performance_test
    UTILS_AVAILABLE = True
except ImportError:
    PerformanceTester = None
    PerformanceMetrics = None
    DatasetStats = None
    quick_performance_test = None
    comprehensive_performance_test = None
    UTILS_AVAILABLE = False

__version__ = "0.5.0"
__all__ = [
    # Core data management
    "FHIRDataStore",
    "QueryResult",
    "DatabaseDialect",
    "DuckDBDialect", 
    "PostgreSQLDialect",
    "ViewRunner",
    
    # FHIRPath public API
    "FHIRPath",
    "SimpleFHIRPath", 
    "parse_fhirpath",
    "fhirpath_to_sql",
    "validate_fhirpath",
    
    # User-friendly helper modules
    "QuickConnect",
    "ConnectedDatabase",
    "ResultFormatter",
    "BatchProcessor",
    
    # Server functionality
    "create_app",
    "FHIRAnalyticsServer",
    "FHIRAnalyticsServerConfig",
    
    # ViewDefinition components
    "ViewDefinition",
    "Column", 
    "SelectStructure",
    
    # Utility modules
    "PerformanceTester",
    "PerformanceMetrics",
    "DatasetStats",
    "quick_performance_test",
    "comprehensive_performance_test",
    
    # Availability flags
    "HELPERS_AVAILABLE",
    "SERVER_AVAILABLE",
    "UTILS_AVAILABLE"
]