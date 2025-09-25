"""
FHIR4DS Helper Modules

User-friendly helper modules that make FHIR4DS easily accessible to healthcare developers,
data analysts, and researchers without requiring deep SQL-on-FHIR expertise.

Core Helper Modules:
- QuickConnect: Simplified database connection management
- QueryBuilder: Fluent API for building common FHIR queries
- Templates: Pre-built ViewDefinitions for healthcare analytics
- ResultFormatter: Multi-format export capabilities
- BatchProcessor: Parallel processing with progress monitoring
- PerformanceMonitor: Query optimization and monitoring

Quick Start:
    from fhir4ds.helpers import QuickConnect, QueryBuilder, Templates
    
    # Connect to database
    db = QuickConnect.duckdb("./healthcare_data.db")
    
    # Build queries with fluent API
    query = (QueryBuilder()
        .resource("Patient")
        .columns(["id", "name.family", "birthDate"])
        .where("active = true")
        .build())
    
    # Execute with templates
    results = db.execute_template("patient_demographics")
    
    # Export results
    df = db.execute_to_dataframe(query)
    db.execute_to_excel([query], "report.xlsx")
"""

# Import connection management (already implemented)
from ..datastore.connections import QuickConnect, ConnectedDatabase

# Import formatters and batch processing (already implemented)
from ..datastore.formatters import ResultFormatter
from ..datastore.batch import BatchProcessor, BatchResult, BatchSummary

# Import new helper modules (to be implemented)
try:
    from .query_builder import QueryBuilder, FHIRQueryBuilder
    QUERY_BUILDER_AVAILABLE = True
except ImportError:
    QueryBuilder = None
    FHIRQueryBuilder = None
    QUERY_BUILDER_AVAILABLE = False

try:
    from .templates import Templates, TemplateLibrary
    TEMPLATES_AVAILABLE = True
except ImportError:
    Templates = None
    TemplateLibrary = None
    TEMPLATES_AVAILABLE = False

try:
    from .performance import PerformanceMonitor, QueryProfiler, OptimizationSuggestions
    PERFORMANCE_AVAILABLE = True
except ImportError:
    PerformanceMonitor = None
    QueryProfiler = None
    OptimizationSuggestions = None
    PERFORMANCE_AVAILABLE = False

__all__ = [
    # Core helper functionality
    "QuickConnect",
    "ConnectedDatabase", 
    "ResultFormatter",
    "BatchProcessor",
    "BatchResult",
    "BatchSummary",
    
    # Advanced helper modules
    "QueryBuilder",
    "FHIRQueryBuilder",
    "Templates", 
    "TemplateLibrary",
    "PerformanceMonitor",
    "QueryProfiler",
    "OptimizationSuggestions",
    
    # Availability flags
    "QUERY_BUILDER_AVAILABLE",
    "TEMPLATES_AVAILABLE", 
    "PERFORMANCE_AVAILABLE"
]