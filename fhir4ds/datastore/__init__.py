"""
FHIR DataStore Package

Centralized data management for FHIR analytics including:
- Core datastore functionality (FHIRDataStore)
- Query result handling (QueryResult) 
- Database connections (QuickConnect)
- Batch processing (BatchProcessor)
- Result formatting (ResultFormatter)
"""

from .core import FHIRDataStore
from .result import QueryResult
from .connections import QuickConnect, ConnectedDatabase
from .batch import BatchProcessor, BatchResult, BatchSummary, create_batch_processor
from .formatters import ResultFormatter

__all__ = [
    # Core classes
    'FHIRDataStore',
    'QueryResult',
    
    # Connection utilities
    'QuickConnect', 
    'ConnectedDatabase',
    
    # Batch processing
    'BatchProcessor',
    'BatchResult', 
    'BatchSummary',
    'create_batch_processor',
    
    # Formatting
    'ResultFormatter',
]