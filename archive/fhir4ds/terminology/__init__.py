"""
FHIR4DS Terminology Services Module

Provides integration with terminology services including VSAC, SNOMED CT,
and other clinical terminology systems for CQL functionality with caching.
Includes extensible registry system for managing multiple terminology services.
"""

from .config import (
    load_vsac_api_key, 
    get_default_terminology_client, 
    get_vsac_client,
    setup_default_registry,
    get_mock_client
)
from .cache import TerminologyCache
from .registry import TerminologyServiceRegistry, get_global_registry

__version__ = "0.1.0"
__all__ = [
    'load_vsac_api_key', 
    'get_default_terminology_client', 
    'get_vsac_client',
    'setup_default_registry',
    'get_mock_client',
    'TerminologyCache',
    'TerminologyServiceRegistry',
    'get_global_registry'
]