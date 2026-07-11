"""
Legacy CTE Manager - DEPRECATED

This module has been replaced by the new pipeline architecture's CTE system.
Use fhir4ds.pipeline.core.advanced_features instead.

The new CTE system provides:
- Thread-safe CTE caching
- Advanced optimization patterns
- LRU cache management
- Smart indexing hints
"""

import warnings

class CTEManager:
    """
    DEPRECATED: Legacy CTE Manager has been replaced by pipeline architecture.
    
    Use fhir4ds.pipeline.core.advanced_features.CTEOptimizer instead.
    """
    
    def __init__(self, *args, **kwargs):
        warnings.warn(
            "CTEManager is deprecated. Use pipeline system CTE features via fhir4ds.pipeline.core.advanced_features",
            DeprecationWarning,
            stacklevel=2
        )
        raise RuntimeError("Legacy CTEManager is deprecated. Use pipeline system CTE features")
    
    def __getattr__(self, name):
        raise RuntimeError("Legacy CTEManager is deprecated. Use pipeline system CTE features")