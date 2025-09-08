"""
Legacy FHIRPath Translator - DEPRECATED

This module has been replaced by the new pipeline architecture.
Use fhir4ds.pipeline.converters.ast_converter instead.

The new pipeline system provides:
- Immutable pipeline operations
- Advanced optimization passes
- Dialect-aware compilation
- CTE optimization
"""

import warnings

class FHIRPathToSQL:
    """
    DEPRECATED: Legacy FHIRPath translator has been replaced by pipeline architecture.
    
    Use fhir4ds.pipeline.converters.ast_converter.PipelineASTBridge instead.
    """
    
    def __init__(self, *args, **kwargs):
        warnings.warn(
            "FHIRPathToSQL is deprecated. Use pipeline system via FHIRPath class with use_pipeline=True",
            DeprecationWarning,
            stacklevel=2
        )
        raise RuntimeError("Legacy FHIRPathToSQL translator is deprecated. Use pipeline system")
    
    def __getattr__(self, name):
        raise RuntimeError("Legacy FHIRPathToSQL translator is deprecated. Use pipeline system")