"""
Legacy SQL Generator - DEPRECATED

This module has been replaced by the new pipeline architecture.
Use fhir4ds.pipeline instead.

All functionality has been migrated to:
- fhir4ds.pipeline.operations.functions
- fhir4ds.pipeline.core.compiler
- fhir4ds.pipeline.converters.ast_converter
"""

import warnings

# Legacy imports for backward compatibility only
from typing import Any, Dict, List, Optional, Union

class SQLGenerator:
    """
    DEPRECATED: Legacy SQL Generator has been replaced by pipeline architecture.
    
    This class now raises an exception to prevent usage.
    Use fhir4ds.pipeline.converters.ast_converter.PipelineASTBridge instead.
    """
    
    def __init__(self, *args, **kwargs):
        warnings.warn(
            "SQLGenerator is deprecated. Use pipeline system via FHIRPath class with use_pipeline=True",
            DeprecationWarning,
            stacklevel=2
        )
        raise RuntimeError("Legacy SQLGenerator translator is deprecated. Use pipeline system via FHIRPath class with use_pipeline=True")
    
    def __getattr__(self, name):
        raise RuntimeError("Legacy SQLGenerator translator is deprecated. Use pipeline system via FHIRPath class with use_pipeline=True")