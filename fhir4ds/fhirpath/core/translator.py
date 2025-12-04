"""
Legacy FHIRPath Translator - DEPRECATED

This module has been replaced by the new pipeline architecture.
However, it is maintained for backwards compatibility.
"""

import warnings
from ..legacy.translator import FHIRPathToSQL as LegacyFHIRPathToSQL

class FHIRPathToSQL(LegacyFHIRPathToSQL):
    """
    DEPRECATED: Legacy FHIRPath translator.
    
    This class is an alias to the legacy implementation for backwards compatibility.
    New code should use fhir4ds.pipeline.converters.ast_converter.PipelineASTBridge.
    """
    
    def __init__(self, *args, **kwargs):
        warnings.warn(
            "FHIRPathToSQL is deprecated. Use pipeline system via FHIRPath class with use_pipeline=True",
            DeprecationWarning,
            stacklevel=2
        )
        super().__init__(*args, **kwargs)
