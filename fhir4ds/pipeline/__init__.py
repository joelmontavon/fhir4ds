"""
FHIRPath Pipeline Architecture

This package provides an immutable pipeline-based architecture for
FHIRPath SQL generation, replacing the monolithic SQLGenerator approach.
"""

from .core.base import SQLState, ExecutionContext, CompiledSQL, PipelineOperation

# Handle missing builder module gracefully
try:
    from .core.builder import FHIRPathPipeline
    BUILDER_AVAILABLE = True
except ImportError:
    FHIRPathPipeline = None
    BUILDER_AVAILABLE = False

from .core.compiler import PipelineCompiler

__all__ = [
    'SQLState',
    'ExecutionContext', 
    'CompiledSQL',
    'PipelineOperation',
    'FHIRPathPipeline',
    'PipelineCompiler'
]