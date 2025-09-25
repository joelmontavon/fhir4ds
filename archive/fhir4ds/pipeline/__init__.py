"""
FHIRPath Pipeline Architecture

This package provides an immutable pipeline-based architecture for
FHIRPath SQL generation, replacing the monolithic SQLGenerator approach.
"""

from .core.base import SQLState, ExecutionContext, CompiledSQL, PipelineOperation
from .core.builder import FHIRPathPipeline
from .core.compiler import PipelineCompiler

__all__ = [
    'SQLState',
    'ExecutionContext', 
    'CompiledSQL',
    'PipelineOperation',
    'FHIRPathPipeline',
    'PipelineCompiler'
]