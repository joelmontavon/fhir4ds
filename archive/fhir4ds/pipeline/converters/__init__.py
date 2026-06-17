"""
AST-to-Pipeline converters for FHIRPath migration.

This package provides converters that bridge the existing AST-based
FHIRPath processing with the new immutable pipeline architecture.
"""

from .ast_converter import ASTToPipelineConverter, PipelineASTBridge, ConversionError

__all__ = [
    'ASTToPipelineConverter',
    'PipelineASTBridge', 
    'ConversionError'
]