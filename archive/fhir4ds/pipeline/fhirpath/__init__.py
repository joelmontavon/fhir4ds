"""
FHIRPath Pipeline Integration Module

This module provides integration between the FHIRPath parser/evaluator and the
pipeline architecture, bridging the gap between FHIRPath expressions and
pipeline operations.
"""

# Re-export key FHIRPath functionality for pipeline use
try:
    from ...fhirpath.fhirpath import FHIRPath
    from ...fhirpath.parser.ast_nodes import *
    from ...fhirpath.parser.parser import FHIRPathParser
    from ...fhirpath.parser.lexer import FHIRPathLexer
except ImportError:
    # Graceful fallback if fhirpath modules are not available
    FHIRPath = None
    FHIRPathParser = None
    FHIRPathLexer = None

__all__ = [
    'FHIRPath',
    'FHIRPathParser', 
    'FHIRPathLexer',
    # AST nodes will be available if imported successfully
]