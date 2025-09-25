"""
FHIRPath parsing and evaluation with comprehensive SQL-on-FHIR v2.0 support.

This package provides both low-level AST components and high-level public APIs
for FHIRPath expression parsing, SQL generation, and choice type resolution.
"""

# Public API facade - recommended for most users
from .fhirpath import (
    FHIRPath, SimpleFHIRPath, 
    parse_fhirpath, fhirpath_to_sql, validate_fhirpath
)

# Low-level parser components - for advanced users
from .parser import FHIRPathParser, FHIRPathLexer
from .parser.ast_nodes import (
    ASTNode, ThisNode, LiteralNode, IdentifierNode, FunctionCallNode,
    BinaryOpNode, UnaryOpNode, PathNode, IndexerNode
)

# Core components - for advanced users and internal use
from .core.choice_types import fhir_choice_types
from .core.constants import FHIR_PRIMITIVE_TYPES_AS_STRING, SQL_OPERATORS

__all__ = [
    # Public API (recommended)
    "FHIRPath", "SimpleFHIRPath", 
    "parse_fhirpath", "fhirpath_to_sql", "validate_fhirpath",
    
    # Low-level parser (advanced)
    "FHIRPathParser", "FHIRPathLexer",
    "ASTNode", "ThisNode", "LiteralNode", "IdentifierNode", "FunctionCallNode",
    "BinaryOpNode", "UnaryOpNode", "PathNode", "IndexerNode",
    
    # Core data (advanced)
    "fhir_choice_types", "FHIR_PRIMITIVE_TYPES_AS_STRING", "SQL_OPERATORS"
]