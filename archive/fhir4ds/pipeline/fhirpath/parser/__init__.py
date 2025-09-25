"""
FHIRPath Parser Package for Pipeline Integration

This package provides FHIRPath parser functionality for the pipeline architecture.
"""

# Re-export parser functionality
try:
    from ....fhirpath.parser.parser import FHIRPathParser, FHIRPathLexer
    from .ast_nodes import *
    
    __all__ = [
        'FHIRPathParser',
        'FHIRPathLexer',
        'ASTNode',
        'BinaryOpNode',
        'FunctionCallNode',
        'IdentifierNode',
        'LiteralNode',
        'PathNode',
        'ThisNode',
        'UnaryOpNode',
        'IndexerNode',
        'VariableNode',
        'TupleNode',
        'ListLiteralNode',
        'IntervalConstructorNode',
        'ResourceQueryNode',
        'CQLQueryExpressionNode'
    ]
    
except ImportError as e:
    import logging
    logger = logging.getLogger(__name__)
    logger.warning(f"FHIRPath parser modules not available: {e}")
    
    __all__ = []