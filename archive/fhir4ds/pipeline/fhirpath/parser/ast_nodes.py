"""
AST Nodes for Pipeline FHIRPath Integration

This module re-exports AST node classes from the main FHIRPath parser
for use in pipeline operations.
"""

# Re-export all AST nodes from the main FHIRPath parser
try:
    from ....fhirpath.parser.ast_nodes import *
    
except ImportError as e:
    import logging
    logger = logging.getLogger(__name__)
    logger.warning(f"FHIRPath AST nodes not available for pipeline: {e}")
    
    # Provide minimal stub classes to prevent import errors
    class ASTNode:
        pass
        
    class BinaryOpNode(ASTNode):
        pass
        
    class FunctionCallNode(ASTNode):
        pass
        
    class IdentifierNode(ASTNode):
        pass
        
    class LiteralNode(ASTNode):
        pass
        
    class PathNode(ASTNode):
        pass