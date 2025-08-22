"""
AST-to-Pipeline Converter for FHIRPath expressions.

This module provides the bridge between the existing AST-based FHIRPath 
parser and the new immutable pipeline architecture. It converts AST nodes
into pipeline operations, enabling gradual migration.
"""

import logging
from typing import Union, Any, List
from ..core.base import PipelineOperation, SQLState, ExecutionContext
from ..core.builder import FHIRPathPipeline
from ..operations.path import PathNavigationOperation, IndexerOperation
from ..operations.literals import LiteralOperation, CollectionLiteralOperation
from ..operations.functions import FunctionCallOperation
from ...fhirpath.parser.ast_nodes import (
    ASTNode, ThisNode, VariableNode, LiteralNode, IdentifierNode,
    FunctionCallNode, BinaryOpNode, UnaryOpNode, PathNode, IndexerNode,
    TupleNode
)

logger = logging.getLogger(__name__)

class ASTToPipelineConverter:
    """
    Converts FHIRPath AST nodes to immutable pipeline operations.
    
    This converter enables gradual migration by providing a bridge
    between the existing parser and the new pipeline architecture.
    
    Example:
        converter = ASTToPipelineConverter()
        pipeline = converter.convert_ast_to_pipeline(ast_node)
        compiled_sql = pipeline.compile(context)
    """
    
    def __init__(self):
        """Initialize the AST converter."""
        self.conversion_stats = {
            'nodes_converted': 0,
            'operations_created': 0,
            'conversions_cached': 0
        }
        self._conversion_cache = {}  # Cache for repeated AST patterns
    
    def convert_ast_to_pipeline(self, ast_node: ASTNode) -> FHIRPathPipeline:
        """
        Convert an AST node to a pipeline.
        
        Args:
            ast_node: Root AST node to convert
            
        Returns:
            FHIRPath pipeline representing the AST
            
        Raises:
            ConversionError: If AST node cannot be converted
        """
        logger.debug(f"Converting AST node: {type(ast_node).__name__}")
        
        try:
            # Check cache for repeated patterns
            ast_key = self._create_ast_key(ast_node)
            if ast_key in self._conversion_cache:
                self.conversion_stats['conversions_cached'] += 1
                return self._conversion_cache[ast_key]
            
            # Convert AST to pipeline operations
            operations = self._convert_node(ast_node)
            
            # Create pipeline from operations
            pipeline = FHIRPathPipeline(operations)
            
            # Cache the result
            self._conversion_cache[ast_key] = pipeline
            
            self.conversion_stats['nodes_converted'] += 1
            logger.debug(f"Converted to pipeline with {len(operations)} operations")
            
            return pipeline
            
        except Exception as e:
            logger.error(f"Failed to convert AST node {type(ast_node).__name__}: {e}")
            raise ConversionError(f"AST conversion failed: {e}") from e
    
    def _convert_node(self, node: ASTNode) -> List[PipelineOperation[SQLState]]:
        """
        Convert a single AST node to pipeline operations.
        
        Args:
            node: AST node to convert
            
        Returns:
            List of pipeline operations
        """
        if isinstance(node, ThisNode):
            return self._convert_this_node(node)
        elif isinstance(node, VariableNode):
            return self._convert_variable_node(node)
        elif isinstance(node, LiteralNode):
            return self._convert_literal_node(node)
        elif isinstance(node, IdentifierNode):
            return self._convert_identifier_node(node)
        elif isinstance(node, FunctionCallNode):
            return self._convert_function_call_node(node)
        elif isinstance(node, BinaryOpNode):
            return self._convert_binary_op_node(node)
        elif isinstance(node, UnaryOpNode):
            return self._convert_unary_op_node(node)
        elif isinstance(node, PathNode):
            return self._convert_path_node(node)
        elif isinstance(node, IndexerNode):
            return self._convert_indexer_node(node)
        elif isinstance(node, TupleNode):
            return self._convert_tuple_node(node)
        else:
            raise ConversionError(f"Unsupported AST node type: {type(node).__name__}")
    
    def _convert_this_node(self, node: ThisNode) -> List[PipelineOperation[SQLState]]:
        """Convert ThisNode ($this) to pipeline operations."""
        # $this represents the current resource context - no operation needed
        # The base table and resource column are handled by the initial state
        return []
    
    def _convert_variable_node(self, node: VariableNode) -> List[PipelineOperation[SQLState]]:
        """Convert VariableNode to pipeline operations."""
        # Variables like $index, $total are context-dependent
        # For now, create a literal that will be resolved at execution time
        operation = LiteralOperation(f"${node.name}", "string")
        self.conversion_stats['operations_created'] += 1
        return [operation]
    
    def _convert_literal_node(self, node: LiteralNode) -> List[PipelineOperation[SQLState]]:
        """Convert LiteralNode to pipeline operations."""
        operation = LiteralOperation(node.value, node.type)
        self.conversion_stats['operations_created'] += 1
        return [operation]
    
    def _convert_identifier_node(self, node: IdentifierNode) -> List[PipelineOperation[SQLState]]:
        """Convert IdentifierNode to pipeline operations."""
        # Identifiers represent field names in FHIR resources
        operation = PathNavigationOperation(node.name)
        self.conversion_stats['operations_created'] += 1
        return [operation]
    
    def _convert_function_call_node(self, node: FunctionCallNode) -> List[PipelineOperation[SQLState]]:
        """Convert FunctionCallNode to pipeline operations."""
        # Convert function arguments
        arg_operations = []
        for arg in node.args:
            arg_ops = self._convert_node(arg)
            arg_operations.extend(arg_ops)
        
        # Create function call operation
        operation = FunctionCallOperation(node.name, arg_operations)
        self.conversion_stats['operations_created'] += 1
        return arg_operations + [operation]
    
    def _convert_binary_op_node(self, node: BinaryOpNode) -> List[PipelineOperation[SQLState]]:
        """Convert BinaryOpNode to pipeline operations."""
        # Convert left and right operands
        left_ops = self._convert_node(node.left)
        right_ops = self._convert_node(node.right)
        
        # Create function call for the binary operator
        operation = FunctionCallOperation(node.operator, left_ops + right_ops)
        self.conversion_stats['operations_created'] += 1
        return left_ops + right_ops + [operation]
    
    def _convert_unary_op_node(self, node: UnaryOpNode) -> List[PipelineOperation[SQLState]]:
        """Convert UnaryOpNode to pipeline operations."""
        # Convert operand
        operand_ops = self._convert_node(node.operand)
        
        # Create function call for the unary operator
        operation = FunctionCallOperation(node.operator, operand_ops)
        self.conversion_stats['operations_created'] += 1
        return operand_ops + [operation]
    
    def _convert_path_node(self, node: PathNode) -> List[PipelineOperation[SQLState]]:
        """Convert PathNode to pipeline operations."""
        operations = []
        
        # Convert each segment in the path, but handle function calls specially
        for i, segment in enumerate(node.segments):
            if isinstance(segment, FunctionCallNode):
                # Function calls operate on the preceding path, not as separate operations
                # For certain functions like 'where', we need to preserve the raw AST for condition parsing
                if segment.name in ['where', 'select', 'join', 'union', 'combine']:
                    # Pass raw AST arguments for functions that need to analyze conditions or literals
                    function_op = FunctionCallOperation(segment.name, segment.args)
                else:
                    # Convert arguments but DON'T add them to the main pipeline - they're not operations
                    arg_operations = []
                    for arg in segment.args:
                        arg_ops = self._convert_node(arg)
                        arg_operations.extend(arg_ops)
                    
                    # Create function call operation with the converted arguments
                    function_op = FunctionCallOperation(segment.name, arg_operations)
                operations.append(function_op)
                self.conversion_stats['operations_created'] += 1
            else:
                # Regular path segments (identifiers, indexers, etc.)
                segment_ops = self._convert_node(segment)
                operations.extend(segment_ops)
                self.conversion_stats['operations_created'] += len(segment_ops)
        
        return operations
    
    def _convert_indexer_node(self, node: IndexerNode) -> List[PipelineOperation[SQLState]]:
        """Convert IndexerNode to pipeline operations."""
        # Convert the expression being indexed
        expr_ops = self._convert_node(node.expression)
        
        # Convert the index
        if isinstance(node.index, LiteralNode) and node.index.type == 'integer':
            # Simple numeric index
            index_op = IndexerOperation(node.index.value)
        else:
            # Complex index expression - convert to operations and use string representation
            index_ops = self._convert_node(node.index)
            # For now, use a simplified approach
            index_op = IndexerOperation("*")  # Placeholder for complex indices
        
        self.conversion_stats['operations_created'] += 1
        return expr_ops + [index_op]
    
    def _convert_tuple_node(self, node: TupleNode) -> List[PipelineOperation[SQLState]]:
        """Convert TupleNode to pipeline operations."""
        # Tuples are complex - for now, create a collection literal
        elements = []
        for key, value in node.elements:
            # For simplicity, just collect the values
            if isinstance(value, LiteralNode):
                elements.append(value.value)
            else:
                # Complex values - use string representation
                elements.append(str(value))
        
        operation = CollectionLiteralOperation(elements)
        self.conversion_stats['operations_created'] += 1
        return [operation]
    
    def _create_ast_key(self, node: ASTNode) -> str:
        """
        Create a cache key for an AST node.
        
        Args:
            node: AST node to create key for
            
        Returns:
            String key for caching
        """
        # Simple key based on node type and basic properties
        if isinstance(node, LiteralNode):
            return f"literal_{node.type}_{node.value}"
        elif isinstance(node, IdentifierNode):
            return f"identifier_{node.name}"
        elif isinstance(node, FunctionCallNode):
            return f"function_{node.name}_{len(node.args)}"
        else:
            return f"{type(node).__name__}_{id(node)}"
    
    def get_conversion_stats(self) -> dict:
        """Get conversion statistics."""
        return self.conversion_stats.copy()
    
    def clear_cache(self) -> None:
        """Clear the conversion cache."""
        self._conversion_cache.clear()
        logger.debug("Conversion cache cleared")

class ConversionError(Exception):
    """Exception raised when AST conversion fails."""
    pass

class PipelineASTBridge:
    """
    Bridge class that integrates pipeline conversion with existing FHIRPath processing.
    
    This class provides the integration point between the existing SQLGenerator
    and the new pipeline architecture, enabling gradual migration.
    """
    
    def __init__(self, converter: ASTToPipelineConverter = None):
        """
        Initialize the bridge.
        
        Args:
            converter: AST converter to use (creates default if None)
        """
        self.converter = converter or ASTToPipelineConverter()
        self.migration_mode = "pipeline_only"  # Legacy translator removed
        self.fallback_to_ast = False  # No fallback available
    
    def process_fhirpath_expression(self, ast_node: ASTNode, 
                                   context: ExecutionContext) -> str:
        """
        Process a FHIRPath expression using the appropriate method.
        
        Args:
            ast_node: Parsed FHIRPath AST
            context: Execution context
            
        Returns:
            Generated SQL string
        """
        if self.migration_mode == "ast_only":
            # Use legacy AST processing
            return self._process_with_ast(ast_node, context)
        
        try:
            # Try pipeline conversion first
            pipeline = self.converter.convert_ast_to_pipeline(ast_node)
            compiled = pipeline.compile(context)
            return compiled.get_full_sql()
            
        except Exception as e:
            if self.fallback_to_ast and self.migration_mode == "gradual":
                logger.warning(f"Pipeline conversion failed, falling back to AST: {e}")
                return self._process_with_ast(ast_node, context)
            else:
                raise ConversionError(f"Pipeline processing failed: {e}") from e
    
    def _process_with_ast(self, ast_node: ASTNode, context: ExecutionContext) -> str:
        """
        Process using legacy AST method.
        
        This would call the existing SQLGenerator logic.
        For now, return a placeholder.
        """
        # This would integrate with the existing SQLGenerator
        # For now, return a placeholder
        return "/* Legacy AST processing - not implemented in bridge */"
    
    def set_migration_mode(self, mode: str) -> None:
        """
        Set migration mode.
        
        Args:
            mode: 'gradual', 'pipeline_only', or 'ast_only'
        """
        valid_modes = {'gradual', 'pipeline_only', 'ast_only'}
        if mode not in valid_modes:
            raise ValueError(f"Invalid migration mode: {mode}. Must be one of {valid_modes}")
        
        self.migration_mode = mode
        logger.info(f"Migration mode set to: {mode}")
    
    def get_pipeline_coverage_stats(self) -> dict:
        """Get statistics on pipeline vs AST usage."""
        return {
            'conversion_stats': self.converter.get_conversion_stats(),
            'migration_mode': self.migration_mode,
            'fallback_enabled': self.fallback_to_ast
        }