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
from ..operations.functions import FunctionCallOperation, InvalidArgumentError, FHIRPathExecutionError
from ..core.base import PipelineValidationError
from ...fhirpath.parser.ast_nodes import (
    ASTNode, ThisNode, VariableNode, LiteralNode, IdentifierNode,
    FunctionCallNode, BinaryOpNode, UnaryOpNode, PathNode, IndexerNode,
    TupleNode, IntervalConstructorNode, ListLiteralNode, CQLQueryExpressionNode
)
from ...cql.core.parser import DateTimeLiteralNode

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
    
    def __init__(self, define_operations=None):
        """Initialize the AST converter.
        
        Args:
            define_operations: Dictionary of define operations for resolving define references
        """
        self.conversion_stats = {
            'nodes_converted': 0,
            'operations_created': 0,
            'conversions_cached': 0
        }
        self._conversion_cache = {}  # Cache for repeated AST patterns
        self.define_operations = define_operations or {}  # For resolving CQL define references
    
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
        elif isinstance(node, IntervalConstructorNode):
            return self._convert_interval_constructor_node(node)
        elif isinstance(node, ListLiteralNode):
            return self._convert_list_literal_node(node)
        elif isinstance(node, DateTimeLiteralNode):
            return self._convert_datetime_literal_node(node)
        elif isinstance(node, CQLQueryExpressionNode):
            return self._convert_cql_query_expression_node(node)
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
        # Check if this string literal is actually a quoted define reference
        if node.type == 'string' and node.value in self.define_operations:
            logger.debug(f"Converting string literal '{node.value}' as define reference")
            # This is a quoted define reference - create a define reference operation
            from ..operations.functions import DefineReferenceOperation
            operation = DefineReferenceOperation(node.value, self.define_operations[node.value], self.define_operations)
            self.conversion_stats['operations_created'] += 1
            return [operation]
        else:
            # Regular literal
            operation = LiteralOperation(node.value, node.type)
            self.conversion_stats['operations_created'] += 1
            return [operation]
    
    def _convert_datetime_literal_node(self, node: DateTimeLiteralNode) -> List[PipelineOperation[SQLState]]:
        """Convert DateTimeLiteralNode to pipeline operations."""
        # DateTimeLiteralNode represents @2023-01-01T00:00:00.000Z
        # Convert to a LiteralOperation with datetime type
        operation = LiteralOperation(node.value, 'datetime')
        self.conversion_stats['operations_created'] += 1
        logger.debug(f"Converted DateTimeLiteralNode '{node.value}' to LiteralOperation")
        return [operation]
    
    def _convert_identifier_node(self, node: IdentifierNode) -> List[PipelineOperation[SQLState]]:
        """Convert IdentifierNode to pipeline operations."""
        # Check if this identifier is a CQL define reference first
        logger.debug(f"Converting IdentifierNode: '{node.name}', define_operations keys: {list(self.define_operations.keys())}")
        if node.name in self.define_operations:
            # This is a define reference - create a define reference operation
            logger.debug(f"Creating DefineReferenceOperation for '{node.name}'")
            # Pass the full define_operations context for recursive resolution
            from ..operations.functions import DefineReferenceOperation
            operation = DefineReferenceOperation(node.name, self.define_operations[node.name], self.define_operations)
            self.conversion_stats['operations_created'] += 1
            return [operation]
        else:
            # This is a FHIR field path identifier
            logger.debug(f"Creating PathNavigationOperation for '{node.name}'")
            operation = PathNavigationOperation(node.name)
            self.conversion_stats['operations_created'] += 1
            return [operation]
    
    def _convert_function_call_node(self, node: FunctionCallNode) -> List[PipelineOperation[SQLState]]:
        """Convert FunctionCallNode to pipeline operations."""
        # Convert function arguments - each argument should be treated as a complete entity
        arg_objects = []
        all_supporting_operations = []
        
        for arg in node.args:
            if isinstance(arg, PathNode):
                # Path arguments should be converted to sub-pipelines
                sub_pipeline = self._convert_path_argument_to_pipeline(arg)
                arg_objects.append(sub_pipeline)
            else:
                # Other arguments (literals, etc.) - convert normally
                arg_ops = self._convert_node(arg)
                if len(arg_ops) == 1:
                    # Single operation becomes the argument
                    arg_objects.append(arg_ops[0])
                else:
                    # Multiple operations - treat the last one as the result
                    all_supporting_operations.extend(arg_ops[:-1])
                    arg_objects.append(arg_ops[-1])
        
        # Create function call operation with properly structured arguments
        operation = FunctionCallOperation(node.name, arg_objects)
        self.conversion_stats['operations_created'] += 1
        return all_supporting_operations + [operation]
    
    def _convert_path_argument_to_pipeline(self, path_node: PathNode) -> 'FHIRPathPipeline':
        """Convert a PathNode argument into a sub-pipeline for function argument evaluation."""
        path_operations = self._convert_path_node(path_node)
        return FHIRPathPipeline(path_operations)
    
    def _convert_binary_op_node(self, node: BinaryOpNode) -> List[PipelineOperation[SQLState]]:
        """Convert BinaryOpNode to pipeline operations."""
        # Convert left and right operands
        left_ops = self._convert_node(node.left)
        right_ops = self._convert_node(node.right)
        
        # For binary operations, we need only the final result of each operand as arguments
        # The final result is represented by the last operation in each sequence
        left_result = left_ops[-1] if left_ops else None
        right_result = right_ops[-1] if right_ops else None
        
        # Create function call for the binary operator with only 2 arguments
        operation = FunctionCallOperation(node.operator, [left_result, right_result])
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
                    # Convert function arguments - each argument should be treated as a complete entity
                    arg_objects = []
                    for arg in segment.args:
                        if isinstance(arg, PathNode):
                            # Path arguments should be converted to sub-pipelines
                            sub_pipeline = self._convert_path_argument_to_pipeline(arg)
                            arg_objects.append(sub_pipeline)
                        else:
                            # Other arguments (literals, etc.) - convert normally
                            arg_ops = self._convert_node(arg)
                            if len(arg_ops) == 1:
                                # Single operation becomes the argument
                                arg_objects.append(arg_ops[0])
                            else:
                                # Multiple operations - use the last one as the result
                                arg_objects.append(arg_ops[-1])
                    
                    # Create function call operation with properly structured arguments
                    function_op = FunctionCallOperation(segment.name, arg_objects)
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
    
    def _convert_interval_constructor_node(self, node: IntervalConstructorNode) -> List[PipelineOperation[SQLState]]:
        """Convert IntervalConstructorNode to pipeline operations."""
        logger.debug(f"Converting IntervalConstructorNode with start={node.start}, end={node.end}")
        
        # Create equivalent FunctionCallNode and convert that instead
        equivalent_function = FunctionCallNode("Interval", [node.start, node.end])
        return self._convert_function_call_node(equivalent_function)
    
    def _convert_list_literal_node(self, node: ListLiteralNode) -> List[PipelineOperation[SQLState]]:
        """Convert ListLiteralNode to pipeline operations."""
        logger.debug(f"Converting ListLiteralNode with {len(node.elements)} elements")
        
        # Create equivalent FunctionCallNode and convert that instead
        equivalent_function = FunctionCallNode("List", node.elements)
        return self._convert_function_call_node(equivalent_function)
    
    def _convert_cql_query_expression_node(self, node: CQLQueryExpressionNode) -> List[PipelineOperation[SQLState]]:
        """Convert CQLQueryExpressionNode to pipeline operations."""
        logger.debug(f"Converting CQLQueryExpressionNode with alias '{node.alias}'")
        
        from ...cql.pipeline.operations.query import CQLQueryOperation
        
        # Convert source expression to pipeline operations
        source_operations = self._convert_node(node.source)
        
        # Convert optional clauses to pipeline operations
        where_pipeline = None
        if node.where_clause:
            where_operations = self._convert_node(node.where_clause)
            where_pipeline = FHIRPathPipeline(where_operations)
        
        sort_pipeline = None
        if node.sort_clause:
            sort_operations = self._convert_node(node.sort_clause)
            sort_pipeline = FHIRPathPipeline(sort_operations)
        
        return_pipeline = None
        if node.return_clause:
            return_operations = self._convert_node(node.return_clause)
            return_pipeline = FHIRPathPipeline(return_operations)
        
        # Create source pipeline from source operations
        if source_operations:
            source_pipeline_op = source_operations[-1]  # Use last operation as source
        else:
            # Fallback: create a simple identifier operation
            from ..operations.path import PathNavigationOperation
            source_pipeline_op = PathNavigationOperation("")
        
        # Create CQL query operation with all the components
        query_operation = CQLQueryOperation(
            source_pipeline=source_pipeline_op,
            where_pipeline=where_pipeline,
            sort_pipeline=sort_pipeline,
            sort_direction=node.sort_direction,
            return_pipeline=return_pipeline,
            alias=node.alias
        )
        
        # Return all source operations plus the query operation
        return source_operations + [query_operation]
    
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
        # Add backward compatibility attribute for existing code
        self.ast_to_pipeline_converter = self.converter
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
            
        except (ValueError, InvalidArgumentError, FHIRPathExecutionError, PipelineValidationError) as e:
            # Re-raise validation exceptions (like function argument validation) directly
            raise e
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