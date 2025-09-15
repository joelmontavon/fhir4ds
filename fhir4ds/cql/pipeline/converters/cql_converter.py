"""
CQL-to-Pipeline Converter.

This module provides the bridge between CQL AST parsing and pipeline execution
by converting CQL AST nodes directly to pipeline operations, bypassing the
intermediate FHIRPath AST conversion.
"""

import logging
from typing import Dict, Any, Optional, Union, List, Tuple
from dataclasses import dataclass

from ...core.parser import (
    CQLASTNode, LibraryNode, DefineNode, RetrieveNode, QueryNode,
    ContextNode, ParameterNode, IncludeNode, WithClauseNode,
    SortClauseNode, LetClauseNode, IntervalLiteralNode
)
from ....fhirpath.parser.ast_nodes import ASTNode, IdentifierNode, FunctionCallNode, PathNode
from ....pipeline.core.base import PipelineOperation, SQLState, ExecutionContext
from ....pipeline.core.builder import FHIRPathPipeline
from ....pipeline.converters.ast_converter import ASTToPipelineConverter
from ..operations import (
    CQLRetrieveOperation, 
    CQLTerminologyOperation,
    CQLQueryOperation,
    CQLWithClauseOperation,
    CQLWithoutClauseOperation,
    CQLDefineOperation
)

logger = logging.getLogger(__name__)

@dataclass
class ConversionContext:
    """Context information for CQL-to-Pipeline conversion."""
    current_context: str = "Patient"
    library_definitions: Dict[str, Any] = None
    parameters: Dict[str, Any] = None
    includes: Dict[str, Any] = None
    aliases: Dict[str, str] = None
    
    def __post_init__(self):
        if self.library_definitions is None:
            self.library_definitions = {}
        if self.parameters is None:
            self.parameters = {}
        if self.includes is None:
            self.includes = {}
        if self.aliases is None:
            self.aliases = {}

class CQLToPipelineConverter:
    """
    Converts CQL AST nodes directly to pipeline operations.
    
    This converter creates a direct path from CQL parsing to pipeline
    execution, avoiding the intermediate FHIRPath AST conversion and
    providing better control over CQL-specific optimizations.
    """
    
    def __init__(self, dialect: Optional[str] = None, define_operations: dict = None):
        """
        Initialize CQL-to-Pipeline converter.
        
        Args:
            dialect: Target database dialect (optional)
            define_operations: Dictionary of define operations for resolving references
        """
        self.dialect = dialect
        self.context = ConversionContext()
        
        # Initialize the AST-to-Pipeline converter for FHIRPath nodes with define operations context
        self.ast_converter = ASTToPipelineConverter(define_operations=define_operations or {})
        
        # Registry of conversion methods
        self.conversion_registry = {
            RetrieveNode: self._convert_retrieve,
            QueryNode: self._convert_query,
            WithClauseNode: self._convert_with_clause,
            SortClauseNode: self._convert_sort_clause,
            DefineNode: self._convert_define,
            LibraryNode: self._convert_library,
            ContextNode: self._convert_context,
            ParameterNode: self._convert_parameter,
            IncludeNode: self._convert_include,
            IntervalLiteralNode: self._convert_interval_literal,
        }
    
    def convert(self, cql_ast: Union[CQLASTNode, ASTNode]) -> Union[PipelineOperation, FHIRPathPipeline]:
        """
        Convert CQL AST node to pipeline operation.
        
        Args:
            cql_ast: CQL AST node to convert
            
        Returns:
            Pipeline operation or FHIRPath pipeline
        """
        logger.debug(f"Converting CQL AST: {type(cql_ast).__name__}")
        
        # Handle CQL-specific nodes
        if isinstance(cql_ast, CQLASTNode):
            logger.debug(f"Processing CQL-specific node: {type(cql_ast).__name__}")
            node_type = type(cql_ast)
            if node_type in self.conversion_registry:
                return self.conversion_registry[node_type](cql_ast)
            else:
                logger.warning(f"No converter for CQL node type: {node_type}")
                return self._convert_fallback_to_fhirpath(cql_ast)
        
        # Handle FHIRPath nodes by building pipeline
        elif isinstance(cql_ast, ASTNode):
            logger.debug(f"Processing FHIRPath AST node: {type(cql_ast).__name__}")
            # Check if this is a FunctionCallNode with CQL arguments
            if isinstance(cql_ast, FunctionCallNode) and self._is_cql_function_with_query_args(cql_ast):
                logger.debug(f"Converting as CQL function call")
                return self._convert_cql_function_call(cql_ast)
            else:
                logger.debug(f"Converting as FHIRPath pipeline")
                return self._convert_fhirpath_to_pipeline(cql_ast)
        
        else:
            raise ValueError(f"Unsupported AST node type: {type(cql_ast)}")
    
    def _convert_retrieve(self, retrieve: RetrieveNode) -> CQLRetrieveOperation:
        """
        Convert CQL retrieve node to CQLRetrieveOperation.
        
        Args:
            retrieve: RetrieveNode to convert
            
        Returns:
            CQLRetrieveOperation
        """
        logger.debug(f"Converting retrieve: {retrieve.resource_type}")
        
        # Extract terminology if present
        terminology = getattr(retrieve, 'terminology', None)
        code_path = getattr(retrieve, 'code_path', None) or "code"
        
        # Determine alias from context or generate one
        alias = self._get_alias_for_resource(retrieve.resource_type)
        
        return CQLRetrieveOperation(
            resource_type=retrieve.resource_type,
            terminology=terminology,
            code_path=code_path,
            alias=alias
        )
    
    def _convert_query(self, query: QueryNode) -> CQLQueryOperation:
        """
        Convert CQL query node to CQLQueryOperation.
        
        Args:
            query: QueryNode to convert
            
        Returns:
            CQLQueryOperation
        """
        logger.debug("Converting query node")
        
        # Convert source to pipeline operation
        source_pipeline = None
        if query.source:
            source_converted = self.convert(query.source)
            if isinstance(source_converted, PipelineOperation):
                source_pipeline = source_converted
            # If it's a FHIRPathPipeline, we need to extract operations
        
        # Convert WHERE clause
        where_condition = None
        where_pipeline = None
        if query.where_clause:
            where_converted = self._convert_where_clause(query.where_clause)
            if isinstance(where_converted, str):
                where_condition = where_converted
            else:
                where_pipeline = where_converted
        
        # Convert RETURN clause
        return_expression = None
        return_pipeline = None
        if query.return_clause:
            return_converted = self._convert_return_clause(query.return_clause)
            if isinstance(return_converted, str):
                return_expression = return_converted
            else:
                return_pipeline = return_converted
        
        # Convert SORT clause
        sort_expression = None
        sort_pipeline = None
        sort_direction = "asc"
        if query.sort_clause:
            sort_converted, direction = self._convert_sort_clause_to_components(query.sort_clause)
            if isinstance(sort_converted, str):
                sort_expression = sort_converted
            else:
                sort_pipeline = sort_converted
            sort_direction = direction
        
        # Determine alias
        alias = self._get_query_alias(query)
        
        return CQLQueryOperation(
            source_pipeline=source_pipeline,
            where_condition=where_condition,
            where_pipeline=where_pipeline,
            return_expression=return_expression,
            return_pipeline=return_pipeline,
            sort_expression=sort_expression,
            sort_pipeline=sort_pipeline,
            sort_direction=sort_direction,
            alias=alias
        )
    
    def _convert_with_clause(self, with_clause: WithClauseNode) -> CQLWithClauseOperation:
        """
        Convert CQL with clause node to CQLWithClauseOperation.
        
        Args:
            with_clause: WithClauseNode to convert
            
        Returns:
            CQLWithClauseOperation
        """
        logger.debug(f"Converting with clause: {with_clause.identifier}")
        
        # Convert the relation expression to a pipeline operation
        relation_operation = self.convert(with_clause.expression)
        
        # Check if there's a condition (such that clause)
        such_that_condition = getattr(with_clause, 'such_that_condition', None)
        such_that_pipeline = getattr(with_clause, 'such_that_expression', None)
        
        if such_that_pipeline:
            such_that_pipeline = self.convert(such_that_pipeline)
        
        return CQLWithClauseOperation(
            identifier=with_clause.identifier,
            relation_operation=relation_operation,
            such_that_condition=such_that_condition,
            such_that_pipeline=such_that_pipeline
        )
    
    def _convert_sort_clause(self, sort_clause: SortClauseNode) -> Tuple[str, str]:
        """
        Convert CQL sort clause to components.
        
        Args:
            sort_clause: SortClauseNode to convert
            
        Returns:
            Tuple of (sort_expression, direction)
        """
        logger.debug("Converting sort clause")
        
        # Convert the sort expression
        if hasattr(sort_clause, 'expression') and sort_clause.expression:
            sort_expr = self._convert_expression_to_sql(sort_clause.expression)
        else:
            sort_expr = "resource"  # Default sort
        
        # Get direction
        direction = getattr(sort_clause, 'direction', 'ASC').lower()
        if direction not in ['asc', 'desc']:
            direction = 'asc'
        
        return sort_expr, direction
    
    def _convert_sort_clause_to_components(self, sort_clause: SortClauseNode) -> Tuple[Union[str, PipelineOperation], str]:
        """
        Convert sort clause to components for CQLQueryOperation.
        
        Args:
            sort_clause: SortClauseNode to convert
            
        Returns:
            Tuple of (sort_expression_or_pipeline, direction)
        """
        # For now, convert to simple string expression
        # TODO: Support pipeline-based sort expressions
        expr, direction = self._convert_sort_clause(sort_clause)
        return expr, direction
    
    def _convert_where_clause(self, where_clause: Any) -> Union[str, PipelineOperation]:
        """
        Convert WHERE clause to appropriate format.
        
        Args:
            where_clause: WHERE clause AST node
            
        Returns:
            String expression or pipeline operation
        """
        # For now, convert to simple SQL string
        # TODO: Support complex pipeline-based WHERE clauses
        if hasattr(where_clause, 'expression'):
            return self._convert_expression_to_sql(where_clause.expression)
        else:
            return self._convert_expression_to_sql(where_clause)
    
    def _convert_return_clause(self, return_clause: Any) -> Union[str, PipelineOperation]:
        """
        Convert RETURN clause to appropriate format.
        
        Args:
            return_clause: RETURN clause AST node
            
        Returns:
            String expression or pipeline operation
        """
        # For now, convert to simple SQL string
        # TODO: Support complex pipeline-based RETURN clauses
        if hasattr(return_clause, 'expression'):
            return self._convert_expression_to_sql(return_clause.expression)
        else:
            return self._convert_expression_to_sql(return_clause)
    
    def _convert_expression_to_sql(self, expression: Any) -> str:
        """
        Convert AST expression to SQL string.
        
        This is a simplified conversion for basic expressions.
        More complex expressions should use pipeline operations.
        
        Args:
            expression: AST expression node
            
        Returns:
            SQL expression string
        """
        if isinstance(expression, IdentifierNode):
            # Convert identifier to JSON extraction
            return f"json_extract(resource, '$.{expression.name}')"
        
        elif isinstance(expression, PathNode):
            # Convert path navigation to JSON extraction
            # PathNode contains a list of segments
            path_parts = []
            for segment in expression.segments:
                if isinstance(segment, IdentifierNode):
                    path_parts.append(segment.name)
                else:
                    # For other types, convert to string
                    path_parts.append(str(segment))
            
            full_path = '.'.join(path_parts)
            return f"json_extract(resource, '$.{full_path}')"
        
        elif isinstance(expression, FunctionCallNode):
            # Handle function calls
            if expression.name.lower() == 'equals' and len(expression.args) == 2:
                left = self._convert_expression_to_sql(expression.args[0])
                right = self._convert_expression_to_sql(expression.args[1])
                return f"({left} = {right})"
            # TODO: Add more function conversions
        
        elif isinstance(expression, str):
            # Literal string
            return f"'{expression}'"
        
        elif isinstance(expression, (int, float)):
            # Literal number
            return str(expression)
        
        # Fallback: convert to string representation
        return str(expression)
    
    def _convert_fhirpath_to_pipeline(self, fhirpath_ast: ASTNode) -> FHIRPathPipeline:
        """
        Convert FHIRPath AST to FHIRPath pipeline.
        
        Args:
            fhirpath_ast: FHIRPath AST node
            
        Returns:
            FHIRPath pipeline
        """
        # Use the proper AST-to-Pipeline converter for comprehensive FHIRPath support
        try:
            return self.ast_converter.convert_ast_to_pipeline(fhirpath_ast)
        except Exception as e:
            logger.error(f"Failed to convert FHIRPath AST {type(fhirpath_ast).__name__}: {e}")
            # Fallback to empty pipeline
            return FHIRPathPipeline()
    
    def _convert_fallback_to_fhirpath(self, cql_ast: CQLASTNode) -> FHIRPathPipeline:
        """
        Fallback conversion for unsupported CQL nodes.
        
        Args:
            cql_ast: CQL AST node
            
        Returns:
            Basic FHIRPath pipeline
        """
        logger.warning(f"Using fallback conversion for {type(cql_ast)}")
        return FHIRPathPipeline()
    
    def _convert_define(self, define: DefineNode) -> CQLDefineOperation:
        """
        Convert CQL define node to executable pipeline operation.
        
        Args:
            define: DefineNode to convert
            
        Returns:
            CQLDefineOperation for execution
        """
        logger.debug(f"Converting define: {define.name}")
        
        # Convert the define expression to a pipeline operation
        converted_expression = self.convert(define.expression)
        
        # Ensure we have a PipelineOperation or can extract one
        if isinstance(converted_expression, PipelineOperation):
            # Direct PipelineOperation - wrap in a pipeline
            expression_pipeline = FHIRPathPipeline([converted_expression])
        elif isinstance(converted_expression, FHIRPathPipeline):
            # FHIRPathPipeline - check if we can extract a single operation for simple expressions
            if len(converted_expression.operations) == 1:
                # Single operation pipeline - this is likely an arithmetic or simple expression
                logger.debug(f"Extracted single operation from FHIRPathPipeline for define '{define.name}'")
                expression_pipeline = converted_expression
            elif len(converted_expression.operations) > 0:
                # Multi-operation pipeline - use as is
                logger.debug(f"Using multi-operation FHIRPathPipeline for define '{define.name}' ({len(converted_expression.operations)} operations)")
                expression_pipeline = converted_expression
            else:
                # Empty pipeline - create basic pipeline
                logger.warning(f"Empty FHIRPathPipeline for define '{define.name}', creating fallback")
                expression_pipeline = FHIRPathPipeline()
        elif hasattr(converted_expression, 'compile'):
            # Some other pipeline-like object
            expression_pipeline = converted_expression
        else:
            # Last resort: create basic FHIRPath pipeline
            logger.warning(f"Unknown converted expression type for define '{define.name}': {type(converted_expression)}")
            expression_pipeline = FHIRPathPipeline()
        
        # Create the define operation
        define_operation = CQLDefineOperation(
            define_name=define.name,
            expression_pipeline=expression_pipeline,
            access_level=define.access_level,
            definition_metadata={'original_expression': str(define.expression)}
        )
        
        # Store in context for reference
        self.context.library_definitions[define.name] = {
            'operation': define_operation,
            'access_level': define.access_level
        }
        
        return define_operation
    
    def _convert_library(self, library: LibraryNode) -> Dict[str, Any]:
        """
        Convert CQL library node.
        
        Args:
            library: LibraryNode to convert
            
        Returns:
            Library information
        """
        logger.info(f"Converting library: {library.name}")
        
        # Convert all library components
        converted_library = {
            'name': library.name,
            'version': library.version,
            'context': library.context,
            'includes': [self.convert(inc) for inc in library.includes],
            'parameters': [self.convert(param) for param in library.parameters],
            'definitions': [self.convert(defn) for defn in library.definitions]
        }
        
        return converted_library
    
    def _convert_context(self, context: ContextNode) -> Dict[str, str]:
        """Convert CQL context node."""
        self.context.current_context = context.context_name
        return {'context': context.context_name}
    
    def _convert_parameter(self, parameter: ParameterNode) -> Dict[str, Any]:
        """Convert CQL parameter node."""
        param_info = {
            'name': parameter.name,
            'type': parameter.parameter_type,
            'default': parameter.default_value
        }
        self.context.parameters[parameter.name] = param_info
        return param_info
    
    def _convert_include(self, include: IncludeNode) -> Dict[str, str]:
        """Convert CQL include node."""
        include_info = {
            'library': include.library_name,
            'version': include.version,
            'alias': include.alias
        }
        if include.alias:
            self.context.includes[include.alias] = include_info
        return include_info
    
    def _get_alias_for_resource(self, resource_type: str) -> str:
        """Get alias for resource type."""
        # Use first letter of resource type as default alias
        return resource_type[0].upper()
    
    def _get_query_alias(self, query: QueryNode) -> Optional[str]:
        """Get alias for query from aliases list."""
        if hasattr(query, 'aliases') and query.aliases:
            return query.aliases[0]
        return None
    
    def convert_library(self, library: LibraryNode) -> Dict[str, Any]:
        """
        Convert entire CQL library to pipeline operations.
        
        Args:
            library: LibraryNode representing parsed library
            
        Returns:
            Dictionary of converted library components
        """
        return self._convert_library(library)
    
    def _is_cql_function_with_query_args(self, function_call: FunctionCallNode) -> bool:
        """
        Check if this is a CQL function call with CQL query arguments.
        
        Args:
            function_call: FunctionCallNode to check
            
        Returns:
            True if this is a CQL function with query arguments
        """
        # List of CQL functions that take query expressions as arguments
        cql_query_functions = {
            'exists', 'count', 'first', 'last', 'sum', 'max', 'min', 
            'avg', 'average', 'stddev', 'variance', 'median', 'mode'
        }
        
        if function_call.name.lower() not in cql_query_functions:
            return False
            
        # Check if any arguments are CQL QueryNodes
        for arg in function_call.args:
            # Import here to avoid circular imports
            from ...core.parser import QueryNode
            if isinstance(arg, QueryNode):
                return True
                
        return False
    
    def _convert_cql_function_call(self, function_call: FunctionCallNode) -> PipelineOperation:
        """
        Convert CQL function calls with query arguments to pipeline operations.
        
        Args:
            function_call: FunctionCallNode with CQL query arguments
            
        Returns:
            Pipeline operation for the function call
        """
        function_name = function_call.name.lower()
        
        if not function_call.args:
            raise ValueError(f"CQL function {function_call.name} requires arguments")
            
        # For now, handle single query argument functions
        query_arg = function_call.args[0]
        
        # Convert the query argument to a pipeline operation
        from ...core.parser import QueryNode
        if isinstance(query_arg, QueryNode):
            source_operation = self.convert(query_arg)
        else:
            raise ValueError(f"Unsupported argument type for CQL function: {type(query_arg)}")
        
        # Create appropriate CQL operation based on function type
        if function_name == 'exists':
            # Exists is essentially a Count > 0 check, but we can create a specialized operation
            from ..operations import CQLQueryOperation
            return CQLQueryOperation(
                source_pipeline=source_operation,
                # EXISTS can be implemented as a LIMIT 1 with COUNT check
                return_expression="CASE WHEN COUNT(*) > 0 THEN true ELSE false END"
            )
            
        elif function_name == 'count':
            from ..operations import CQLQueryOperation
            return CQLQueryOperation(
                source_pipeline=source_operation,
                return_expression="COUNT(*)"
            )
            
        elif function_name in ['first', 'last']:
            from ..operations import CQLQueryOperation
            # First/Last can be implemented with LIMIT 1 and appropriate ordering
            return CQLQueryOperation(
                source_pipeline=source_operation,
                # For now, return the first/last resource
                # TODO: Support proper ordering for First/Last
                return_expression="resource"
            )
            
        elif function_name in ['sum', 'max', 'min', 'avg', 'average']:
            from ..operations import CQLQueryOperation
            # These require a field to aggregate - for now return placeholder
            agg_func = function_name.upper()
            if agg_func == 'AVERAGE':
                agg_func = 'AVG'
            return CQLQueryOperation(
                source_pipeline=source_operation,
                return_expression=f"{agg_func}(1)"  # Placeholder - needs field extraction
            )
            
        else:
            # Fallback for other functions
            from ..operations import CQLQueryOperation
            return CQLQueryOperation(
                source_pipeline=source_operation,
                return_expression="resource"
            )

    def _convert_interval_literal(self, interval: IntervalLiteralNode) -> FHIRPathPipeline:
        """
        Convert CQL interval literal to FHIRPath pipeline.

        Args:
            interval: IntervalLiteralNode to convert

        Returns:
            FHIRPath pipeline with interval literal operation
        """
        logger.debug(f"Converting interval literal: {interval}")

        from ....pipeline.operations.literals import LiteralOperation
        from ....pipeline.core.builder import FHIRPathPipeline

        # Format the interval as a string representation for SQL generation
        start_bracket = '[' if interval.start_inclusive else '('
        end_bracket = ']' if interval.end_inclusive else ')'
        interval_value = f"Interval{start_bracket}{interval.start_value}, {interval.end_value}{end_bracket}"

        # Create a literal operation for the interval
        literal_op = LiteralOperation(value=interval_value, value_type='value')

        # Create and return FHIRPath pipeline with the literal operation
        pipeline = FHIRPathPipeline(operations=[literal_op])

        logger.debug(f"Created interval literal pipeline: {interval_value}")
        return pipeline

    def reset_context(self):
        """Reset conversion context."""
        self.context = ConversionContext()