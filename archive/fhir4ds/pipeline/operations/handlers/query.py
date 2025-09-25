"""
Query function handler for CQL/FHIRPath operations.

This module implements CQL query operations including:
- from() for Cartesian product of collections  
- where() for filtering collections
- return() for projection operations
- sort() for ordering collections
- aggregate() for aggregation operations
- retrievenode() for resource retrieval
"""

import logging
from typing import List, Any
from .base import FunctionHandler
from ...core.base import SQLState, ExecutionContext, ContextMode

logger = logging.getLogger(__name__)


class InvalidArgumentError(Exception):
    """Raised when function arguments are invalid."""
    pass


class QueryFunctionHandler(FunctionHandler):
    """Handler for CQL query FHIRPath functions."""
    
    def __init__(self):
        """Initialize with function arguments."""
        self.args = []
    
    def get_supported_functions(self) -> List[str]:
        """Return list of query function names this handler supports."""
        return ['from', 'where', 'return', 'sort', 'aggregate', 'retrievenode']
    
    def handle_function(self, function_name: str, input_state: SQLState, 
                       context: ExecutionContext, args: List[Any]) -> SQLState:
        """Execute the specified query function."""
        # Store args for the handler methods
        self.args = args
        func_name = function_name.lower()
        
        # Route to appropriate handler method
        if func_name == 'from':
            return self._handle_from_query(input_state, context)
        elif func_name == 'sort':
            return self._handle_sort_query(input_state, context)
        elif func_name == 'aggregate':
            return self._handle_aggregate_query(input_state, context)
        elif func_name == 'where':
            return self._handle_where_query(input_state, context)
        elif func_name == 'return':
            return self._handle_return_query(input_state, context)
        elif func_name == 'retrievenode':
            return self._handle_retrievenode_query(input_state, context)
        else:
            raise InvalidArgumentError(f"Unsupported query function: {function_name}")
    
    # =================
    # Handler Methods  
    # =================
    
    def _handle_from_query(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle FROM query (Cartesian product of collections)."""
        logger.debug(f"Executing FROM query with {len(self.args)} arguments")
        
        # For now, implement a basic Cartesian product placeholder
        # This should create SQL that joins multiple collections
        
        # Extract collection expressions and aliases
        collections = []
        aliases = []
        
        # Parse args: should be alternating collection expressions and alias names
        for i in range(0, len(self.args), 2):
            if i + 1 < len(self.args):
                collection_expr = self.args[i]
                alias = self.args[i + 1] if isinstance(self.args[i + 1], str) else f"alias_{i}"
                
                # Convert collection expression to SQL
                collection_sql = self._evaluate_logical_argument(collection_expr, input_state, context)
                collections.append(collection_sql)
                aliases.append(alias)
        
        # Generate basic Cartesian product SQL
        # For now, return a simple JSON array representing the collections
        if collections:
            combined_sql = f"json_array({', '.join(collections)})"
        else:
            combined_sql = "json_array()"
        
        return input_state.evolve(
            sql_fragment=combined_sql,
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    def _handle_sort_query(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle SORT query."""
        # For now, return the input unchanged - full sorting would require SQL ORDER BY
        return input_state.evolve(
            sql_fragment=input_state.sql_fragment,
            is_collection=True
        )
    
    def _handle_aggregate_query(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle AGGREGATE query."""
        # For now, return the input unchanged - full aggregation would require complex SQL
        return input_state.evolve(
            sql_fragment=input_state.sql_fragment,
            is_collection=True
        )
    
    def _handle_where_query(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle WHERE query."""
        # For now, return the input unchanged - full filtering would require SQL WHERE clause
        return input_state.evolve(
            sql_fragment=input_state.sql_fragment,
            is_collection=True
        )
    
    def _handle_return_query(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle RETURN query."""
        # For now, return the input unchanged - full return would require SQL SELECT
        return input_state.evolve(
            sql_fragment=input_state.sql_fragment,
            is_collection=input_state.is_collection
        )
    
    def _handle_retrievenode_query(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """
        Handle RetrieveNode query for resource retrieval.
        
        This method handles the case where a RetrieveNode is converted to a string
        and parsed as a function call "retrievenode()". It generates SQL to retrieve
        all resources of a particular type from the FHIR database.
        """
        logger.info("🔧 Handling RetrieveNode query - converting to resource retrieval SQL")
        
        # FIXED: Advanced context-aware resource type detection
        resource_type = "Patient"  # Default fallback
        
        # Method 1: Check if arguments contain resource type information
        if self.args and len(self.args) > 0:
            # First argument might contain resource type information
            arg_str = str(self.args[0]).strip()
            if arg_str and arg_str != '[]':
                # Extract resource type from argument
                resource_type = arg_str
                logger.debug(f"Extracted resource type from arguments: {resource_type}")
        
        # Method 2: Check if the input_state has resource_type information
        if hasattr(input_state, 'resource_type') and input_state.resource_type:
            resource_type = input_state.resource_type
            logger.debug(f"Using resource type from input state: {resource_type}")
        
        # Method 3: Check for context information
        if hasattr(context, 'current_resource_type') and context.current_resource_type:
            resource_type = context.current_resource_type
            logger.debug(f"Using resource type from context: {resource_type}")
        
        logger.info(f"🎯 RetrieveNode resolved to resource type: {resource_type}")
        
        # Generate SQL for resource retrieval using the base table
        base_table = input_state.base_table
        json_column = input_state.json_column
        
        # Create a collection query that selects all resources of the specified type
        sql_fragment = f"SELECT {json_column} FROM {base_table} WHERE {json_column}->>'resourceType' = '{resource_type}'"
        
        logger.debug(f"Generated RetrieveNode SQL: {sql_fragment}")
        
        return SQLState(
            base_table=base_table,
            json_column=json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=ContextMode.COLLECTION,
            resource_type=resource_type,
            is_collection=True,
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    # =================
    # Helper Methods
    # =================
    
    def _evaluate_logical_argument(self, arg: Any, input_state: SQLState, 
                                   context: ExecutionContext) -> str:
        """Evaluate query function argument to SQL fragment."""
        # Handle simple literals first  
        if not hasattr(arg, '__class__'):
            # Simple literal - convert boolean values
            if arg is True:
                return "TRUE"
            elif arg is False:
                return "FALSE" 
            elif arg is None:
                return "NULL"
            else:
                return str(arg)
        
        # Handle pipeline operations (like LiteralOperation)
        if hasattr(arg, 'execute'):
            # This is a pipeline operation - execute it to get the SQL fragment
            try:
                result_state = arg.execute(input_state, context)
                return result_state.sql_fragment
            except Exception as e:
                raise InvalidArgumentError(f"Failed to execute pipeline operation argument: {e}")
        
        # Handle LiteralOperation objects directly (if not handled by execute)
        if hasattr(arg, 'value') and hasattr(arg, 'literal_type'):
            # This is a LiteralOperation from the pipeline
            if arg.value in ['true', True]:
                return "TRUE"
            elif arg.value in ['false', False]:
                return "FALSE"
            elif arg.value is None or arg.value == 'null':
                return "NULL"
            else:
                return str(arg.value)
        
        # For other complex objects, try AST conversion
        try:
            return self._convert_ast_argument_to_sql(arg, input_state, context)
        except Exception as e:
            raise InvalidArgumentError(f"Failed to evaluate query argument: {e}")
    
    def _convert_ast_argument_to_sql(self, ast_node: Any, input_state: SQLState,
                                    context: ExecutionContext) -> str:
        """Convert AST node argument to SQL fragment."""
        from ...converters.ast_converter import ASTToPipelineConverter
        
        converter = ASTToPipelineConverter()
        base_state = self._create_base_copy(input_state)
        
        arg_pipeline = converter.convert_ast_to_pipeline(ast_node)
        current_state = base_state
        
        for op in arg_pipeline.operations:
            current_state = op.execute(current_state, context)
        
        return current_state.sql_fragment
    
    def _create_base_copy(self, input_state: SQLState) -> SQLState:
        """Create a fresh base state copy for argument evaluation."""
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=input_state.json_column,
            resource_type=input_state.resource_type
        )