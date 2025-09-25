"""
List function handler for FHIRPath operations.

This module implements list manipulation functions including:
- list() constructor for creating lists
- flatten() for flattening nested arrays
- distinct() for removing duplicates
- union(), intersect(), except() for set operations
- first(), last(), tail(), take(), skip() for list slicing
- singleton(), repeat() for list creation
"""

from typing import List, Any
from .base import FunctionHandler
from ...core.base import SQLState, ExecutionContext, ContextMode


class InvalidArgumentError(Exception):
    """Raised when function arguments are invalid."""
    pass


class ListFunctionHandler(FunctionHandler):
    """Handler for list manipulation FHIRPath functions."""
    
    def __init__(self):
        """Initialize with function arguments."""
        self.args = []
    
    def get_supported_functions(self) -> List[str]:
        """Return list of function names this handler supports."""
        return [
            'list', 'flatten', 'distinct', 'union', 'intersect', 'except',
            'first', 'last', 'tail', 'take', 'skip', 'singleton', 'repeat'
        ]
    
    def handle_function(self, function_name: str, input_state: SQLState, 
                       context: ExecutionContext, args: List[Any]) -> SQLState:
        """Execute the specified list function."""
        # Store args for the handler methods
        self.args = args
        func_name = function_name.lower()
        
        # Route to appropriate handler method
        if func_name == 'list':
            return self._handle_list_constructor(input_state, context)
        elif func_name == 'flatten':
            return self._handle_list_flatten(input_state, context)
        elif func_name == 'distinct':
            return self._handle_list_distinct(input_state, context)
        elif func_name == 'union':
            return self._handle_list_union(input_state, context)
        elif func_name == 'intersect':
            return self._handle_list_intersect(input_state, context)
        elif func_name == 'except':
            return self._handle_list_except(input_state, context)
        elif func_name == 'first':
            return self._handle_list_first(input_state, context)
        elif func_name == 'last':
            return self._handle_list_last(input_state, context)
        elif func_name == 'tail':
            return self._handle_list_tail(input_state, context)
        elif func_name == 'take':
            return self._handle_list_take(input_state, context)
        elif func_name == 'skip':
            return self._handle_list_skip(input_state, context)
        elif func_name == 'singleton':
            return self._handle_list_singleton(input_state, context)
        elif func_name == 'repeat':
            return self._handle_list_repeat(input_state, context)
        else:
            raise InvalidArgumentError(f"Unsupported list function: {function_name}")
    
    # =================
    # Handler Methods
    # =================
    
    def _handle_list_constructor(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list constructor: List(item1, item2, ...)"""
        # Convert all arguments to SQL fragments
        item_sqls = []
        for arg in self.args:
            item_sql = self._evaluate_logical_argument(arg, input_state, context)
            item_sqls.append(item_sql)
        
        # Create JSON array with all items
        if item_sqls:
            sql_fragment = f"json_array({', '.join(item_sqls)})"
        else:
            sql_fragment = "json_array()"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=True,  # List is a collection
            context_mode=ContextMode.COLLECTION
        )
    
    def _handle_list_flatten(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list flatten operation."""
        # Placeholder implementation - would flatten nested arrays
        sql_fragment = input_state.sql_fragment  # For now, return as-is
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=True
        )
    
    def _handle_list_distinct(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list distinct operation."""
        # Remove duplicates from array
        sql_fragment = f"""(
            SELECT json_group_array(DISTINCT value)
            FROM json_each({input_state.sql_fragment})
        )"""
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=True
        )
    
    def _handle_list_union(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list union operation."""
        if not self.args:
            return input_state
        
        # Placeholder: combine with other list
        other_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        sql_fragment = f"json_array_union({input_state.sql_fragment}, {other_sql})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=True
        )
    
    def _handle_list_intersect(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list intersect operation."""
        if not self.args:
            return input_state
        
        # Placeholder: intersect with other list
        other_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        sql_fragment = f"json_array_intersect({input_state.sql_fragment}, {other_sql})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=True
        )
    
    def _handle_list_except(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list except operation."""
        if not self.args:
            return input_state
        
        # Placeholder: except with other list
        other_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        sql_fragment = f"json_array_except({input_state.sql_fragment}, {other_sql})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=True
        )
    
    def _handle_list_first(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list first operation."""
        # Get first element from array
        sql_fragment = f"json_extract({input_state.sql_fragment}, '$[0]')"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_list_last(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list last operation."""
        # Get last element from array
        sql_fragment = f"json_extract({input_state.sql_fragment}, '$[#-1]')"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_list_tail(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list tail operation (all except first)."""
        # Placeholder: return all elements except first
        sql_fragment = f"json_tail({input_state.sql_fragment})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=True
        )
    
    def _handle_list_take(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list take operation."""
        if not self.args:
            return input_state
        
        count = str(self.args[0])
        sql_fragment = f"json_take({input_state.sql_fragment}, {count})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=True
        )
    
    def _handle_list_skip(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list skip operation."""
        if not self.args:
            return input_state
        
        count = str(self.args[0])
        sql_fragment = f"json_skip({input_state.sql_fragment}, {count})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=True
        )
    
    def _handle_list_singleton(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list singleton operation."""
        # Create array with single element
        sql_fragment = f"json_array({input_state.sql_fragment})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=True
        )
    
    def _handle_list_repeat(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list repeat operation."""
        if not self.args:
            return input_state
        
        count = str(self.args[0])
        sql_fragment = f"json_repeat({input_state.sql_fragment}, {count})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=True
        )
    
    # =================
    # Helper Methods
    # =================
    
    def _evaluate_logical_argument(self, arg: Any, input_state: SQLState, 
                                   context: ExecutionContext) -> str:
        """Evaluate list function argument to SQL fragment."""
        # Handle simple literals first  
        if not hasattr(arg, '__class__'):
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
            try:
                result_state = arg.execute(input_state, context)
                return result_state.sql_fragment
            except Exception as e:
                raise InvalidArgumentError(f"Failed to execute pipeline operation argument: {e}")
        
        # Handle LiteralOperation objects directly
        if hasattr(arg, 'value') and hasattr(arg, 'literal_type'):
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
            raise InvalidArgumentError(f"Failed to evaluate list argument: {e}")
    
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