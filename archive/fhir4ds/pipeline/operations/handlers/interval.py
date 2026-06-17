"""
Interval function handler for FHIRPath operations.

This module implements interval operations including:
- interval() constructor for creating intervals
- Temporal relationship functions: before, after, meets, during, overlaps
- Inclusion functions: contains, includes, included_in, starts, ends
- Comparison functions: same_as, same_or_before, same_or_after
- Utility functions: width, size, start, end, point_from
"""

from typing import List, Any
from .base import FunctionHandler
from ...core.base import SQLState, ExecutionContext, ContextMode


class InvalidArgumentError(Exception):
    """Raised when function arguments are invalid."""
    pass


class IntervalFunctionHandler(FunctionHandler):
    """Handler for interval FHIRPath functions."""
    
    def __init__(self):
        """Initialize with function arguments."""
        self.args = []
    
    def get_supported_functions(self) -> List[str]:
        """Return list of interval function names this handler supports."""
        return [
            'interval', 'contains', 'overlaps', 'before', 'after', 'meets', 'during',
            'includes', 'included_in', 'starts', 'ends', 'same_as', 'same_or_before',
            'same_or_after', 'width', 'size', 'point_from', 'start', 'end'
        ]
    
    def handle_function(self, function_name: str, input_state: SQLState, 
                       context: ExecutionContext, args: List[Any]) -> SQLState:
        """Execute the specified interval function."""
        # Store args for the handler methods
        self.args = args
        func_name = function_name.lower()
        
        # Route to appropriate handler method
        if func_name == 'interval':
            return self._handle_interval_constructor(input_state, context)
        elif func_name == 'contains':
            return self._handle_interval_contains(input_state, context)
        elif func_name == 'overlaps':
            return self._handle_interval_overlaps(input_state, context)
        elif func_name == 'before':
            return self._handle_interval_before(input_state, context)
        elif func_name == 'after':
            return self._handle_interval_after(input_state, context)
        elif func_name == 'starts':
            return self._handle_interval_starts(input_state, context)
        elif func_name == 'ends':
            return self._handle_interval_ends(input_state, context)
        elif func_name == 'meets':
            return self._handle_interval_meets(input_state, context)
        elif func_name == 'during':
            return self._handle_interval_during(input_state, context)
        elif func_name == 'includes':
            return self._handle_interval_includes(input_state, context)
        elif func_name == 'included_in':
            return self._handle_interval_included_in(input_state, context)
        elif func_name == 'same_as':
            return self._handle_interval_same_as(input_state, context)
        elif func_name == 'same_or_before':
            return self._handle_interval_same_or_before(input_state, context)
        elif func_name == 'same_or_after':
            return self._handle_interval_same_or_after(input_state, context)
        elif func_name == 'width':
            return self._handle_interval_width(input_state, context)
        elif func_name == 'size':
            return self._handle_interval_size(input_state, context)
        elif func_name == 'point_from':
            return self._handle_interval_point_from(input_state, context)
        elif func_name == 'start':
            return self._handle_interval_start(input_state, context)
        elif func_name == 'end':
            return self._handle_interval_end(input_state, context)
        else:
            raise InvalidArgumentError(f"Unsupported interval function: {function_name}")
    
    # =================
    # Handler Methods
    # =================
    
    def _handle_interval_constructor(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval constructor: Interval(start, end)"""
        if len(self.args) != 2:
            raise ValueError("Interval constructor requires exactly 2 arguments")
        
        # Convert arguments to SQL fragments
        start_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        end_sql = self._evaluate_logical_argument(self.args[1], input_state, context)
        
        # Create JSON representation of the interval
        # In a full implementation, this would use database-specific interval types
        sql_fragment = f"""JSON_OBJECT(
            'type', 'interval',
            'start', {start_sql},
            'end', {end_sql},
            'startInclusive', true,
            'endInclusive', true
        )"""
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,  # Interval is not a collection
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_interval_contains(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval contains operation: interval contains value"""
        if len(self.args) != 1:
            raise ValueError("Interval contains requires exactly 1 argument")
        
        # Implement contains logic
        value_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        
        sql_fragment = f"""(
            {value_sql} BETWEEN 
            CAST(JSON_EXTRACT({input_state.sql_fragment}, '$.start') AS NUMERIC) AND
            CAST(JSON_EXTRACT({input_state.sql_fragment}, '$.end') AS NUMERIC)
        )"""
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,  # Boolean result is not a collection
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_interval_overlaps(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval overlaps operation: interval1 overlaps interval2"""
        # Placeholder implementation - would need full interval comparison logic
        sql_fragment = "TRUE"  # Simplified for now
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    # Temporal relationship functions
    def _handle_interval_before(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval before operation.""" 
        return self._handle_interval_overlaps(input_state, context)  # Placeholder
    
    def _handle_interval_after(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval after operation."""
        return self._handle_interval_overlaps(input_state, context)  # Placeholder
    
    def _handle_interval_starts(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval starts operation."""
        return self._handle_interval_overlaps(input_state, context)  # Placeholder
    
    def _handle_interval_ends(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval ends operation."""
        return self._handle_interval_overlaps(input_state, context)  # Placeholder
    
    def _handle_interval_meets(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval meets operation."""
        return self._handle_interval_overlaps(input_state, context)  # Placeholder
        
    def _handle_interval_during(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval during operation."""
        return self._handle_interval_overlaps(input_state, context)  # Placeholder
    
    def _handle_interval_includes(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval includes operation."""
        return self._handle_interval_overlaps(input_state, context)  # Placeholder
    
    def _handle_interval_included_in(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval included_in operation."""
        return self._handle_interval_overlaps(input_state, context)  # Placeholder
    
    # Comparison functions
    def _handle_interval_same_as(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval same_as operation."""
        return self._handle_interval_overlaps(input_state, context)  # Placeholder
    
    def _handle_interval_same_or_before(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval same_or_before operation."""
        return self._handle_interval_overlaps(input_state, context)  # Placeholder
    
    def _handle_interval_same_or_after(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval same_or_after operation."""
        return self._handle_interval_overlaps(input_state, context)  # Placeholder
    
    # Utility functions
    def _handle_interval_width(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval width operation."""
        sql_fragment = f"""(
            CAST(JSON_EXTRACT({input_state.sql_fragment}, '$.end') AS NUMERIC) -
            CAST(JSON_EXTRACT({input_state.sql_fragment}, '$.start') AS NUMERIC)
        )"""
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_interval_size(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval size operation (same as width for numeric intervals)."""
        return self._handle_interval_width(input_state, context)
    
    def _handle_interval_point_from(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval point_from operation."""
        # Placeholder: extract a point from an interval
        sql_fragment = f"CAST(JSON_EXTRACT({input_state.sql_fragment}, '$.start') AS NUMERIC)"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_interval_start(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval start operation."""
        sql_fragment = f"CAST(JSON_EXTRACT({input_state.sql_fragment}, '$.start') AS NUMERIC)"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_interval_end(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval end operation."""
        sql_fragment = f"CAST(JSON_EXTRACT({input_state.sql_fragment}, '$.end') AS NUMERIC)"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    # =================
    # Helper Methods
    # =================
    
    def _evaluate_logical_argument(self, arg: Any, input_state: SQLState, 
                                   context: ExecutionContext) -> str:
        """Evaluate interval function argument to SQL fragment."""
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
            raise InvalidArgumentError(f"Failed to evaluate interval argument: {e}")
    
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