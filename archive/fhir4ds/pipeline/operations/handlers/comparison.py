"""
Comparison function handler for FHIRPath operations.

This module implements comparison operators including:
- Equality operators (=, !=, <>)
- Relational operators (<, <=, >, >=)
- Membership operators (in, contains)
- Equivalence operator (~)
"""

from typing import List, Any
from .base import FunctionHandler
from ...core.base import SQLState, ExecutionContext, ContextMode


class InvalidArgumentError(Exception):
    """Raised when function arguments are invalid."""
    pass


class ConversionError(Exception):
    """Raised when AST conversion fails."""
    pass


class ComparisonFunctionHandler(FunctionHandler):
    """Handler for comparison and relational FHIRPath functions."""
    
    def __init__(self, args: List[Any] = None):
        """Initialize with function arguments."""
        self.args = args or []
    
    def get_supported_functions(self) -> List[str]:
        """Return list of comparison function names this handler supports."""
        return [
            '=', '!=', '<>', '<', '<=', '>', '>=', 
            'in', 'contains', '~'
        ]
    
    def handle_function(self, function_name: str, input_state: SQLState, 
                       context: ExecutionContext, args: List[Any]) -> SQLState:
        """Execute the specified comparison function."""
        # Set args for this function call
        self.args = args
        
        # Route to appropriate handler method
        handler_map = {
            '=': self._handle_equals,
            '!=': self._handle_not_equals,
            '<>': self._handle_not_equals,
            '<': self._handle_less_than,
            '<=': self._handle_less_equals,
            '>': self._handle_greater_than,
            '>=': self._handle_greater_equals,
            'in': self._handle_in,
            'contains': self._handle_contains_comparison,
            '~': self._handle_equivalent,
        }
        
        handler_func = handler_map.get(function_name)
        if not handler_func:
            raise InvalidArgumentError(f"Unsupported comparison function: {function_name}")
        
        return handler_func(input_state, context)
    
    # =================
    # Handler Methods
    # =================
    
    def _handle_equals(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle equals (=) operator."""
        if not self.args:
            raise ValueError("Equals operator requires at least one argument")
        
        # Get the right operand from arguments
        if hasattr(self.args[0], 'value'):
            right_value = f"'{self.args[0].value}'"
        else:
            right_value = str(self.args[0])
        
        sql_fragment = f"{input_state.sql_fragment} = {right_value}"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_not_equals(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle not equals (!= or <>) operator."""
        if not self.args:
            raise ValueError("Not equals operator requires at least one argument")
        
        # Get the right operand from arguments
        if hasattr(self.args[0], 'value'):
            right_value = f"'{self.args[0].value}'"
        else:
            right_value = str(self.args[0])
        
        sql_fragment = f"{input_state.sql_fragment} != {right_value}"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_less_than(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle less than (<) operator."""
        if not self.args:
            raise ValueError("Less than operator requires at least one argument")
        
        # Get the right operand from arguments - properly evaluate function calls
        if hasattr(self.args[0], 'value'):
            right_value = str(self.args[0].value)
        else:
            # For complex objects like function calls, evaluate them properly
            try:
                right_value = self._evaluate_union_argument(self.args[0], input_state, context)
            except Exception:
                # Fallback to string representation
                right_value = str(self.args[0])
        
        sql_fragment = f"{input_state.sql_fragment} < {right_value}"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_less_equals(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle less than or equals (<=) operator."""
        if not self.args:
            raise ValueError("Less than or equals operator requires at least one argument")
        
        # Get the right operand from arguments - properly evaluate function calls
        if hasattr(self.args[0], 'value'):
            right_value = str(self.args[0].value)
        else:
            # For complex objects like function calls, evaluate them properly
            try:
                right_value = self._evaluate_union_argument(self.args[0], input_state, context)
            except Exception:
                # Fallback to string representation
                right_value = str(self.args[0])
        
        sql_fragment = f"{input_state.sql_fragment} <= {right_value}"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_greater_than(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle greater than (>) operator."""
        if not self.args:
            raise ValueError("Greater than operator requires at least one argument")
        
        # Get the right operand from arguments - properly evaluate function calls
        if hasattr(self.args[0], 'value'):
            right_value = str(self.args[0].value)
        else:
            # For complex objects like function calls, evaluate them properly
            try:
                right_value = self._evaluate_union_argument(self.args[0], input_state, context)
            except Exception:
                # Fallback to string representation
                right_value = str(self.args[0])
        
        sql_fragment = f"{input_state.sql_fragment} > {right_value}"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_greater_equals(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle greater than or equals (>=) operator."""
        if not self.args:
            raise ValueError("Greater than or equals operator requires at least one argument")
        
        # Get the right operand from arguments - properly evaluate function calls
        if hasattr(self.args[0], 'value'):
            right_value = str(self.args[0].value)
        else:
            # For complex objects like function calls, evaluate them properly
            try:
                right_value = self._evaluate_union_argument(self.args[0], input_state, context)
            except Exception:
                # Fallback to string representation
                right_value = str(self.args[0])
        
        sql_fragment = f"{input_state.sql_fragment} >= {right_value}"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_in(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle 'in' operator."""
        if not self.args:
            raise ValueError("In operator requires at least one argument")
        
        # Handle collection argument for IN clause
        if isinstance(self.args[0], (list, tuple)):
            values = [f"'{v}'" if isinstance(v, str) else str(v) for v in self.args[0]]
            right_value = f"({', '.join(values)})"
        else:
            right_value = f"({self.args[0]})"
        
        sql_fragment = f"{input_state.sql_fragment} IN {right_value}"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_contains_comparison(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle 'contains' comparison operator."""
        if not self.args:
            raise ValueError("Contains operator requires at least one argument")
        
        # Get the search value
        if hasattr(self.args[0], 'value'):
            search_value = f"'%{self.args[0].value}%'"
        else:
            search_value = f"'%{self.args[0]}%'"
        
        # Use LIKE for contains functionality
        sql_fragment = f"{input_state.sql_fragment} LIKE {search_value}"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_equivalent(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle equivalent (~) operator with type-aware comparison."""
        if not self.args:
            raise ValueError("Equivalent operator requires at least one argument")
        
        # Convert argument to SQL fragment - use logical argument evaluator like other operators
        right_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        
        # The equivalent operator (~) performs type-aware comparison
        # For numeric types, it compares values accounting for type coercion
        # For example, 5 ~ 5.0 should return true even though types differ
        # Use dialect-specific equivalent check
        sql_fragment = context.dialect.generate_equivalent_check(input_state.sql_fragment, right_sql)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    # =================
    # Helper Methods
    # =================
    
    def _create_scalar_result(self, input_state: SQLState, sql_fragment: str) -> SQLState:
        """Helper to create scalar results consistently."""
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _create_collection_result(self, input_state: SQLState, sql_fragment: str) -> SQLState:
        """Helper to create collection results consistently."""
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    def _evaluate_logical_argument(self, arg: Any, input_state: SQLState, 
                                   context: ExecutionContext) -> str:
        """Evaluate logical function argument to SQL fragment."""
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
        except ConversionError as e:
            raise InvalidArgumentError(f"Failed to evaluate logical argument: {e}")
    
    def _evaluate_union_argument(self, arg: Any, input_state: SQLState, context: ExecutionContext) -> str:
        """Evaluate union function argument to SQL fragment."""
        if not hasattr(arg, '__class__'):
            return str(arg)  # Simple literal
        
        # Check if this is a pipeline operation (like FunctionCallOperation)
        if hasattr(arg, 'execute') and callable(getattr(arg, 'execute')):
            # This is a pipeline operation - execute it directly
            base_state = self._create_base_copy(input_state)
            result_state = arg.execute(base_state, context)
            return result_state.sql_fragment
        
        # For AST nodes, use the AST converter
        try:
            return self._convert_ast_argument_to_sql(arg, input_state, context)
        except ConversionError as e:
            raise InvalidArgumentError(f"Failed to evaluate union argument: {e}")
    
    def _convert_ast_argument_to_sql(self, ast_node: Any, input_state: SQLState,
                                    context: ExecutionContext) -> str:
        """Convert AST node argument to SQL fragment."""
        from ...converters.ast_converter import ASTToPipelineConverter
        
        converter = ASTToPipelineConverter()
        base_state = self._create_base_copy(input_state)  # Use helper method
        
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