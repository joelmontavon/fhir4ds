"""
Logical function handler for FHIRPath operations.

This module implements logical operators with three-valued logic including:
- Boolean operators (and, or, not)
- Implication operator (implies)  
- Exclusive OR operator (xor)
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


class LogicalFunctionHandler(FunctionHandler):
    """Handler for logical FHIRPath functions with three-valued logic support."""
    
    def __init__(self, args: List[Any] = None):
        """Initialize with function arguments."""
        self.args = args or []
    
    def get_supported_functions(self) -> List[str]:
        """Return list of logical function names this handler supports."""
        return ['and', 'or', 'not', 'implies', 'xor']
    
    def handle_function(self, function_name: str, input_state: SQLState, 
                       context: ExecutionContext, args: List[Any]) -> SQLState:
        """Execute the specified logical function."""
        # Set args for this function call
        self.args = args
        self.func_name = function_name.lower()
        
        # Route to appropriate handler method
        handler_map = {
            'and': self._handle_and,
            'or': self._handle_or,
            'not': self._handle_not,
            'implies': self._handle_implies,
            'xor': self._handle_xor,
        }
        
        handler_func = handler_map.get(self.func_name)
        if not handler_func:
            raise InvalidArgumentError(f"Unsupported logical function: {function_name}")
        
        return handler_func(input_state, context)
    
    # =================
    # Handler Methods
    # =================
    
    def _handle_and(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle logical AND with three-valued logic."""
        if len(self.args) != 2:
            raise ValueError("AND operator requires exactly 2 arguments")
        
        # Convert arguments to SQL fragments
        left_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        right_sql = self._evaluate_logical_argument(self.args[1], input_state, context)
        
        # Implement three-valued logic for AND: 
        # T and T = T, T and F = F, T and null = null
        # F and T = F, F and F = F, F and null = F  
        # null and T = null, null and F = F, null and null = null
        sql_fragment = f"""(
            CASE 
                WHEN ({left_sql}) IS FALSE OR ({right_sql}) IS FALSE THEN FALSE
                WHEN ({left_sql}) IS NULL OR ({right_sql}) IS NULL THEN NULL
                ELSE ({left_sql}) AND ({right_sql})
            END
        )"""
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_or(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle logical OR with three-valued logic."""
        if len(self.args) != 2:
            raise ValueError("OR operator requires exactly 2 arguments")
        
        # Convert arguments to SQL fragments
        left_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        right_sql = self._evaluate_logical_argument(self.args[1], input_state, context)
        
        # Implement three-valued logic for OR:
        # T or T = T, T or F = T, T or null = T
        # F or T = T, F or F = F, F or null = null
        # null or T = T, null or F = null, null or null = null
        sql_fragment = f"""(
            CASE 
                WHEN ({left_sql}) IS TRUE OR ({right_sql}) IS TRUE THEN TRUE
                WHEN ({left_sql}) IS NULL OR ({right_sql}) IS NULL THEN NULL
                ELSE ({left_sql}) OR ({right_sql})
            END
        )"""
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_not(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle logical NOT with three-valued logic."""
        if len(self.args) != 1:
            raise ValueError("NOT operator requires exactly 1 argument")
        
        # Convert argument to SQL fragment
        operand_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        
        # Implement three-valued logic for NOT:
        # not T = F, not F = T, not null = null
        sql_fragment = f"""(
            CASE 
                WHEN ({operand_sql}) IS NULL THEN NULL
                ELSE NOT ({operand_sql})
            END
        )"""
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_implies(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle logical IMPLIES with three-valued logic."""
        if len(self.args) != 2:
            raise ValueError("IMPLIES operator requires exactly 2 arguments")
        
        # Convert arguments to SQL fragments
        left_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        right_sql = self._evaluate_logical_argument(self.args[1], input_state, context)
        
        # Implement three-valued logic for IMPLIES (A implies B = NOT A OR B):
        # T implies T = T, T implies F = F, T implies null = null
        # F implies T = T, F implies F = T, F implies null = T
        # null implies T = T, null implies F = null, null implies null = null
        sql_fragment = f"""(
            CASE 
                WHEN ({left_sql}) IS FALSE THEN TRUE
                WHEN ({left_sql}) IS NULL THEN 
                    CASE 
                        WHEN ({right_sql}) IS TRUE THEN TRUE
                        ELSE NULL
                    END
                ELSE ({right_sql})
            END
        )"""
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_xor(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle logical XOR with three-valued logic."""
        if len(self.args) != 2:
            raise ValueError("XOR operator requires exactly 2 arguments")
        
        # Convert arguments to SQL fragments
        left_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        right_sql = self._evaluate_logical_argument(self.args[1], input_state, context)
        
        # Implement three-valued logic for XOR:
        # T xor T = F, T xor F = T, T xor null = null
        # F xor T = T, F xor F = F, F xor null = null
        # null xor T = null, null xor F = null, null xor null = null
        sql_fragment = f"""(
            CASE 
                WHEN ({left_sql}) IS NULL OR ({right_sql}) IS NULL THEN NULL
                ELSE ({left_sql}) != ({right_sql})
            END
        )"""
        
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