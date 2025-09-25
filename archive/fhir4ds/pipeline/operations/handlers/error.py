"""
Error and messaging function handler for FHIRPath operations.

This module implements error and messaging functions including:
- message() for debugging/logging purposes
- error() for error condition handling
"""

from typing import List, Any
from .base import FunctionHandler
from ...core.base import SQLState, ExecutionContext, ContextMode


class InvalidArgumentError(Exception):
    """Raised when function arguments are invalid."""
    pass


class ErrorFunctionHandler(FunctionHandler):
    """Handler for error and messaging FHIRPath functions."""
    
    def get_supported_functions(self) -> List[str]:
        """Return list of error function names this handler supports."""
        return ['message', 'error']
    
    def handle_function(self, function_name: str, input_state: SQLState, 
                       context: ExecutionContext, args: List[Any]) -> SQLState:
        """Execute the specified error/messaging function."""
        # Store args for the handler methods
        self.args = args
        func_name = function_name.lower()
        
        # Route to appropriate handler method
        if func_name == 'message':
            return self._handle_message(input_state, context)
        elif func_name == 'error':
            return self._handle_error(input_state, context)
        else:
            raise InvalidArgumentError(f"Unsupported error function: {function_name}")
    
    # =================
    # Handler Methods
    # =================
    
    def _handle_message(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle message() function for debugging/logging."""
        # Message function typically takes (source, condition, code, severity, message)
        # For SQL generation, we'll return the source value and include a comment about the message
        
        if self.args and len(self.args) >= 4:
            message_text = str(self.args[4]) if len(self.args) > 4 else "'message'"
            sql_fragment = f"/* MESSAGE: {message_text} */ {input_state.sql_fragment}"
        else:
            sql_fragment = f"/* MESSAGE */ {input_state.sql_fragment}"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_error(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle error() function for error conditions."""
        # Error function raises a runtime error - for SQL generation, we'll return a placeholder
        error_msg = str(self.args[0]) if self.args else "'error'"
        sql_fragment = f"CASE WHEN TRUE THEN {error_msg} ELSE NULL END"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )