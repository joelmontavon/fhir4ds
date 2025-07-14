"""
DateTime function handlers for FHIRPath expressions.

This module handles datetime operations like now(), today(), and timeOfDay().
These functions return current timestamp, date, and time respectively.
"""

from typing import List, Any, Optional


class DateTimeFunctionHandler:
    """Handles datetime function processing for FHIRPath to SQL conversion."""
    
    def __init__(self, generator):
        """
        Initialize the datetime function handler.
        
        Args:
            generator: Reference to main SQLGenerator for complex operations
        """
        self.generator = generator
        self.dialect = generator.dialect
        
    def can_handle(self, function_name: str) -> bool:
        """Check if this handler can process the given function."""
        datetime_functions = {
            'now', 'today', 'timeofday'
        }
        return function_name.lower() in datetime_functions
    
    def handle_function(self, func_name: str, base_expr: str, func_node) -> str:
        """
        Handle datetime function and return SQL expression.
        
        Args:
            func_name: Name of the function to handle
            base_expr: Base SQL expression to apply function to
            func_node: Function AST node with arguments
            
        Returns:
            SQL expression for the function result
        """
        func_name = func_name.lower()
        
        if func_name == 'now':
            return self._handle_now(base_expr, func_node)
        elif func_name == 'today':
            return self._handle_today(base_expr, func_node)
        elif func_name == 'timeofday':
            return self._handle_timeofday(base_expr, func_node)
        else:
            raise ValueError(f"Unsupported datetime function: {func_name}")
    
    def _handle_now(self, base_expr: str, func_node) -> str:
        """Handle now() function - returns current datetime."""
        # now() function - returns current datetime
        if len(func_node.args) != 0:
            raise ValueError("now() function takes no arguments")
        
        # Direct implementation - returns current timestamp
        # Note: This is a context-independent function (doesn't use base_expr)
        return f"CURRENT_TIMESTAMP"
    
    def _handle_today(self, base_expr: str, func_node) -> str:
        """Handle today() function - returns current date."""
        # today() function - returns current date
        if len(func_node.args) != 0:
            raise ValueError("today() function takes no arguments")
        
        # Direct implementation - returns current date
        # Note: This is a context-independent function (doesn't use base_expr)
        return f"CURRENT_DATE"
    
    def _handle_timeofday(self, base_expr: str, func_node) -> str:
        """Handle timeOfDay() function - returns current time."""
        # timeOfDay() function - returns current time
        if len(func_node.args) != 0:
            raise ValueError("timeOfDay() function takes no arguments")
        
        # Direct implementation - returns current time
        # Note: This is a context-independent function (doesn't use base_expr)
        return f"CURRENT_TIME"