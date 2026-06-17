"""
DateTime function handlers for FHIRPath expressions.

This module handles datetime operations like now(), today(), and timeOfDay().
These functions return current timestamp, date, and time respectively.
"""

from typing import List, Any, Optional
from ..base_handler import BaseFunctionHandler


class DateTimeFunctionHandler(BaseFunctionHandler):
    """Handles datetime function processing for FHIRPath to SQL conversion."""
    
    def __init__(self, generator, cte_builder=None):
        """
        Initialize the datetime function handler.
        
        Args:
            generator: Reference to main SQLGenerator for complex operations
            cte_builder: Optional CTEBuilder instance for CTE management
        """
        super().__init__(generator, cte_builder)
        self.generator = generator
        self.dialect = generator.dialect
        
    def get_supported_functions(self) -> List[str]:
        """Return list of datetime function names this handler supports."""
        return ['now', 'today', 'timeofday', 'lowboundary', 'highboundary']

    def can_handle(self, function_name: str) -> bool:
        """Check if this handler can process the given function."""
        datetime_functions = {
            'now', 'today', 'timeofday', 'lowboundary', 'highboundary'
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
        elif func_name == 'lowboundary':
            return self._handle_low_boundary(base_expr, func_node)
        elif func_name == 'highboundary':
            return self._handle_high_boundary(base_expr, func_node)
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

    def _handle_low_boundary(self, base_expr: str, func_node) -> str:
        """Handle lowBoundary() function - returns lower boundary for date/time precision."""
        # lowBoundary() function takes optional precision argument
        if len(func_node.args) > 1:
            raise ValueError("lowBoundary() function takes at most one argument")
        
        # Get precision if provided, otherwise determine from value type
        precision = None
        if len(func_node.args) == 1:
            precision_arg = func_node.args[0]
            # Extract precision from literal or use default
            if hasattr(precision_arg, 'literal'):
                precision = precision_arg.literal.strip('"\'')
        
        # Generate boundary calculation based on data type and precision
        if precision == 'year' or not precision:
            # For year precision: start of year
            return f"CASE WHEN {base_expr} IS NOT NULL THEN DATE_TRUNC('year', CAST({base_expr} AS TIMESTAMP)) ELSE NULL END"
        elif precision == 'month':
            # For month precision: start of month  
            return f"CASE WHEN {base_expr} IS NOT NULL THEN DATE_TRUNC('month', CAST({base_expr} AS TIMESTAMP)) ELSE NULL END"
        elif precision == 'day':
            # For day precision: start of day
            return f"CASE WHEN {base_expr} IS NOT NULL THEN DATE_TRUNC('day', CAST({base_expr} AS TIMESTAMP)) ELSE NULL END"
        elif precision == 'hour':
            # For hour precision: start of hour
            return f"CASE WHEN {base_expr} IS NOT NULL THEN DATE_TRUNC('hour', CAST({base_expr} AS TIMESTAMP)) ELSE NULL END"
        elif precision == 'minute':
            # For minute precision: start of minute
            return f"CASE WHEN {base_expr} IS NOT NULL THEN DATE_TRUNC('minute', CAST({base_expr} AS TIMESTAMP)) ELSE NULL END"
        elif precision == 'second':
            # For second precision: start of second
            return f"CASE WHEN {base_expr} IS NOT NULL THEN DATE_TRUNC('second', CAST({base_expr} AS TIMESTAMP)) ELSE NULL END"
        else:
            # Default: assume datetime and return start of day
            return f"CASE WHEN {base_expr} IS NOT NULL THEN DATE_TRUNC('day', CAST({base_expr} AS TIMESTAMP)) ELSE NULL END"

    def _handle_high_boundary(self, base_expr: str, func_node) -> str:
        """Handle highBoundary() function - returns upper boundary for date/time precision."""
        # highBoundary() function takes optional precision argument
        if len(func_node.args) > 1:
            raise ValueError("highBoundary() function takes at most one argument")
        
        # Get precision if provided, otherwise determine from value type
        precision = None
        if len(func_node.args) == 1:
            precision_arg = func_node.args[0]
            # Extract precision from literal or use default
            if hasattr(precision_arg, 'literal'):
                precision = precision_arg.literal.strip('"\'')
        
        # Generate boundary calculation based on data type and precision
        if precision == 'year' or not precision:
            # For year precision: end of year
            return f"CASE WHEN {base_expr} IS NOT NULL THEN DATE_TRUNC('year', CAST({base_expr} AS TIMESTAMP)) + INTERVAL '1 year' - INTERVAL '1 microsecond' ELSE NULL END"
        elif precision == 'month':
            # For month precision: end of month
            return f"CASE WHEN {base_expr} IS NOT NULL THEN DATE_TRUNC('month', CAST({base_expr} AS TIMESTAMP)) + INTERVAL '1 month' - INTERVAL '1 microsecond' ELSE NULL END"
        elif precision == 'day':
            # For day precision: end of day
            return f"CASE WHEN {base_expr} IS NOT NULL THEN DATE_TRUNC('day', CAST({base_expr} AS TIMESTAMP)) + INTERVAL '1 day' - INTERVAL '1 microsecond' ELSE NULL END"
        elif precision == 'hour':
            # For hour precision: end of hour
            return f"CASE WHEN {base_expr} IS NOT NULL THEN DATE_TRUNC('hour', CAST({base_expr} AS TIMESTAMP)) + INTERVAL '1 hour' - INTERVAL '1 microsecond' ELSE NULL END"
        elif precision == 'minute':
            # For minute precision: end of minute
            return f"CASE WHEN {base_expr} IS NOT NULL THEN DATE_TRUNC('minute', CAST({base_expr} AS TIMESTAMP)) + INTERVAL '1 minute' - INTERVAL '1 microsecond' ELSE NULL END"
        elif precision == 'second':
            # For second precision: end of second
            return f"CASE WHEN {base_expr} IS NOT NULL THEN DATE_TRUNC('second', CAST({base_expr} AS TIMESTAMP)) + INTERVAL '1 second' - INTERVAL '1 microsecond' ELSE NULL END"
        else:
            # Default: assume datetime and return end of day
            return f"CASE WHEN {base_expr} IS NOT NULL THEN DATE_TRUNC('day', CAST({base_expr} AS TIMESTAMP)) + INTERVAL '1 day' - INTERVAL '1 microsecond' ELSE NULL END"