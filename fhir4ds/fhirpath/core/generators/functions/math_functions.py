"""
Mathematical function handlers for FHIRPath expressions.

This module handles mathematical operations like abs, ceiling, floor, round,
sqrt, truncate, exp, ln, log, power, and other numerical functions.
"""

from typing import List, Any, Optional
from ..base_handler import BaseFunctionHandler


class MathFunctionHandler(BaseFunctionHandler):
    """Handles mathematical function processing for FHIRPath to SQL conversion."""
    
    def __init__(self, generator, cte_builder=None):
        """
        Initialize the math function handler.
        
        Args:
            generator: Reference to main SQLGenerator for complex operations
            cte_builder: Optional CTEBuilder instance for CTE management
        """
        super().__init__(generator, cte_builder)
        self.generator = generator
        self.dialect = generator.dialect
        
    def get_supported_functions(self) -> List[str]:
        """Return list of math function names this handler supports."""
        return [
            'abs', 'ceiling', 'floor', 'round', 'sqrt', 'truncate',
            'exp', 'ln', 'log', 'power'
        ]
    
    def get_legacy_function_patterns(self) -> List[str]:
        """
        Return legacy math function patterns that match original hardcoded patterns.
        
        Phase 4.5: Exact patterns from original hardcoded list in ViewRunner
        """
        return [
            'abs()', 'sqrt()', 'power(', 'exp()', 'ln()',
            'ceiling()', 'floor()', 'round(', 'truncate('
        ]

    def can_handle(self, function_name: str) -> bool:
        """Check if this handler can process the given function."""
        math_functions = {
            'abs', 'ceiling', 'floor', 'round', 'sqrt', 'truncate',
            'exp', 'ln', 'log', 'power'
        }
        return function_name.lower() in math_functions
    
    def handle_function(self, func_name: str, base_expr: str, func_node) -> str:
        """
        Handle mathematical function and return SQL expression.
        
        Args:
            func_name: Name of the function to handle
            base_expr: Base SQL expression to apply function to
            func_node: Function AST node with arguments
            
        Returns:
            SQL expression for the function result
        """
        func_name = func_name.lower()
        
        if func_name == 'abs':
            return self._handle_abs(base_expr, func_node)
        elif func_name == 'ceiling':
            return self._handle_ceiling(base_expr, func_node)
        elif func_name == 'floor':
            return self._handle_floor(base_expr, func_node)
        elif func_name == 'round':
            return self._handle_round(base_expr, func_node)
        elif func_name == 'sqrt':
            return self._handle_sqrt(base_expr, func_node)
        elif func_name == 'truncate':
            return self._handle_truncate(base_expr, func_node)
        elif func_name == 'exp':
            return self._handle_exp(base_expr, func_node)
        elif func_name == 'ln':
            return self._handle_ln(base_expr, func_node)
        elif func_name == 'log':
            return self._handle_log(base_expr, func_node)
        elif func_name == 'power':
            return self._handle_power(base_expr, func_node)
        else:
            raise ValueError(f"Unsupported mathematical function: {func_name}")
    
    def _handle_abs(self, base_expr: str, func_node) -> str:
        """Handle abs() function - absolute value."""
        if len(func_node.args) != 0:
            raise ValueError("abs() function takes no arguments")
        
        # Direct implementation for scalar math functions
        return f"ABS(CAST({base_expr} AS DOUBLE))"
    
    def _handle_ceiling(self, base_expr: str, func_node) -> str:
        """Handle ceiling() function - round up to integer."""
        if len(func_node.args) != 0:
            raise ValueError("ceiling() function takes no arguments")
        
        # Direct implementation for scalar math functions
        return f"CEIL(CAST({base_expr} AS DOUBLE))"
    
    def _handle_floor(self, base_expr: str, func_node) -> str:
        """Handle floor() function - round down to integer."""
        if len(func_node.args) != 0:
            raise ValueError("floor() function takes no arguments")
        
        # Direct implementation for scalar math functions
        return f"FLOOR(CAST({base_expr} AS DOUBLE))"
    
    def _handle_round(self, base_expr: str, func_node) -> str:
        """Handle round() function - round to nearest integer with optional precision."""
        if len(func_node.args) == 0:
            # round() without precision - round to nearest integer
            return f"ROUND(CAST({base_expr} AS DOUBLE))"
        elif len(func_node.args) == 1:
            # round(precision) with precision
            precision_expr = self.generator.visit(func_node.args[0])
            return f"ROUND(CAST({base_expr} AS DOUBLE), CAST({precision_expr} AS INTEGER))"
        else:
            raise ValueError("round() function takes 0 or 1 arguments")
    
    def _handle_sqrt(self, base_expr: str, func_node) -> str:
        """Handle sqrt() function - square root."""
        if len(func_node.args) != 0:
            raise ValueError("sqrt() function takes no arguments")
        
        # Direct implementation for scalar math functions
        # Note: Need to handle negative numbers (sqrt of negative is null/error)
        return f"CASE WHEN CAST({base_expr} AS DOUBLE) >= 0 THEN SQRT(CAST({base_expr} AS DOUBLE)) ELSE NULL END"
    
    def _handle_truncate(self, base_expr: str, func_node) -> str:
        """Handle truncate() function - remove decimal part (round towards zero)."""
        if len(func_node.args) != 0:
            raise ValueError("truncate() function takes no arguments")
        
        # Direct implementation for scalar math functions
        # TRUNCATE function removes decimal part by rounding towards zero
        # For positive numbers: TRUNCATE(3.7) = 3, for negative: TRUNCATE(-3.7) = -3
        return f"TRUNC(CAST({base_expr} AS DOUBLE))"
    
    def _handle_exp(self, base_expr: str, func_node) -> str:
        """Handle exp() function - exponential function (e^x)."""
        if len(func_node.args) != 0:
            raise ValueError("exp() function takes no arguments")
        
        # Direct implementation - exponential function
        # Note: Need to handle overflow cases (exp of very large numbers)
        return f"CASE WHEN CAST({base_expr} AS DOUBLE) > 700 THEN NULL ELSE EXP(CAST({base_expr} AS DOUBLE)) END"
    
    def _handle_ln(self, base_expr: str, func_node) -> str:
        """Handle ln() function - natural logarithm (log base e)."""
        if len(func_node.args) != 0:
            raise ValueError("ln() function takes no arguments")
        
        # Direct implementation - natural logarithm
        # Note: Need to handle domain restrictions (ln of non-positive numbers is undefined)
        return f"CASE WHEN CAST({base_expr} AS DOUBLE) > 0 THEN LN(CAST({base_expr} AS DOUBLE)) ELSE NULL END"
    
    def _handle_log(self, base_expr: str, func_node) -> str:
        """Handle log() function - base-10 logarithm."""
        if len(func_node.args) != 0:
            raise ValueError("log() function takes no arguments")
        
        # Direct implementation - base-10 logarithm
        # Note: Need to handle domain restrictions (log of non-positive numbers is undefined)
        return f"CASE WHEN CAST({base_expr} AS DOUBLE) > 0 THEN LOG10(CAST({base_expr} AS DOUBLE)) ELSE NULL END"
    
    def _handle_power(self, base_expr: str, func_node) -> str:
        """Handle power() function - raises base to the power of exponent."""
        if len(func_node.args) != 1:
            raise ValueError("power() function requires exactly 1 argument (exponent)")
        
        # Get the exponent expression
        exponent_expr = self.generator.visit(func_node.args[0])
        
        # Direct implementation - power function
        # Note: Need to handle special cases (0^0, negative bases with fractional exponents)
        return f"""CASE 
            WHEN CAST({base_expr} AS DOUBLE) = 0 AND CAST({exponent_expr} AS DOUBLE) = 0 THEN 1
            WHEN CAST({base_expr} AS DOUBLE) < 0 AND CAST({exponent_expr} AS DOUBLE) != floor(CAST({exponent_expr} AS DOUBLE)) THEN NULL
            WHEN CAST({base_expr} AS DOUBLE) = 0 AND CAST({exponent_expr} AS DOUBLE) < 0 THEN NULL
            ELSE POWER(CAST({base_expr} AS DOUBLE), CAST({exponent_expr} AS DOUBLE))
        END"""