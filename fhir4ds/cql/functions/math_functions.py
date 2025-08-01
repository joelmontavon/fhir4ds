"""
CQL Mathematical Functions

Extends FHIRPath mathematical functions with CQL-specific operations including
aggregates (Min, Max, Sum, Avg) and mathematical functions not present in FHIRPath.
"""

import logging
from typing import Any, List, Dict, Union
from ...fhirpath.parser.ast_nodes import LiteralNode

logger = logging.getLogger(__name__)


class CQLMathFunctionHandler:
    """
    CQL mathematical function handler providing both FHIRPath and CQL-specific functions.
    
    Provides CQL-specific mathematical and aggregate functions along with
    all FHIRPath mathematical functions (abs, ceiling, floor, round, 
    sqrt, truncate, exp, ln, log, power) reimplemented for CQL context.
    """
    
    def __init__(self, dialect: str = "duckdb"):
        """Initialize CQL math function handler with dialect support."""
        self.dialect = dialect
        
        # Register all mathematical functions (CQL-specific + FHIRPath compatible)
        self.function_map = {
            # CQL-specific aggregate functions
            'min': self.min,
            'max': self.max,
            'sum': self.sum,
            'avg': self.avg,
            'average': self.avg,  # CQL alias for avg
            'predecessor': self.predecessor,
            'successor': self.successor,
            
            # FHIRPath mathematical functions (reimplemented for CQL)
            'abs': self.abs,
            'ceiling': self.ceiling,
            'floor': self.floor,
            'round': self.round,
            'sqrt': self.sqrt,
            'truncate': self.truncate,
            'exp': self.exp,
            'ln': self.ln,
            'log': self.log,
            'power': self.power,
        }
    
    def min(self, expression: Any) -> LiteralNode:
        """
        CQL Min() function - find minimum value in collection or between arguments.
        
        Supports:
        - Min(collection) - minimum value in collection
        - Min(a, b) - minimum of two values
        
        Args:
            expression: Collection expression or first argument
            
        Returns:
            LiteralNode with SQL expression for minimum value
        """
        logger.debug("Generating CQL Min() function")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        
        # Handle collection case: Min(collection)
        if isinstance(expr_val, str):
            sql = f"""
(
    SELECT MIN(CAST(value AS DOUBLE))
    FROM (
        SELECT json_array_elements_text({expr_val}) AS value
    ) subq
    WHERE value IS NOT NULL AND value != 'null'
)
""".strip()
        else:
            # Handle multiple arguments case would need parser integration
            # For now, fallback to SQL MIN aggregate
            sql = f"MIN({expr_val})"
        
        return LiteralNode(value=sql, type='sql')
    
    def max(self, expression: Any) -> LiteralNode:
        """
        CQL Max() function - find maximum value in collection or between arguments.
        
        Supports:
        - Max(collection) - maximum value in collection
        - Max(a, b) - maximum of two values
        
        Args:
            expression: Collection expression or first argument
            
        Returns:
            LiteralNode with SQL expression for maximum value
        """
        logger.debug("Generating CQL Max() function")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        
        # Handle collection case: Max(collection)
        if isinstance(expr_val, str):
            sql = f"""
(
    SELECT MAX(CAST(value AS DOUBLE))
    FROM (
        SELECT json_array_elements_text({expr_val}) AS value
    ) subq
    WHERE value IS NOT NULL AND value != 'null'
)
""".strip()
        else:
            # Handle multiple arguments case would need parser integration
            # For now, fallback to SQL MAX aggregate
            sql = f"MAX(CAST({expr_val} AS DOUBLE))"
        
        return LiteralNode(value=sql, type='sql')
    
    def sum(self, expression: Any) -> LiteralNode:
        """
        CQL Sum() function - sum all values in collection.
        
        Args:
            expression: Collection expression to sum
            
        Returns:
            LiteralNode with SQL expression for sum of values
        """
        logger.debug("Generating CQL Sum() function")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        
        if isinstance(expr_val, str):
            sql = f"""
(
    SELECT SUM(CAST(value AS DOUBLE))
    FROM (
        SELECT json_array_elements_text({expr_val}) AS value
    ) subq
    WHERE value IS NOT NULL AND value != 'null'
    AND value ~ '^-?[0-9]+(\\.[0-9]+)?$'
)
""".strip()
        else:
            sql = f"SUM(CAST({expr_val} AS DOUBLE))"
        
        return LiteralNode(value=sql, type='sql')
    
    def avg(self, expression: Any) -> LiteralNode:
        """
        CQL Avg() function - average of all values in collection.
        
        Args:
            expression: Collection expression to average
            
        Returns:
            LiteralNode with SQL expression for average of values
        """
        logger.debug("Generating CQL Avg() function")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        
        if isinstance(expr_val, str):
            sql = f"""
(
    SELECT AVG(CAST(value AS DOUBLE))
    FROM (
        SELECT json_array_elements_text({expr_val}) AS value
    ) subq
    WHERE value IS NOT NULL AND value != 'null'
    AND value ~ '^-?[0-9]+(\\.[0-9]+)?$'
)
""".strip()
        else:
            sql = f"AVG(CAST({expr_val} AS DOUBLE))"
        
        return LiteralNode(value=sql, type='sql')
    
    def predecessor(self, expression: Any, precision: str = None) -> LiteralNode:
        """
        CQL Predecessor() function - get previous value for given type.
        
        Supports:
        - Integer: n - 1
        - Decimal: n - precision_unit  
        - Date/DateTime: subtract 1 unit of given precision
        
        Args:
            expression: Value to get predecessor of
            precision: For date/time types (year, month, day, hour, minute, second)
            
        Returns:
            LiteralNode with SQL expression for predecessor value
        """
        logger.debug(f"Generating CQL Predecessor() function with precision: {precision}")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        
        if precision:
            # Date/DateTime predecessor with precision
            precision_map = {
                'year': 'YEAR',
                'month': 'MONTH', 
                'day': 'DAY',
                'hour': 'HOUR',
                'minute': 'MINUTE',
                'second': 'SECOND'
            }
            
            if precision.lower() in precision_map:
                interval_unit = precision_map[precision.lower()]
                if self.dialect == "duckdb":
                    sql = f"({expr_val} - INTERVAL 1 {interval_unit})"
                elif self.dialect == "postgresql":
                    sql = f"({expr_val} - INTERVAL '1 {interval_unit}')"
                else:
                    sql = f"({expr_val} - 1)"
            else:
                sql = f"({expr_val} - 1)"
        else:
            # Numeric predecessor (subtract 1)
            sql = f"({expr_val} - 1)"
        
        return LiteralNode(value=sql, type='sql')
    
    def successor(self, expression: Any, precision: str = None) -> LiteralNode:
        """
        CQL Successor() function - get next value for given type.
        
        Supports:
        - Integer: n + 1
        - Decimal: n + precision_unit
        - Date/DateTime: add 1 unit of given precision
        
        Args:
            expression: Value to get successor of
            precision: For date/time types (year, month, day, hour, minute, second)
            
        Returns:
            LiteralNode with SQL expression for successor value
        """
        logger.debug(f"Generating CQL Successor() function with precision: {precision}")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        
        if precision:
            # Date/DateTime successor with precision
            precision_map = {
                'year': 'YEAR',
                'month': 'MONTH',
                'day': 'DAY', 
                'hour': 'HOUR',
                'minute': 'MINUTE',
                'second': 'SECOND'
            }
            
            if precision.lower() in precision_map:
                interval_unit = precision_map[precision.lower()]
                if self.dialect == "duckdb":
                    sql = f"({expr_val} + INTERVAL 1 {interval_unit})"
                elif self.dialect == "postgresql":
                    sql = f"({expr_val} + INTERVAL '1 {interval_unit}')"
                else:
                    sql = f"({expr_val} + 1)"
            else:
                sql = f"({expr_val} + 1)"
        else:
            # Numeric successor (add 1)
            sql = f"({expr_val} + 1)"
        
        return LiteralNode(value=sql, type='sql')
    
    # FHIRPath Mathematical Functions (reimplemented for CQL compatibility)
    
    def abs(self, expression: Any) -> LiteralNode:
        """CQL/FHIRPath abs() function - absolute value."""
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            elif hasattr(arg, 'operator') and hasattr(arg, 'operand'):
                # Handle UnaryOpNode like -5
                if arg.operator == '-':
                    operand_val = extract_value(arg.operand)
                    return f"-{operand_val}"
                else:
                    return f"{arg.operator}{extract_value(arg.operand)}"
            else:
                return arg
        
        expr_val = extract_value(expression)
        sql = f"ABS(CAST({expr_val} AS DOUBLE))"
        return LiteralNode(value=sql, type='sql')
    
    def ceiling(self, expression: Any) -> LiteralNode:
        """CQL/FHIRPath ceiling() function - round up to integer."""
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return arg
        
        expr_val = extract_value(expression)
        sql = f"CEIL(CAST({expr_val} AS DOUBLE))"
        return LiteralNode(value=sql, type='sql')
    
    def floor(self, expression: Any) -> LiteralNode:
        """CQL/FHIRPath floor() function - round down to integer."""
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return arg
        
        expr_val = extract_value(expression)
        sql = f"FLOOR(CAST({expr_val} AS DOUBLE))"
        return LiteralNode(value=sql, type='sql')
    
    def round(self, expression: Any, precision: Any = None) -> LiteralNode:
        """CQL/FHIRPath round() function - round to nearest integer or precision."""
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            elif hasattr(arg, 'operator') and hasattr(arg, 'operand'):
                # Handle UnaryOpNode like -5
                if arg.operator == '-':
                    operand_val = extract_value(arg.operand)
                    return f"-{operand_val}"
                else:
                    return f"{arg.operator}{extract_value(arg.operand)}"
            else:
                return arg
        
        expr_val = extract_value(expression)
        if precision is not None:
            prec_val = extract_value(precision)
            sql = f"ROUND(CAST({expr_val} AS DOUBLE), {prec_val})"
        else:
            sql = f"ROUND(CAST({expr_val} AS DOUBLE))"
        return LiteralNode(value=sql, type='sql')
    
    def sqrt(self, expression: Any) -> LiteralNode:
        """CQL/FHIRPath sqrt() function - square root."""
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return arg
        
        expr_val = extract_value(expression)
        sql = f"SQRT(CAST({expr_val} AS DOUBLE))"
        return LiteralNode(value=sql, type='sql')
    
    def truncate(self, expression: Any) -> LiteralNode:
        """CQL/FHIRPath truncate() function - remove decimal part."""
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return arg
        
        expr_val = extract_value(expression)
        sql = f"TRUNC(CAST({expr_val} AS DOUBLE))"
        return LiteralNode(value=sql, type='sql')
    
    def exp(self, expression: Any) -> LiteralNode:
        """CQL/FHIRPath exp() function - exponential (e^x)."""
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return arg
        
        expr_val = extract_value(expression)
        sql = f"EXP(CAST({expr_val} AS DOUBLE))"
        return LiteralNode(value=sql, type='sql')
    
    def ln(self, expression: Any) -> LiteralNode:
        """CQL/FHIRPath ln() function - natural logarithm."""
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return arg
        
        expr_val = extract_value(expression)
        sql = f"LN(CAST({expr_val} AS DOUBLE))"
        return LiteralNode(value=sql, type='sql')
    
    def log(self, expression: Any) -> LiteralNode:
        """CQL/FHIRPath log() function - base-10 logarithm."""
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return arg
        
        expr_val = extract_value(expression)
        sql = f"LOG10(CAST({expr_val} AS DOUBLE))"
        return LiteralNode(value=sql, type='sql')
    
    def power(self, base: Any, exponent: Any) -> LiteralNode:
        """CQL/FHIRPath power() function - base^exponent."""
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return arg
        
        base_val = extract_value(base)
        exp_val = extract_value(exponent)
        sql = f"POW(CAST({base_val} AS DOUBLE), CAST({exp_val} AS DOUBLE))"
        return LiteralNode(value=sql, type='sql')
    
    def get_supported_functions(self) -> List[str]:
        """Get list of all supported CQL mathematical functions."""
        return list(self.function_map.keys())
    
    def is_aggregate_function(self, function_name: str) -> bool:
        """Check if function is an aggregate function requiring GROUP BY."""
        aggregate_functions = {'min', 'max', 'sum', 'avg', 'average', 'count'}
        return function_name.lower() in aggregate_functions
    
    def generate_cql_function_sql(self, function_name: str, args: List[Any], 
                                dialect: str = None) -> str:
        """
        Generate SQL for CQL mathematical function call.
        
        Args:
            function_name: Name of the function to call
            args: Function arguments
            dialect: Database dialect (overrides instance dialect)
            
        Returns:
            SQL expression for function call
        """
        if dialect:
            old_dialect = self.dialect
            self.dialect = dialect
        
        try:
            function_name_lower = function_name.lower()
            
            if function_name_lower in self.function_map:
                handler = self.function_map[function_name_lower]
                
                if len(args) == 1:
                    result = handler(args[0])
                    # Extract SQL from LiteralNode if needed
                    return result.value if hasattr(result, 'value') else result
                elif len(args) == 2 and function_name_lower in ['predecessor', 'successor']:
                    result = handler(args[0], args[1])
                    return result.value if hasattr(result, 'value') else result
                else:
                    # Multiple arguments - need special handling
                    return self._handle_multiple_args(function_name_lower, args)
            else:
                logger.warning(f"Unknown CQL mathematical function: {function_name}")
                return f"-- Unknown function: {function_name}({', '.join(map(str, args))})"
                
        finally:
            if dialect:
                self.dialect = old_dialect
    
    def _handle_multiple_args(self, function_name: str, args: List[Any]) -> str:
        """Handle functions with multiple arguments."""
        if function_name in ['min', 'max']:
            # Min(a, b, c) or Max(a, b, c) with multiple arguments
            args_sql = [f"CAST({arg} AS DOUBLE)" for arg in args]
            
            if function_name == 'min':
                # Use LEAST function for multiple arguments
                if self.dialect == "postgresql":
                    return f"LEAST({', '.join(args_sql)})"
                else:  # DuckDB
                    return f"LEAST({', '.join(args_sql)})"
            else:  # max
                # Use GREATEST function for multiple arguments
                if self.dialect == "postgresql":
                    return f"GREATEST({', '.join(args_sql)})"
                else:  # DuckDB
                    return f"GREATEST({', '.join(args_sql)})"
        
        # Fallback for unknown multi-arg functions
        return f"-- Unsupported multi-arg function: {function_name}({', '.join(map(str, args))})"