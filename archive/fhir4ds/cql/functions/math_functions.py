"""
CQL Mathematical Functions

Extends FHIRPath mathematical functions with CQL-specific operations including
aggregates (Min, Max, Sum, Avg) and mathematical functions not present in FHIRPath.
"""

import logging
from typing import Any, List, Dict, Union
from ...fhirpath.parser.ast_nodes import LiteralNode
from .arithmetic_operators import CQLArithmeticOperators

logger = logging.getLogger(__name__)


class CQLMathFunctionHandler:
    """
    CQL mathematical function handler providing both FHIRPath and CQL-specific functions.
    
    Provides CQL-specific mathematical and aggregate functions along with
    all FHIRPath mathematical functions (abs, ceiling, floor, round, 
    sqrt, truncate, exp, ln, log, power) reimplemented for CQL context.
    """
    
    def __init__(self, dialect: str = "duckdb", dialect_handler=None):
        """Initialize CQL math function handler with dialect support."""
        self.dialect = dialect

        # Inject dialect handler for database-specific operations
        if dialect_handler is None:
            from ...dialects import DuckDBDialect, PostgreSQLDialect
            from ...config import get_database_url
            if dialect.lower() == "postgresql":
                # Use centralized configuration
                conn_str = get_database_url('postgresql')
                self.dialect_handler = PostgreSQLDialect(conn_str)
            else:  # default to DuckDB
                self.dialect_handler = DuckDBDialect()
        else:
            self.dialect_handler = dialect_handler

        # Initialize arithmetic operators with dialect handler
        self.arithmetic_ops = CQLArithmeticOperators(dialect, dialect_handler)
        
        # Register all mathematical functions (CQL-specific + FHIRPath compatible)
        self.function_map = {
            # CQL-specific aggregate functions
            'min': self.min,
            'max': self.max,
            'sum': self.sum,
            'avg': self.avg,
            'average': self.avg,  # CQL alias for avg
            'count': self.count,
            
            # CQL collection ordering functions
            'first': self.first,
            'last': self.last,
            
            # CQL statistical functions
            'stddev': self.stddev,
            'stdev': self.stddev,  # CQL alias for stddev
            'variance': self.variance,
            'median': self.median,
            'mode': self.mode,
            'percentile': self.percentile,
            
            'predecessor': self.predecessor,
            'successor': self.successor,

            # Basic arithmetic operations (imported from arithmetic operators)
            'add': self.add,
            'subtract': self.subtract,
            'multiply': self.multiply,
            'divide': self.divide,

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
    
    def count(self, expression: Any) -> LiteralNode:
        """
        CQL Count() function - count elements in collection.
        
        Args:
            expression: Collection expression to count
            
        Returns:
            LiteralNode with SQL expression for count of elements
            
        Example: Count([1, 2, 3, null]) → 3 (null values not counted)
        """
        logger.debug("Generating CQL Count() function")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        
        # Handle collection case: Count(collection)
        if isinstance(expr_val, str):
            # Count non-null values in JSON array
            sql = f"""
(
    SELECT COUNT(*)
    FROM (
        SELECT json_array_elements_text({expr_val}) AS value
    ) subq
    WHERE value IS NOT NULL AND value != 'null'
)
""".strip()
        else:
            # Handle single value or aggregate count
            sql = f"COUNT({expr_val})"
        
        return LiteralNode(value=sql, type='sql')
    
    def first(self, expression: Any) -> LiteralNode:
        """
        CQL First() function - get first element from ordered collection.
        
        Args:
            expression: Collection expression to get first element from
            
        Returns:
            LiteralNode with SQL expression for first element
            
        Example: First([3, 1, 4, 2]) → 3 (first in original order)
        """
        logger.debug("Generating CQL First() function")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        
        # Handle collection case: First(collection)
        if isinstance(expr_val, str) and (expr_val.strip().startswith('[') or 'json_array' in expr_val):
            # Get first element from JSON array, maintaining original order
            sql = f"""
(
    SELECT value
    FROM (
        SELECT json_array_elements_text({expr_val}) AS value,
               ROW_NUMBER() OVER () AS row_num
    ) subq
    WHERE value IS NOT NULL AND value != 'null'
    ORDER BY row_num
    LIMIT 1
)
""".strip()
        else:
            # Handle single value - return the value itself
            sql = str(expr_val)
        
        return LiteralNode(value=sql, type='sql')
    
    def last(self, expression: Any) -> LiteralNode:
        """
        CQL Last() function - get last element from ordered collection.
        
        Args:
            expression: Collection expression to get last element from
            
        Returns:
            LiteralNode with SQL expression for last element
            
        Example: Last([3, 1, 4, 2]) → 2 (last in original order)
        """
        logger.debug("Generating CQL Last() function")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        
        # Handle collection case: Last(collection)
        if isinstance(expr_val, str) and (expr_val.strip().startswith('[') or 'json_array' in expr_val):
            # Get last element from JSON array, maintaining original order
            sql = f"""
(
    SELECT value
    FROM (
        SELECT json_array_elements_text({expr_val}) AS value,
               ROW_NUMBER() OVER () AS row_num
    ) subq
    WHERE value IS NOT NULL AND value != 'null'
    ORDER BY row_num DESC
    LIMIT 1
)
""".strip()
        else:
            # Handle single value - return the value itself
            sql = str(expr_val)
        
        return LiteralNode(value=sql, type='sql')
    
    def stddev(self, expression: Any) -> LiteralNode:
        """
        CQL StdDev() function - calculate standard deviation of collection values.
        
        Args:
            expression: Collection expression to calculate standard deviation
            
        Returns:
            LiteralNode with SQL expression for standard deviation
            
        Example: StdDev([1, 2, 3, 4, 5]) → 1.58...
        """
        logger.debug("Generating CQL StdDev() function")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        
        # Handle collection case: StdDev(collection)
        if isinstance(expr_val, str):
            # Calculate standard deviation from JSON array using dialect abstraction
            sql = self.dialect_handler.generate_json_aggregate_function('stddev', expr_val)
        else:
            # Handle aggregate case using dialect abstraction
            cast_sql = self.dialect_handler.generate_standard_type_cast(expr_val, 'double')
            sql = f"STDDEV({cast_sql.replace(f'CAST({expr_val} AS ', 'CAST(').replace(')', '')})"
        
        return LiteralNode(value=sql, type='sql')
    
    def variance(self, expression: Any) -> LiteralNode:
        """
        CQL Variance() function - calculate variance of collection values.
        
        Args:
            expression: Collection expression to calculate variance
            
        Returns:
            LiteralNode with SQL expression for variance
            
        Example: Variance([1, 2, 3, 4, 5]) → 2.5
        """
        logger.debug("Generating CQL Variance() function")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        
        # Handle collection case: Variance(collection)
        if isinstance(expr_val, str):
            # Calculate variance from JSON array using dialect abstraction
            sql = self.dialect_handler.generate_json_aggregate_function('variance', expr_val)
        else:
            # Handle aggregate case using dialect abstraction
            cast_sql = self.dialect_handler.generate_standard_type_cast(expr_val, 'double')
            sql = self.dialect_handler.generate_aggregate_function('variance', cast_sql)
        
        return LiteralNode(value=sql, type='sql')
    
    def median(self, expression: Any) -> LiteralNode:
        """
        CQL Median() function - calculate median of collection values.
        
        Args:
            expression: Collection expression to calculate median
            
        Returns:
            LiteralNode with SQL expression for median
            
        Example: Median([1, 2, 3, 4, 5]) → 3
        """
        logger.debug("Generating CQL Median() function")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        
        # Handle collection case: Median(collection)
        if isinstance(expr_val, str):
            # Calculate median from JSON array using dialect abstraction
            sql = self.dialect_handler.generate_percentile_function(expr_val, '0.5')
        else:
            # Handle aggregate case using dialect abstraction
            cast_sql = self.dialect_handler.generate_standard_type_cast(expr_val, 'double')
            sql = self.dialect_handler.generate_percentile_calculation(cast_sql, 0.5)
        
        return LiteralNode(value=sql, type='sql')
    
    def mode(self, expression: Any) -> LiteralNode:
        """
        CQL Mode() function - find most frequent value in collection.
        
        Args:
            expression: Collection expression to find mode
            
        Returns:
            LiteralNode with SQL expression for mode
            
        Example: Mode([1, 2, 2, 3, 2]) → 2
        """
        logger.debug("Generating CQL Mode() function")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        
        # Handle collection case: Mode(collection)
        if isinstance(expr_val, str):
            # Find mode from JSON array using frequency counting
            sql = f"""
(
    SELECT value
    FROM (
        SELECT json_array_elements_text({expr_val}) AS value,
               COUNT(*) as frequency
        FROM (SELECT 1) dummy
        WHERE value IS NOT NULL AND value != 'null'
        GROUP BY value
        ORDER BY frequency DESC, value
        LIMIT 1
    ) mode_subq
)
""".strip()
        else:
            # Handle aggregate case - mode is complex for single values
            # Return the value itself for single values
            sql = str(expr_val)
        
        return LiteralNode(value=sql, type='sql')
    
    def percentile(self, expression: Any, percentile_value: Any) -> LiteralNode:
        """
        CQL Percentile() function - calculate specified percentile of collection values.
        
        Args:
            expression: Collection expression to calculate percentile
            percentile_value: Percentile to calculate (0-100)
            
        Returns:
            LiteralNode with SQL expression for percentile
            
        Example: Percentile([1, 2, 3, 4, 5], 50) → 3 (50th percentile = median)
        """
        logger.debug("Generating CQL Percentile() function")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        percentile_val = extract_value(percentile_value)
        
        # Convert percentile from 0-100 to 0-1 scale
        percentile_fraction = f"({percentile_val} / 100.0)"
        
        # Handle collection case: Percentile(collection, n)
        if isinstance(expr_val, str):
            # Calculate percentile from JSON array using dialect abstraction
            sql = self.dialect_handler.generate_percentile_function(expr_val, percentile_fraction)
        else:
            # Handle aggregate case using dialect abstraction
            cast_sql = self.dialect_handler.generate_standard_type_cast(expr_val, 'double')
            sql = self.dialect_handler.generate_percentile_calculation(cast_sql, float(percentile_val) / 100.0)
        
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
                # Use dialect abstraction for date arithmetic
                interval_expr = f"1 {interval_unit}"
                sql = self.dialect_handler.generate_interval_arithmetic(expr_val, interval_expr, 'subtract')
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
                # Use dialect abstraction for date arithmetic
                sql = f"({self.dialect_handler.generate_date_arithmetic(expr_val, '1', interval_unit)})"
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
        aggregate_functions = {'min', 'max', 'sum', 'avg', 'average', 'count', 
                             'stddev', 'stdev', 'variance', 'median', 'mode', 'percentile'}
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
                elif len(args) == 2 and function_name_lower in ['predecessor', 'successor', 'percentile']:
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
                # Use LEAST function for multiple arguments (same for both dialects)
                return f"LEAST({', '.join(args_sql)})"
            else:  # max
                # Use GREATEST function for multiple arguments (same for both dialects)
                return f"GREATEST({', '.join(args_sql)})"
        
        # Fallback for unknown multi-arg functions
        return f"-- Unsupported multi-arg function: {function_name}({', '.join(map(str, args))})"

    # Basic arithmetic operations with null handling
    def add(self, left: Any, right: Any) -> LiteralNode:
        """CQL addition with null propagation."""
        return self.arithmetic_ops.add(left, right)

    def subtract(self, left: Any, right: Any) -> LiteralNode:
        """CQL subtraction with null propagation."""
        return self.arithmetic_ops.subtract(left, right)

    def multiply(self, left: Any, right: Any) -> LiteralNode:
        """CQL multiplication with null propagation."""
        return self.arithmetic_ops.multiply(left, right)

    def divide(self, left: Any, right: Any) -> LiteralNode:
        """CQL division with null propagation and division by zero handling."""
        return self.arithmetic_ops.divide(left, right)