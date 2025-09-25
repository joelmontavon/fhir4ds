"""
CQL Type Operators - Type checking, casting, and conversion functions.

Implements CQL's type system operations including As, Is, Convert operators
and all type conversion functions according to the CQL specification.

This module fills a critical gap in CQL compliance by providing:
- As<T>() - Type casting with null safety
- Is<T>() - Runtime type checking
- Convert to<T>() - Type conversion with error handling
- All ConvertsTo*() functions for type validation
- All To*() conversion functions for type coercion
"""

import logging
from typing import Any, Union, Optional, Dict, List
from ...fhirpath.parser.ast_nodes import LiteralNode

logger = logging.getLogger(__name__)


class CQLTypeOperators:
    """
    CQL type operators with proper null handling and dialect support.

    Implements all CQL type system operations with strict compliance to
    CQL's type conversion rules and null propagation.
    """

    def __init__(self, dialect: str = "duckdb", dialect_handler=None):
        """Initialize CQL type operators handler."""
        self.dialect = dialect

        # Inject dialect handler for database-specific operations
        if dialect_handler is None:
            from ...dialects import create_dialect
            self.dialect_handler = create_dialect(dialect)
        else:
            self.dialect_handler = dialect_handler

        # Register type operators
        self.operator_map = {
            'as': self.as_operator,
            'is': self.is_operator,
            'convert': self.convert_operator,
        }

        # Register type conversion functions
        self.conversion_map = {
            # To* conversion functions
            'toboolean': self.to_boolean,
            'todate': self.to_date,
            'todatetime': self.to_datetime,
            'todecimal': self.to_decimal,
            'tointeger': self.to_integer,
            'tolong': self.to_long,
            'toquantity': self.to_quantity,
            'toratio': self.to_ratio,
            'tostring': self.to_string,
            'totime': self.to_time,

            # ConvertsTo* validation functions
            'convertstoboolean': self.converts_to_boolean,
            'convertstodate': self.converts_to_date,
            'convertstodatetime': self.converts_to_datetime,
            'convertstodecimal': self.converts_to_decimal,
            'convertstointeger': self.converts_to_integer,
            'convertstolong': self.converts_to_long,
            'convertstoquantity': self.converts_to_quantity,
            'convertstoratio': self.converts_to_ratio,
            'convertstostring': self.converts_to_string,
            'convertstotime': self.converts_to_time,
        }

    def as_operator(self, expression: Any, target_type: str) -> LiteralNode:
        """
        CQL As<T> operator - Type casting with null safety.

        In CQL, As returns null if the expression cannot be cast to the target type,
        unlike traditional casting which would throw an exception.

        Args:
            expression: Expression to cast
            target_type: Target type name (e.g., 'Integer', 'String', 'DateTime')

        Returns:
            LiteralNode containing SQL expression for safe type casting
        """
        logger.debug(f"Generating CQL As<{target_type}> operation")

        expr_val = self._extract_value(expression)

        # Use dialect abstraction for type casting
        cast_sql = self.dialect_handler.generate_safe_cast(expr_val, target_type)

        sql = f"""
CASE
    WHEN ({expr_val}) IS NULL THEN NULL
    ELSE {cast_sql}
END
        """.strip()

        return LiteralNode(value=sql, type='sql')

    def is_operator(self, expression: Any, target_type: str) -> LiteralNode:
        """
        CQL Is<T> operator - Runtime type checking.

        Tests if the runtime type of the expression matches or derives from
        the specified type.

        Args:
            expression: Expression to check
            target_type: Target type name

        Returns:
            LiteralNode containing SQL expression for type checking
        """
        logger.debug(f"Generating CQL Is<{target_type}> operation")

        expr_val = self._extract_value(expression)

        # Use dialect abstraction for type checking
        type_check_sql = self.dialect_handler.generate_type_check(expr_val, target_type)

        sql = f"""
CASE
    WHEN ({expr_val}) IS NULL THEN NULL
    ELSE {type_check_sql}
END
        """.strip()

        return LiteralNode(value=sql, type='sql')

    def convert_operator(self, expression: Any, target_type: str) -> LiteralNode:
        """
        CQL Convert to<T> operator - Type conversion with error handling.

        Converts a value to the specified type. Returns null if conversion fails.

        Args:
            expression: Expression to convert
            target_type: Target type name

        Returns:
            LiteralNode containing SQL expression for type conversion
        """
        logger.debug(f"Generating CQL Convert to<{target_type}> operation")

        expr_val = self._extract_value(expression)

        # Use dialect abstraction for type conversion
        convert_sql = self.dialect_handler.generate_type_conversion(expr_val, target_type)

        sql = f"""
CASE
    WHEN ({expr_val}) IS NULL THEN NULL
    ELSE {convert_sql}
END
        """.strip()

        return LiteralNode(value=sql, type='sql')

    # To* Conversion Functions

    def to_boolean(self, expression: Any) -> LiteralNode:
        """
        CQL ToBoolean() function - Convert expression to boolean.

        Converts string values according to CQL rules:
        - 'true', 't', '1' -> true
        - 'false', 'f', '0' -> false
        - null/empty -> null
        - Other strings -> null
        """
        logger.debug("Generating CQL ToBoolean() function")

        expr_val = self._extract_value(expression)

        # Use dialect abstraction for boolean conversion
        convert_sql = self.dialect_handler.generate_boolean_conversion(expr_val)

        return LiteralNode(value=convert_sql, type='sql')

    def to_date(self, expression: Any) -> LiteralNode:
        """CQL ToDate() function - Convert expression to date."""
        logger.debug("Generating CQL ToDate() function")

        expr_val = self._extract_value(expression)
        convert_sql = self.dialect_handler.generate_date_conversion(expr_val)

        return LiteralNode(value=convert_sql, type='sql')

    def to_datetime(self, expression: Any) -> LiteralNode:
        """CQL ToDateTime() function - Convert expression to datetime."""
        logger.debug("Generating CQL ToDateTime() function")

        expr_val = self._extract_value(expression)
        convert_sql = self.dialect_handler.generate_datetime_conversion(expr_val)

        return LiteralNode(value=convert_sql, type='sql')

    def to_decimal(self, expression: Any) -> LiteralNode:
        """CQL ToDecimal() function - Convert expression to decimal."""
        logger.debug("Generating CQL ToDecimal() function")

        expr_val = self._extract_value(expression)
        convert_sql = self.dialect_handler.generate_decimal_conversion(expr_val)

        return LiteralNode(value=convert_sql, type='sql')

    def to_integer(self, expression: Any) -> LiteralNode:
        """CQL ToInteger() function - Convert expression to integer."""
        logger.debug("Generating CQL ToInteger() function")

        expr_val = self._extract_value(expression)
        convert_sql = self.dialect_handler.generate_integer_conversion(expr_val)

        return LiteralNode(value=convert_sql, type='sql')

    def to_long(self, expression: Any) -> LiteralNode:
        """CQL ToLong() function - Convert expression to long integer."""
        logger.debug("Generating CQL ToLong() function")

        expr_val = self._extract_value(expression)
        convert_sql = self.dialect_handler.generate_long_conversion(expr_val)

        return LiteralNode(value=convert_sql, type='sql')

    def to_quantity(self, expression: Any, unit: Optional[str] = None) -> LiteralNode:
        """CQL ToQuantity() function - Convert expression to quantity."""
        logger.debug("Generating CQL ToQuantity() function")

        expr_val = self._extract_value(expression)
        unit_val = unit or "'1'"  # Default unit
        convert_sql = self.dialect_handler.generate_quantity_conversion(expr_val, unit_val)

        return LiteralNode(value=convert_sql, type='sql')

    def to_ratio(self, numerator: Any, denominator: Any) -> LiteralNode:
        """CQL ToRatio() function - Convert expressions to ratio."""
        logger.debug("Generating CQL ToRatio() function")

        num_val = self._extract_value(numerator)
        den_val = self._extract_value(denominator)
        convert_sql = self.dialect_handler.generate_ratio_conversion(num_val, den_val)

        return LiteralNode(value=convert_sql, type='sql')

    def to_string(self, expression: Any) -> LiteralNode:
        """CQL ToString() function - Convert expression to string."""
        logger.debug("Generating CQL ToString() function")

        expr_val = self._extract_value(expression)
        convert_sql = self.dialect_handler.generate_string_conversion(expr_val)

        return LiteralNode(value=convert_sql, type='sql')

    def to_time(self, expression: Any) -> LiteralNode:
        """CQL ToTime() function - Convert expression to time."""
        logger.debug("Generating CQL ToTime() function")

        expr_val = self._extract_value(expression)
        convert_sql = self.dialect_handler.generate_time_conversion(expr_val)

        return LiteralNode(value=convert_sql, type='sql')

    # ConvertsTo* Validation Functions

    def converts_to_boolean(self, expression: Any) -> LiteralNode:
        """CQL ConvertsToBoolean() function - Check if expression can convert to boolean."""
        logger.debug("Generating CQL ConvertsToBoolean() function")

        expr_val = self._extract_value(expression)
        check_sql = self.dialect_handler.generate_converts_to_boolean_check(expr_val)

        return LiteralNode(value=check_sql, type='sql')

    def converts_to_date(self, expression: Any) -> LiteralNode:
        """CQL ConvertsToDate() function - Check if expression can convert to date."""
        logger.debug("Generating CQL ConvertsToDate() function")

        expr_val = self._extract_value(expression)
        check_sql = self.dialect_handler.generate_converts_to_date_check(expr_val)

        return LiteralNode(value=check_sql, type='sql')

    def converts_to_datetime(self, expression: Any) -> LiteralNode:
        """CQL ConvertsToDateTime() function - Check if expression can convert to datetime."""
        logger.debug("Generating CQL ConvertsToDateTime() function")

        expr_val = self._extract_value(expression)
        check_sql = self.dialect_handler.generate_converts_to_datetime_check(expr_val)

        return LiteralNode(value=check_sql, type='sql')

    def converts_to_decimal(self, expression: Any) -> LiteralNode:
        """CQL ConvertsToDecimal() function - Check if expression can convert to decimal."""
        logger.debug("Generating CQL ConvertsToDecimal() function")

        expr_val = self._extract_value(expression)
        check_sql = self.dialect_handler.generate_converts_to_decimal_check(expr_val)

        return LiteralNode(value=check_sql, type='sql')

    def converts_to_integer(self, expression: Any) -> LiteralNode:
        """CQL ConvertsToInteger() function - Check if expression can convert to integer."""
        logger.debug("Generating CQL ConvertsToInteger() function")

        expr_val = self._extract_value(expression)
        check_sql = self.dialect_handler.generate_converts_to_integer_check(expr_val)

        return LiteralNode(value=check_sql, type='sql')

    def converts_to_long(self, expression: Any) -> LiteralNode:
        """CQL ConvertsToLong() function - Check if expression can convert to long."""
        logger.debug("Generating CQL ConvertsToLong() function")

        expr_val = self._extract_value(expression)
        check_sql = self.dialect_handler.generate_converts_to_long_check(expr_val)

        return LiteralNode(value=check_sql, type='sql')

    def converts_to_quantity(self, expression: Any) -> LiteralNode:
        """CQL ConvertsToQuantity() function - Check if expression can convert to quantity."""
        logger.debug("Generating CQL ConvertsToQuantity() function")

        expr_val = self._extract_value(expression)
        check_sql = self.dialect_handler.generate_converts_to_quantity_check(expr_val)

        return LiteralNode(value=check_sql, type='sql')

    def converts_to_ratio(self, expression: Any) -> LiteralNode:
        """CQL ConvertsToRatio() function - Check if expression can convert to ratio."""
        logger.debug("Generating CQL ConvertsToRatio() function")

        expr_val = self._extract_value(expression)
        check_sql = self.dialect_handler.generate_converts_to_ratio_check(expr_val)

        return LiteralNode(value=check_sql, type='sql')

    def converts_to_string(self, expression: Any) -> LiteralNode:
        """CQL ConvertsToString() function - Check if expression can convert to string."""
        logger.debug("Generating CQL ConvertsToString() function")

        expr_val = self._extract_value(expression)
        check_sql = self.dialect_handler.generate_converts_to_string_check(expr_val)

        return LiteralNode(value=check_sql, type='sql')

    def converts_to_time(self, expression: Any) -> LiteralNode:
        """CQL ConvertsToTime() function - Check if expression can convert to time."""
        logger.debug("Generating CQL ConvertsToTime() function")

        expr_val = self._extract_value(expression)
        check_sql = self.dialect_handler.generate_converts_to_time_check(expr_val)

        return LiteralNode(value=check_sql, type='sql')

    def _extract_value(self, arg: Any) -> str:
        """
        Extract value from various input types.

        Args:
            arg: Input argument (could be AST node, string, number, etc.)

        Returns:
            String representation suitable for SQL generation
        """
        if hasattr(arg, 'value'):
            return str(arg.value)
        elif isinstance(arg, (int, float)):
            return str(arg)
        elif isinstance(arg, str):
            return arg
        else:
            return str(arg)

    def generate_type_operation_sql(self, operator: str, expression: Any,
                                   target_type: str = None) -> str:
        """
        Generate SQL for type operations.

        Args:
            operator: Type operator (as, is, convert, etc.)
            expression: Expression to operate on
            target_type: Target type for casting/checking (required for as/is/convert)

        Returns:
            SQL string with proper null handling
        """
        operator_lower = operator.lower()

        if operator_lower in self.operator_map:
            if target_type is None:
                raise ValueError(f"Target type required for operator: {operator}")
            return self.operator_map[operator_lower](expression, target_type).value
        elif operator_lower in self.conversion_map:
            return self.conversion_map[operator_lower](expression).value
        else:
            raise ValueError(f"Unsupported type operator: {operator}")

    def get_supported_operators(self) -> List[str]:
        """Get list of supported type operators and conversion functions."""
        return list(self.operator_map.keys()) + list(self.conversion_map.keys())

    def get_supported_types(self) -> List[str]:
        """Get list of supported CQL types for conversion."""
        return [
            'Boolean', 'Date', 'DateTime', 'Decimal', 'Integer', 'Long',
            'Quantity', 'Ratio', 'String', 'Time'
        ]

    def get_function_info(self) -> Dict[str, Dict[str, Any]]:
        """Get information about supported type functions."""
        return {
            'as': {
                'description': 'Safe type casting with null handling',
                'syntax': 'expression as Type',
                'returns': 'Type or null',
                'example': 'someValue as Integer'
            },
            'is': {
                'description': 'Runtime type checking',
                'syntax': 'expression is Type',
                'returns': 'Boolean',
                'example': 'someValue is String'
            },
            'convert': {
                'description': 'Type conversion with error handling',
                'syntax': 'convert expression to Type',
                'returns': 'Type or null',
                'example': 'convert someValue to DateTime'
            },
            'toboolean': {
                'description': 'Convert to boolean using CQL rules',
                'syntax': 'ToBoolean(expression)',
                'returns': 'Boolean or null',
                'example': 'ToBoolean("true")'
            },
            'tostring': {
                'description': 'Convert to string representation',
                'syntax': 'ToString(expression)',
                'returns': 'String or null',
                'example': 'ToString(42)'
            },
            'convertstoboolean': {
                'description': 'Check if value can convert to boolean',
                'syntax': 'ConvertsToBoolean(expression)',
                'returns': 'Boolean',
                'example': 'ConvertsToBoolean("maybe")'
            }
        }