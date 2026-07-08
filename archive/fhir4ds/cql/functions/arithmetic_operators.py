"""
CQL Arithmetic Operators with Null Handling

Implements CQL-compliant arithmetic operations (+, -, *, /) with proper
null propagation according to CQL's three-valued logic system.
"""

import logging
from typing import Any, Union, Optional
from ...fhirpath.parser.ast_nodes import LiteralNode

logger = logging.getLogger(__name__)


class CQLArithmeticOperators:
    """
    CQL arithmetic operators with proper null handling.

    Implements all CQL arithmetic operations with strict compliance to
    CQL's null propagation rules where any arithmetic operation with null
    returns null.
    """

    def __init__(self, dialect: str = "duckdb", dialect_handler=None):
        """Initialize CQL arithmetic operators handler."""
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

        # Register arithmetic operators
        self.operator_map = {
            '+': self.add,
            '-': self.subtract,
            '*': self.multiply,
            '/': self.divide,
            'mod': self.modulo,
            '^': self.power,
        }

    def add(self, left: Any, right: Any) -> LiteralNode:
        """
        CQL addition operator with null propagation.

        Args:
            left: Left operand
            right: Right operand

        Returns:
            LiteralNode containing SQL expression for addition with null handling
        """
        logger.debug("Generating CQL addition with null handling")

        left_val = self._extract_value(left)
        right_val = self._extract_value(right)

        # Generate SQL with null propagation
        sql = f"""
CASE
    WHEN ({left_val}) IS NULL OR ({right_val}) IS NULL THEN NULL
    ELSE ({left_val}) + ({right_val})
END
        """.strip()

        return LiteralNode(value=sql, type='sql')

    def subtract(self, left: Any, right: Any) -> LiteralNode:
        """
        CQL subtraction operator with null propagation.

        Args:
            left: Left operand
            right: Right operand

        Returns:
            LiteralNode containing SQL expression for subtraction with null handling
        """
        logger.debug("Generating CQL subtraction with null handling")

        left_val = self._extract_value(left)
        right_val = self._extract_value(right)

        sql = f"""
CASE
    WHEN ({left_val}) IS NULL OR ({right_val}) IS NULL THEN NULL
    ELSE ({left_val}) - ({right_val})
END
        """.strip()

        return LiteralNode(value=sql, type='sql')

    def multiply(self, left: Any, right: Any) -> LiteralNode:
        """
        CQL multiplication operator with null propagation.

        Args:
            left: Left operand
            right: Right operand

        Returns:
            LiteralNode containing SQL expression for multiplication with null handling
        """
        logger.debug("Generating CQL multiplication with null handling")

        left_val = self._extract_value(left)
        right_val = self._extract_value(right)

        sql = f"""
CASE
    WHEN ({left_val}) IS NULL OR ({right_val}) IS NULL THEN NULL
    ELSE ({left_val}) * ({right_val})
END
        """.strip()

        return LiteralNode(value=sql, type='sql')

    def divide(self, left: Any, right: Any) -> LiteralNode:
        """
        CQL division operator with null propagation and division by zero handling.

        In CQL, division by zero returns null (not an error).

        Args:
            left: Left operand (dividend)
            right: Right operand (divisor)

        Returns:
            LiteralNode containing SQL expression for division with null and zero handling
        """
        logger.debug("Generating CQL division with null and zero handling")

        left_val = self._extract_value(left)
        right_val = self._extract_value(right)

        # CQL requires division by zero to return null
        sql = f"""
CASE
    WHEN ({left_val}) IS NULL OR ({right_val}) IS NULL THEN NULL
    WHEN ({right_val}) = 0 THEN NULL
    ELSE ({left_val}) / CAST(({right_val}) AS DOUBLE)
END
        """.strip()

        return LiteralNode(value=sql, type='sql')

    def modulo(self, left: Any, right: Any) -> LiteralNode:
        """
        CQL modulo operator with null propagation and zero handling.

        Args:
            left: Left operand
            right: Right operand

        Returns:
            LiteralNode containing SQL expression for modulo with null handling
        """
        logger.debug("Generating CQL modulo with null handling")

        left_val = self._extract_value(left)
        right_val = self._extract_value(right)

        # Both dialects use % for modulo operator
        mod_op = '%'

        sql = f"""
CASE
    WHEN ({left_val}) IS NULL OR ({right_val}) IS NULL THEN NULL
    WHEN ({right_val}) = 0 THEN NULL
    ELSE ({left_val}) {mod_op} ({right_val})
END
        """.strip()

        return LiteralNode(value=sql, type='sql')

    def power(self, base: Any, exponent: Any) -> LiteralNode:
        """
        CQL power/exponentiation operator with null propagation.

        Args:
            base: Base value
            exponent: Exponent value

        Returns:
            LiteralNode containing SQL expression for power with null handling
        """
        logger.debug("Generating CQL power function with null handling")

        base_val = self._extract_value(base)
        exp_val = self._extract_value(exponent)

        # Use dialect abstraction for power function
        power_func = self.dialect_handler.generate_power_operation(base_val, exp_val)

        sql = f"""
CASE
    WHEN ({base_val}) IS NULL OR ({exp_val}) IS NULL THEN NULL
    ELSE {power_func}
END
        """.strip()

        return LiteralNode(value=sql, type='sql')

    def negate(self, operand: Any) -> LiteralNode:
        """
        CQL unary negation operator with null propagation.

        Args:
            operand: Value to negate

        Returns:
            LiteralNode containing SQL expression for negation with null handling
        """
        logger.debug("Generating CQL negation with null handling")

        operand_val = self._extract_value(operand)

        sql = f"""
CASE
    WHEN ({operand_val}) IS NULL THEN NULL
    ELSE -({operand_val})
END
        """.strip()

        return LiteralNode(value=sql, type='sql')

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

    def generate_arithmetic_sql(self, operator: str, left: Any, right: Any = None) -> str:
        """
        Generate SQL for arithmetic operations.

        Args:
            operator: Arithmetic operator (+, -, *, /, etc.)
            left: Left operand
            right: Right operand (None for unary operations)

        Returns:
            SQL string with proper null handling
        """
        if operator in self.operator_map:
            if right is None:
                # Unary operation (only negation supported)
                if operator == '-':
                    return self.negate(left).value
                else:
                    raise ValueError(f"Unsupported unary operator: {operator}")
            else:
                # Binary operation
                return self.operator_map[operator](left, right).value
        else:
            raise ValueError(f"Unsupported arithmetic operator: {operator}")

    def get_supported_operators(self) -> list:
        """Get list of supported arithmetic operators."""
        return list(self.operator_map.keys()) + ['-']  # Include unary minus


class CQLComparisonOperators:
    """
    CQL comparison operators with three-valued logic.

    Implements CQL comparison operations (=, !=, <, >, <=, >=) with proper
    null handling where comparisons with null return null.
    """

    def __init__(self, dialect: str = "duckdb"):
        """Initialize CQL comparison operators handler."""
        self.dialect = dialect

    def equal(self, left: Any, right: Any) -> LiteralNode:
        """
        CQL equality operator with null handling.

        In CQL: null = anything → null (not false)
        """
        left_val = self._extract_value(left)
        right_val = self._extract_value(right)

        sql = f"""
CASE
    WHEN ({left_val}) IS NULL OR ({right_val}) IS NULL THEN NULL
    WHEN ({left_val}) = ({right_val}) THEN true
    ELSE false
END
        """.strip()

        return LiteralNode(value=sql, type='sql')

    def not_equal(self, left: Any, right: Any) -> LiteralNode:
        """
        CQL inequality operator with null handling.

        In CQL: null != anything → null (not true)
        """
        left_val = self._extract_value(left)
        right_val = self._extract_value(right)

        sql = f"""
CASE
    WHEN ({left_val}) IS NULL OR ({right_val}) IS NULL THEN NULL
    WHEN ({left_val}) != ({right_val}) THEN true
    ELSE false
END
        """.strip()

        return LiteralNode(value=sql, type='sql')

    def less_than(self, left: Any, right: Any) -> LiteralNode:
        """CQL less than operator with null handling."""
        left_val = self._extract_value(left)
        right_val = self._extract_value(right)

        sql = f"""
CASE
    WHEN ({left_val}) IS NULL OR ({right_val}) IS NULL THEN NULL
    WHEN ({left_val}) < ({right_val}) THEN true
    ELSE false
END
        """.strip()

        return LiteralNode(value=sql, type='sql')

    def greater_than(self, left: Any, right: Any) -> LiteralNode:
        """CQL greater than operator with null handling."""
        left_val = self._extract_value(left)
        right_val = self._extract_value(right)

        sql = f"""
CASE
    WHEN ({left_val}) IS NULL OR ({right_val}) IS NULL THEN NULL
    WHEN ({left_val}) > ({right_val}) THEN true
    ELSE false
END
        """.strip()

        return LiteralNode(value=sql, type='sql')

    def less_than_or_equal(self, left: Any, right: Any) -> LiteralNode:
        """CQL less than or equal operator with null handling."""
        left_val = self._extract_value(left)
        right_val = self._extract_value(right)

        sql = f"""
CASE
    WHEN ({left_val}) IS NULL OR ({right_val}) IS NULL THEN NULL
    WHEN ({left_val}) <= ({right_val}) THEN true
    ELSE false
END
        """.strip()

        return LiteralNode(value=sql, type='sql')

    def greater_than_or_equal(self, left: Any, right: Any) -> LiteralNode:
        """CQL greater than or equal operator with null handling."""
        left_val = self._extract_value(left)
        right_val = self._extract_value(right)

        sql = f"""
CASE
    WHEN ({left_val}) IS NULL OR ({right_val}) IS NULL THEN NULL
    WHEN ({left_val}) >= ({right_val}) THEN true
    ELSE false
END
        """.strip()

        return LiteralNode(value=sql, type='sql')

    def _extract_value(self, arg: Any) -> str:
        """Extract value from various input types."""
        if hasattr(arg, 'value'):
            return str(arg.value)
        elif isinstance(arg, (int, float)):
            return str(arg)
        elif isinstance(arg, str):
            return arg
        else:
            return str(arg)