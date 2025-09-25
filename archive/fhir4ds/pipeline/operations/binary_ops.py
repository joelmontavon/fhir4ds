"""
Binary operation support for FHIRPath pipeline system.

This module provides specialized handling for binary operations like =, !=, <, >,
which require proper execution of both operands before combining their results.
"""

from typing import List, Optional, Union
import logging
from ..core.base import PipelineOperation, SQLState, ExecutionContext

logger = logging.getLogger(__name__)

class BinaryOperationBase(PipelineOperation[SQLState]):
    """
    Base class for binary operations that need to execute both operands.
    
    This handles the complex case where both left and right operands are 
    themselves pipeline operations that need to be executed to get SQL fragments.
    """
    
    def __init__(self, operator: str, left_operations: List[PipelineOperation[SQLState]], 
                 right_operations: List[PipelineOperation[SQLState]]):
        """
        Initialize binary operation.
        
        Args:
            operator: Binary operator (=, !=, <, >, etc.)
            left_operations: Operations to execute for left operand
            right_operations: Operations to execute for right operand
        """
        self.operator = operator
        self.left_operations = left_operations or []
        self.right_operations = right_operations or []
        
    def execute(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """
        Execute binary operation by running both operands and combining results.
        
        Args:
            input_state: Current SQL state
            context: Execution context
            
        Returns:
            New SQL state with binary operation applied
        """
        # Execute left operand operations
        left_state = input_state
        for operation in self.left_operations:
            left_state = operation.execute(left_state, context)
        
        # Execute right operand operations (starting from original input state)
        right_state = input_state
        for operation in self.right_operations:
            right_state = operation.execute(right_state, context)
            
        # Get SQL fragments from both operands
        left_sql = left_state.sql_fragment or left_state.get_effective_base()
        right_sql = right_state.sql_fragment or right_state.get_effective_base()
        
        # Generate the binary operation SQL
        comparison_sql = self._generate_comparison_sql(left_sql, right_sql, context)
        
        return input_state.evolve(
            sql_fragment=comparison_sql,
            is_collection=False,  # Comparisons typically return boolean scalars
            context_mode=input_state.context_mode  # Keep original context mode
        )
        
    def _generate_comparison_sql(self, left_sql: str, right_sql: str,
                               context: ExecutionContext) -> str:
        """
        Generate SQL for binary comparison operation using dialect-aware comparison.

        Args:
            left_sql: SQL expression for left operand
            right_sql: SQL expression for right operand
            context: Execution context

        Returns:
            SQL comparison expression
        """
        # Use dialect's enhanced comparison method for proper type casting
        if hasattr(context.dialect, 'generate_path_condition_comparison'):
            # Delegate to dialect for enhanced type handling
            comparison_sql = context.dialect.generate_path_condition_comparison(
                left_sql, self.operator, right_sql
            )
            return f"({comparison_sql})"
        else:
            # Fallback to basic comparison for dialects without enhanced method
            return self._generate_basic_comparison_sql(left_sql, right_sql)

    def _generate_basic_comparison_sql(self, left_sql: str, right_sql: str) -> str:
        """Basic comparison SQL generation (fallback)."""
        # Handle different operators
        if self.operator == '=':
            return f"({left_sql} = {right_sql})"
        elif self.operator in ('!=', '<>'):
            return f"({left_sql} != {right_sql})"
        elif self.operator == '<':
            return f"({left_sql} < {right_sql})"
        elif self.operator == '<=':
            return f"({left_sql} <= {right_sql})"
        elif self.operator == '>':
            return f"({left_sql} > {right_sql})"
        elif self.operator == '>=':
            return f"({left_sql} >= {right_sql})"
        elif self.operator in ('+', '-', '*', '/'):
            # Arithmetic operations require numeric types - cast both operands
            logger.debug(f"Processing arithmetic operation: {left_sql} {self.operator} {right_sql}")
            left_numeric = self._ensure_numeric_type(left_sql)
            right_numeric = self._ensure_numeric_type(right_sql)
            result = f"({left_numeric} {self.operator} {right_numeric})"
            logger.debug(f"Arithmetic operation result: {result}")
            return result
        else:
            # For other operators, use generic format
            return f"({left_sql} {self.operator} {right_sql})"

    def _ensure_numeric_type(self, sql_expression: str) -> str:
        """
        Ensure SQL expression returns a numeric type for arithmetic operations.

        Args:
            sql_expression: SQL expression that may be string-typed

        Returns:
            SQL expression that returns numeric type
        """
        logger.debug(f"Converting to numeric type: {sql_expression}")

        # If it's a json_extract_string call, convert to json_extract and cast to decimal
        if 'json_extract_string(' in sql_expression:
            # Replace json_extract_string with json_extract and cast to DECIMAL
            numeric_expr = sql_expression.replace('json_extract_string(', 'json_extract(')
            result = f"CAST({numeric_expr} AS DECIMAL)"
            logger.debug(f"Converted json_extract_string to: {result}")
            return result

        # If it's already a numeric expression or literal, use as-is
        if (sql_expression.strip().replace('.', '').replace('-', '').isdigit() or
            'CAST(' in sql_expression or
            'json_extract(' in sql_expression):
            logger.debug(f"Using as-is (already numeric): {sql_expression}")
            return sql_expression

        # Default: try to cast to DECIMAL
        result = f"CAST({sql_expression} AS DECIMAL)"
        logger.debug(f"Default cast to DECIMAL: {result}")
        return result
            
    def get_operation_name(self) -> str:
        """Get human-readable operation name."""
        return f"binary_op({self.operator})"
        
    def optimize_for_dialect(self, dialect) -> 'BinaryOperationBase':
        """
        Optimize binary operation for specific dialect.
        
        Args:
            dialect: Target database dialect
            
        Returns:
            Potentially optimized binary operation
        """
        # Optimize constituent operations
        optimized_left = [op.optimize_for_dialect(dialect) for op in self.left_operations]
        optimized_right = [op.optimize_for_dialect(dialect) for op in self.right_operations]
        
        return BinaryOperationBase(self.operator, optimized_left, optimized_right)
        
    def validate_preconditions(self, input_state: SQLState, context: ExecutionContext) -> None:
        """
        Validate binary operation preconditions.
        
        Args:
            input_state: Input SQL state
            context: Execution context
            
        Raises:
            ValueError: If preconditions not met
        """
        # Validate that we have operations for both sides
        if not self.left_operations:
            raise ValueError("Binary operation missing left operand")
        if not self.right_operations:
            raise ValueError("Binary operation missing right operand")
            
        # Validate constituent operations
        temp_state = input_state
        for op in self.left_operations:
            op.validate_preconditions(temp_state, context)
            temp_state = op.execute(temp_state, context)
            
        temp_state = input_state
        for op in self.right_operations:
            op.validate_preconditions(temp_state, context)  
            temp_state = op.execute(temp_state, context)
            
    def estimate_complexity(self, input_state: SQLState, context: ExecutionContext) -> int:
        """
        Estimate complexity of binary operation.
        
        Args:
            input_state: Input SQL state
            context: Execution context
            
        Returns:
            Complexity score (0-10)
        """
        # Sum complexity of constituent operations plus overhead
        left_complexity = sum(op.estimate_complexity(input_state, context) 
                            for op in self.left_operations)
        right_complexity = sum(op.estimate_complexity(input_state, context)
                             for op in self.right_operations)
        
        # Binary operation adds moderate overhead
        return min(left_complexity + right_complexity + 2, 10)


class ComparisonOperation(BinaryOperationBase):
    """Specialized binary operation for comparisons (=, !=, <, >, etc.)."""
    
    def _generate_comparison_sql(self, left_sql: str, right_sql: str,
                               context: ExecutionContext) -> str:
        """Generate optimized SQL for comparison operations."""
        # Handle null comparisons specially
        if 'NULL' in right_sql.upper():
            if self.operator == '=':
                return f"({left_sql} IS NULL)"
            elif self.operator in ('!=', '<>'):
                return f"({left_sql} IS NOT NULL)"

        # Handle array-to-scalar comparisons for FHIRPath expressions
        if self._is_array_extraction(left_sql) and self._is_scalar_value(right_sql):
            return self._generate_array_scalar_comparison(left_sql, right_sql, context)

        # Use parent implementation for standard comparisons
        return super()._generate_comparison_sql(left_sql, right_sql, context)

    def _is_array_extraction(self, sql: str) -> bool:
        """Check if SQL expression extracts an array using [*] pattern or json_group_array."""
        return ('$[*].' in sql or "'$[*]." in sql or
                'json_group_array(' in sql or 'jsonb_agg(' in sql)

    def _is_scalar_value(self, sql: str) -> bool:
        """Check if SQL expression is a scalar literal value."""
        sql = sql.strip()
        # Check for quoted strings, numbers, or boolean values
        return (sql.startswith("'") and sql.endswith("'")) or sql.isdigit() or sql in ['true', 'false', 'TRUE', 'FALSE']

    def _generate_array_scalar_comparison(self, left_sql: str, right_sql: str,
                                        context: ExecutionContext) -> str:
        """Generate array-to-scalar comparison using dialect-specific logic."""
        # Delegate to dialect for database-specific array comparison logic
        if hasattr(context.dialect, 'generate_array_scalar_comparison'):
            return context.dialect.generate_array_scalar_comparison(
                left_sql, self.operator, right_sql
            )
        else:
            # Fallback to direct comparison if dialect doesn't support it
            return super()._generate_comparison_sql(left_sql, right_sql, context)
        
    def get_operation_name(self) -> str:
        """Get human-readable operation name."""
        return f"comparison({self.operator})"


class LogicalOperation(BinaryOperationBase):
    """Specialized binary operation for logical operations (and, or)."""
    
    def _generate_comparison_sql(self, left_sql: str, right_sql: str,
                               context: ExecutionContext) -> str:
        """Generate SQL for logical operations."""
        if self.operator.lower() == 'and':
            return f"({left_sql} AND {right_sql})"
        elif self.operator.lower() == 'or':
            return f"({left_sql} OR {right_sql})"
        else:
            # Fallback to generic format
            return super()._generate_comparison_sql(left_sql, right_sql, context)
            
    def get_operation_name(self) -> str:
        """Get human-readable operation name."""
        return f"logical({self.operator})"