"""
Base handler for function handlers with CTE support.

This module provides the base class and common patterns for function handlers
that need to use the CTEBuilder system. It includes CTE decision logic,
fallback patterns, and common utilities for SQL generation.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, TYPE_CHECKING, Dict, Any

if TYPE_CHECKING:
    from ..generator import SQLGenerator
    from ..cte_builder import CTEBuilder


class BaseFunctionHandler(ABC):
    """
    Base class for function handlers with CTE support.
    
    This class provides common patterns for CTE usage, fallback logic,
    and utilities that all function handlers can use. It establishes
    a consistent interface for CTE-aware function processing.
    
    Key Features:
    - CTE benefit analysis for deciding when to use CTEs
    - Fallback patterns for when CTEs aren't beneficial
    - Common utility methods for SQL generation
    - Consistent error handling and validation
    - Integration with CTEBuilder for dependency management
    
    Subclasses must implement:
    - get_supported_functions(): Return list of function names
    - can_handle(function_name): Check if function is supported
    - handle_function(func_name, base_expr, func_node): Process function call
    """
    
    def __init__(self, generator: 'SQLGenerator', cte_builder: Optional['CTEBuilder'] = None):
        """
        Initialize the function handler.
        
        Args:
            generator: Reference to main SQLGenerator for complex operations
            cte_builder: Optional CTEBuilder instance for CTE management
                        If None, CTEs will not be used
        """
        self.generator = generator
        self.cte_builder = cte_builder
        self.dialect = generator.dialect
        
        # CTE configuration from generator
        self.enable_cte = getattr(generator, 'enable_cte', True)
        
        # Performance thresholds for CTE decisions
        self.cte_thresholds = {
            'min_expression_length': 100,
            'max_simple_operations': 3,
            'min_subquery_count': 2,
            'min_case_statements': 3,
            'min_json_operations': 5
        }
    
    @abstractmethod
    def get_supported_functions(self) -> List[str]:
        """
        Return list of function names this handler supports.
        
        Returns:
            List of function names (case-insensitive)
            
        Example:
            return ['startswith', 'endswith', 'contains', 'substring']
        """
        pass
    
    @abstractmethod
    def can_handle(self, function_name: str) -> bool:
        """
        Check if this handler can process the given function.
        
        Args:
            function_name: Name of the function to check
            
        Returns:
            True if this handler supports the function
        """
        pass
    
    @abstractmethod
    def handle_function(self, func_name: str, base_expr: str, func_node) -> str:
        """
        Handle function call and return SQL expression.
        
        Args:
            func_name: Name of the function to handle
            base_expr: Base SQL expression to apply function to
            func_node: Function AST node with arguments
            
        Returns:
            SQL expression for the function result
        """
        pass
    
    def should_use_cte(self, base_expr: str, operation: str, **kwargs) -> bool:
        """
        Determine if CTE should be used for this operation.
        
        This method analyzes the complexity of the expression and operation
        to decide whether creating a CTE would be beneficial. The decision
        is based on multiple factors including expression length, complexity,
        and the type of operation being performed.
        
        Args:
            base_expr: Base SQL expression to analyze
            operation: Type of operation (e.g., 'where', 'select', 'transform')
            **kwargs: Additional context for CTE decision
                     - force_cte: Force CTE usage regardless of analysis
                     - disable_cte: Disable CTE usage regardless of analysis
                     - complexity_score: Pre-calculated complexity score
                     
        Returns:
            True if CTE should be used, False for inline expression
            
        Decision Factors:
        1. CTE system availability and enablement
        2. Expression complexity (length, nested operations)
        3. Operation type (some operations benefit more from CTEs)
        4. Performance characteristics
        5. Explicit overrides from kwargs
        """
        # Check if CTE system is available and enabled
        if not self.cte_builder or not self.enable_cte:
            return False
        
        # Check for explicit overrides
        if kwargs.get('force_cte', False):
            return True
        if kwargs.get('disable_cte', False):
            return False
        
        # Use pre-calculated complexity score if provided
        if 'complexity_score' in kwargs:
            return kwargs['complexity_score'] >= 5
        
        # Analyze expression complexity
        complexity_score = self._calculate_complexity_score(base_expr)
        
        # Operation-specific adjustments
        operation_multipliers = {
            'where': 1.2,      # WHERE clauses benefit from CTEs
            'select': 1.5,     # SELECT transforms often benefit
            'exists': 1.1,     # Simple boolean operations
            'count': 1.3,      # Aggregations can benefit
            'transform': 1.4,  # Complex transformations
            'filter': 1.2,     # Filtering operations
            'substring': 0.8,  # Simple string operations
            'startswith': 0.7, # Simple pattern matching
            'endswith': 0.7,   # Simple pattern matching
        }
        
        adjusted_score = complexity_score * operation_multipliers.get(operation, 1.0)
        
        # Decision threshold (adjustable based on performance testing)
        threshold = 4.0
        return adjusted_score >= threshold
    
    def _calculate_complexity_score(self, expression: str) -> float:
        """
        Calculate complexity score for an SQL expression.
        
        This method analyzes various aspects of an SQL expression to determine
        its complexity. Higher scores indicate more complex expressions that
        would benefit from CTE optimization.
        
        Args:
            expression: SQL expression to analyze
            
        Returns:
            Complexity score (0.0 to 10.0+)
            
        Complexity Factors:
        - Length of expression
        - Number of subqueries (SELECT statements)
        - JSON operations
        - CASE statements
        - Function calls
        - Nested parentheses
        """
        if not expression:
            return 0.0
        
        score = 0.0
        expr_lower = expression.lower()
        
        # Length factor (normalized)
        length_factor = min(len(expression) / 200, 2.0)
        score += length_factor
        
        # Subquery complexity
        select_count = expr_lower.count('select')
        if select_count > 1:
            score += (select_count - 1) * 1.5
        
        # JSON operations
        json_ops = [
            'json_extract', 'json_type', 'json_array_length',
            'json_each', 'json_group_array', 'json_object'
        ]
        json_count = sum(expr_lower.count(op) for op in json_ops)
        score += json_count * 0.8
        
        # CASE statements
        case_count = expr_lower.count('case')
        score += case_count * 0.7
        
        # Complex function calls
        complex_functions = [
            'substring', 'replace', 'coalesce', 'cast',
            'upper', 'lower', 'trim', 'concat'
        ]
        func_count = sum(expr_lower.count(func) for func in complex_functions)
        score += func_count * 0.5
        
        # Nested parentheses depth
        max_depth = self._calculate_parentheses_depth(expression)
        if max_depth > 3:
            score += (max_depth - 3) * 0.6
        
        # WHERE clauses and joins
        where_count = expr_lower.count('where')
        join_count = expr_lower.count('join')
        score += (where_count + join_count) * 0.4
        
        return score
    
    def _calculate_parentheses_depth(self, expression: str) -> int:
        """
        Calculate maximum nesting depth of parentheses in expression.
        
        Args:
            expression: SQL expression to analyze
            
        Returns:
            Maximum depth of nested parentheses
        """
        max_depth = 0
        current_depth = 0
        
        for char in expression:
            if char == '(':
                current_depth += 1
                max_depth = max(max_depth, current_depth)
            elif char == ')':
                current_depth = max(0, current_depth - 1)
        
        return max_depth
    
    def create_cte_if_beneficial(self, operation: str, sql: str, 
                                dependencies: Optional[List[str]] = None,
                                **kwargs) -> str:
        """
        Create CTE if beneficial, otherwise return original SQL.
        
        This is the main method for conditionally creating CTEs. It analyzes
        whether a CTE would be beneficial and either creates one or returns
        the original SQL for inline use.
        
        Args:
            operation: Operation type for CTE naming
            sql: SQL expression that might become a CTE
            dependencies: CTE dependencies if CTE is created
            **kwargs: Additional context passed to should_use_cte()
            
        Returns:
            Either CTE reference or original SQL
            
        Example:
            # Will create CTE if beneficial
            result = self.create_cte_if_beneficial(
                'filter', 
                complex_filter_sql,
                dependencies=['base_cte']
            )
            # Returns either "(SELECT result FROM filter_1)" or complex_filter_sql
        """
        if self.should_use_cte(sql, operation, **kwargs):
            cte_name = self.cte_builder.create_cte(operation, sql, dependencies)
            
            # Determine what column to select from CTE
            result_column = kwargs.get('result_column', None)
            return self.cte_builder.reference(cte_name, result_column)
        else:
            return sql
    
    def generate_cte_sql(self, operation: str, main_logic: str, 
                        table_name: Optional[str] = None) -> str:
        """
        Generate properly formatted CTE SQL with standard structure.
        
        This method creates well-formatted CTE SQL with consistent structure
        that includes proper SELECT, FROM, and WHERE clauses as needed.
        
        Args:
            operation: Type of operation for documentation
            main_logic: Core SQL logic for the CTE
            table_name: Table name to use (defaults to generator's table)
            
        Returns:
            Complete CTE SQL ready for use
            
        Example:
            cte_sql = self.generate_cte_sql(
                'filter',
                "CASE WHEN active = true THEN 1 ELSE 0 END as is_active",
                "patients"
            )
            # Returns: "SELECT CASE WHEN active = true THEN 1 ELSE 0 END as is_active FROM patients"
        """
        if not table_name:
            table_name = self.generator.table_name
        
        # Format CTE SQL with consistent structure
        return f"""
        SELECT 
            {main_logic}
        FROM {table_name}
        """.strip()
    
    def handle_function_with_args(self, func_name: str, base_expr: str, 
                                 func_node, expected_args: int) -> List[str]:
        """
        Validate function arguments and return processed argument SQL.
        
        This utility method handles common argument validation and processing
        that most function handlers need.
        
        Args:
            func_name: Function name for error messages
            base_expr: Base expression (not used but kept for consistency)
            func_node: Function AST node with arguments
            expected_args: Expected number of arguments
            
        Returns:
            List of processed argument SQL expressions
            
        Raises:
            ValueError: If argument count doesn't match expected
            
        Example:
            args = self.handle_function_with_args('startswith', base_expr, func_node, 1)
            prefix_sql = args[0]
        """
        if len(func_node.args) != expected_args:
            raise ValueError(f"{func_name}() function requires exactly {expected_args} argument(s), got {len(func_node.args)}")
        
        # Process each argument through the generator
        processed_args = []
        for arg in func_node.args:
            arg_sql = self.generator.visit(arg)
            processed_args.append(arg_sql)
        
        return processed_args
    
    def create_case_expression(self, condition: str, true_value: str, 
                              false_value: str = "NULL", 
                              null_check: Optional[str] = None) -> str:
        """
        Create standardized CASE expression with optional null checking.
        
        This utility creates consistent CASE expressions that many function
        handlers need, with optional null checking for robustness.
        
        Args:
            condition: Main condition to evaluate
            true_value: Value when condition is true
            false_value: Value when condition is false (default: NULL)
            null_check: Optional expression to check for NULL (added as first condition)
            
        Returns:
            Complete CASE expression
            
        Example:
            case_expr = self.create_case_expression(
                "CAST(name AS VARCHAR) LIKE 'John%'",
                "true", 
                "false",
                null_check="name IS NOT NULL"
            )
            # Returns: CASE WHEN name IS NOT NULL THEN 
            #            CASE WHEN CAST(name AS VARCHAR) LIKE 'John%' THEN true ELSE false END
            #          ELSE false END
        """
        if null_check:
            return f"""
            CASE 
                WHEN {null_check} THEN
                    CASE WHEN {condition} THEN {true_value} ELSE {false_value} END
                ELSE {false_value}
            END
            """.strip()
        else:
            return f"""
            CASE 
                WHEN {condition} THEN {true_value} 
                ELSE {false_value} 
            END
            """.strip()
    
    def handle_array_operation(self, base_expr: str, element_logic: str,
                              operation_type: str = 'transform') -> str:
        """
        Handle operations on JSON arrays with consistent patterns.
        
        This utility provides a standardized way to handle array operations
        that many function handlers need, such as filtering or transforming
        array elements.
        
        Args:
            base_expr: Expression that should yield a JSON array
            element_logic: SQL logic to apply to each array element
                          Use 'value' to reference current element
            operation_type: Type of operation for CTE naming
            
        Returns:
            SQL expression that processes the array
            
        Example:
            result = self.handle_array_operation(
                "json_extract(resource, '$.name')",
                "UPPER(CAST(value AS VARCHAR))",
                "transform"
            )
        """
        array_sql = f"""
        CASE 
            WHEN json_type({base_expr}) = 'ARRAY' THEN (
                SELECT json_group_array({element_logic})
                FROM json_each({base_expr}, '$')
                WHERE ({element_logic}) IS NOT NULL
            )
            WHEN {base_expr} IS NOT NULL THEN json_array({element_logic.replace('value', base_expr)})
            ELSE json_array()
        END
        """
        
        # For testing purposes, return the SQL directly rather than creating CTE
        # In practice, this would use create_cte_if_beneficial
        return array_sql
    
    def get_debug_info(self) -> Dict[str, Any]:
        """
        Return debugging information about handler state.
        
        Returns:
            Dictionary with handler configuration and state information
        """
        return {
            'handler_type': self.__class__.__name__,
            'supported_functions': self.get_supported_functions(),
            'cte_enabled': self.enable_cte,
            'cte_builder_available': self.cte_builder is not None,
            'thresholds': self.cte_thresholds.copy(),
            'generator_table': self.generator.table_name,
            'dialect': self.dialect.__class__.__name__ if self.dialect else None
        }
    
    def validate_function_call(self, func_name: str, func_node) -> None:
        """
        Validate function call structure and arguments.
        
        Args:
            func_name: Function name being called
            func_node: Function AST node to validate
            
        Raises:
            ValueError: If function call is invalid
        """
        if not func_node:
            raise ValueError(f"Function node is required for {func_name}()")
        
        if not hasattr(func_node, 'args'):
            raise ValueError(f"Function node missing args attribute for {func_name}()")
        
        if not isinstance(func_node.args, list):
            raise ValueError(f"Function node args must be a list for {func_name}()")
    
    def __str__(self) -> str:
        """String representation for debugging."""
        return f"{self.__class__.__name__}(functions={len(self.get_supported_functions())}, cte_enabled={self.enable_cte})"
    
    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return f"{self.__class__.__name__}(supported={self.get_supported_functions()}, cte_builder={self.cte_builder is not None})"