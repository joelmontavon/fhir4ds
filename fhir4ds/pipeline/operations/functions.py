"""
Complete FHIRPath Function Operations for Pipeline Architecture.

This module implements ALL 70+ FHIRPath functions from the legacy implementation,
organized by category with full dialect optimization support.

Categories:
- Collection Functions (35+ functions)
- String Functions (15+ functions) 
- Type Conversion Functions (15+ functions)
- Math Functions (10+ functions)
- DateTime Functions (5+ functions)
"""

from typing import List, Any, Optional, Dict, Union
import logging
from functools import lru_cache
from ..core.base import PipelineOperation, SQLState, ExecutionContext, ContextMode
from .function_handlers import HandlerRegistry

logger = logging.getLogger(__name__)

class FHIRPathExecutionError(Exception):
    """Base exception for FHIRPath execution errors."""
    pass

class InvalidArgumentError(FHIRPathExecutionError):
    """Raised when function arguments are invalid."""
    pass

class UnsupportedOperationError(FHIRPathExecutionError):
    """Raised when operation is not yet implemented."""
    pass

class ConversionError(FHIRPathExecutionError):
    """Raised when AST conversion fails."""
    pass

class FunctionCallOperation(PipelineOperation[SQLState]):
    """
    Complete FHIRPath function call operation supporting all 70+ functions.
    
    This replaces the scattered function handlers in the legacy implementation
    with a unified, pipeline-based approach.
    """
    
    # Collection Functions (35+) - EXTRACTED to functions/collection.py
    COLLECTION_FUNCTIONS = frozenset({
        'exists', 'empty', 'first', 'last', 'count', 'length', 'select', 'where',
        'all', 'distinct', 'single', 'tail', 'skip', 'take', 'union', 'combine',
        'intersect', 'exclude', 'alltrue', 'allfalse', 'anytrue', 'anyfalse',
        'contains', 'children', 'descendants', 'isdistinct', 'subsetof', 'supersetof',
        'iif', 'repeat', 'aggregate', 'flatten', 'in'
    })
    
    # String Functions (15+) - EXTRACTED to functions/string.py
    STRING_FUNCTIONS = frozenset({
        'substring', 'startswith', 'endswith', 'indexof', 'replace', 'toupper', 'tolower',
        'upper', 'lower', 'trim', 'split', 'tochars', 'matches', 'replacematches', 'join'
    })
    
    # Type Conversion Functions (16+) - EXTRACTED to functions/type_conversion.py
    TYPE_FUNCTIONS = frozenset({
        'toboolean', 'tostring', 'tointeger', 'todecimal', 'todate', 'todatetime', 'totime',
        'toquantity', 'convertstoboolean', 'convertstodecimal', 'convertstointeger',
        'convertstodate', 'convertstodatetime', 'convertstotime', 'as', 'is', 'oftype',
        'quantity',  # Quantity constructor function
        'valueset', 'code',  # System constructor functions
        'toconcept',  # Concept conversion function
        'tuple'  # Tuple constructor function
    })
    
    # Math Functions (26+) - EXTRACTED to functions/math.py
    MATH_FUNCTIONS = frozenset({
        'abs', 'ceiling', 'floor', 'round', 'sqrt', 'truncate', 'exp', 'ln', 'log', 'power',
        '+', '-', '*', '/', '^',  # Basic arithmetic operators (^ is power)
        'precision',  # Precision calculation function
        'max', 'min',        # Min/Max functions
        'greatest', 'least', # SQL-style Min/Max functions
        'avg', 'sum', 'count', 'product',  # Aggregate functions
        'median', 'mode', 'populationstddev', 'populationvariance'  # Statistical functions
    })
    
    # DateTime Functions (14+) - EXTRACTED to functions/datetime.py
    DATETIME_FUNCTIONS = frozenset({
        'now', 'today', 'timeofday', 'lowboundary', 'highboundary',
        'datetime',  # DateTime constructor function
        'ageinyears',  # Context-dependent age calculation function
        'ageinyearsat',  # CQL AgeInYearsAt function - age calculation with specific date
        # Component extraction functions
        'hour_from', 'minute_from', 'second_from', 'year_from', 'month_from', 'day_from'
    })
    # Logical Functions (5) - EXTRACTED to handlers/logical.py
    LOGICAL_FUNCTIONS = frozenset({
        'and', 'or', 'not', 'implies', 'xor'
    })
    
    # Interval Functions (19) - EXTRACTED to handlers/interval.py
    # INTERVAL_FUNCTIONS = frozenset({
    #     'interval', 'contains', 'overlaps', 'before', 'after', 'meets', 'during',
    #     'includes', 'included_in', 'starts', 'ends', 'same_as', 'same_or_before',
    #     'same_or_after', 'width', 'size', 'point_from', 'start', 'end'
    # })
    
    # List Functions (13) - EXTRACTED to handlers/list.py
    # LIST_FUNCTIONS = frozenset({
    #     'list', 'flatten', 'distinct', 'union', 'intersect', 'except',
    #     'first', 'last', 'tail', 'take', 'skip', 'singleton', 'repeat'
    # })
    
    # Comparison Operators (8+) - EXTRACTED to functions/comparison.py
    COMPARISON_FUNCTIONS = frozenset({
        '=', '!=', '<>', '<', '<=', '>', '>=', 'in', 'contains', '~'
    })
    
    # Query Operators (CQL query syntax) (6) - EXTRACTED to handlers/query.py
    # QUERY_FUNCTIONS = frozenset({
    #     'from', 'where', 'return', 'sort', 'aggregate', 'retrievenode'
    # })
    
    # Error and Messaging Functions (2) - EXTRACTED to handlers/error.py
    # ERROR_FUNCTIONS = frozenset({
    #     'message', 'error'
    # })
    
    # FHIR-specific Functions (2) - EXTRACTED to handlers/fhir.py
    # FHIR_FUNCTIONS = frozenset({
    #     'getvalue', 'resolve'
    # })
    
    # All supported functions - ALL FUNCTIONS NOW HANDLED BY MODULAR HANDLERS!
    # Legacy dispatch system only used for unhandled edge cases
    ALL_SUPPORTED_FUNCTIONS = (COLLECTION_FUNCTIONS | STRING_FUNCTIONS | TYPE_FUNCTIONS | 
                             MATH_FUNCTIONS | DATETIME_FUNCTIONS | COMPARISON_FUNCTIONS)
    
    # Note: COLLECTION_FUNCTIONS, STRING_FUNCTIONS, TYPE_FUNCTIONS, MATH_FUNCTIONS, 
    # DATETIME_FUNCTIONS, and COMPARISON_FUNCTIONS have been extracted to modular handlers
    
    def __init__(self, func_name: str, args: List[Any]):
        """
        Initialize function call operation.
        
        Args:
            func_name: Name of function to call
            args: Function arguments (can be literals, other operations, or pipelines)
        """
        self.func_name = func_name.lower()
        self.args = args or []
        self._handler_registry = HandlerRegistry()
        self._validate_function()
    
    def _validate_function(self) -> None:
        """Validate function name and arguments."""
        if self.func_name not in self.ALL_SUPPORTED_FUNCTIONS:
            logger.warning(f"Function '{self.func_name}' may not be fully supported")
    
    def execute(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """
        Execute function call based on function category.
        
        Args:
            input_state: Current SQL state
            context: Execution context with dialect
            
        Returns:
            New SQL state with function applied
        """
        # Try delegation pattern first (new refactored handlers)
        handler = self._handler_registry.get_handler(self.func_name)
        if handler is not None:
            logger.debug(f"Using delegated handler for function '{self.func_name}'")
            return handler.handle_function(self.func_name, input_state, context, self.args)
        
        # No functions should reach this point - all are handled by modular handlers
        raise ValueError(f"Function '{self.func_name}' not handled by any registered handler")
    
    def _handle_union(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """
        Handle union function with proper argument validation.
        
        Args:
            input_state: Current SQL state
            context: Execution context
            
        Returns:
            New SQL state with union applied
            
        Raises:
            InvalidArgumentError: If no arguments provided
        """
        if not self.args:
            raise InvalidArgumentError("union() function requires another collection argument")
        
        # This method is kept for compatibility with existing tests
        # Actual union handling is done by the collection handler
        handler = self._handler_registry.get_handler("union")
        if handler is not None:
            return handler.handle_function("union", input_state, context, self.args)
        
        raise ValueError("Union handler not found")
    
    def _build_union_sql_duckdb(self, col1: str, col2: str) -> str:
        """
        Build DuckDB-specific SQL for union operation.
        
        This method is kept for test compatibility.
        """
        return f"""
        SELECT
            CASE 
                WHEN json_type({col1}) = 'ARRAY' AND json_type({col2}) = 'ARRAY' THEN
                    json_group_array(DISTINCT value) 
                FROM (
                    SELECT value FROM json_each({col1})
                    UNION
                    SELECT value FROM json_each({col2})
                )
                ELSE json_array({col1}, {col2})
            END
        """
    
    def _build_union_sql_postgresql(self, col1: str, col2: str) -> str:
        """
        Build PostgreSQL-specific SQL for union operation.
        
        This method is kept for test compatibility.
        """
        return f"""
        SELECT
            CASE 
                WHEN jsonb_typeof({col1}) = 'array' AND jsonb_typeof({col2}) = 'array' THEN
                    jsonb_agg(DISTINCT value)
                FROM (
                    SELECT value FROM jsonb_array_elements({col1})
                    UNION
                    SELECT value FROM jsonb_array_elements({col2})
                ) t
                ELSE jsonb_build_array({col1}, {col2})
            END
        """


    # ====================================
    # HELPER METHODS FOR CONSISTENT STATE MANAGEMENT
    # ====================================
    
    def _create_collection_result(self, input_state: SQLState, sql_fragment: str) -> SQLState:
        """Helper to create collection results consistently."""
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    def _create_scalar_result(self, input_state: SQLState, sql_fragment: str) -> SQLState:
        """Helper to create scalar results consistently."""
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _evaluate_union_argument(self, arg: Any, input_state: SQLState, 
                                context: ExecutionContext) -> str:
        """Evaluate union function argument to SQL fragment."""
        if not hasattr(arg, '__class__'):
            return str(arg)  # Simple literal
        
        # Check if this is a pipeline operation (like FunctionCallOperation)
        if hasattr(arg, 'execute') and callable(getattr(arg, 'execute')):
            # This is a pipeline operation - execute it directly
            base_state = self._create_base_copy(input_state)
            result_state = arg.execute(base_state, context)
            return result_state.sql_fragment
        
        # For AST nodes, use the AST converter
        try:
            return self._convert_ast_argument_to_sql(arg, input_state, context)
        except ConversionError as e:
            raise InvalidArgumentError(f"Failed to evaluate union argument: {e}")
    
    def _convert_ast_argument_to_sql(self, ast_node: Any, input_state: SQLState,
                                    context: ExecutionContext) -> str:
        """Convert AST node argument to SQL fragment."""
        from ..converters.ast_converter import ASTToPipelineConverter
        
        converter = ASTToPipelineConverter()
        base_state = self._create_base_copy(input_state)  # Use helper method
        
        arg_pipeline = converter.convert_ast_to_pipeline(ast_node)
        current_state = base_state
        
        for op in arg_pipeline.operations:
            current_state = op.execute(current_state, context)
        
        return current_state.sql_fragment
    
    def _create_base_copy(self, input_state: SQLState) -> SQLState:
        """Create a fresh base state copy for argument evaluation."""
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=input_state.json_column,
            resource_type=input_state.resource_type
        )
    
    
    def _add_debug_info(self, sql: str, operation: str, context: ExecutionContext) -> str:
        """Add debug information to SQL when in debug mode."""
        if logger.isEnabledFor(logging.DEBUG) or context.debug_mode:
            return f"/* FHIRPath {operation} */ {sql}"
        return sql
    
    @lru_cache(maxsize=256)
    def _get_cached_sql_pattern(self, pattern_type: str, context: ExecutionContext, *args) -> str:
        """Get cached SQL patterns for common operations."""
        cache_key = f"{pattern_type}_{context.dialect.name.upper()}_{args}"
        
        if pattern_type in ("exists_collection", "empty_collection"):
            # Use dialect method for collection checks
            is_collection = True
            fragment_placeholder = "{fragment}"
            pattern = context.dialect.generate_exists_check(fragment_placeholder, is_collection)
            
            if pattern_type == "empty_collection":
                # Negate the exists check for empty
                if pattern.startswith("(") and pattern.endswith(")"):
                    # Replace > 0 with = 0 for empty check
                    pattern = pattern.replace("> 0", "= 0")
                else:
                    pattern = f"NOT {pattern}"
            return pattern
        elif pattern_type in ("exists_scalar", "empty_scalar"):
            # Use dialect method for scalar checks  
            is_collection = False
            fragment_placeholder = "{fragment}"
            pattern = context.dialect.generate_exists_check(fragment_placeholder, is_collection)
            
            if pattern_type == "empty_scalar":
                # Negate the exists check for empty
                pattern = pattern.replace("IS NOT NULL", "IS NULL")
            return pattern
        else:
            # Return uncached pattern
            return None

    # ====================================
    # COLLECTION FUNCTIONS (35+ functions)
    # ====================================
    
    # Error functions EXTRACTED to handlers/error.py
    
    # =========================================
    # FHIR-SPECIFIC FUNCTIONS (2+ functions)
    # =========================================
    
    # FHIR functions EXTRACTED to handlers/fhir.py

    # =========================================
    # COMPARISON OPERATORS (8+ operators)
    # =========================================
    
    # Logical functions EXTRACTED to handlers/logical.py
    
    # Interval functions completely EXTRACTED to handlers/interval.py
    
    def get_operation_name(self) -> str:
        """Get human-readable operation name."""
        args_preview = f"({len(self.args)} args)" if self.args else "()"
        return f"function({self.func_name}{args_preview})"
    
    def validate_preconditions(self, input_state: SQLState, context: ExecutionContext) -> None:
        """
        Validate function preconditions.
        
        Args:
            input_state: Input SQL state
            context: Execution context
            
        Raises:
            ValueError: If preconditions not met
        """
        # Basic validation - function must be supported by some handler
        if self.func_name not in self.ALL_SUPPORTED_FUNCTIONS:
            handler = self._handler_registry.get_handler(self.func_name)
            if handler is None:
                raise ValueError(f"Unsupported function: {self.func_name}")
    
    def estimate_complexity(self, input_state: SQLState, context: ExecutionContext) -> int:
        """
        Estimate computational complexity of this function.
        
        Args:
            input_state: Input SQL state
            context: Execution context
            
        Returns:
            Estimated complexity score (higher = more complex)
        """
        # Base complexity by function type
        complexity_map = {
            'exists': 1, 'empty': 1, 'count': 2, 'length': 1,
            'first': 1, 'last': 1, 'single': 2,
            'where': 3, 'select': 3, 'all': 4, 'distinct': 3,
            'union': 3, 'intersect': 4, 'combine': 3,
            'contains': 2, 'in': 2, 'substring': 2, 
            'upper': 1, 'lower': 1, 'trim': 1,
            'abs': 1, 'round': 1, 'sqrt': 2, 'power': 3,
            'sum': 2, 'avg': 3, 'max': 2, 'min': 2,
            'now': 1, 'today': 1, 'datetime': 2,
            'tostring': 1, 'tointeger': 2, 'toboolean': 2,
            'and': 1, 'or': 1, 'not': 1, 'implies': 2
        }
        
        base_complexity = complexity_map.get(self.func_name, 2)  # Default complexity
        
        # Add complexity for arguments
        arg_complexity = len(self.args) * 0.5
        
        # Collection functions on collections are more complex
        if input_state.is_collection and self.func_name in ['where', 'select', 'all', 'distinct']:
            base_complexity += 2
        
        return int(base_complexity + arg_complexity)
    
    def optimize_for_dialect(self, dialect) -> 'FunctionCallOperation':
        """
        Optimize define reference operation for specific dialect.
        
        Args:
            dialect: Target dialect for optimization
            
        Returns:
            Optimized define reference operation
        """
        # Define references are dialect-agnostic since they resolve to other expressions
        return self
    
    def __repr__(self) -> str:
        """String representation."""
        args_str = f", args={len(self.args)}" if self.args else ""
        return f"FunctionCallOperation(func_name='{self.func_name}'{args_str})"
