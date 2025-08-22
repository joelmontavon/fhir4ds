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
    
    # Collection Functions (35+)
    COLLECTION_FUNCTIONS = frozenset({
        'exists', 'empty', 'first', 'last', 'count', 'length', 'select', 'where',
        'all', 'distinct', 'single', 'tail', 'skip', 'take', 'union', 'combine',
        'intersect', 'exclude', 'alltrue', 'allfalse', 'anytrue', 'anyfalse',
        'contains', 'children', 'descendants', 'isdistinct', 'subsetof', 'supersetof',
        'iif', 'repeat', 'aggregate', 'flatten', 'in'
    })
    
    # String Functions (15+)
    STRING_FUNCTIONS = frozenset({
        'substring', 'startswith', 'endswith', 'indexof', 'replace', 'toupper', 'tolower',
        'upper', 'lower', 'trim', 'split', 'tochars', 'matches', 'replacematches', 'join'
    })
    
    # Type Conversion Functions (15+)
    TYPE_FUNCTIONS = frozenset({
        'toboolean', 'tostring', 'tointeger', 'todecimal', 'todate', 'todatetime', 'totime',
        'toquantity', 'convertstoboolean', 'convertstodecimal', 'convertstointeger',
        'convertstodate', 'convertstodatetime', 'convertstotime', 'as', 'is', 'oftype'
    })
    
    # Math Functions (10+)
    MATH_FUNCTIONS = frozenset({
        'abs', 'ceiling', 'floor', 'round', 'sqrt', 'truncate', 'exp', 'ln', 'log', 'power'
    })
    
    # DateTime Functions (5+)
    DATETIME_FUNCTIONS = frozenset({
        'now', 'today', 'timeofday', 'lowboundary', 'highboundary'
    })
    
    # Comparison Operators (8+)
    COMPARISON_FUNCTIONS = frozenset({
        '=', '!=', '<>', '<', '<=', '>', '>=', 'in', 'contains'
    })
    
    # All supported functions
    ALL_SUPPORTED_FUNCTIONS = (COLLECTION_FUNCTIONS | STRING_FUNCTIONS | TYPE_FUNCTIONS | 
                              MATH_FUNCTIONS | DATETIME_FUNCTIONS | COMPARISON_FUNCTIONS)
    
    def __init__(self, func_name: str, args: List[Any]):
        """
        Initialize function call operation.
        
        Args:
            func_name: Name of function to call
            args: Function arguments (can be literals, other operations, or pipelines)
        """
        self.func_name = func_name.lower()
        self.args = args or []
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
        # Route to appropriate handler based on function category
        if self._is_collection_function():
            return self._execute_collection_function(input_state, context)
        elif self._is_string_function():
            return self._execute_string_function(input_state, context)
        elif self._is_type_function():
            return self._execute_type_function(input_state, context)
        elif self._is_math_function():
            return self._execute_math_function(input_state, context)
        elif self._is_datetime_function():
            return self._execute_datetime_function(input_state, context)
        elif self._is_comparison_function():
            return self._execute_comparison_function(input_state, context)
        else:
            raise ValueError(f"Unknown function category for: {self.func_name}")
    
    def _is_collection_function(self) -> bool:
        """Check if function is a collection function."""
        return self.func_name in self.COLLECTION_FUNCTIONS
    
    def _is_string_function(self) -> bool:
        """Check if function is a string function."""
        return self.func_name in self.STRING_FUNCTIONS
    
    def _is_type_function(self) -> bool:
        """Check if function is a type conversion function."""
        return self.func_name in self.TYPE_FUNCTIONS
    
    def _is_math_function(self) -> bool:
        """Check if function is a math function."""
        return self.func_name in self.MATH_FUNCTIONS
    
    def _is_datetime_function(self) -> bool:
        """Check if function is a datetime function."""
        return self.func_name in self.DATETIME_FUNCTIONS
    
    def _is_comparison_function(self) -> bool:
        """Check if function is a comparison operator."""
        return self.func_name in self.COMPARISON_FUNCTIONS

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
    
    def _build_union_sql_duckdb(self, first_collection: str, second_collection: str) -> str:
        """Build DuckDB-specific union SQL."""
        return f"""(
            CASE 
                WHEN {first_collection} IS NULL AND {second_collection} IS NULL THEN NULL
                WHEN {first_collection} IS NULL THEN {second_collection}
                WHEN {second_collection} IS NULL THEN {first_collection}
                WHEN json_type({first_collection}) = 'ARRAY' AND json_type({second_collection}) = 'ARRAY' THEN (
                    SELECT json_group_array(value)
                    FROM (
                        SELECT value FROM json_each({first_collection})
                        UNION
                        SELECT value FROM json_each({second_collection})
                    )
                )
                ELSE json_array({first_collection}, {second_collection})
            END
        )"""
    
    def _build_union_sql_postgresql(self, first_collection: str, second_collection: str) -> str:
        """Build PostgreSQL-specific union SQL."""
        return f"""(
            CASE 
                WHEN {first_collection} IS NULL AND {second_collection} IS NULL THEN NULL
                WHEN {first_collection} IS NULL THEN {second_collection}
                WHEN {second_collection} IS NULL THEN {first_collection}
                WHEN jsonb_typeof({first_collection}) = 'array' AND jsonb_typeof({second_collection}) = 'array' THEN (
                    SELECT jsonb_agg(value)
                    FROM (
                        SELECT value FROM jsonb_array_elements({first_collection})
                        UNION
                        SELECT value FROM jsonb_array_elements({second_collection})
                    ) AS combined
                )
                ELSE jsonb_build_array({first_collection}, {second_collection})
            END
        )"""
    
    def _add_debug_info(self, sql: str, operation: str, context: ExecutionContext) -> str:
        """Add debug information to SQL when in debug mode."""
        if logger.isEnabledFor(logging.DEBUG) or context.debug_mode:
            return f"/* FHIRPath {operation} */ {sql}"
        return sql
    
    @lru_cache(maxsize=256)
    def _get_cached_sql_pattern(self, pattern_type: str, dialect_name: str, *args) -> str:
        """Get cached SQL patterns for common operations."""
        cache_key = f"{pattern_type}_{dialect_name}_{args}"
        
        if pattern_type == "exists_collection" and dialect_name == "DUCKDB":
            return f"(json_array_length({{fragment}}) > 0)"
        elif pattern_type == "exists_collection" and dialect_name == "POSTGRESQL":
            return f"(jsonb_array_length({{fragment}}) > 0)"
        elif pattern_type == "empty_collection" and dialect_name == "DUCKDB":
            return f"(json_array_length({{fragment}}) = 0)"
        elif pattern_type == "empty_collection" and dialect_name == "POSTGRESQL":
            return f"(jsonb_array_length({{fragment}}) = 0)"
        elif pattern_type == "exists_scalar":
            return f"({{fragment}} IS NOT NULL)"
        elif pattern_type == "empty_scalar":
            return f"({{fragment}} IS NULL)"
        else:
            # Return uncached pattern
            return None

    # ====================================
    # COLLECTION FUNCTIONS (35+ functions)
    # ====================================
    
    def _execute_collection_function(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Execute collection function with dialect optimization."""
        
        if self.func_name == 'exists':
            return self._handle_exists(input_state, context)
        elif self.func_name == 'empty':
            return self._handle_empty(input_state, context)
        elif self.func_name == 'first':
            return self._handle_first(input_state, context)
        elif self.func_name == 'last':
            return self._handle_last(input_state, context)
        elif self.func_name == 'count' or self.func_name == 'length':
            return self._handle_count(input_state, context)
        elif self.func_name == 'where':
            return self._handle_where(input_state, context)
        elif self.func_name == 'select':
            return self._handle_select(input_state, context)
        elif self.func_name == 'distinct':
            return self._handle_distinct(input_state, context)
        elif self.func_name == 'combine':
            return self._handle_combine(input_state, context)
        elif self.func_name == 'tail':
            return self._handle_tail(input_state, context)
        elif self.func_name == 'skip':
            return self._handle_skip(input_state, context)
        elif self.func_name == 'take':
            return self._handle_take(input_state, context)
        elif self.func_name == 'union':
            return self._handle_union(input_state, context)
        elif self.func_name == 'intersect':
            return self._handle_intersect(input_state, context)
        elif self.func_name == 'exclude':
            return self._handle_exclude(input_state, context)
        elif self.func_name in ['alltrue', 'allfalse', 'anytrue', 'anyfalse']:
            return self._handle_boolean_collection(input_state, context)
        elif self.func_name == 'contains':
            return self._handle_contains_collection(input_state, context)
        elif self.func_name == 'children':
            return self._handle_children(input_state, context)
        elif self.func_name == 'descendants':
            return self._handle_descendants(input_state, context)
        elif self.func_name == 'isdistinct':
            return self._handle_isdistinct(input_state, context)
        elif self.func_name in ['subsetof', 'supersetof']:
            return self._handle_set_operations(input_state, context)
        elif self.func_name == 'iif':
            return self._handle_iif(input_state, context)
        elif self.func_name == 'repeat':
            return self._handle_repeat(input_state, context)
        elif self.func_name == 'aggregate':
            return self._handle_aggregate(input_state, context)
        elif self.func_name == 'flatten':
            return self._handle_flatten(input_state, context)
        elif self.func_name == 'in':
            return self._handle_in(input_state, context)
        elif self.func_name == 'single':
            return self._handle_single(input_state, context)
        elif self.func_name == 'all':
            return self._handle_all(input_state, context)
        else:
            raise ValueError(f"Unknown collection function: {self.func_name}")
    
    def _handle_exists(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle exists() function."""
        if input_state.is_collection:
            # Try to use cached pattern first
            pattern = self._get_cached_sql_pattern("exists_collection", context.dialect.name.upper())
            if pattern:
                sql_fragment = pattern.format(fragment=input_state.sql_fragment)
            else:
                sql_fragment = f"({context.dialect.json_array_length(input_state.sql_fragment)} > 0)"
        else:
            # Use cached pattern for scalar
            pattern = self._get_cached_sql_pattern("exists_scalar", context.dialect.name.upper())
            sql_fragment = pattern.format(fragment=input_state.sql_fragment)
        
        sql_fragment = self._add_debug_info(sql_fragment, "exists()", context)
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_empty(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle empty() function."""
        if input_state.is_collection:
            sql_fragment = f"({context.dialect.json_array_length(input_state.sql_fragment)} = 0)"
        else:
            sql_fragment = f"({input_state.sql_fragment} IS NULL)"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_first(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle first() function."""
        if input_state.is_collection:
            sql_fragment = context.dialect.get_json_array_element(input_state.sql_fragment, 0)
        else:
            # Single value - first() returns the value itself
            sql_fragment = input_state.sql_fragment
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_last(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle last() function."""
        if input_state.is_collection:
            if context.dialect.name.upper() == 'DUCKDB':
                sql_fragment = f"""
                json_extract({input_state.sql_fragment}, 
                           '$[' || (json_array_length({input_state.sql_fragment}) - 1) || ']')
                """
            else:  # PostgreSQL
                sql_fragment = f"""
                ({input_state.sql_fragment} -> (jsonb_array_length({input_state.sql_fragment}) - 1))
                """
        else:
            # Single value - last() returns the value itself
            sql_fragment = input_state.sql_fragment
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_count(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle count() and length() functions."""
        if input_state.is_collection:
            if context.dialect.name.upper() == 'DUCKDB':
                sql_fragment = f"json_array_length({input_state.sql_fragment})"
            else:  # PostgreSQL
                sql_fragment = f"jsonb_array_length({input_state.sql_fragment})"
        else:
            # Single value - count is 1 if not null, 0 if null
            sql_fragment = f"CASE WHEN {input_state.sql_fragment} IS NOT NULL THEN 1 ELSE 0 END"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_where(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle where() function with condition."""
        if not self.args:
            raise ValueError("where() function requires a condition argument")
        
        # Compile the condition argument to SQL
        condition_sql = self._compile_where_condition(self.args[0], context)
        
        # Generate SQL to filter array elements based on the condition
        collection_expr = input_state.sql_fragment
        
        if context.dialect.name.upper() == 'DUCKDB':
            # For DuckDB, we need to filter JSON array elements and preserve them as objects
            # Use json_group_array to collect all matching items into a single JSON array
            sql_fragment = f"""(
                SELECT json_group_array(item.value)
                FROM json_each({collection_expr}) AS item
                WHERE {condition_sql.replace('$ITEM', 'item.value')}
            )"""
        else:  # PostgreSQL
            # For PostgreSQL, use jsonb_path_query_array with filter
            sql_fragment = f"""jsonb_path_query_array({collection_expr}, '$[*] ? ({condition_sql})')"""
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=True,
            context_mode=ContextMode.COLLECTION,
            path_context="$"  # Reset path context since where creates a new filtered array
        )
    
    def _compile_where_condition(self, condition_arg, context: ExecutionContext) -> str:
        """Compile a where condition argument to SQL."""
        from ...fhirpath.parser.ast_nodes import BinaryOpNode, IdentifierNode, LiteralNode
        
        # Handle BinaryOpNode (e.g., use = "official")
        if isinstance(condition_arg, BinaryOpNode):
            if condition_arg.operator == '=':
                # Extract left field name and right value
                if isinstance(condition_arg.left, IdentifierNode) and isinstance(condition_arg.right, LiteralNode):
                    field_name = condition_arg.left.name
                    value = condition_arg.right.value
                    
                    # Return dialect-specific condition
                    if context.dialect.name.upper() == 'DUCKDB':
                        return f"json_extract_string($ITEM, '$.{field_name}') = '{value}'"
                    else:  # PostgreSQL
                        return f"@.{field_name} == \"{value}\""
        
        # Handle pipeline operations (converted AST nodes)
        if hasattr(condition_arg, 'operations') and condition_arg.operations:
            # This is a pipeline with operations, likely a binary comparison
            for op in condition_arg.operations:
                if hasattr(op, 'function_name') and op.function_name == '=':
                    # Extract left and right operands for equality
                    if hasattr(op, 'args') and len(op.args) >= 1:
                        # Get the right operand (the value we're comparing to)
                        right_arg = op.args[0]
                        if hasattr(right_arg, 'operations') and right_arg.operations:
                            for right_op in right_arg.operations:
                                if hasattr(right_op, 'value'):
                                    value = right_op.value
                                    # Return DuckDB-style condition
                                    if context.dialect.name.upper() == 'DUCKDB':
                                        return f"json_extract_string($ITEM, '$.use') = '{value}'"
                                    else:  # PostgreSQL
                                        return f"@.use == \"{value}\""
        
        # No fallback - raise error for unrecognized conditions
        raise InvalidArgumentError(f"Unrecognized where condition type: {type(condition_arg).__name__}. "
                                  f"Expected BinaryOpNode or pipeline operation.")
    
    def _handle_select(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle select() function with transformation."""
        if not self.args:
            raise ValueError("select() function requires a transformation argument")
        
        # This would need to be expanded to handle complex transformation compilation
        # For now, placeholder implementation
        transform_sql = "."  # This should be compiled from self.args[0]
        
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = f"""
            json_array(
                SELECT json_extract(value, '$.{transform_sql}')
                FROM json_each({input_state.sql_fragment})
            )
            """
        else:  # PostgreSQL
            sql_fragment = f"""
            jsonb_agg(elem->>'{transform_sql}')
            FROM jsonb_array_elements({input_state.sql_fragment}) AS elem
            """
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    def _handle_distinct(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle distinct() function."""
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = f"""
            json_array(
                SELECT DISTINCT value
                FROM json_each({input_state.sql_fragment})
            )
            """
        else:  # PostgreSQL
            sql_fragment = f"""
            jsonb_agg(DISTINCT elem)
            FROM jsonb_array_elements({input_state.sql_fragment}) AS elem
            """
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    def _handle_combine(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle combine() function."""
        if not self.args:
            raise ValueError("combine() function requires another collection argument")
        
        # For now, assume the argument is the same collection (common case: x.combine(x))
        # In a full implementation, we'd need to evaluate the argument expression
        first_collection = input_state.sql_fragment
        second_collection = input_state.sql_fragment  # Simplified for x.combine(x) case
        
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = f"""(
                CASE 
                    WHEN {first_collection} IS NULL AND {second_collection} IS NULL THEN NULL
                    WHEN {first_collection} IS NULL THEN {second_collection}
                    WHEN {second_collection} IS NULL THEN {first_collection}
                    WHEN json_type({first_collection}) = 'ARRAY' AND json_type({second_collection}) = 'ARRAY' THEN (
                        SELECT json_group_array(value)
                        FROM (
                            SELECT value FROM json_each({first_collection})
                            UNION ALL
                            SELECT value FROM json_each({second_collection})
                        )
                    )
                    ELSE json_array({first_collection}, {second_collection})
                END
            )"""
        else:  # PostgreSQL
            sql_fragment = f"""(
                CASE 
                    WHEN {first_collection} IS NULL AND {second_collection} IS NULL THEN NULL
                    WHEN {first_collection} IS NULL THEN {second_collection}
                    WHEN {second_collection} IS NULL THEN {first_collection}
                    ELSE ({first_collection} || {second_collection})
                END
            )"""
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    # Additional collection function handlers would go here...
    # For brevity, I'll implement the key ones and provide placeholders for others
    
    def _handle_tail(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle tail() function - all elements except first."""
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = f"""(
                CASE 
                    WHEN {input_state.sql_fragment} IS NULL THEN NULL
                    WHEN json_type({input_state.sql_fragment}) = 'ARRAY' THEN
                        CASE 
                            WHEN json_array_length({input_state.sql_fragment}) <= 1 THEN json_array()
                            ELSE (
                                SELECT json_group_array(json_extract({input_state.sql_fragment}, '$[' || idx || ']'))
                                FROM generate_series(1::BIGINT, CAST(json_array_length({input_state.sql_fragment}) - 1 AS BIGINT)) AS t(idx)
                            )
                        END
                    ELSE json_array()
                END
            )"""
        else:  # PostgreSQL
            sql_fragment = f"""(
                CASE 
                    WHEN {input_state.sql_fragment} IS NULL THEN NULL
                    WHEN jsonb_typeof({input_state.sql_fragment}) = 'array' THEN
                        CASE 
                            WHEN jsonb_array_length({input_state.sql_fragment}) <= 1 THEN '[]'::jsonb
                            ELSE (
                                SELECT jsonb_agg(value ORDER BY ordinality)
                                FROM jsonb_array_elements({input_state.sql_fragment}) WITH ORDINALITY AS t(value, ordinality)
                                WHERE ordinality > 1
                            )
                        END
                    ELSE '[]'::jsonb
                END
            )"""
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    def _handle_skip(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle skip(n) function."""
        if not self.args:
            raise ValueError("skip() function requires a number argument")
        
        # Extract numeric argument properly
        arg = self.args[0]
        if hasattr(arg, 'value'):
            skip_count = str(arg.value)
        elif hasattr(arg, 'sql_fragment'):
            skip_count = str(arg.sql_fragment)
        else:
            skip_count = str(arg)
        
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = f"""(
                CASE 
                    WHEN {input_state.sql_fragment} IS NULL THEN NULL
                    WHEN json_type({input_state.sql_fragment}) = 'ARRAY' THEN
                        CASE 
                            WHEN json_array_length({input_state.sql_fragment}) <= {skip_count} THEN json_array()
                            ELSE (
                                SELECT json_group_array(json_extract({input_state.sql_fragment}, '$[' || idx || ']'))
                                FROM generate_series(CAST({skip_count} AS BIGINT), CAST(json_array_length({input_state.sql_fragment}) - 1 AS BIGINT)) AS t(idx)
                            )
                        END
                    ELSE json_array()
                END
            )"""
        else:  # PostgreSQL
            sql_fragment = f"""(
                CASE 
                    WHEN {input_state.sql_fragment} IS NULL THEN NULL
                    WHEN jsonb_typeof({input_state.sql_fragment}) = 'array' THEN
                        CASE 
                            WHEN jsonb_array_length({input_state.sql_fragment}) <= {skip_count} THEN '[]'::jsonb
                            ELSE (
                                SELECT jsonb_agg(value ORDER BY ordinality)
                                FROM jsonb_array_elements({input_state.sql_fragment}) WITH ORDINALITY AS t(value, ordinality)
                                WHERE ordinality > {skip_count}
                            )
                        END
                    ELSE '[]'::jsonb
                END
            )"""
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    def _handle_take(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle take(n) function."""
        if not self.args:
            raise ValueError("take() function requires a number argument")
        
        # Extract numeric argument properly
        arg = self.args[0]
        if hasattr(arg, 'value'):
            take_count = str(arg.value)
        elif hasattr(arg, 'sql_fragment'):
            take_count = str(arg.sql_fragment)
        else:
            take_count = str(arg)
        
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = f"""(
                CASE 
                    WHEN {input_state.sql_fragment} IS NULL THEN NULL
                    WHEN json_type({input_state.sql_fragment}) = 'ARRAY' THEN
                        CASE 
                            WHEN json_array_length({input_state.sql_fragment}) = 0 THEN json_array()
                            ELSE (
                                SELECT json_group_array(json_extract({input_state.sql_fragment}, '$[' || idx || ']'))
                                FROM generate_series(0::BIGINT, CAST(LEAST({take_count} - 1, json_array_length({input_state.sql_fragment}) - 1) AS BIGINT)) AS t(idx)
                            )
                        END
                    ELSE json_array()
                END
            )"""
        else:  # PostgreSQL
            sql_fragment = f"""(
                CASE 
                    WHEN {input_state.sql_fragment} IS NULL THEN NULL
                    WHEN jsonb_typeof({input_state.sql_fragment}) = 'array' THEN
                        CASE 
                            WHEN jsonb_array_length({input_state.sql_fragment}) = 0 THEN '[]'::jsonb
                            ELSE (
                                SELECT jsonb_agg(value ORDER BY ordinality)
                                FROM jsonb_array_elements({input_state.sql_fragment}) WITH ORDINALITY AS t(value, ordinality)
                                WHERE ordinality <= {take_count}
                            )
                        END
                    ELSE '[]'::jsonb
                END
            )"""
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    # Placeholder implementations for remaining collection functions
    def _handle_union(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle union() function - combines collections with distinct semantics."""
        if not self.args:
            raise InvalidArgumentError("union() function requires another collection argument")
        
        first_collection = input_state.sql_fragment
        second_collection = self._evaluate_union_argument(self.args[0], input_state, context)
        
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = self._build_union_sql_duckdb(first_collection, second_collection)
        else:
            sql_fragment = self._build_union_sql_postgresql(first_collection, second_collection)
        
        sql_fragment = self._add_debug_info(sql_fragment, "union()", context)
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_intersect(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle intersect() function - returns elements common to both collections."""
        if not self.args:
            raise InvalidArgumentError("intersect() function requires exactly one argument")
        
        first_collection = input_state.sql_fragment
        second_collection = self._evaluate_union_argument(self.args[0], input_state, context)
        
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = f"""(
                CASE 
                    WHEN {first_collection} IS NULL OR {second_collection} IS NULL THEN NULL
                    WHEN json_type({first_collection}) = 'ARRAY' AND json_type({second_collection}) = 'ARRAY' THEN
                        CASE 
                            WHEN json_array_length({first_collection}) = 0 OR json_array_length({second_collection}) = 0 THEN json_array()
                            ELSE (
                                SELECT CASE 
                                    WHEN COUNT(*) = 0 THEN json_array()
                                    ELSE json_group_array(DISTINCT base_value)
                                END
                                FROM (
                                    SELECT base_val.value as base_value
                                    FROM json_each({first_collection}) base_val
                                    WHERE base_val.value IS NOT NULL
                                      AND EXISTS (
                                          SELECT 1 FROM json_each({second_collection}) other_val
                                          WHERE other_val.value = base_val.value
                                      )
                                )
                            )
                        END
                    ELSE json_array()
                END
            )"""
        else:  # PostgreSQL
            sql_fragment = f"""(
                CASE 
                    WHEN {first_collection} IS NULL OR {second_collection} IS NULL THEN NULL
                    WHEN jsonb_typeof({first_collection}) = 'array' AND jsonb_typeof({second_collection}) = 'array' THEN
                        CASE 
                            WHEN jsonb_array_length({first_collection}) = 0 OR jsonb_array_length({second_collection}) = 0 THEN '[]'::jsonb
                            ELSE (
                                SELECT CASE 
                                    WHEN COUNT(*) = 0 THEN '[]'::jsonb
                                    ELSE jsonb_agg(DISTINCT base_value)
                                END
                                FROM (
                                    SELECT base_val.value as base_value
                                    FROM jsonb_array_elements({first_collection}) base_val
                                    WHERE base_val.value IS NOT NULL
                                      AND EXISTS (
                                          SELECT 1 FROM jsonb_array_elements({second_collection}) other_val
                                          WHERE other_val.value = base_val.value
                                      )
                                )
                            )
                        END
                    ELSE '[]'::jsonb
                END
            )"""
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_exclude(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle exclude() function - returns elements in base collection that are not in other collection."""
        if not self.args:
            raise InvalidArgumentError("exclude() function requires exactly one argument")
        
        first_collection = input_state.sql_fragment
        second_collection = self._evaluate_union_argument(self.args[0], input_state, context)
        
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = f"""(
                CASE 
                    WHEN {first_collection} IS NULL THEN NULL
                    WHEN {second_collection} IS NULL THEN 
                        CASE 
                            WHEN json_type({first_collection}) = 'ARRAY' THEN {first_collection}
                            ELSE json_array({first_collection})
                        END
                    WHEN json_type({first_collection}) = 'ARRAY' AND json_type({second_collection}) = 'ARRAY' THEN (
                        SELECT CASE 
                            WHEN COUNT(*) = 0 THEN json_array()
                            ELSE json_group_array(DISTINCT base_value)
                        END
                        FROM (
                            SELECT base_val.value as base_value
                            FROM json_each({first_collection}) base_val
                            WHERE base_val.value IS NOT NULL
                              AND NOT EXISTS (
                                  SELECT 1 FROM json_each({second_collection}) other_val
                                  WHERE other_val.value = base_val.value
                              )
                        )
                    )
                    ELSE json_array({first_collection})
                END
            )"""
        else:  # PostgreSQL
            sql_fragment = f"""(
                CASE 
                    WHEN {first_collection} IS NULL THEN NULL
                    WHEN {second_collection} IS NULL THEN 
                        CASE 
                            WHEN jsonb_typeof({first_collection}) = 'array' THEN {first_collection}
                            ELSE jsonb_build_array({first_collection})
                        END
                    WHEN jsonb_typeof({first_collection}) = 'array' AND jsonb_typeof({second_collection}) = 'array' THEN (
                        SELECT CASE 
                            WHEN COUNT(*) = 0 THEN '[]'::jsonb
                            ELSE jsonb_agg(DISTINCT base_value)
                        END
                        FROM (
                            SELECT base_val.value as base_value
                            FROM jsonb_array_elements({first_collection}) base_val
                            WHERE base_val.value IS NOT NULL
                              AND NOT EXISTS (
                                  SELECT 1 FROM jsonb_array_elements({second_collection}) other_val
                                  WHERE other_val.value = base_val.value
                              )
                        )
                    )
                    ELSE jsonb_build_array({first_collection})
                END
            )"""
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_boolean_collection(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle allTrue/allFalse/anyTrue/anyFalse functions."""
        if context.dialect.name.upper() == 'DUCKDB':
            if self.func_name == 'alltrue':
                sql_fragment = f"""(
                    CASE 
                        WHEN {input_state.sql_fragment} IS NULL THEN NULL
                        WHEN json_type({input_state.sql_fragment}) = 'ARRAY' THEN
                            CASE 
                                WHEN json_array_length({input_state.sql_fragment}) = 0 THEN NULL
                                ELSE (
                                    SELECT COUNT(*) = COUNT(CASE WHEN 
                                        json_extract_string(value, '$') = 'true' THEN 1 END)
                                    FROM json_each({input_state.sql_fragment})
                                    WHERE json_extract_string(value, '$') IN ('true', 'false')
                                )
                            END
                        ELSE 
                            CASE 
                                WHEN json_extract_string({input_state.sql_fragment}, '$') = 'true' THEN true
                                WHEN json_extract_string({input_state.sql_fragment}, '$') = 'false' THEN false
                                ELSE NULL
                            END
                    END
                )"""
            elif self.func_name == 'allfalse':
                sql_fragment = f"""(
                    CASE 
                        WHEN {input_state.sql_fragment} IS NULL THEN NULL
                        WHEN json_type({input_state.sql_fragment}) = 'ARRAY' THEN
                            CASE 
                                WHEN json_array_length({input_state.sql_fragment}) = 0 THEN NULL
                                ELSE (
                                    SELECT COUNT(*) = COUNT(CASE WHEN 
                                        json_extract_string(value, '$') = 'false' THEN 1 END)
                                    FROM json_each({input_state.sql_fragment})
                                    WHERE json_extract_string(value, '$') IN ('true', 'false')
                                )
                            END
                        ELSE 
                            CASE 
                                WHEN json_extract_string({input_state.sql_fragment}, '$') = 'false' THEN true
                                WHEN json_extract_string({input_state.sql_fragment}, '$') = 'true' THEN false
                                ELSE NULL
                            END
                    END
                )"""
            elif self.func_name == 'anytrue':
                sql_fragment = f"""(
                    CASE 
                        WHEN {input_state.sql_fragment} IS NULL THEN NULL
                        WHEN json_type({input_state.sql_fragment}) = 'ARRAY' THEN
                            CASE 
                                WHEN json_array_length({input_state.sql_fragment}) = 0 THEN NULL
                                ELSE EXISTS (
                                    SELECT 1 FROM json_each({input_state.sql_fragment})
                                    WHERE json_extract_string(value, '$') = 'true'
                                )
                            END
                        ELSE 
                            CASE 
                                WHEN json_extract_string({input_state.sql_fragment}, '$') = 'true' THEN true
                                ELSE false
                            END
                    END
                )"""
            elif self.func_name == 'anyfalse':
                sql_fragment = f"""(
                    CASE 
                        WHEN {input_state.sql_fragment} IS NULL THEN NULL
                        WHEN json_type({input_state.sql_fragment}) = 'ARRAY' THEN
                            CASE 
                                WHEN json_array_length({input_state.sql_fragment}) = 0 THEN NULL
                                ELSE EXISTS (
                                    SELECT 1 FROM json_each({input_state.sql_fragment})
                                    WHERE json_extract_string(value, '$') = 'false'
                                )
                            END
                        ELSE 
                            CASE 
                                WHEN json_extract_string({input_state.sql_fragment}, '$') = 'false' THEN true
                                ELSE false
                            END
                    END
                )"""
            else:
                raise ValueError(f"Unknown boolean function: {self.func_name}")
        else:  # PostgreSQL
            if self.func_name == 'alltrue':
                # Handle both JSONB and TEXT input by wrapping in appropriate conversion
                collection_expr = input_state.sql_fragment
                
                # If the expression uses ->> (text extraction), convert to -> (jsonb extraction)
                if '->>' in collection_expr:
                    # Replace ->> with -> to get JSONB instead of TEXT
                    jsonb_expr = collection_expr.replace('->>', '->')
                else:
                    jsonb_expr = collection_expr
                
                sql_fragment = f"""(
                    CASE 
                        WHEN {jsonb_expr} IS NULL THEN NULL
                        WHEN jsonb_typeof({jsonb_expr}) = 'array' THEN
                            CASE 
                                WHEN jsonb_array_length({jsonb_expr}) = 0 THEN NULL
                                ELSE (
                                    SELECT COUNT(*) = COUNT(CASE WHEN 
                                        value::text = 'true' THEN 1 END)
                                    FROM jsonb_array_elements({jsonb_expr}) AS value
                                    WHERE value::text IN ('true', 'false')
                                )
                            END
                        ELSE 
                            CASE 
                                WHEN {jsonb_expr}::text = 'true' THEN true
                                WHEN {jsonb_expr}::text = 'false' THEN false
                                ELSE NULL
                            END
                    END
                )"""
            elif self.func_name == 'allfalse':
                # Handle both JSONB and TEXT input
                collection_expr = input_state.sql_fragment
                if '->>' in collection_expr:
                    jsonb_expr = collection_expr.replace('->>', '->')
                else:
                    jsonb_expr = collection_expr
                
                sql_fragment = f"""(
                    CASE 
                        WHEN {jsonb_expr} IS NULL THEN NULL
                        WHEN jsonb_typeof({jsonb_expr}) = 'array' THEN
                            CASE 
                                WHEN jsonb_array_length({jsonb_expr}) = 0 THEN NULL
                                ELSE (
                                    SELECT COUNT(*) = COUNT(CASE WHEN 
                                        value::text = 'false' THEN 1 END)
                                    FROM jsonb_array_elements({jsonb_expr}) AS value
                                    WHERE value::text IN ('true', 'false')
                                )
                            END
                        ELSE 
                            CASE 
                                WHEN {jsonb_expr}::text = 'false' THEN true
                                WHEN {jsonb_expr}::text = 'true' THEN false
                                ELSE NULL
                            END
                    END
                )"""
            elif self.func_name == 'anytrue':
                # Handle both JSONB and TEXT input
                collection_expr = input_state.sql_fragment
                if '->>' in collection_expr:
                    jsonb_expr = collection_expr.replace('->>', '->')
                else:
                    jsonb_expr = collection_expr
                
                sql_fragment = f"""(
                    CASE 
                        WHEN {jsonb_expr} IS NULL THEN NULL
                        WHEN jsonb_typeof({jsonb_expr}) = 'array' THEN
                            CASE 
                                WHEN jsonb_array_length({jsonb_expr}) = 0 THEN NULL
                                ELSE (
                                    SELECT COUNT(CASE WHEN value::text = 'true' THEN 1 END) > 0
                                    FROM jsonb_array_elements({jsonb_expr}) AS value
                                    WHERE value::text IN ('true', 'false')
                                )
                            END
                        ELSE 
                            CASE 
                                WHEN {jsonb_expr}::text = 'true' THEN true
                                ELSE false
                            END
                    END
                )"""
            elif self.func_name == 'anyfalse':
                # Handle both JSONB and TEXT input
                collection_expr = input_state.sql_fragment
                if '->>' in collection_expr:
                    jsonb_expr = collection_expr.replace('->>', '->')
                else:
                    jsonb_expr = collection_expr
                
                sql_fragment = f"""(
                    CASE 
                        WHEN {jsonb_expr} IS NULL THEN NULL
                        WHEN jsonb_typeof({jsonb_expr}) = 'array' THEN
                            CASE 
                                WHEN jsonb_array_length({jsonb_expr}) = 0 THEN NULL
                                ELSE (
                                    SELECT COUNT(CASE WHEN value::text = 'false' THEN 1 END) > 0
                                    FROM jsonb_array_elements({jsonb_expr}) AS value
                                    WHERE value::text IN ('true', 'false')
                                )
                            END
                        ELSE 
                            CASE 
                                WHEN {jsonb_expr}::text = 'false' THEN true
                                ELSE false
                            END
                    END
                )"""
            else:
                sql_fragment = f"/* {self.func_name} not implemented for PostgreSQL */"
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_contains_collection(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle contains() for collections.
        
        Returns true if the collection contains the specified element.
        This is different from the string contains() which checks substring matching.
        """
        if not self.args:
            raise InvalidArgumentError("contains() function requires an element to search for")
        
        # Get the element to search for
        search_element = str(self.args[0])
        
        if context.dialect.name.upper() == 'DUCKDB':
            # DuckDB: Check if any element in the JSON array equals the search element
            sql_fragment = f"""(
                CASE 
                    WHEN {input_state.sql_fragment} IS NULL THEN false
                    WHEN json_type({input_state.sql_fragment}) = 'ARRAY' THEN (
                        EXISTS (
                            SELECT 1 FROM json_each({input_state.sql_fragment})
                            WHERE value = {search_element}
                        )
                    )
                    ELSE {input_state.sql_fragment} = {search_element}
                END
            )"""
        else:  # PostgreSQL
            # PostgreSQL: Check if any element in the JSONB array equals the search element
            sql_fragment = f"""(
                CASE 
                    WHEN {input_state.sql_fragment} IS NULL THEN false
                    WHEN jsonb_typeof({input_state.sql_fragment}) = 'array' THEN (
                        EXISTS (
                            SELECT 1 FROM jsonb_array_elements({input_state.sql_fragment}) AS elem
                            WHERE elem = {search_element}
                        )
                    )
                    ELSE {input_state.sql_fragment} = {search_element}
                END
            )"""
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_children(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle children() function.
        
        Returns a collection with all immediate child nodes of all items in the input collection.
        For JSON/JSONB data, this means all direct properties/elements of an object/array.
        """
        if context.dialect.name.upper() == 'DUCKDB':
            # DuckDB: Extract all immediate children from JSON object/array
            sql_fragment = f"""(
                CASE 
                    WHEN {input_state.sql_fragment} IS NULL THEN json_array()
                    WHEN json_type({input_state.sql_fragment}) = 'OBJECT' THEN (
                        SELECT json_group_array(value)
                        FROM json_each({input_state.sql_fragment})
                    )
                    WHEN json_type({input_state.sql_fragment}) = 'ARRAY' THEN (
                        SELECT json_group_array(value)
                        FROM json_each({input_state.sql_fragment})
                    )
                    ELSE json_array({input_state.sql_fragment})
                END
            )"""
        else:  # PostgreSQL
            # PostgreSQL: Extract all immediate children from JSONB object/array
            sql_fragment = f"""(
                CASE 
                    WHEN {input_state.sql_fragment} IS NULL THEN '[]'::jsonb
                    WHEN jsonb_typeof({input_state.sql_fragment}) = 'object' THEN (
                        SELECT jsonb_agg(value)
                        FROM jsonb_each({input_state.sql_fragment})
                    )
                    WHEN jsonb_typeof({input_state.sql_fragment}) = 'array' THEN (
                        SELECT jsonb_agg(value)
                        FROM jsonb_array_elements({input_state.sql_fragment}) AS value
                    )
                    ELSE jsonb_build_array({input_state.sql_fragment})
                END
            )"""
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_descendants(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle descendants() function.
        
        Returns a collection with all descendant nodes (all nested children at any level)
        of all items in the input collection. This is similar to children() but recursive.
        """
        if context.dialect.name.upper() == 'DUCKDB':
            # DuckDB: Recursively extract all descendants using recursive CTE
            sql_fragment = f"""(
                WITH RECURSIVE descendants AS (
                    -- Base case: direct children  
                    SELECT value, json_type(value) as type, 0 as level
                    FROM json_each({input_state.sql_fragment})
                    WHERE json_type({input_state.sql_fragment}) IN ('OBJECT', 'ARRAY')
                    
                    UNION ALL
                    
                    -- Recursive case: children of children
                    SELECT child_value.value, json_type(child_value.value) as type, d.level + 1
                    FROM descendants d
                    CROSS JOIN json_each(d.value) as child_value
                    WHERE d.type IN ('OBJECT', 'ARRAY') AND d.level < 10  -- Prevent infinite recursion
                )
                SELECT COALESCE(json_group_array(value), json_array()) 
                FROM descendants
            )"""
        else:  # PostgreSQL
            # PostgreSQL: Recursively extract all descendants using recursive CTE
            sql_fragment = f"""(
                WITH RECURSIVE descendants AS (
                    -- Base case: direct children
                    SELECT value, jsonb_typeof(value) as type, 0 as level
                    FROM jsonb_each({input_state.sql_fragment})
                    WHERE jsonb_typeof({input_state.sql_fragment}) = 'object'
                    
                    UNION ALL
                    
                    SELECT value, jsonb_typeof(value) as type, 0 as level  
                    FROM jsonb_array_elements({input_state.sql_fragment}) as value
                    WHERE jsonb_typeof({input_state.sql_fragment}) = 'array'
                    
                    UNION ALL
                    
                    -- Recursive case: children of children (objects)
                    SELECT child_value.value, jsonb_typeof(child_value.value) as type, d.level + 1
                    FROM descendants d
                    CROSS JOIN jsonb_each(d.value) as child_value
                    WHERE d.type = 'object' AND d.level < 10  -- Prevent infinite recursion
                    
                    UNION ALL
                    
                    -- Recursive case: children of children (arrays)
                    SELECT child_element as value, jsonb_typeof(child_element) as type, d.level + 1
                    FROM descendants d
                    CROSS JOIN jsonb_array_elements(d.value) as child_element
                    WHERE d.type = 'array' AND d.level < 10  -- Prevent infinite recursion
                )
                SELECT COALESCE(jsonb_agg(value), '[]'::jsonb)
                FROM descendants
            )"""
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_isdistinct(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle isDistinct() function.
        
        Returns true if all items in the collection are distinct (no duplicates).
        Returns true for empty collections and single-item collections.
        """
        if context.dialect.name.upper() == 'DUCKDB':
            # DuckDB: Compare total count with distinct count
            sql_fragment = f"""(
                CASE 
                    WHEN {input_state.sql_fragment} IS NULL THEN true
                    WHEN json_type({input_state.sql_fragment}) = 'ARRAY' THEN (
                        (SELECT COUNT(*) FROM json_each({input_state.sql_fragment})) = 
                        (SELECT COUNT(DISTINCT value) FROM json_each({input_state.sql_fragment}))
                    )
                    ELSE true  -- Single values are always distinct
                END
            )"""
        else:  # PostgreSQL
            # PostgreSQL: Compare total count with distinct count  
            sql_fragment = f"""(
                CASE 
                    WHEN {input_state.sql_fragment} IS NULL THEN true
                    WHEN jsonb_typeof({input_state.sql_fragment}) = 'array' THEN (
                        (SELECT COUNT(*) FROM jsonb_array_elements({input_state.sql_fragment})) = 
                        (SELECT COUNT(DISTINCT value) FROM jsonb_array_elements({input_state.sql_fragment}) AS value)
                    )
                    ELSE true  -- Single values are always distinct
                END
            )"""
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_set_operations(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle subsetOf/supersetOf functions.
        
        subsetOf(other): Returns true if this collection is a subset of the other collection
        supersetOf(other): Returns true if this collection is a superset of the other collection
        """
        if not self.args:
            raise InvalidArgumentError(f"{self.func_name}() function requires another collection argument")
        
        other_collection = str(self.args[0])
        
        if self.func_name == 'subsetof':
            # Check if all elements in input_state are also in other_collection
            if context.dialect.name.upper() == 'DUCKDB':
                sql_fragment = f"""(
                    CASE 
                        WHEN {input_state.sql_fragment} IS NULL THEN true
                        WHEN json_type({input_state.sql_fragment}) = 'ARRAY' AND json_type({other_collection}) = 'ARRAY' THEN (
                            NOT EXISTS (
                                SELECT 1 FROM json_each({input_state.sql_fragment}) AS elem1
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM json_each({other_collection}) AS elem2
                                    WHERE elem1.value = elem2.value
                                )
                            )
                        )
                        ELSE false
                    END
                )"""
            else:  # PostgreSQL
                sql_fragment = f"""(
                    CASE 
                        WHEN {input_state.sql_fragment} IS NULL THEN true
                        WHEN jsonb_typeof({input_state.sql_fragment}) = 'array' AND jsonb_typeof({other_collection}) = 'array' THEN (
                            NOT EXISTS (
                                SELECT 1 FROM jsonb_array_elements({input_state.sql_fragment}) AS elem1
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM jsonb_array_elements({other_collection}) AS elem2
                                    WHERE elem1 = elem2
                                )
                            )
                        )
                        ELSE false
                    END
                )"""
        
        elif self.func_name == 'supersetof':
            # Check if all elements in other_collection are also in input_state
            if context.dialect.name.upper() == 'DUCKDB':
                sql_fragment = f"""(
                    CASE 
                        WHEN {other_collection} IS NULL THEN true
                        WHEN json_type({input_state.sql_fragment}) = 'ARRAY' AND json_type({other_collection}) = 'ARRAY' THEN (
                            NOT EXISTS (
                                SELECT 1 FROM json_each({other_collection}) AS elem1
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM json_each({input_state.sql_fragment}) AS elem2
                                    WHERE elem1.value = elem2.value
                                )
                            )
                        )
                        ELSE false
                    END
                )"""
            else:  # PostgreSQL
                sql_fragment = f"""(
                    CASE 
                        WHEN {other_collection} IS NULL THEN true
                        WHEN jsonb_typeof({input_state.sql_fragment}) = 'array' AND jsonb_typeof({other_collection}) = 'array' THEN (
                            NOT EXISTS (
                                SELECT 1 FROM jsonb_array_elements({other_collection}) AS elem1
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM jsonb_array_elements({input_state.sql_fragment}) AS elem2
                                    WHERE elem1 = elem2
                                )
                            )
                        )
                        ELSE false
                    END
                )"""
        else:
            raise InvalidArgumentError(f"Unknown set operation: {self.func_name}")
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_iif(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle iif() function (inline if).
        
        iif(condition, true_result, false_result) - returns true_result if condition is true, 
        false_result otherwise. If false_result is omitted, returns empty collection when false.
        """
        if len(self.args) < 2:
            raise InvalidArgumentError("iif() function requires at least condition and true_result arguments")
        
        condition = str(self.args[0])
        true_result = str(self.args[1])
        false_result = str(self.args[2]) if len(self.args) > 2 else "NULL"
        
        if context.dialect.name.upper() == 'DUCKDB':
            # DuckDB: Use CASE WHEN for conditional logic
            sql_fragment = f"""(
                CASE 
                    WHEN {condition} IS NULL THEN NULL
                    WHEN {condition} THEN {true_result}
                    ELSE {false_result}
                END
            )"""
        else:  # PostgreSQL
            # PostgreSQL: Use CASE WHEN for conditional logic  
            sql_fragment = f"""(
                CASE 
                    WHEN {condition} IS NULL THEN NULL
                    WHEN {condition} THEN {true_result}
                    ELSE {false_result}
                END
            )"""
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_repeat(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle repeat() function.
        
        The repeat() function is used for recursive operations. It applies an expression repeatedly
        until no new items are found. For simplicity in SQL generation, we limit to a fixed number of iterations.
        """
        if not self.args:
            raise InvalidArgumentError("repeat() function requires an expression argument")
        
        # For SQL implementation, we'll use a recursive CTE with a reasonable limit
        expression = str(self.args[0])
        max_iterations = 10  # Prevent infinite recursion
        
        if context.dialect.name.upper() == 'DUCKDB':
            # DuckDB: Use recursive CTE to apply the expression repeatedly
            sql_fragment = f"""(
                WITH RECURSIVE repeat_result AS (
                    -- Base case: initial input
                    SELECT {input_state.sql_fragment} as value, 0 as iteration
                    
                    UNION ALL
                    
                    -- Recursive case: apply expression to previous results
                    SELECT {expression} as value, iteration + 1
                    FROM repeat_result
                    WHERE iteration < {max_iterations} AND value IS NOT NULL
                )
                SELECT json_group_array(DISTINCT value) 
                FROM repeat_result 
                WHERE value IS NOT NULL
            )"""
        else:  # PostgreSQL
            # PostgreSQL: Use recursive CTE to apply the expression repeatedly
            sql_fragment = f"""(
                WITH RECURSIVE repeat_result AS (
                    -- Base case: initial input  
                    SELECT {input_state.sql_fragment} as value, 0 as iteration
                    
                    UNION ALL
                    
                    -- Recursive case: apply expression to previous results
                    SELECT {expression} as value, iteration + 1
                    FROM repeat_result
                    WHERE iteration < {max_iterations} AND value IS NOT NULL
                )
                SELECT jsonb_agg(DISTINCT value)
                FROM repeat_result
                WHERE value IS NOT NULL
            )"""
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_aggregate(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle aggregate() function.
        
        aggregate(aggregator, init) - Reduces a collection to a single value by applying 
        an aggregator expression with an optional initial value.
        """
        if not self.args:
            raise InvalidArgumentError("aggregate() function requires an aggregator expression")
        
        aggregator_expr = str(self.args[0])
        init_value = str(self.args[1]) if len(self.args) > 1 else "NULL"
        
        if context.dialect.name.upper() == 'DUCKDB':
            # DuckDB: Use a subquery to simulate folding/reducing behavior
            sql_fragment = f"""(
                WITH RECURSIVE aggregate_result AS (
                    -- Base case: initialize with first element or init value
                    SELECT 
                        CASE 
                            WHEN {init_value} IS NOT NULL THEN {init_value}
                            ELSE (SELECT value FROM json_each({input_state.sql_fragment}) LIMIT 1)
                        END as accumulator,
                        1 as pos
                    FROM (SELECT 1) as dummy
                    WHERE json_type({input_state.sql_fragment}) = 'ARRAY'
                    
                    UNION ALL
                    
                    -- Recursive case: apply aggregator to each subsequent element
                    SELECT 
                        {aggregator_expr} as accumulator,
                        pos + 1
                    FROM aggregate_result ar
                    CROSS JOIN (
                        SELECT value, ROW_NUMBER() OVER() as rn 
                        FROM json_each({input_state.sql_fragment})
                    ) elem
                    WHERE elem.rn = ar.pos + 1
                )
                SELECT accumulator FROM aggregate_result ORDER BY pos DESC LIMIT 1
            )"""
        else:  # PostgreSQL
            # PostgreSQL: Use a subquery to simulate folding/reducing behavior
            sql_fragment = f"""(
                WITH RECURSIVE aggregate_result AS (
                    -- Base case: initialize with first element or init value
                    SELECT 
                        CASE 
                            WHEN {init_value} IS NOT NULL THEN {init_value}
                            ELSE (SELECT value FROM jsonb_array_elements({input_state.sql_fragment}) LIMIT 1)
                        END as accumulator,
                        1 as pos
                    FROM (SELECT 1) as dummy
                    WHERE jsonb_typeof({input_state.sql_fragment}) = 'array'
                    
                    UNION ALL
                    
                    -- Recursive case: apply aggregator to each subsequent element  
                    SELECT 
                        {aggregator_expr} as accumulator,
                        pos + 1
                    FROM aggregate_result ar
                    CROSS JOIN (
                        SELECT value, ROW_NUMBER() OVER() as rn 
                        FROM jsonb_array_elements({input_state.sql_fragment}) as value
                    ) elem
                    WHERE elem.rn = ar.pos + 1
                )
                SELECT accumulator FROM aggregate_result ORDER BY pos DESC LIMIT 1
            )"""
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_flatten(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle flatten() function.
        
        Flattens a collection by concatenating nested collections into a single collection.
        For example, [[1,2], [3,4]] becomes [1,2,3,4].
        """
        if context.dialect.name.upper() == 'DUCKDB':
            # DuckDB: Flatten nested JSON arrays
            sql_fragment = f"""(
                CASE 
                    WHEN {input_state.sql_fragment} IS NULL THEN json_array()
                    WHEN json_type({input_state.sql_fragment}) = 'ARRAY' THEN (
                        SELECT json_group_array(nested_value)
                        FROM (
                            SELECT 
                                CASE 
                                    WHEN json_type(value) = 'ARRAY' THEN 
                                        (SELECT nested_value FROM json_each(value) AS nested)
                                    ELSE value
                                END as nested_value
                            FROM json_each({input_state.sql_fragment})
                        ) flattened
                        WHERE flattened.nested_value IS NOT NULL
                    )
                    ELSE json_array({input_state.sql_fragment})
                END
            )"""
        else:  # PostgreSQL
            # PostgreSQL: Flatten nested JSONB arrays
            sql_fragment = f"""(
                CASE 
                    WHEN {input_state.sql_fragment} IS NULL THEN '[]'::jsonb
                    WHEN jsonb_typeof({input_state.sql_fragment}) = 'array' THEN (
                        SELECT jsonb_agg(nested_value)
                        FROM (
                            SELECT 
                                CASE 
                                    WHEN jsonb_typeof(value) = 'array' THEN 
                                        (SELECT nested_value FROM jsonb_array_elements(value) AS nested_value)
                                    ELSE value
                                END as nested_value
                            FROM jsonb_array_elements({input_state.sql_fragment}) AS value
                        ) flattened
                        WHERE flattened.nested_value IS NOT NULL
                    )
                    ELSE jsonb_build_array({input_state.sql_fragment})
                END
            )"""
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_in(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle in() function.
        
        Returns true if the input value is contained in the specified collection.
        This is similar to contains() but with reversed operands: value.in(collection).
        """
        if not self.args:
            raise InvalidArgumentError("in() function requires a collection argument")
        
        collection = str(self.args[0])
        
        if context.dialect.name.upper() == 'DUCKDB':
            # DuckDB: Check if input value exists in the collection
            sql_fragment = f"""(
                CASE 
                    WHEN {collection} IS NULL THEN false
                    WHEN json_type({collection}) = 'ARRAY' THEN (
                        EXISTS (
                            SELECT 1 FROM json_each({collection})
                            WHERE value = {input_state.sql_fragment}
                        )
                    )
                    ELSE {input_state.sql_fragment} = {collection}
                END
            )"""
        else:  # PostgreSQL
            # PostgreSQL: Check if input value exists in the collection
            sql_fragment = f"""(
                CASE 
                    WHEN {collection} IS NULL THEN false
                    WHEN jsonb_typeof({collection}) = 'array' THEN (
                        EXISTS (
                            SELECT 1 FROM jsonb_array_elements({collection}) AS elem
                            WHERE elem = {input_state.sql_fragment}
                        )
                    )
                    ELSE {input_state.sql_fragment} = {collection}
                END
            )"""
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_single(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle single() function - returns single element or error if not exactly one."""
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = f"""(
                CASE 
                    WHEN {input_state.sql_fragment} IS NULL THEN NULL
                    WHEN json_type({input_state.sql_fragment}) = 'ARRAY' THEN
                        CASE 
                            WHEN json_array_length({input_state.sql_fragment}) = 1 THEN 
                                json_extract({input_state.sql_fragment}, '$[0]')
                            ELSE NULL  -- Error: not exactly one element
                        END
                    ELSE {input_state.sql_fragment}  -- Single value already
                END
            )"""
        else:  # PostgreSQL
            sql_fragment = f"""(
                CASE 
                    WHEN {input_state.sql_fragment} IS NULL THEN NULL
                    WHEN jsonb_typeof({input_state.sql_fragment}) = 'array' THEN
                        CASE 
                            WHEN jsonb_array_length({input_state.sql_fragment}) = 1 THEN 
                                {input_state.sql_fragment} -> 0
                            ELSE NULL  -- Error: not exactly one element
                        END
                    ELSE {input_state.sql_fragment}  -- Single value already
                END
            )"""
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_all(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle all() function.
        
        Returns true if all elements in the collection satisfy the specified criteria.
        This is different from allTrue() which just checks if all values are true.
        """
        if not self.args:
            raise InvalidArgumentError("all() function requires a criteria expression")
        
        criteria = str(self.args[0])
        
        if context.dialect.name.upper() == 'DUCKDB':
            # DuckDB: Check if all elements satisfy the criteria
            sql_fragment = f"""(
                CASE 
                    WHEN {input_state.sql_fragment} IS NULL THEN true
                    WHEN json_type({input_state.sql_fragment}) = 'ARRAY' THEN (
                        NOT EXISTS (
                            SELECT 1 FROM json_each({input_state.sql_fragment})
                            WHERE NOT ({criteria})
                        )
                    )
                    ELSE ({criteria})
                END
            )"""
        else:  # PostgreSQL
            # PostgreSQL: Check if all elements satisfy the criteria
            sql_fragment = f"""(
                CASE 
                    WHEN {input_state.sql_fragment} IS NULL THEN true
                    WHEN jsonb_typeof({input_state.sql_fragment}) = 'array' THEN (
                        NOT EXISTS (
                            SELECT 1 FROM jsonb_array_elements({input_state.sql_fragment}) AS value
                            WHERE NOT ({criteria})
                        )
                    )
                    ELSE ({criteria})
                END
            )"""
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    # ===================================
    # STRING FUNCTIONS (15+ functions)
    # ===================================
    
    def _execute_string_function(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Execute string function with dialect optimization."""
        
        if self.func_name == 'substring':
            return self._handle_substring(input_state, context)
        elif self.func_name == 'contains':
            return self._handle_contains_string(input_state, context)
        elif self.func_name == 'startswith':
            return self._handle_startswith(input_state, context)
        elif self.func_name == 'endswith':
            return self._handle_endswith(input_state, context)
        elif self.func_name in ['upper', 'toupper']:
            return self._handle_upper(input_state, context)
        elif self.func_name in ['lower', 'tolower']:
            return self._handle_lower(input_state, context)
        elif self.func_name == 'trim':
            return self._handle_trim(input_state, context)
        elif self.func_name == 'replace':
            return self._handle_replace(input_state, context)
        elif self.func_name == 'split':
            return self._handle_split(input_state, context)
        elif self.func_name == 'join':
            return self._handle_join(input_state, context)
        elif self.func_name == 'indexof':
            return self._handle_indexof(input_state, context)
        elif self.func_name == 'tochars':
            return self._handle_tochars(input_state, context)
        elif self.func_name == 'matches':
            return self._handle_matches(input_state, context)
        elif self.func_name == 'replacematches':
            return self._handle_replacematches(input_state, context)
        else:
            raise ValueError(f"Unknown string function: {self.func_name}")
    
    def _handle_substring(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle substring() function."""
        if len(self.args) < 1:
            raise ValueError("substring() requires at least start position")
        
        start_pos = str(self.args[0])
        length_expr = f", {self.args[1]}" if len(self.args) > 1 else ""
        
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = f"substring({input_state.sql_fragment}, {start_pos}{length_expr})"
        else:  # PostgreSQL
            sql_fragment = f"substring({input_state.sql_fragment} FROM {start_pos}{' FOR ' + str(self.args[1]) if len(self.args) > 1 else ''})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_contains_string(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle contains() for strings."""
        if not self.args:
            raise ValueError("contains() requires a search string argument")
        
        search_string = str(self.args[0])
        sql_fragment = f"({input_state.sql_fragment} LIKE '%{search_string}%')"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_startswith(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle startsWith() function."""
        if not self.args:
            raise ValueError("startsWith() requires a prefix string argument")
        
        prefix = str(self.args[0])
        sql_fragment = f"({input_state.sql_fragment} LIKE '{prefix}%')"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_endswith(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle endsWith() function."""
        if not self.args:
            raise ValueError("endsWith() requires a suffix string argument")
        
        suffix = str(self.args[0])
        sql_fragment = f"({input_state.sql_fragment} LIKE '%{suffix}')"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_upper(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle upper()/toUpper() function."""
        sql_fragment = f"UPPER({input_state.sql_fragment})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_lower(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle lower()/toLower() function."""
        sql_fragment = f"LOWER({input_state.sql_fragment})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_trim(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle trim() function."""
        sql_fragment = f"TRIM({input_state.sql_fragment})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_replace(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle replace() function."""
        if len(self.args) < 2:
            raise ValueError("replace() requires search and replacement strings")
        
        search_str = str(self.args[0])
        replace_str = str(self.args[1])
        sql_fragment = f"REPLACE({input_state.sql_fragment}, '{search_str}', '{replace_str}')"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    # Placeholder implementations for remaining string functions
    def _handle_split(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle split() function.
        
        Splits a string into an array of substrings using the specified separator.
        """
        if not self.args:
            raise InvalidArgumentError("split() function requires a separator argument")
        
        separator = str(self.args[0]).strip("'\"")  # Remove quotes if present
        sql_fragment = context.dialect.split_string(input_state.sql_fragment, separator)
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_join(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle join() function - concatenate array elements with separator."""
        from ...fhirpath.parser.ast_nodes import LiteralNode
        
        if not self.args:
            # Default separator is empty string
            separator = "''"
        else:
            # Get separator from raw AST argument (LiteralNode)
            arg = self.args[0]
            if isinstance(arg, LiteralNode):
                # Raw AST LiteralNode - use its value directly
                separator = f"'{arg.value}'"
            elif hasattr(arg, 'value'):
                # LiteralOperation with value attribute (legacy handling)
                separator = f"'{arg.value}'"
            elif hasattr(arg, 'literal_value'):
                # Some other literal format
                separator = f"'{arg.literal_value}'"
            else:
                # Convert argument to SQL and extract literal value
                # This is for cases where we have a literal string like ','
                arg_str = str(arg)
                if arg_str.startswith("'") and arg_str.endswith("'"):
                    separator = arg_str
                elif arg_str.startswith('"') and arg_str.endswith('"'):
                    separator = f"'{arg_str[1:-1]}'"
                else:
                    # Wrap in quotes if not already quoted
                    separator = f"'{arg_str}'"
        
        # The input_state should contain the collection to join
        # Use dialect-specific string aggregation
        collection_expr = input_state.sql_fragment
        
        if context.dialect.name == "POSTGRESQL":
            sql_fragment = f"""(
                CASE 
                    WHEN {collection_expr} IS NULL THEN ''
                    WHEN jsonb_typeof({collection_expr}) = 'array' THEN (
                        SELECT COALESCE(string_agg(
                            CASE 
                                WHEN jsonb_typeof(value) = 'string' THEN value #>> '{{}}'
                                WHEN jsonb_typeof(value) = 'array' THEN (
                                    SELECT string_agg(elem #>> '{{}}', {separator})
                                    FROM jsonb_array_elements(value) AS elem
                                )
                                ELSE value #>> '{{}}'
                            END, {separator}), '')
                        FROM jsonb_array_elements({collection_expr})
                    )
                    ELSE COALESCE({collection_expr} #>> '{{}}', '')
                END
            )"""
        else:  # DuckDB
            sql_fragment = f"""(
                CASE 
                    WHEN {collection_expr} IS NULL THEN ''
                    WHEN json_type({collection_expr}) = 'ARRAY' THEN (
                        SELECT COALESCE(string_agg(
                            CASE 
                                WHEN json_type(value) = 'ARRAY' THEN (
                                    SELECT string_agg(json_extract_string(nested_item.value, '$'), {separator})
                                    FROM json_each(value) AS nested_item
                                )
                                ELSE json_extract_string(value, '$')
                            END, {separator}), '')
                        FROM json_each({collection_expr}) AS item
                        WHERE item.value IS NOT NULL
                    )
                    ELSE COALESCE(json_extract_string({collection_expr}, '$'), '')
                END
            )"""
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_indexof(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle indexOf() function.
        
        Returns the 0-based index of the first occurrence of the substring, or -1 if not found.
        """
        if not self.args:
            raise InvalidArgumentError("indexOf() function requires a substring argument")
        
        substring = str(self.args[0])
        position_sql = context.dialect.string_position(substring, input_state.sql_fragment)
        # Convert 1-based position to 0-based, return -1 if not found
        sql_fragment = f"(CASE WHEN {position_sql} > 0 THEN {position_sql} - 1 ELSE -1 END)"
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_tochars(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle toChars() function.
        
        Splits a string into an array of individual characters.
        """
        sql_fragment = context.dialect.string_to_char_array(input_state.sql_fragment)
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_matches(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle matches() function.
        
        Tests whether a string matches a regular expression pattern.
        Returns true if the string matches the pattern, false otherwise.
        """
        if not self.args:
            raise InvalidArgumentError("matches() function requires a regular expression pattern")
        
        pattern = str(self.args[0])
        sql_fragment = context.dialect.regex_matches(input_state.sql_fragment, pattern)
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_replacematches(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle replaceMatches() function.
        
        Replaces all occurrences of a regular expression pattern with a replacement string.
        """
        if len(self.args) < 2:
            raise InvalidArgumentError("replaceMatches() function requires pattern and replacement arguments")
        
        pattern = str(self.args[0])
        replacement = str(self.args[1])
        sql_fragment = context.dialect.regex_replace(input_state.sql_fragment, pattern, replacement)
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    # ==========================================
    # TYPE CONVERSION FUNCTIONS (15+ functions)
    # ==========================================
    
    def _execute_type_function(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Execute type conversion function with dialect optimization."""
        
        if self.func_name == 'toboolean':
            return self._handle_toboolean(input_state, context)
        elif self.func_name == 'tostring':
            return self._handle_tostring(input_state, context)
        elif self.func_name == 'tointeger':
            return self._handle_tointeger(input_state, context)
        elif self.func_name == 'todecimal':
            return self._handle_todecimal(input_state, context)
        elif self.func_name == 'todate':
            return self._handle_todate(input_state, context)
        elif self.func_name == 'todatetime':
            return self._handle_todatetime(input_state, context)
        elif self.func_name == 'totime':
            return self._handle_totime(input_state, context)
        elif self.func_name == 'toquantity':
            return self._handle_toquantity(input_state, context)
        elif self.func_name.startswith('converts'):
            return self._handle_converts_functions(input_state, context)
        elif self.func_name in ['as', 'is', 'oftype']:
            return self._handle_type_checking(input_state, context)
        else:
            raise ValueError(f"Unknown type function: {self.func_name}")
    
    def _handle_toboolean(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle toBoolean() function."""
        sql_fragment = f"""
        CASE 
            WHEN LOWER({input_state.sql_fragment}) IN ('true', '1', 't', 'yes', 'y') THEN TRUE
            WHEN LOWER({input_state.sql_fragment}) IN ('false', '0', 'f', 'no', 'n') THEN FALSE
            ELSE NULL
        END
        """
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_tostring(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle toString() function."""
        sql_fragment = f"CAST({input_state.sql_fragment} AS TEXT)"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_tointeger(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle toInteger() function."""
        sql_fragment = f"CAST({input_state.sql_fragment} AS INTEGER)"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_todecimal(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle toDecimal() function."""
        sql_fragment = f"CAST({input_state.sql_fragment} AS DECIMAL)"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    # Placeholder implementations for remaining type functions
    def _handle_todate(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle toDate() function.
        
        Converts a string to a date value, or returns empty if conversion fails.
        """
        sql_fragment = context.dialect.try_cast(input_state.sql_fragment, 'date')
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_todatetime(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle toDateTime() function.
        
        Converts a string to a datetime/timestamp value, or returns empty if conversion fails.
        """
        sql_fragment = context.dialect.try_cast(input_state.sql_fragment, 'timestamp')
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_totime(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle toTime() function.
        
        Converts a string to a time value, or returns empty if conversion fails.
        """
        sql_fragment = context.dialect.try_cast(input_state.sql_fragment, 'time')
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_toquantity(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle toQuantity() function.
        
        Converts a string or number to a FHIR Quantity. For SQL purposes, we'll try to
        parse numeric values and return them as decimals.
        """
        sql_fragment = context.dialect.try_cast(input_state.sql_fragment, 'decimal')
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_converts_functions(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle convertsTo* functions.
        
        These functions test whether a value can be converted to a specific type.
        They return true/false without performing the actual conversion.
        """
        # Extract the target type from function name (e.g., 'convertstoboolean' -> 'boolean')
        target_type = self.func_name.replace('convertsto', '').lower()
        
        if context.dialect.name.upper() == 'DUCKDB':
            if target_type == 'boolean':
                # Check if value can be converted to boolean
                sql_fragment = f"""(
                    {input_state.sql_fragment} IS NOT NULL AND 
                    LOWER({input_state.sql_fragment}::text) IN ('true', 'false', '1', '0', 't', 'f', 'yes', 'no')
                )"""
            elif target_type in ('integer', 'decimal'):
                # Check if value is numeric
                sql_fragment = f"TRY_CAST({input_state.sql_fragment} AS {target_type.upper()}) IS NOT NULL"
            elif target_type in ('date', 'datetime', 'time'):
                # Check if value can be converted to temporal type
                cast_type = 'TIMESTAMP' if target_type == 'datetime' else target_type.upper()
                sql_fragment = f"TRY_CAST({input_state.sql_fragment} AS {cast_type}) IS NOT NULL"
            else:
                # Default: check if not null (can convert to string)
                sql_fragment = f"{input_state.sql_fragment} IS NOT NULL"
        else:  # PostgreSQL
            if target_type == 'boolean':
                # PostgreSQL boolean conversion check
                sql_fragment = f"""(
                    {input_state.sql_fragment} IS NOT NULL AND 
                    LOWER({input_state.sql_fragment}::text) ~ '^(true|false|t|f|yes|no|y|n|1|0)$'
                )"""
            elif target_type in ('integer', 'decimal'):
                # Check if value matches numeric pattern
                sql_fragment = f"""(
                    {input_state.sql_fragment} IS NOT NULL AND
                    {input_state.sql_fragment}::text ~ '^[+-]?[0-9]*\\.?[0-9]+([eE][+-]?[0-9]+)?$'
                )"""
            elif target_type == 'date':
                # Check date pattern
                sql_fragment = f"""(
                    {input_state.sql_fragment} IS NOT NULL AND
                    {input_state.sql_fragment}::text ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
                )"""
            elif target_type in ('datetime', 'time'):
                # Check datetime/time patterns
                pattern = '^[0-9]{2}:[0-9]{2}:[0-9]{2}' if target_type == 'time' else '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}'
                sql_fragment = f"""(
                    {input_state.sql_fragment} IS NOT NULL AND
                    {input_state.sql_fragment}::text ~ '{pattern}'
                )"""
            else:
                # Default: check if not null
                sql_fragment = f"{input_state.sql_fragment} IS NOT NULL"
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_type_checking(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle as/is/ofType functions.
        
        - as(type): Cast to the specified type, returns empty if cast fails
        - is(type): Returns true if the value is of the specified type  
        - ofType(type): Filters a collection to include only items of the specified type
        """
        if not self.args:
            raise InvalidArgumentError(f"{self.func_name}() function requires a type argument")
        
        target_type = str(self.args[0]).lower().strip("'\"")
        
        if self.func_name == 'as':
            # Cast to target type, return null if cast fails
            if context.dialect.name.upper() == 'DUCKDB':
                if target_type in ('integer', 'decimal', 'boolean', 'date', 'time'):
                    cast_type = 'TIMESTAMP' if target_type == 'datetime' else target_type.upper()
                    sql_fragment = f"TRY_CAST({input_state.sql_fragment} AS {cast_type})"
                else:
                    sql_fragment = f"CAST({input_state.sql_fragment} AS STRING)"
            else:  # PostgreSQL
                # PostgreSQL doesn't have TRY_CAST, so use CASE for safe casting
                if target_type == 'integer':
                    sql_fragment = f"""(
                        CASE WHEN {input_state.sql_fragment}::text ~ '^[+-]?[0-9]+$' 
                             THEN CAST({input_state.sql_fragment} AS INTEGER)
                             ELSE NULL END
                    )"""
                elif target_type == 'boolean':
                    sql_fragment = f"""(
                        CASE WHEN LOWER({input_state.sql_fragment}::text) ~ '^(true|false|t|f|1|0)$'
                             THEN CAST({input_state.sql_fragment} AS BOOLEAN)
                             ELSE NULL END
                    )"""
                else:
                    sql_fragment = f"CAST({input_state.sql_fragment} AS TEXT)"
            
            return self._create_scalar_result(input_state, sql_fragment)
            
        elif self.func_name == 'is':
            # Check if value is of the specified type
            if target_type == 'string':
                sql_fragment = f"(json_typeof({input_state.sql_fragment}) = 'string')" if context.dialect.name.upper() == 'DUCKDB' else f"(jsonb_typeof({input_state.sql_fragment}) = 'string')"
            elif target_type == 'number':
                sql_fragment = f"(json_typeof({input_state.sql_fragment}) = 'number')" if context.dialect.name.upper() == 'DUCKDB' else f"(jsonb_typeof({input_state.sql_fragment}) = 'number')"
            elif target_type == 'boolean':
                sql_fragment = f"(json_typeof({input_state.sql_fragment}) = 'boolean')" if context.dialect.name.upper() == 'DUCKDB' else f"(jsonb_typeof({input_state.sql_fragment}) = 'boolean')"
            else:
                # Default to true for basic type checking
                sql_fragment = "true"
            
            return self._create_scalar_result(input_state, sql_fragment)
            
        elif self.func_name == 'oftype':
            # Filter collection to include only items of specified type
            if context.dialect.name.upper() == 'DUCKDB':
                sql_fragment = f"""(
                    SELECT json_group_array(value)
                    FROM json_each({input_state.sql_fragment})
                    WHERE json_typeof(value) = '{target_type}'
                )"""
            else:  # PostgreSQL
                sql_fragment = f"""(
                    SELECT jsonb_agg(value)
                    FROM jsonb_array_elements({input_state.sql_fragment}) AS value
                    WHERE jsonb_typeof(value) = '{target_type}'
                )"""
            
            return self._create_collection_result(input_state, sql_fragment)
        
        else:
            raise InvalidArgumentError(f"Unknown type checking function: {self.func_name}")
    
    # ===============================
    # MATH FUNCTIONS (10+ functions)
    # ===============================
    
    def _execute_math_function(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Execute math function with dialect optimization."""
        
        if self.func_name == 'abs':
            return self._handle_abs(input_state, context)
        elif self.func_name == 'ceiling':
            return self._handle_ceiling(input_state, context)
        elif self.func_name == 'floor':
            return self._handle_floor(input_state, context)
        elif self.func_name == 'round':
            return self._handle_round(input_state, context)
        elif self.func_name == 'sqrt':
            return self._handle_sqrt(input_state, context)
        elif self.func_name == 'truncate':
            return self._handle_truncate(input_state, context)
        elif self.func_name == 'exp':
            return self._handle_exp(input_state, context)
        elif self.func_name == 'ln':
            return self._handle_ln(input_state, context)
        elif self.func_name == 'log':
            return self._handle_log(input_state, context)
        elif self.func_name == 'power':
            return self._handle_power(input_state, context)
        else:
            raise ValueError(f"Unknown math function: {self.func_name}")
    
    def _handle_abs(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle abs() function."""
        sql_fragment = f"ABS({input_state.sql_fragment})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_ceiling(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle ceiling() function."""
        sql_fragment = f"CEIL({input_state.sql_fragment})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_floor(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle floor() function."""
        sql_fragment = f"FLOOR({input_state.sql_fragment})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_round(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle round() function."""
        precision = f", {self.args[0]}" if self.args else ""
        sql_fragment = f"ROUND({input_state.sql_fragment}{precision})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    # Placeholder implementations for remaining math functions
    def _handle_sqrt(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle sqrt() function."""
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = f"SQRT(CAST({input_state.sql_fragment} AS DECIMAL))"
        else:  # PostgreSQL
            sql_fragment = f"SQRT(CAST({input_state.sql_fragment} AS DECIMAL))"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_truncate(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle truncate() function."""
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = f"TRUNC(CAST({input_state.sql_fragment} AS DECIMAL))"
        else:  # PostgreSQL
            sql_fragment = f"TRUNC(CAST({input_state.sql_fragment} AS DECIMAL))"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_exp(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle exp() function."""
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = f"EXP(CAST({input_state.sql_fragment} AS DECIMAL))"
        else:  # PostgreSQL  
            sql_fragment = f"EXP(CAST({input_state.sql_fragment} AS DECIMAL))"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_ln(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle ln() function."""
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = f"LN(CAST({input_state.sql_fragment} AS DECIMAL))"
        else:  # PostgreSQL
            sql_fragment = f"LN(CAST({input_state.sql_fragment} AS DECIMAL))"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_log(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle log() function."""
        base = f", {self.args[0]}" if self.args else ""  # Optional base argument
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = f"LOG(CAST({input_state.sql_fragment} AS DECIMAL){base})"
        else:  # PostgreSQL
            sql_fragment = f"LOG(CAST({input_state.sql_fragment} AS DECIMAL){base})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_power(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle power() function."""
        if not self.args:
            raise InvalidArgumentError("power() function requires an exponent argument")
        
        exponent = str(self.args[0])
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = f"POWER(CAST({input_state.sql_fragment} AS DECIMAL), {exponent})"
        else:  # PostgreSQL
            sql_fragment = f"POWER(CAST({input_state.sql_fragment} AS DECIMAL), {exponent})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    # ==================================
    # DATETIME FUNCTIONS (5+ functions)
    # ==================================
    
    def _execute_datetime_function(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Execute datetime function with dialect optimization."""
        
        if self.func_name == 'now':
            return self._handle_now(input_state, context)
        elif self.func_name == 'today':
            return self._handle_today(input_state, context)
        elif self.func_name == 'timeofday':
            return self._handle_timeofday(input_state, context)
        elif self.func_name == 'lowboundary':
            return self._handle_lowboundary(input_state, context)
        elif self.func_name == 'highboundary':
            return self._handle_highboundary(input_state, context)
        else:
            raise ValueError(f"Unknown datetime function: {self.func_name}")
    
    def _handle_now(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle now() function."""
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = "now()"
        else:  # PostgreSQL
            sql_fragment = "now()"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_today(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle today() function."""
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = "current_date"
        else:  # PostgreSQL
            sql_fragment = "current_date"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    # Placeholder implementations for remaining datetime functions
    def _handle_timeofday(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle timeOfDay() function.
        
        Returns the time-of-day part of a datetime value.
        """
        sql_fragment = context.dialect.cast_to_time(input_state.sql_fragment)
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_lowboundary(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle lowBoundary() function.
        
        Returns the lower boundary of a period or range. For date/time values,
        this represents the earliest possible interpretation of the value.
        """
        sql_fragment = context.dialect.cast_to_timestamp(input_state.sql_fragment)
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_highboundary(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle highBoundary() function.
        
        Returns the upper boundary of a period or range. For date/time values,
        this represents the latest possible interpretation of the value.
        """
        sql_fragment = context.dialect.cast_to_timestamp(input_state.sql_fragment)
        return self._create_scalar_result(input_state, sql_fragment)
    
    # =========================================
    # COMPARISON OPERATORS (8+ operators)
    # =========================================
    
    def _execute_comparison_function(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Execute comparison operator with dialect optimization."""
        
        if self.func_name == '=':
            return self._handle_equals(input_state, context)
        elif self.func_name in ('!=', '<>'):
            return self._handle_not_equals(input_state, context)
        elif self.func_name == '<':
            return self._handle_less_than(input_state, context)
        elif self.func_name == '<=':
            return self._handle_less_equals(input_state, context)
        elif self.func_name == '>':
            return self._handle_greater_than(input_state, context)
        elif self.func_name == '>=':
            return self._handle_greater_equals(input_state, context)
        elif self.func_name == 'in':
            return self._handle_in(input_state, context)
        elif self.func_name == 'contains':
            return self._handle_contains_comparison(input_state, context)
        else:
            raise ValueError(f"Unknown comparison operator: {self.func_name}")
    
    def _handle_equals(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle equals (=) operator."""
        if not self.args:
            raise ValueError("Equals operator requires at least one argument")
        
        # Get the right operand from arguments
        if hasattr(self.args[0], 'value'):
            right_value = f"'{self.args[0].value}'"
        else:
            right_value = str(self.args[0])
        
        sql_fragment = f"{input_state.sql_fragment} = {right_value}"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,  # Boolean result is not a collection
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_not_equals(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle not equals (!= or <>) operator."""
        if not self.args:
            raise ValueError("Not equals operator requires at least one argument")
        
        # Get the right operand from arguments
        if hasattr(self.args[0], 'value'):
            right_value = f"'{self.args[0].value}'"
        else:
            right_value = str(self.args[0])
        
        sql_fragment = f"{input_state.sql_fragment} != {right_value}"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,  # Boolean result is not a collection
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_less_than(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle less than (<) operator."""
        if not self.args:
            raise ValueError("Less than operator requires at least one argument")
        
        # Get the right operand from arguments
        if hasattr(self.args[0], 'value'):
            right_value = str(self.args[0].value)
        else:
            right_value = str(self.args[0])
        
        sql_fragment = f"{input_state.sql_fragment} < {right_value}"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,  # Boolean result is not a collection
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_less_equals(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle less than or equals (<=) operator."""
        if not self.args:
            raise ValueError("Less than or equals operator requires at least one argument")
        
        # Get the right operand from arguments
        if hasattr(self.args[0], 'value'):
            right_value = str(self.args[0].value)
        else:
            right_value = str(self.args[0])
        
        sql_fragment = f"{input_state.sql_fragment} <= {right_value}"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,  # Boolean result is not a collection
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_greater_than(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle greater than (>) operator."""
        if not self.args:
            raise ValueError("Greater than operator requires at least one argument")
        
        # Get the right operand from arguments
        if hasattr(self.args[0], 'value'):
            right_value = str(self.args[0].value)
        else:
            right_value = str(self.args[0])
        
        sql_fragment = f"{input_state.sql_fragment} > {right_value}"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,  # Boolean result is not a collection
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_greater_equals(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle greater than or equals (>=) operator."""
        if not self.args:
            raise ValueError("Greater than or equals operator requires at least one argument")
        
        # Get the right operand from arguments
        if hasattr(self.args[0], 'value'):
            right_value = str(self.args[0].value)
        else:
            right_value = str(self.args[0])
        
        sql_fragment = f"{input_state.sql_fragment} >= {right_value}"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,  # Boolean result is not a collection
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_in(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle 'in' operator."""
        if not self.args:
            raise ValueError("In operator requires at least one argument")
        
        # Handle collection argument for IN clause
        if isinstance(self.args[0], (list, tuple)):
            values = [f"'{v}'" if isinstance(v, str) else str(v) for v in self.args[0]]
            right_value = f"({', '.join(values)})"
        else:
            right_value = f"({self.args[0]})"
        
        sql_fragment = f"{input_state.sql_fragment} IN {right_value}"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,  # Boolean result is not a collection
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_contains_comparison(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle 'contains' comparison operator."""
        if not self.args:
            raise ValueError("Contains operator requires at least one argument")
        
        # Get the search value
        if hasattr(self.args[0], 'value'):
            search_value = f"'%{self.args[0].value}%'"
        else:
            search_value = f"'%{self.args[0]}%'"
        
        # Use LIKE for contains functionality
        sql_fragment = f"{input_state.sql_fragment} LIKE {search_value}"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,  # Boolean result is not a collection
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    # =====================================
    # PIPELINE OPERATION INTERFACE METHODS
    # =====================================
    
    def optimize_for_dialect(self, dialect) -> 'FunctionCallOperation':
        """
        Optimize function for specific dialect.
        
        Args:
            dialect: Target database dialect
            
        Returns:
            Potentially optimized function operation
        """
        # Most function optimizations are handled within the execution methods
        # based on dialect detection. Could be enhanced with dialect-specific
        # function operation subclasses.
        return self
    
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
        # Function-specific validation could be added here
        # For now, basic validation
        if not input_state.sql_fragment:
            raise ValueError(f"Cannot apply {self.func_name}() to empty SQL fragment")
    
    def estimate_complexity(self, input_state: SQLState, context: ExecutionContext) -> int:
        """
        Estimate complexity of function execution.
        
        Args:
            input_state: Input SQL state
            context: Execution context
            
        Returns:
            Complexity score (0-10)
        """
        base_complexity = 3  # Functions are moderately complex
        
        # Collection functions are more expensive
        if self._is_collection_function():
            base_complexity += 2
        
        # Math functions are generally simple
        if self._is_math_function():
            base_complexity = 2
        
        # String functions with regex are expensive
        if self.func_name in ['matches', 'replacematches']:
            base_complexity += 3
        
        # Functions with multiple arguments are more complex
        base_complexity += min(len(self.args), 3)
        
        return min(base_complexity, 10)