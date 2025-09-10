"""
Collection function handler for FHIRPath collection operations.

This module contains all collection-related function implementations extracted
from the main FunctionCallOperation class to improve modularity and maintainability.
"""

from typing import List, Any, Optional
import logging
import re
from functools import lru_cache

from .base import FunctionHandler
from ...core.base import SQLState, ExecutionContext, ContextMode


logger = logging.getLogger(__name__)


class FHIRPathExecutionError(Exception):
    """Base exception for FHIRPath execution errors."""
    pass


class InvalidArgumentError(FHIRPathExecutionError):
    """Raised when function arguments are invalid."""
    pass


class ConversionError(FHIRPathExecutionError):
    """Raised when AST conversion fails."""
    pass


class CollectionFunctionHandler(FunctionHandler):
    """
    Handler for FHIRPath collection functions.
    
    Supports: exists, empty, first, last, count, where, select, distinct, combine,
    tail, skip, take, union, intersect, exclude, contains_collection, children,
    descendants, isdistinct, iif, repeat, aggregate, flatten, in, all, boolean_collection
    """
    
    def __init__(self, function_name: str, args: List[Any] = None):
        """Initialize the collection function handler."""
        self.func_name = function_name.lower()
        self.args = args or []
    
    def get_supported_functions(self) -> List[str]:
        """Return list of collection function names this handler supports."""
        return [
            'exists', 'empty', 'first', 'last', 'count', 'length', 'where', 'select',
            'distinct', 'combine', 'tail', 'skip', 'take', 'union', 'intersect',
            'exclude', 'contains', 'children', 'descendants', 'isdistinct', 'single',
            'iif', 'repeat', 'aggregate', 'flatten', 'in', 'all', 'alltrue',
            'allfalse', 'anytrue', 'anyfalse', 'subsetof', 'supersetof'
        ]
    
    def handle_function(self, function_name: str, input_state: SQLState,
                       context: ExecutionContext, args: List[Any]) -> SQLState:
        """
        Execute the specified collection function.
        
        Args:
            function_name: Name of the function to execute
            input_state: Current SQL state
            context: Execution context containing dialect and other settings
            args: Function arguments
            
        Returns:
            Updated SQL state after function execution
        """
        # Update internal state
        self.func_name = function_name.lower()
        self.args = args or []
        
        # Route to appropriate handler
        if self.func_name == 'exists':
            return self._handle_exists(input_state, context)
        elif self.func_name == 'empty':
            return self._handle_empty(input_state, context)
        elif self.func_name == 'first':
            return self._handle_first(input_state, context)
        elif self.func_name == 'last':
            return self._handle_last(input_state, context)
        elif self.func_name in ['count', 'length']:
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
        elif self.func_name == 'contains':
            return self._handle_contains_collection(input_state, context)
        elif self.func_name == 'children':
            return self._handle_children(input_state, context)
        elif self.func_name == 'descendants':
            return self._handle_descendants(input_state, context)
        elif self.func_name == 'isdistinct':
            return self._handle_isdistinct(input_state, context)
        elif self.func_name == 'single':
            return self._handle_single(input_state, context)
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
        elif self.func_name == 'all':
            return self._handle_all(input_state, context)
        elif self.func_name in ['alltrue', 'allfalse', 'anytrue', 'anyfalse']:
            return self._handle_boolean_collection(input_state, context)
        elif self.func_name in ['subsetof', 'supersetof']:
            return self._handle_set_operations(input_state, context)
        else:
            raise ValueError(f"Unsupported collection function: {function_name}")

    # ===================================
    # COLLECTION FUNCTION IMPLEMENTATIONS
    # ===================================

    def _handle_exists(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle exists() function."""
        if input_state.is_collection:
            # Try to use cached pattern first
            pattern = self._get_cached_sql_pattern("exists_collection", context)
            if pattern:
                sql_fragment = pattern.format(fragment=input_state.sql_fragment)
            else:
                sql_fragment = f"({context.dialect.json_array_length(input_state.sql_fragment)} > 0)"
        else:
            # Use cached pattern for scalar
            pattern = self._get_cached_sql_pattern("exists_scalar", context)
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
            sql_fragment = context.dialect.generate_json_extract_last(input_state.sql_fragment)
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
            # Handle null collections by returning 0 instead of NULL
            sql_fragment = f"COALESCE({context.dialect.generate_json_array_length(input_state.sql_fragment)}, 0)"
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
        
        sql_fragment = context.dialect.generate_where_clause_filter(collection_expr, condition_sql)
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=True,
            context_mode=ContextMode.COLLECTION,
            path_context="$"  # Reset path context since where creates a new filtered array
        )
    
    def _handle_select(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle select() function with transformation."""
        if not self.args:
            raise ValueError("select() function requires a transformation argument")
        
        # This would need to be expanded to handle complex transformation compilation
        # For now, placeholder implementation
        transform_sql = "."  # This should be compiled from self.args[0]
        
        sql_fragment = context.dialect.generate_select_transformation(input_state.sql_fragment, transform_sql)
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    def _handle_distinct(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle distinct() function."""
        sql_fragment = context.dialect.generate_array_distinct_operation(input_state.sql_fragment)
        
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
        
        sql_fragment = context.dialect.generate_collection_combine(first_collection, second_collection)
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    def _handle_tail(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle tail() function - all elements except first."""
        sql_fragment = context.dialect.generate_array_tail_operation(input_state.sql_fragment)
        
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
        
        sql_fragment = context.dialect.generate_array_slice_operation(input_state.sql_fragment, int(skip_count))
        
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
        
        sql_fragment = context.dialect.generate_array_slice_operation(input_state.sql_fragment, 0, int(take_count))
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    def _handle_union(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle union() function - combines collections with distinct semantics."""
        if not self.args:
            raise InvalidArgumentError("union() function requires another collection argument")
        
        first_collection = input_state.sql_fragment
        second_collection = self._evaluate_union_argument(self.args[0], input_state, context)
        
        # Optimization: Detect resource retrieval queries and use simple UNION ALL
        if self._is_simple_union_candidate(first_collection) and self._is_simple_union_candidate(second_collection):
            # Extract clean resource queries and create UNION ALL
            first_queries = self._extract_resource_queries(first_collection)
            second_queries = self._extract_resource_queries(second_collection)
            
            if first_queries and second_queries:
                # Combine all queries with UNION ALL
                all_queries = first_queries + second_queries
                sql_fragment = f"({' UNION ALL '.join(all_queries)})"
                logger.debug(f"🔧 Optimized union for {len(all_queries)} resource retrievals: UNION ALL")
            else:
                # Fall back to regular union logic
                sql_fragment = f"({first_collection} UNION ALL {second_collection})"
                logger.debug(f"🔧 Basic union optimization: UNION ALL")
        else:
            # Use complex JSON array union logic for general cases
            sql_fragment = context.dialect.generate_union_operation(first_collection, second_collection)
        
        sql_fragment = self._add_debug_info(sql_fragment, "union()", context)
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_intersect(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle intersect() function - returns elements common to both collections."""
        if not self.args:
            raise InvalidArgumentError("intersect() function requires exactly one argument")
        
        first_collection = input_state.sql_fragment
        second_collection = self._evaluate_union_argument(self.args[0], input_state, context)
        
        sql_fragment = context.dialect.generate_intersect_operation(first_collection, second_collection)
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_exclude(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle exclude() function - returns elements in base collection that are not in other collection."""
        if not self.args:
            raise InvalidArgumentError("exclude() function requires exactly one argument")
        
        first_collection = input_state.sql_fragment
        second_collection = self._evaluate_union_argument(self.args[0], input_state, context)
        
        sql_fragment = context.dialect.generate_exclude_operation(first_collection, second_collection)
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_boolean_collection(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle allTrue/allFalse/anyTrue/anyFalse functions."""
        if self.func_name == 'alltrue':
            sql_fragment = context.dialect.generate_boolean_all_true(input_state.sql_fragment)
        elif self.func_name == 'allfalse':
            sql_fragment = context.dialect.generate_boolean_all_false(input_state.sql_fragment)
        elif self.func_name == 'anytrue':
            sql_fragment = context.dialect.generate_boolean_any_true(input_state.sql_fragment)
        elif self.func_name == 'anyfalse':
            sql_fragment = context.dialect.generate_boolean_any_false(input_state.sql_fragment)
        else:
            raise ValueError(f"Unknown boolean function: {self.func_name}")
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=False,
            context_mode=ContextMode.SINGLE
        )
    
    def _handle_contains_collection(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle contains() for collections.
        
        Returns true if the collection contains the specified element.
        This is different from the string contains() which checks substring matching.
        """
        if not self.args:
            raise InvalidArgumentError("contains() function requires an element to search for")
        
        # Get the element to search for
        search_element = str(self.args[0])
        
        sql_fragment = context.dialect.generate_collection_contains_element(input_state.sql_fragment, search_element)
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_children(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle children() function.
        
        Returns a collection with all immediate child nodes of all items in the input collection.
        For JSON/JSONB data, this means all direct properties/elements of an object/array.
        """
        sql_fragment = context.dialect.generate_children_extraction(input_state.sql_fragment)
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_descendants(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle descendants() function.
        
        Returns a collection with all descendant nodes (all nested children at any level)
        of all items in the input collection. This is similar to children() but recursive.
        """
        sql_fragment = context.dialect.generate_recursive_descendants_with_cte(input_state.sql_fragment)
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_isdistinct(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle isDistinct() function.
        
        Returns true if all items in the collection are distinct (no duplicates).
        Returns true for empty collections and single-item collections.
        """
        sql_fragment = context.dialect.generate_collection_distinct_check(input_state.sql_fragment)
        
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
            sql_fragment = context.dialect.generate_subset_check(input_state.sql_fragment, other_collection)
        elif self.func_name == 'supersetof':
            # Check if all elements in other_collection are also in input_state
            sql_fragment = context.dialect.generate_superset_check(input_state.sql_fragment, other_collection)
        else:
            raise ValueError(f"Unknown set comparison function: {self.func_name}")
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=False,
            context_mode=ContextMode.SINGLE
        )
    
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
        
        sql_fragment = context.dialect.generate_iif_expression(condition, true_result, false_result)
        
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
        
        sql_fragment = context.dialect.generate_repeat_operation(input_state.sql_fragment, expression, max_iterations)
        
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
        
        sql_fragment = context.dialect.generate_aggregate_operation(input_state.sql_fragment, aggregator_expr, init_value)
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_flatten(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle flatten() function.
        
        Flattens a collection by concatenating nested collections into a single collection.
        For example, [[1,2], [3,4]] becomes [1,2,3,4].
        """
        sql_fragment = context.dialect.generate_flatten_operation(input_state.sql_fragment)
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_in(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle in() function.
        
        Returns true if the input value is contained in the specified collection.
        This is similar to contains() but with reversed operands: value.in(collection).
        """
        if not self.args:
            raise InvalidArgumentError("in() function requires a collection argument")
        
        collection = str(self.args[0])
        
        sql_fragment = context.dialect.generate_element_in_collection(input_state.sql_fragment, collection)
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_single(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle single() function - returns single element or error if not exactly one."""
        sql_fragment = context.dialect.generate_single_element_check(input_state.sql_fragment)
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_all(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle all() function.
        
        Returns true if all elements in the collection satisfy the specified criteria.
        This is different from allTrue() which just checks if all values are true.
        """
        if not self.args:
            raise InvalidArgumentError("all() function requires a criteria expression")
        
        criteria = str(self.args[0])
        
        sql_fragment = context.dialect.generate_all_criteria_check(input_state.sql_fragment, criteria)
        
        return self._create_scalar_result(input_state, sql_fragment)

    # ===================================
    # HELPER METHODS
    # ===================================

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
                pattern = f"NOT {pattern}"
            return pattern
        
        return ""
    
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
        from ...converters.ast_converter import ASTToPipelineConverter
        
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
    
    def _is_simple_union_candidate(self, sql_fragment: str) -> bool:
        """Check if SQL fragment is a candidate for simple UNION ALL optimization."""
        # Check if it contains resource retrieval queries, even if wrapped in complex logic
        return (
            self._is_resource_retrieval_query(sql_fragment) or
            (
                'SELECT resource FROM fhir_resources' in sql_fragment and
                (
                    'UNION ALL' in sql_fragment or  # Already has UNION ALL
                    'CASE' in sql_fragment  # Complex CASE with resource queries inside
                )
            )
        )
    
    def _is_resource_retrieval_query(self, sql_fragment: str) -> bool:
        """Check if SQL fragment is a resource retrieval query."""
        # Resource retrieval queries typically contain SELECT and fhir_resources
        sql_clean = sql_fragment.strip().strip('(').strip(')')
        return (
            'SELECT resource FROM fhir_resources' in sql_clean or
            'SELECT resource\n            FROM fhir_resources' in sql_clean
        )
    
    def _extract_resource_queries(self, sql_fragment: str) -> list:
        """Extract clean resource retrieval queries from potentially complex SQL."""
        queries = []
        
        # Clean the fragment
        sql_clean = sql_fragment.strip().strip('(').strip(')')
        
        # Method 1: Try to find complete SELECT statements with balanced parentheses
        select_starts = []
        i = 0
        while i < len(sql_clean):
            if sql_clean[i:i+6].upper() == 'SELECT':
                # Found a SELECT - now find the matching end
                paren_count = 0
                start = i
                j = i
                
                # Look backwards to find opening paren
                while start > 0 and sql_clean[start-1] not in '(':
                    start -= 1
                if start > 0:
                    start -= 1  # Include the opening paren
                
                # Look forward to find the complete query
                while j < len(sql_clean):
                    if sql_clean[j] == '(':
                        paren_count += 1
                    elif sql_clean[j] == ')':
                        paren_count -= 1
                        if paren_count == 0:
                            # Found the end
                            query = sql_clean[start:j+1].strip()
                            if 'SELECT resource FROM fhir_resources' in query.upper():
                                queries.append(query)
                            break
                    j += 1
            i += 1
        
        # Method 2: If no balanced queries found, try simpler regex
        if not queries:
            # Try to match simple pattern
            pattern = r'\(\s*SELECT resource\s+FROM fhir_resources.*?\)'
            matches = re.findall(pattern, sql_clean, re.IGNORECASE | re.DOTALL)
            queries.extend([m.strip() for m in matches if m.strip()])
        
        # Method 3: If still no matches but we detect it's a simple resource query
        if not queries and self._is_resource_retrieval_query(sql_fragment):
            queries.append(sql_clean)
        
        # Remove duplicates while preserving order
        unique_queries = []
        for q in queries:
            if q not in unique_queries:
                unique_queries.append(q)
        
        return unique_queries

    def _compile_where_condition(self, condition_arg, context: ExecutionContext) -> str:
        """Compile a where condition argument to SQL."""
        from ...fhirpath.parser.ast_nodes import BinaryOpNode, IdentifierNode, LiteralNode, PathNode, FunctionCallNode
        
        # Handle BinaryOpNode (e.g., use = "official", active != false, age > 18, logical operations)
        if isinstance(condition_arg, BinaryOpNode):
            # Handle logical operators (and, or)
            if condition_arg.operator.lower() in ['and', 'or']:
                left_condition = self._compile_where_condition(condition_arg.left, context)
                right_condition = self._compile_where_condition(condition_arg.right, context)
                
                # Combine conditions with logical operator
                logical_op = condition_arg.operator.upper()
                return context.dialect.generate_logical_combine(left_condition, logical_op, right_condition)
            
            # Map CQL/FHIRPath comparison operators to SQL operators
            operator_mapping = {
                '=': ('=', '=='),     # (DuckDB, PostgreSQL)
                '==': ('=', '=='),
                '!=': ('!=', '!='), 
                '<>': ('<>', '!='),
                '<': ('<', '<'),
                '>': ('>', '>'),
                '<=': ('<=', '<='),
                '>=': ('>=', '>=')
            }
            
            if condition_arg.operator in operator_mapping:
                duckdb_op, pg_op = operator_mapping[condition_arg.operator]
                
                # Extract left field expression and right value
                left_expr = self._compile_operand(condition_arg.left, context, '$ITEM')
                right_expr = self._compile_operand(condition_arg.right, context, '$ITEM')
                
                # Return dialect-specific condition
                return context.dialect.generate_path_condition_comparison(left_expr, condition_arg.operator, right_expr)
        
        # Handle PathNode with function calls (e.g., url.startsWith('http://example.org'))
        if isinstance(condition_arg, PathNode):
            return self._compile_path_condition(condition_arg, context)
        
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
                                    return context.dialect.generate_field_equality_condition('use', value)
        
        # No fallback - raise error for unrecognized conditions
        raise InvalidArgumentError(f"Unrecognized where condition type: {type(condition_arg).__name__}. "
                                  f"Expected BinaryOpNode, PathNode, or pipeline operation.")
    
    def _compile_operand(self, operand, context: ExecutionContext, item_placeholder: str) -> str:
        """Compile an operand (left or right side of binary operation) to SQL."""
        from ...fhirpath.parser.ast_nodes import IdentifierNode, LiteralNode, PathNode, FunctionCallNode
        
        # Handle simple identifiers (field names)
        if isinstance(operand, IdentifierNode):
            field_name = operand.name
            return context.dialect.generate_field_extraction(item_placeholder, field_name)
        
        # Handle literal values
        elif isinstance(operand, LiteralNode):
            value = operand.value
            if isinstance(value, str):
                return f"'{value}'"
            elif isinstance(value, bool):
                return 'true' if value else 'false'
            elif isinstance(value, (int, float)):
                return str(value)
            else:
                return f"'{value}'"
        
        # Handle path expressions (e.g., patient.name.given)
        elif isinstance(operand, PathNode):
            if len(operand.segments) == 1 and isinstance(operand.segments[0], IdentifierNode):
                # Simple single-segment path
                field_name = operand.segments[0].name
                return context.dialect.generate_field_extraction(item_placeholder, field_name)
            elif len(operand.segments) == 2 and isinstance(operand.segments[1], FunctionCallNode):
                # Handle path.function() pattern (e.g., family.exists(), given.count())
                field_name = operand.segments[0].name if isinstance(operand.segments[0], IdentifierNode) else str(operand.segments[0])
                function_call = operand.segments[1]
                
                if function_call.name == 'exists':
                    # Handle field.exists() - check if field is not null
                    return context.dialect.generate_field_exists_check(item_placeholder, field_name)
                elif function_call.name in ['count', 'length']:
                    # Handle field.count() or field.length()
                    if function_call.name == 'count':
                        return context.dialect.generate_field_count_operation(item_placeholder, field_name)
                    else:  # length
                        return context.dialect.generate_field_length_operation(item_placeholder, field_name)
                else:
                    raise InvalidArgumentError(f"Unsupported function in where condition: {function_call.name}")
            else:
                raise InvalidArgumentError(f"Complex path expressions not yet supported: {operand}")
        
        # Handle other operand types
        else:
            raise InvalidArgumentError(f"Unsupported operand type: {type(operand).__name__}")
    
    def _compile_path_condition(self, path_node, context: ExecutionContext) -> str:
        """Compile PathNode conditions to SQL."""
        from ...fhirpath.parser.ast_nodes import IdentifierNode, FunctionCallNode
        
        # Handle simple path with function call (e.g., url.startsWith('http://example.org'))
        if len(path_node.segments) == 2:
            if isinstance(path_node.segments[0], IdentifierNode) and isinstance(path_node.segments[1], FunctionCallNode):
                field_name = path_node.segments[0].name
                function_call = path_node.segments[1]
                
                # Handle different string functions in path conditions
                if function_call.name in ['startswith', 'startsWith']:
                    # Handle field.startsWith(value)
                    if len(function_call.args) == 1:
                        search_value = str(function_call.args[0])
                        return context.dialect.generate_field_startswith_condition('$ITEM', field_name, search_value)
                
                elif function_call.name in ['endswith', 'endsWith']:
                    # Handle field.endsWith(value)
                    if len(function_call.args) == 1:
                        search_value = str(function_call.args[0])
                        return context.dialect.generate_field_endswith_condition('$ITEM', field_name, search_value)
                
                elif function_call.name == 'contains':
                    # Handle field.contains(value)
                    if len(function_call.args) == 1:
                        search_value = str(function_call.args[0])
                        return context.dialect.generate_field_contains_condition('$ITEM', field_name, search_value)
                
                # Handle exists() function
                elif function_call.name == 'exists' and len(function_call.args) == 0:
                    # Handle field.exists() - check if field is not null
                    return context.dialect.generate_field_exists_check('$ITEM', field_name)
        
        # Fallback for unsupported path conditions
        raise InvalidArgumentError(f"Unsupported path condition: {path_node}. "
                                  f"Currently supports field.startsWith(), field.endsWith(), field.contains()")