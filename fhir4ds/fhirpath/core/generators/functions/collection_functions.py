"""
Collection function handlers for FHIRPath expressions.

This module handles collection operations like exists, empty, first, last,
count, select, where, and other collection manipulation functions.
"""

from typing import List, Any, Optional
from ..base_handler import BaseFunctionHandler


class CollectionFunctionHandler(BaseFunctionHandler):
    """Handles collection function processing for FHIRPath to SQL conversion."""
    
    def __init__(self, generator, cte_builder=None):
        """
        Initialize the collection function handler.
        
        Args:
            generator: Reference to main SQLGenerator for complex operations
            cte_builder: Optional CTEBuilder instance for CTE management
        """
        super().__init__(generator, cte_builder)
        self.generator = generator
        self.dialect = generator.dialect
        
    def get_supported_functions(self) -> List[str]:
        """Return list of collection function names this handler supports."""
        return [
            'exists', 'empty', 'first', 'last', 'count', 'length',
            'select', 'where', 'all', 'distinct', 'single', 'tail',
            'skip', 'take', 'union', 'combine', 'intersect', 'exclude',
            'alltrue', 'allfalse', 'anytrue', 'anyfalse', 'contains',
            'children', 'descendants', 'isdistinct', 'subsetof', 'supersetof',
            # Phase 4.3: Advanced collection functions
            'iif', 'repeat', 'aggregate', 'flatten', 'in',
            # Phase 4.3.1: FHIRPath specification camelCase variants
            'allTrue', 'allFalse', 'anyTrue', 'anyFalse', 'isDistinct', 'subsetOf', 'supersetOf'
        ]
    
    def get_legacy_function_patterns(self) -> List[str]:
        """
        Return legacy collection function patterns that match original hardcoded patterns.
        
        Phase 4.5: Exact patterns from original hardcoded list in ViewRunner
        """
        return [
            'count()', 'distinct()', 'skip(', 'take(',
            'union(', 'intersect(', 'exclude(',
            'allTrue()', 'anyTrue()', 'allFalse()', 'anyFalse()',
            'single()', 'tail()'  # Phase 4.6: Add single() and tail() to function detection patterns
        ]

    def can_handle(self, function_name: str) -> bool:
        """Check if this handler can process the given function."""
        collection_functions = {
            'exists', 'empty', 'first', 'last', 'count', 'length',
            'select', 'where', 'all', 'distinct', 'single', 'tail',
            'skip', 'take', 'union', 'combine', 'intersect', 'exclude',
            'alltrue', 'allfalse', 'anytrue', 'anyfalse', 'contains',
            'children', 'descendants', 'isdistinct', 'subsetof', 'supersetof',
            # Phase 4.3: Advanced collection functions
            'iif', 'repeat', 'aggregate', 'flatten', 'in',
            # Phase 4.3.1: FHIRPath specification camelCase variants
            'alltrue', 'allfalse', 'anytrue', 'anyfalse', 'isdistinct', 'subsetof', 'supersetof'
        }
        return function_name.lower() in collection_functions
    
    def handle_function(self, func_name: str, base_expr: str, func_node) -> str:
        """
        Handle collection function and return SQL expression.
        
        Args:
            func_name: Name of the function to handle
            base_expr: Base SQL expression to apply function to
            func_node: Function AST node with arguments
            
        Returns:
            SQL expression for the function result
        """
        func_name = func_name.lower()
        
        if func_name == 'exists':
            return self._handle_exists(base_expr, func_node)
        elif func_name == 'empty':
            return self._handle_empty(base_expr, func_node)
        elif func_name == 'first':
            return self._handle_first(base_expr, func_node)
        elif func_name == 'last':
            return self._handle_last(base_expr, func_node)
        elif func_name == 'count':
            return self._handle_count(base_expr, func_node)
        elif func_name == 'length':
            return self._handle_length(base_expr, func_node)
        elif func_name == 'select':
            return self._handle_select(base_expr, func_node)
        elif func_name == 'where':
            return self._handle_where(base_expr, func_node)
        elif func_name == 'all':
            return self._handle_all(base_expr, func_node)
        elif func_name == 'distinct':
            return self._handle_distinct(base_expr, func_node)
        elif func_name == 'single':
            return self._handle_single(base_expr, func_node)
        elif func_name == 'tail':
            return self._handle_tail(base_expr, func_node)
        elif func_name == 'skip':
            return self._handle_skip(base_expr, func_node)
        elif func_name == 'take':
            return self._handle_take(base_expr, func_node)
        elif func_name == 'union':
            return self._handle_union(base_expr, func_node)
        elif func_name == 'combine':
            return self._handle_combine(base_expr, func_node)
        elif func_name == 'intersect':
            return self._handle_intersect(base_expr, func_node)
        elif func_name == 'exclude':
            return self._handle_exclude(base_expr, func_node)
        elif func_name == 'alltrue':
            return self._handle_alltrue(base_expr, func_node)
        elif func_name == 'allfalse':
            return self._handle_allfalse(base_expr, func_node)
        elif func_name == 'anytrue':
            return self._handle_anytrue(base_expr, func_node)
        elif func_name == 'anyfalse':
            return self._handle_anyfalse(base_expr, func_node)
        elif func_name == 'contains':
            return self._handle_contains(base_expr, func_node)
        elif func_name == 'children':
            return self._handle_children(base_expr, func_node)
        elif func_name == 'descendants':
            return self._handle_descendants(base_expr, func_node)
        elif func_name == 'isdistinct':
            return self._handle_isdistinct(base_expr, func_node)
        elif func_name == 'subsetof':
            return self._handle_subsetof(base_expr, func_node)
        elif func_name == 'supersetof':
            return self._handle_supersetof(base_expr, func_node)
        elif func_name == 'iif':
            return self._handle_iif(base_expr, func_node)
        elif func_name == 'repeat':
            return self._handle_repeat(base_expr, func_node)
        elif func_name == 'aggregate':
            return self._handle_aggregate(base_expr, func_node)
        elif func_name == 'flatten':
            return self._handle_flatten(base_expr, func_node)
        elif func_name == 'in':
            return self._handle_in(base_expr, func_node)
        else:
            raise ValueError(f"Unsupported collection function: {func_name}")
    
    def _handle_exists(self, base_expr: str, func_node) -> str:
        """Handle exists() function."""
        # PHASE 2D: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'exists'):
            try:
                return self.generator._generate_exists_with_cte(func_node, base_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for exists(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        if not func_node.args: # exists()
            return f"""
            CASE 
                WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN {self.generator.get_json_array_length(base_expr)} > 0
                ELSE ({base_expr} IS NOT NULL AND NOT ({self.generator.get_json_type(base_expr)} = 'OBJECT' AND {self.generator.get_json_array_length(f"{self.dialect.json_extract_function}({base_expr}, '$.keys()')")} = 0))
            END
            """
        else: # exists(criteria) - equivalent to (collection.where(criteria)).exists()
            from ...parser.ast_nodes import FunctionCallNode
            where_node = FunctionCallNode(name='where', args=[func_node.args[0]])
            sql_after_where = self.generator.apply_function_to_expression(where_node, base_expr)
            # Now apply simple .exists() to the result of the where clause
            return self.generator.apply_function_to_expression(FunctionCallNode(name='exists', args=[]), f"({sql_after_where})")
    
    def _handle_empty(self, base_expr: str, func_node) -> str:
        """Handle empty() function."""
        # PHASE 5A: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'empty'):
            try:
                return self.generator._generate_empty_with_cte(base_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for empty(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        # Use dialect-aware type comparison
        array_type_check = f"{self.generator.get_json_type(base_expr)} = 'ARRAY'"
        object_type_check = f"{self.generator.get_json_type(base_expr)} = 'OBJECT'"
        
        return f"""
        CASE 
            WHEN {array_type_check} THEN {self.generator.get_json_array_length(base_expr)} = 0
            ELSE ({base_expr} IS NULL OR ({object_type_check} AND {self.generator.get_json_array_length(f"{self.dialect.json_extract_function}({base_expr}, '$.keys()')")} = 0))
        END
        """
    
    def _handle_first(self, base_expr: str, func_node) -> str:
        """Handle first() function."""
        # PHASE 2: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'first'):
            try:
                return self.generator._generate_first_with_cte(base_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for first(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        # Use base expression directly - optimization system handles complexity properly
        optimized_base = base_expr
        
        return f"""
        CASE 
            WHEN {optimized_base} IS NULL THEN NULL
            ELSE COALESCE({self.generator.extract_json_object(optimized_base, '$[0]')}, {optimized_base})
        END
        """
    
    def _handle_last(self, base_expr: str, func_node) -> str:
        """Handle last() function."""
        # PHASE 2C: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'last'):
            try:
                return self.generator._generate_last_with_cte(base_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for last(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        return f"""
        CASE 
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN 
                {self.dialect.json_extract_function}({base_expr}, {self.dialect.string_concat(self.dialect.string_concat("'$['", f'({self.generator.get_json_array_length(base_expr)} - 1)'), "']'")})
            ELSE {base_expr}
        END
        """
    
    def _handle_count(self, base_expr: str, func_node) -> str:
        """Handle count() function."""
        # PHASE 3: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'count'):
            try:
                return self.generator._generate_count_with_cte(base_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for count(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        return f"""
        CASE 
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN {self.generator.get_json_array_length(base_expr)}
            WHEN {base_expr} IS NOT NULL THEN 1
            ELSE 0
        END
        """
    
    def _handle_length(self, base_expr: str, func_node) -> str:
        """Handle length() function."""
        # PHASE 5C: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'length'):
            try:
                return self.generator._generate_length_with_cte(base_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for length(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        return f"""
        CASE 
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN {self.generator.get_json_array_length(base_expr)}
            WHEN {base_expr} IS NOT NULL THEN 1
            ELSE 0
        END
        """
    
    def _handle_select(self, base_expr: str, func_node) -> str:
        """Handle select() function."""
        # PHASE 4: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'select'):
            try:
                return self.generator._generate_select_with_cte(func_node, base_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"Complex select expression detected, falling back to original implementation")
                print(f"CTE generation failed for select(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        # Generate expression for array elements (uses 'value' from json_each)
        alias = self.generator.generate_alias()
        array_element_expression_generator = type(self.generator)(
            table_name=f"json_each({base_expr})",
            json_column="value",
            resource_type=self.generator.resource_type,
            dialect=self.generator.dialect
        )
        # Configure nested generator for proper CTE handling
        array_element_expression = array_element_expression_generator.visit(func_node.args[0])
        
        # Merge CTEs and state from nested generator using centralized method
        self.generator.merge_nested_generator_state(array_element_expression_generator)
        
        # Generate expression for non-array case (reuse same expression to avoid duplication)
        non_array_element_expression_generator = type(self.generator)(
            table_name=self.generator.table_name,
            json_column=self.generator.json_column,
            resource_type=self.generator.resource_type,
            dialect=self.generator.dialect
        )
        # Configure nested generator for proper CTE handling
        non_array_element_expression = non_array_element_expression_generator.visit(func_node.args[0])
        
        # Merge CTEs and state from non-array case using centralized method
        self.generator.merge_nested_generator_state(non_array_element_expression_generator)
        
        return f"""
        CASE 
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN (
                SELECT {self.generator.aggregate_to_json_array(f"({array_element_expression})")}
                FROM {self.generator.iterate_json_array(base_expr, "$")}
                WHERE ({array_element_expression}) IS NOT NULL
            )
            WHEN {base_expr} IS NOT NULL THEN {self.generator.dialect.json_array_function}({non_array_element_expression})
            ELSE {self.generator.dialect.json_array_function}()
        END
        """
    
    def _handle_where(self, base_expr: str, func_node) -> str:
        """
        Handle where() function with enhanced pattern support.
        
        Phase 4.3: Enhanced where clause patterns for complex filtering
        """
        # PHASE 4B: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'where'):
            try:
                return self.generator._generate_where_with_cte(func_node, base_expr)
            except Exception as e:
                # Fallback to enhanced implementation if CTE generation fails
                print(f"CTE generation failed for where(), falling back to enhanced implementation: {e}")
        
        # Phase 4.3: Enhanced implementation with complex pattern support
        if hasattr(func_node, 'args') and len(func_node.args) > 0:
            # Set context for WHERE clause generation
            old_context = self.generator.in_where_context
            self.generator.in_where_context = True
            
            try:
                # Analyze condition complexity to determine processing approach
                condition_complexity = self._analyze_where_condition_complexity(func_node.args[0])
                
                if condition_complexity['is_complex']:
                    return self._handle_complex_where(base_expr, func_node.args[0], condition_complexity)
                else:
                    return self._handle_simple_where(base_expr, func_node.args[0])
                    
            finally:
                self.generator.in_where_context = old_context
        else:
            return base_expr
    
    def _analyze_where_condition_complexity(self, condition_node) -> dict:
        """
        Analyze where condition complexity to determine processing approach.
        
        Phase 4.3: Complexity analysis for optimized where processing
        """
        complexity = {
            'is_complex': False,
            'has_functions': False,
            'has_boolean_logic': False,
            'has_nested_where': False,
            'has_type_operations': False,
            'complexity_score': 0
        }
        
        # Convert condition to string for pattern analysis
        condition_str = str(condition_node)
        
        # Check for function usage
        function_patterns = [
            'startswith', 'endswith', 'contains', 'length', 'upper', 'lower',
            'abs', 'sqrt', 'power', 'count', 'exists', 'first', 'last'
        ]
        
        for pattern in function_patterns:
            if pattern in condition_str.lower():
                complexity['has_functions'] = True
                complexity['complexity_score'] += 2
                break
        
        # Check for boolean logic
        boolean_patterns = [' and ', ' or ', ' not ', '&&', '||', '!']
        for pattern in boolean_patterns:
            if pattern in condition_str.lower():
                complexity['has_boolean_logic'] = True
                complexity['complexity_score'] += 1
                break
        
        # Check for nested where clauses
        if '.where(' in condition_str:
            complexity['has_nested_where'] = True
            complexity['complexity_score'] += 3
        
        # Check for type operations
        type_patterns = [' is ', '.as(', 'oftype(']
        for pattern in type_patterns:
            if pattern in condition_str.lower():
                complexity['has_type_operations'] = True
                complexity['complexity_score'] += 2
                break
        
        # Determine if complex based on score
        complexity['is_complex'] = complexity['complexity_score'] > 1
        
        return complexity
    
    def _handle_simple_where(self, base_expr: str, condition_node) -> str:
        """Handle simple where conditions with basic optimization."""
        # Create nested generator for where condition processing
        nested_generator = type(self.generator)(
            table_name=self.generator.table_name,
            json_column="value",  # In where context, we operate on array elements
            resource_type=self.generator.resource_type,
            dialect=self.generator.dialect
        )
        # Configure nested generator for WHERE context
        nested_generator.in_where_context = True
        
        where_condition = nested_generator.visit(condition_node)
        
        # Merge CTEs and state from where condition generator
        self.generator.merge_nested_generator_state(nested_generator)
        
        return f"""
        (SELECT {self.generator.aggregate_to_json_array('value')}
         FROM json_each({base_expr}) 
         WHERE {where_condition})
        """
    
    def _handle_complex_where(self, base_expr: str, condition_node, complexity: dict) -> str:
        """
        Handle complex where conditions with enhanced processing.
        
        Phase 4.3: Enhanced complex where clause handling
        """
        # For complex conditions, use a more sophisticated approach
        
        # Create an enhanced nested generator with function handler access
        nested_generator = type(self.generator)(
            table_name=self.generator.table_name,
            json_column="value",  # In where context, we operate on array elements
            resource_type=self.generator.resource_type,
            dialect=self.generator.dialect
        )
        
        # Configure nested generator for WHERE context with enhanced capabilities
        nested_generator.in_where_context = True
        
        # Enable enhanced function processing for complex conditions
        if complexity['has_functions']:
            # Ensure function handlers are available in where context
            nested_generator.collection_function_handler = self.generator.collection_function_handler
            nested_generator.string_function_handler = self.generator.string_function_handler
            nested_generator.math_function_handler = self.generator.math_function_handler
            nested_generator.type_function_handler = self.generator.type_function_handler
            nested_generator.datetime_function_handler = self.generator.datetime_function_handler
        
        try:
            where_condition = nested_generator.visit(condition_node)
            
            # Merge CTEs and state from where condition generator
            self.generator.merge_nested_generator_state(nested_generator)
            
            # For very complex conditions, consider using CTE optimization
            if complexity['complexity_score'] >= 4:
                return self._generate_complex_where_with_optimization(base_expr, where_condition)
            else:
                return f"""
                (SELECT {self.generator.aggregate_to_json_array('value')}
                 FROM json_each({base_expr}) 
                 WHERE {where_condition})
                """
        except Exception as e:
            # Fallback to simple processing if complex processing fails
            print(f"Complex where processing failed, falling back to simple: {e}")
            return self._handle_simple_where(base_expr, condition_node)
    
    def _generate_complex_where_with_optimization(self, base_expr: str, where_condition: str) -> str:
        """Generate optimized SQL for very complex where conditions."""
        # For very complex conditions, use subquery optimization
        return f"""
        (SELECT {self.generator.aggregate_to_json_array('filtered_value')}
         FROM (
             SELECT value as filtered_value
             FROM json_each({base_expr})
             WHERE {where_condition}
         ) complex_filter)
        """
    
    def _handle_all(self, base_expr: str, func_node) -> str:
        """Handle all() function."""
        # PHASE 6: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'all'):
            try:
                return self.generator._generate_all_with_cte(func_node, base_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for all(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        if hasattr(func_node, 'args') and len(func_node.args) > 0:
            condition = self.generator.visit(func_node.args[0])
            return f"""
            CASE 
                WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                    (SELECT COUNT(*) = {self.generator.get_json_array_length(base_expr)}
                     FROM json_each({base_expr}) 
                     WHERE {condition})
                ELSE ({condition})
            END
            """
        else:
            return "true"
    
    def _handle_distinct(self, base_expr: str, func_node) -> str:
        """Handle distinct() function."""
        # PHASE 6B: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'distinct'):
            try:
                return self.generator._generate_distinct_with_cte(base_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for distinct(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        return f"""
        (SELECT {self.generator.aggregate_to_json_array('DISTINCT value')}
         FROM json_each({base_expr}))
        """
    
    # Placeholder implementations for additional collection functions
    # These would need to be extracted from the main generator as well
    
    def _handle_single(self, base_expr: str, func_node) -> str:
        """Handle single() function - returns single element from collection or error if not exactly one element."""
        # Validate arguments first - single() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError("single() function takes no arguments")
        
        # Check if CTE should be used
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'single'):
            try:
                return self.generator._generate_single_with_cte(base_expr, func_node)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for single(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN NULL
                    WHEN {self.generator.get_json_array_length(base_expr)} = 1 THEN (
                        SELECT value 
                        FROM {self.generator.iterate_json_array(base_expr, '$')}
                        LIMIT 1
                    )
                    ELSE NULL
                END
            ELSE {base_expr}
        END
        """.strip()
    
    def _handle_tail(self, base_expr: str, func_node) -> str:
        """Handle tail() function - returns collection with all elements except the first."""
        # Validate arguments first - tail() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError("tail() function takes no arguments")
        
        # Check if CTE should be used
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'tail'):
            try:
                return self.generator._generate_tail_with_cte(base_expr, func_node)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for tail(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN NULL
                    WHEN {self.generator.get_json_array_length(base_expr)} = 1 THEN NULL
                    ELSE (
                        SELECT {self.generator.aggregate_to_json_array('value')}
                        FROM (
                            SELECT value, ROW_NUMBER() OVER () as rn
                            FROM {self.generator.iterate_json_array(base_expr, '$')}
                        ) indexed_values
                        WHERE rn > 1
                    )
                END
            ELSE NULL
        END
        """.strip()
    
    def _handle_skip(self, base_expr: str, func_node) -> str:
        """Handle skip() function - returns collection with first N elements skipped."""
        # Validate arguments first - skip() requires exactly one argument
        if len(func_node.args) != 1:
            raise ValueError("skip() function requires exactly one argument")
        
        # Use optimized implementation for skip function
        
        # Original implementation (fallback) - fixed for GROUP BY issues
        skip_count = self.generator.visit(func_node.args[0])
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {skip_count} IS NULL THEN NULL
            WHEN {skip_count} < 0 THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN NULL
                    WHEN {skip_count} = 0 THEN {base_expr}
                    WHEN {skip_count} >= {self.generator.get_json_array_length(base_expr)} THEN NULL
                    ELSE 
                        -- Use dialect-specific array slice to avoid subquery GROUP BY issues
                        {self.dialect.array_slice_function(base_expr, f"({skip_count}) + 1", self.generator.get_json_array_length(base_expr))}
                END
            ELSE 
                CASE 
                    WHEN {skip_count} = 0 THEN {self.dialect.json_array_function}({base_expr})
                    ELSE NULL
                END
        END
        """.strip()
    
    def _handle_take(self, base_expr: str, func_node) -> str:
        """Handle take() function - returns collection with first N elements."""
        # Validate arguments first - take() requires exactly one argument
        if len(func_node.args) != 1:
            raise ValueError("take() function requires exactly one argument")
        
        # Use optimized implementation for take function
        
        # Original implementation (fallback) - fixed for GROUP BY issues
        take_count = self.generator.visit(func_node.args[0])
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {take_count} IS NULL THEN NULL
            WHEN {take_count} < 0 THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN NULL
                    WHEN {take_count} = 0 THEN NULL
                    WHEN {take_count} >= {self.generator.get_json_array_length(base_expr)} THEN {base_expr}
                    ELSE 
                        -- Use dialect-specific array slice to avoid subquery GROUP BY issues
                        {self.dialect.array_slice_function(base_expr, "1", f"({take_count})")}
                END
            ELSE 
                CASE 
                    WHEN {take_count} > 0 THEN {self.dialect.json_array_function}({base_expr})
                    ELSE NULL
                END
        END
        """.strip()
    
    def _handle_union(self, base_expr: str, func_node) -> str:
        """Handle union() function - returns union of two collections with distinct elements."""
        # Validate arguments first - union() requires exactly one argument
        if len(func_node.args) != 1:
            raise ValueError("union() function requires exactly one argument")
        
        # Use optimized implementation for union function
        
        # Original implementation (fallback) - fixed for GROUP BY and DISTINCT issues
        other_collection = self.generator.visit(func_node.args[0])
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL AND {other_collection} IS NULL THEN NULL
            WHEN {base_expr} IS NULL THEN
                CASE 
                    WHEN {self.generator.get_json_type(other_collection)} = 'ARRAY' THEN {other_collection}
                    ELSE {self.dialect.json_array_function}({other_collection})
                END
            WHEN {other_collection} IS NULL THEN
                CASE 
                    WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN {base_expr}
                    ELSE {self.dialect.json_array_function}({base_expr})
                END
            ELSE 
                -- Use dialect-specific array union to avoid subquery and DISTINCT issues
                {self.dialect.array_union_function(base_expr, other_collection)}
        END
        """.strip()
    
    def _handle_combine(self, base_expr: str, func_node) -> str:
        """Handle combine() function - returns concatenation of two collections without removing duplicates."""
        # Validate arguments first - combine() requires exactly 1 argument
        if len(func_node.args) != 1:
            raise ValueError("combine() function requires exactly one argument")
        
        # Use optimized implementation for combine function
        
        # Original implementation (fallback)
        other_collection = self.generator.visit(func_node.args[0])
        
        # Fix GROUP BY issues by using dialect-specific array concatenation without subqueries
        return f"""
        CASE 
            WHEN {base_expr} IS NULL AND {other_collection} IS NULL THEN NULL
            WHEN {base_expr} IS NULL THEN
                CASE 
                    WHEN {self.generator.get_json_type(other_collection)} = 'ARRAY' THEN {other_collection}
                    ELSE {self.dialect.json_array_function}({other_collection})
                END
            WHEN {other_collection} IS NULL THEN
                CASE 
                    WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN {base_expr}
                    ELSE {self.dialect.json_array_function}({base_expr})
                END
            ELSE 
                -- Proper array concatenation without subqueries using dialect methods
                {self.dialect.array_concat_function(base_expr, other_collection)}
        END
        """.strip()
    
    def _handle_intersect(self, base_expr: str, func_node) -> str:
        """Handle intersect() function - returns elements common to both collections."""
        if len(func_node.args) != 1:
            raise ValueError("intersect() function requires exactly one argument")
        
        # Check if CTE should be used
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'intersect'):
            try:
                return self.generator._generate_intersect_with_cte(base_expr, func_node)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for intersect(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        other_collection = self.generator.visit(func_node.args[0])
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL OR {other_collection} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' AND {self.generator.get_json_type(other_collection)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 OR {self.generator.get_json_array_length(other_collection)} = 0 THEN NULL
                    ELSE (
                        SELECT CASE 
                            WHEN COUNT(*) = 0 THEN NULL
                            ELSE {self.generator.aggregate_to_json_array('DISTINCT base_value')}
                        END
                        FROM (
                            SELECT base_val.value as base_value
                            FROM {self.generator.iterate_json_array(base_expr, '$')} base_val
                            WHERE base_val.value IS NOT NULL
                        ) base_values
                        WHERE EXISTS (
                            SELECT 1 FROM {self.generator.iterate_json_array(other_collection, '$')} other_val
                            WHERE other_val.value = base_values.base_value
                        )
                    )
                END
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' AND {self.generator.get_json_type(other_collection)} != 'ARRAY' THEN
                CASE 
                    WHEN EXISTS (
                        SELECT 1 FROM {self.generator.iterate_json_array(base_expr, '$')} base_val
                        WHERE base_val.value = {other_collection}
                    ) THEN {self.generator.aggregate_to_json_array(other_collection)}
                    ELSE NULL
                END
            WHEN {self.generator.get_json_type(base_expr)} != 'ARRAY' AND {self.generator.get_json_type(other_collection)} = 'ARRAY' THEN
                CASE 
                    WHEN EXISTS (
                        SELECT 1 FROM {self.generator.iterate_json_array(other_collection, '$')} other_val
                        WHERE other_val.value = {base_expr}
                    ) THEN {self.generator.aggregate_to_json_array(base_expr)}
                    ELSE NULL
                END
            ELSE 
                CASE 
                    WHEN {base_expr} = {other_collection} THEN {self.generator.aggregate_to_json_array(base_expr)}
                    ELSE NULL
                END
        END
        """.strip()
    
    def _handle_exclude(self, base_expr: str, func_node) -> str:
        """Handle exclude() function - returns elements in base collection that are not in other collection."""
        if len(func_node.args) != 1:
            raise ValueError("exclude() function requires exactly one argument")
        
        # PHASE 1: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'exclude'):
            try:
                return self.generator._generate_exclude_with_cte(base_expr, func_node)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for exclude(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        other_collection = self.generator.visit(func_node.args[0])
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {other_collection} IS NULL THEN 
                CASE 
                    WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN {base_expr}
                    ELSE {self.generator.dialect.json_array_function}({base_expr})
                END
            ELSE (
                SELECT {self.generator.dialect.json_array_agg_function}(DISTINCT base_value)
                FROM (
                    SELECT CASE 
                        WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN base_val.value
                        ELSE {base_expr}
                    END as base_value
                    FROM {self.generator.iterate_json_array(base_expr, "$")} base_val
                    WHERE CASE 
                        WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN true
                        ELSE base_val.value = {base_expr}
                    END
                    AND NOT EXISTS (
                        SELECT 1 FROM (
                            SELECT CASE 
                                WHEN {self.generator.get_json_type(other_collection)} = 'ARRAY' THEN other_val.value
                                ELSE {other_collection}
                            END as other_value
                            FROM {self.generator.iterate_json_array(other_collection, "$")} other_val
                            WHERE CASE 
                                WHEN {self.generator.get_json_type(other_collection)} = 'ARRAY' THEN true
                                ELSE other_val.value = {other_collection}
                            END
                        ) other_collection
                        WHERE other_collection.other_value = base_value
                    )
                ) exclusion
                WHERE base_value IS NOT NULL
            )
        END
        """
    
    def _handle_alltrue(self, base_expr: str, func_node) -> str:
        """Handle alltrue() function - returns true if all elements in collection are true."""
        if len(func_node.args) != 0:
            raise ValueError("allTrue() function takes no arguments")
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN NULL
                    ELSE (
                        SELECT 
                            CASE 
                                -- If any element is false, return false
                                WHEN COUNT(CASE WHEN 
                                    (CASE 
                                        WHEN {self.dialect.json_type_function}(value) = 'BOOLEAN' THEN CAST(value AS BOOLEAN)
                                        WHEN CAST(value AS VARCHAR) = 'true' THEN true
                                        WHEN CAST(value AS VARCHAR) = 'false' THEN false
                                        ELSE NULL
                                    END) = false 
                                    THEN 1 END) > 0 THEN false
                                -- If any element is null/non-boolean, return null (FHIRPath spec)
                                WHEN COUNT(CASE WHEN 
                                    (CASE 
                                        WHEN {self.dialect.json_type_function}(value) = 'BOOLEAN' THEN CAST(value AS BOOLEAN)
                                        WHEN CAST(value AS VARCHAR) = 'true' THEN true
                                        WHEN CAST(value AS VARCHAR) = 'false' THEN false
                                        ELSE NULL
                                    END) IS NULL 
                                    THEN 1 END) > 0 THEN NULL
                                -- If all elements are true, return true
                                ELSE true
                            END
                        FROM {self.generator.iterate_json_array(base_expr, '$')}
                    )
                END
            ELSE 
                CASE 
                    WHEN {self.dialect.json_type_function}({base_expr}) = 'BOOLEAN' THEN CAST({base_expr} AS BOOLEAN)
                    WHEN CAST({base_expr} AS VARCHAR) = 'true' THEN true
                    WHEN CAST({base_expr} AS VARCHAR) = 'false' THEN false
                    ELSE NULL
                END
        END
        """.strip()
    
    def _handle_allfalse(self, base_expr: str, func_node) -> str:
        """Handle allfalse() function - returns true if all elements in collection are false."""
        if len(func_node.args) != 0:
            raise ValueError("allFalse() function takes no arguments")
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN NULL
                    ELSE (
                        SELECT 
                            CASE 
                                -- If any element is true, return false
                                WHEN COUNT(CASE WHEN 
                                    (CASE 
                                        WHEN {self.dialect.json_type_function}(value) = 'BOOLEAN' THEN CAST(value AS BOOLEAN)
                                        WHEN CAST(value AS VARCHAR) = 'true' THEN true
                                        WHEN CAST(value AS VARCHAR) = 'false' THEN false
                                        ELSE NULL
                                    END) = true 
                                    THEN 1 END) > 0 THEN false
                                -- If any element is null/non-boolean, return false (strict evaluation)
                                WHEN COUNT(CASE WHEN 
                                    (CASE 
                                        WHEN {self.dialect.json_type_function}(value) = 'BOOLEAN' THEN CAST(value AS BOOLEAN)
                                        WHEN CAST(value AS VARCHAR) = 'true' THEN true
                                        WHEN CAST(value AS VARCHAR) = 'false' THEN false
                                        ELSE NULL
                                    END) IS NULL 
                                    THEN 1 END) > 0 THEN false
                                -- If all elements are false, return true
                                ELSE true
                            END
                        FROM {self.generator.iterate_json_array(base_expr, '$')}
                    )
                END
            ELSE 
                CASE 
                    WHEN {self.dialect.json_type_function}({base_expr}) = 'BOOLEAN' THEN NOT CAST({base_expr} AS BOOLEAN)
                    WHEN CAST({base_expr} AS VARCHAR) = 'false' THEN true
                    WHEN CAST({base_expr} AS VARCHAR) = 'true' THEN false
                    ELSE NULL
                END
        END
        """.strip()
    
    def _handle_anytrue(self, base_expr: str, func_node) -> str:
        """Handle anytrue() function - returns true if any element in collection is true."""
        if len(func_node.args) != 0:
            raise ValueError("anyTrue() function takes no arguments")
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN NULL
                    ELSE (
                        SELECT 
                            CASE 
                                -- If any element is true, return true
                                WHEN COUNT(CASE WHEN 
                                    (CASE 
                                        WHEN {self.dialect.json_type_function}(value) = 'BOOLEAN' THEN CAST(value AS BOOLEAN)
                                        WHEN CAST(value AS VARCHAR) = 'true' THEN true
                                        WHEN CAST(value AS VARCHAR) = 'false' THEN false
                                        ELSE NULL
                                    END) = true 
                                    THEN 1 END) > 0 THEN true
                                -- If all elements are false or null/non-boolean, return false
                                ELSE false
                            END
                        FROM {self.generator.iterate_json_array(base_expr, '$')}
                    )
                END
            ELSE 
                CASE 
                    WHEN {self.dialect.json_type_function}({base_expr}) = 'BOOLEAN' THEN CAST({base_expr} AS BOOLEAN)
                    WHEN CAST({base_expr} AS VARCHAR) = 'true' THEN true
                    WHEN CAST({base_expr} AS VARCHAR) = 'false' THEN false
                    ELSE NULL
                END
        END
        """.strip()
    
    def _handle_anyfalse(self, base_expr: str, func_node) -> str:
        """Handle anyfalse() function - returns true if any element in collection is false."""
        if len(func_node.args) != 0:
            raise ValueError("anyFalse() function takes no arguments")
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN NULL
                    ELSE (
                        SELECT 
                            CASE 
                                -- If any element is false, return true
                                WHEN COUNT(CASE WHEN 
                                    (CASE 
                                        WHEN {self.dialect.json_type_function}(value) = 'BOOLEAN' THEN CAST(value AS BOOLEAN)
                                        WHEN CAST(value AS VARCHAR) = 'true' THEN true
                                        WHEN CAST(value AS VARCHAR) = 'false' THEN false
                                        ELSE NULL
                                    END) = false 
                                    THEN 1 END) > 0 THEN true
                                -- If all elements are true or null/non-boolean, return false
                                ELSE false
                            END
                        FROM {self.generator.iterate_json_array(base_expr, '$')}
                    )
                END
            ELSE 
                CASE 
                    WHEN {self.dialect.json_type_function}({base_expr}) = 'BOOLEAN' THEN NOT CAST({base_expr} AS BOOLEAN)
                    WHEN CAST({base_expr} AS VARCHAR) = 'false' THEN true
                    WHEN CAST({base_expr} AS VARCHAR) = 'true' THEN false
                    ELSE NULL
                END
        END
        """.strip()
    
    # Phase 4.3.1: FHIRPath specification camelCase handlers (delegate to lowercase implementations)
    def _handle_allTrue(self, base_expr: str, func_node) -> str:
        """Handle allTrue() function - FHIRPath specification camelCase."""
        return self._handle_alltrue(base_expr, func_node)
    
    def _handle_allFalse(self, base_expr: str, func_node) -> str:
        """Handle allFalse() function - FHIRPath specification camelCase."""
        return self._handle_allfalse(base_expr, func_node)
    
    def _handle_anyTrue(self, base_expr: str, func_node) -> str:
        """Handle anyTrue() function - FHIRPath specification camelCase.""" 
        return self._handle_anytrue(base_expr, func_node)
    
    def _handle_anyFalse(self, base_expr: str, func_node) -> str:
        """Handle anyFalse() function - FHIRPath specification camelCase."""
        return self._handle_anyfalse(base_expr, func_node)
    
    def _handle_isDistinct(self, base_expr: str, func_node) -> str:
        """Handle isDistinct() function - FHIRPath specification camelCase."""
        return self._handle_isdistinct(base_expr, func_node)
    
    def _handle_subsetOf(self, base_expr: str, func_node) -> str:
        """Handle subsetOf() function - FHIRPath specification camelCase."""
        return self._handle_subsetof(base_expr, func_node)
    
    def _handle_supersetOf(self, base_expr: str, func_node) -> str:
        """Handle supersetOf() function - FHIRPath specification camelCase."""
        return self._handle_supersetof(base_expr, func_node)
    
    def _handle_contains(self, base_expr: str, func_node) -> str:
        """Handle contains() function."""
        if len(func_node.args) != 1:
            raise ValueError("contains() function requires exactly one argument")
        
        # PHASE 4B: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'contains'):
            try:
                return self.generator._generate_contains_with_cte(func_node, base_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for contains(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        search_value = self.generator.visit(func_node.args[0])
        return f"""
        CASE 
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN (
                SELECT CASE WHEN COUNT(*) > 0 THEN true ELSE false END
                FROM {self.generator.iterate_json_array(base_expr, "$")}
                WHERE value = {search_value}
            )
            WHEN {base_expr} = {search_value} THEN true
            ELSE false
        END
        """
    
    def _handle_children(self, base_expr: str, func_node) -> str:
        """Handle children() function - delegate to main generator."""
        # Validate arguments first - children() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError("children() function takes no arguments")
        return self.generator._handle_function_fallback('children', base_expr, func_node)
    
    def _handle_descendants(self, base_expr: str, func_node) -> str:
        """Handle descendants() function - delegate to main generator."""
        # Validate arguments first - descendants() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError("descendants() function takes no arguments")
        return self.generator._handle_function_fallback('descendants', base_expr, func_node)
    
    def _handle_isdistinct(self, base_expr: str, func_node) -> str:
        """Handle isDistinct() function - returns true if all elements in collection are distinct."""
        # Validate arguments first - isDistinct() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError("isDistinct() function takes no arguments")
        
        # Check if CTE should be used
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'isdistinct'):
            try:
                return self.generator._generate_isdistinct_with_cte(base_expr, func_node)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for isDistinct(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN true
                    WHEN {self.generator.get_json_array_length(base_expr)} = 1 THEN true
                    ELSE (
                        SELECT 
                            CASE 
                                WHEN COUNT(DISTINCT value) = COUNT(value) THEN true
                                ELSE false
                            END
                        FROM {self.generator.iterate_json_array(base_expr, '$')}
                        WHERE value IS NOT NULL
                    )
                END
            ELSE true
        END
        """.strip()
    
    def _handle_subsetof(self, base_expr: str, func_node) -> str:
        """Handle subsetOf() function - returns true if base collection is a subset of other collection."""
        # Validate arguments first - subsetOf() requires exactly 1 argument
        if len(func_node.args) != 1:
            raise ValueError("subsetOf() function requires exactly one argument")
        
        # Check if CTE should be used
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'subsetof'):
            try:
                return self.generator._generate_subsetof_with_cte(base_expr, func_node)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for subsetOf(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        other_collection = self.generator.visit(func_node.args[0])
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN true
            WHEN {other_collection} IS NULL THEN false
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' AND {self.generator.get_json_type(other_collection)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN true
                    ELSE (
                        SELECT 
                            CASE 
                                WHEN COUNT(*) = 0 THEN true
                                ELSE false
                            END
                        FROM (
                            SELECT base_val.value as base_value
                            FROM {self.generator.iterate_json_array(base_expr, '$')} base_val
                            WHERE base_val.value IS NOT NULL
                        ) base_values
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {self.generator.iterate_json_array(other_collection, '$')} other_val
                            WHERE other_val.value = base_values.base_value
                        )
                    )
                END
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' AND {self.generator.get_json_type(other_collection)} != 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN true
                    WHEN {self.generator.get_json_array_length(base_expr)} = 1 THEN (
                        SELECT 
                            CASE 
                                WHEN base_val.value = {other_collection} THEN true
                                ELSE false
                            END
                        FROM {self.generator.iterate_json_array(base_expr, '$')} base_val
                        LIMIT 1
                    )
                    ELSE false
                END
            WHEN {self.generator.get_json_type(base_expr)} != 'ARRAY' AND {self.generator.get_json_type(other_collection)} = 'ARRAY' THEN
                EXISTS (
                    SELECT 1 FROM {self.generator.iterate_json_array(other_collection, '$')} other_val
                    WHERE other_val.value = {base_expr}
                )
            ELSE 
                {base_expr} = {other_collection}
        END
        """.strip()
    
    def _handle_supersetof(self, base_expr: str, func_node) -> str:
        """Handle supersetOf() function - returns true if base collection is a superset of other collection."""
        # Validate arguments first - supersetOf() requires exactly 1 argument
        if len(func_node.args) != 1:
            raise ValueError("supersetOf() function requires exactly one argument")
        
        # Check if CTE should be used
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'supersetof'):
            try:
                return self.generator._generate_supersetof_with_cte(base_expr, func_node)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for supersetOf(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        other_collection = self.generator.visit(func_node.args[0])
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN false
            WHEN {other_collection} IS NULL THEN true
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' AND {self.generator.get_json_type(other_collection)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(other_collection)} = 0 THEN true
                    ELSE (
                        SELECT 
                            CASE 
                                WHEN COUNT(*) = 0 THEN true
                                ELSE false
                            END
                        FROM (
                            SELECT other_val.value as other_value
                            FROM {self.generator.iterate_json_array(other_collection, '$')} other_val
                            WHERE other_val.value IS NOT NULL
                        ) other_values
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {self.generator.iterate_json_array(base_expr, '$')} base_val
                            WHERE base_val.value = other_values.other_value
                        )
                    )
                END
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' AND {self.generator.get_json_type(other_collection)} != 'ARRAY' THEN
                EXISTS (
                    SELECT 1 FROM {self.generator.iterate_json_array(base_expr, '$')} base_val
                    WHERE base_val.value = {other_collection}
                )
            WHEN {self.generator.get_json_type(base_expr)} != 'ARRAY' AND {self.generator.get_json_type(other_collection)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(other_collection)} = 0 THEN true
                    WHEN {self.generator.get_json_array_length(other_collection)} = 1 THEN (
                        SELECT 
                            CASE 
                                WHEN other_val.value = {base_expr} THEN true
                                ELSE false
                            END
                        FROM {self.generator.iterate_json_array(other_collection, '$')} other_val
                        LIMIT 1
                    )
                    ELSE false
                END
            ELSE 
                {base_expr} = {other_collection}
        END
        """.strip()
    
    def _handle_iif(self, base_expr: str, func_node) -> str:
        """
        Handle iif() function - conditional expression (ternary operator).
        
        Phase 4.3: Implementation of conditional iif(condition, true_result, false_result)
        """
        if len(func_node.args) != 3:
            raise ValueError("iif() function requires exactly 3 arguments: condition, true_result, false_result")
        
        # Process the three arguments
        condition_sql = self.generator.visit(func_node.args[0])
        true_result_sql = self.generator.visit(func_node.args[1])
        false_result_sql = self.generator.visit(func_node.args[2])
        
        return f"""
        CASE 
            WHEN {condition_sql} = true THEN {true_result_sql}
            ELSE {false_result_sql}
        END
        """.strip()
    
    def _handle_repeat(self, base_expr: str, func_node) -> str:
        """
        Handle repeat() function - iterative expression evaluation.
        
        Phase 4.3: Implementation of repeat(expression) for iterative processing
        """
        if len(func_node.args) != 1:
            raise ValueError("repeat() function requires exactly 1 argument: expression")
        
        # For now, implement a simplified version that applies the expression once
        # Full implementation would require recursive evaluation
        expr_sql = self.generator.visit(func_node.args[0])
        
        # TODO: Implement full iterative evaluation
        # This is a simplified implementation that applies the expression once
        return f"""
        CASE 
            WHEN {base_expr} IS NOT NULL THEN {expr_sql}
            ELSE NULL
        END
        """.strip()
    
    def _handle_aggregate(self, base_expr: str, func_node) -> str:
        """
        Handle aggregate() function - custom aggregation with user-defined expressions.
        
        Phase 4.3: Implementation of aggregate(aggregator_expression, initial_value)
        """
        if len(func_node.args) < 1 or len(func_node.args) > 2:
            raise ValueError("aggregate() function requires 1 or 2 arguments: aggregator_expression[, initial_value]")
        
        aggregator_sql = self.generator.visit(func_node.args[0])
        initial_value = "NULL"
        if len(func_node.args) == 2:
            initial_value = self.generator.visit(func_node.args[1])
        
        # Implement aggregation over array elements
        return f"""
        CASE 
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN (
                SELECT 
                    COALESCE(
                        (SELECT {aggregator_sql} 
                         FROM {self.generator.iterate_json_array(base_expr, '$')} elem
                         ORDER BY elem.key
                         LIMIT 1
                        ), 
                        {initial_value}
                    )
            )
            ELSE {initial_value}
        END
        """.strip()
    
    def _handle_flatten(self, base_expr: str, func_node) -> str:
        """
        Handle flatten() function - recursive array flattening.
        
        Phase 4.3: Implementation of flatten() for nested array flattening
        """
        if len(func_node.args) != 0:
            raise ValueError("flatten() function takes no arguments")
        
        # Implement array flattening - handle one level of nesting for now
        return f"""
        CASE 
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN (
                SELECT {self.generator.aggregate_to_json_array('elem_value')}
                FROM (
                    SELECT 
                        CASE 
                            WHEN {self.generator.get_json_type('elem.value')} = 'ARRAY' THEN
                                (SELECT inner_elem.value 
                                 FROM {self.generator.iterate_json_array('elem.value', '$')} inner_elem)
                            ELSE elem.value
                        END as elem_value
                    FROM {self.generator.iterate_json_array(base_expr, '$')} elem
                ) flattened
                WHERE elem_value IS NOT NULL
            )
            ELSE {base_expr}
        END
        """.strip()
    
    def _handle_in(self, base_expr: str, func_node) -> str:
        """
        Handle in() function - membership testing.
        
        Phase 4.3: Implementation of in(collection) for membership testing
        """
        if len(func_node.args) != 1:
            raise ValueError("in() function requires exactly 1 argument: collection")
        
        collection_sql = self.generator.visit(func_node.args[0])
        
        return f"""
        CASE 
            WHEN {self.generator.get_json_type(collection_sql)} = 'ARRAY' THEN (
                SELECT 
                    CASE 
                        WHEN COUNT(*) > 0 THEN true 
                        ELSE false 
                    END
                FROM {self.generator.iterate_json_array(collection_sql, '$')} elem
                WHERE elem.value = {base_expr}
            )
            ELSE {base_expr} = {collection_sql}
        END
        """.strip()