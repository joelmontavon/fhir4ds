"""
Collection function handlers for FHIRPath expressions.

This module handles collection operations like exists, empty, first, last,
count, select, where, and other collection manipulation functions.
"""

from typing import List, Any, Optional


class CollectionFunctionHandler:
    """Handles collection function processing for FHIRPath to SQL conversion."""
    
    def __init__(self, generator):
        """
        Initialize the collection function handler.
        
        Args:
            generator: Reference to main SQLGenerator for complex operations
        """
        self.generator = generator
        self.dialect = generator.dialect
        
    def get_iteration_index_column(self) -> str:
        """Get the correct column name for array iteration index based on dialect."""
        if self.dialect.name == "POSTGRESQL":
            return "ordinality"
        else:  # DuckDB and others
            return "key"
    
    def get_zero_based_index_condition(self, index_column: str, comparison: str, value: str) -> str:
        """Get a zero-based index condition that works for both dialects."""
        if self.dialect.name == "POSTGRESQL":
            # PostgreSQL ordinality starts from 1, so we need to adjust
            if comparison == ">":
                return f"CAST({index_column} AS INTEGER) > ({value} + 1)"
            elif comparison == ">=":
                return f"CAST({index_column} AS INTEGER) >= ({value} + 1)"
            elif comparison == "<":
                return f"CAST({index_column} AS INTEGER) <= ({value})"
            elif comparison == "<=":
                return f"CAST({index_column} AS INTEGER) <= ({value})"
            else:
                return f"CAST({index_column} AS INTEGER) {comparison} ({value})"
        else:  # DuckDB and others use 0-based indexing
            return f"CAST({index_column} AS INTEGER) {comparison} ({value})"
        
    def can_handle(self, function_name: str) -> bool:
        """Check if this handler can process the given function."""
        collection_functions = {
            'exists', 'empty', 'first', 'last', 'count', 'length',
            'select', 'where', 'all', 'distinct', 'single', 'tail',
            'skip', 'take', 'union', 'combine', 'intersect', 'exclude',
            'alltrue', 'allfalse', 'anytrue', 'anyfalse', 'contains',
            'children', 'descendants', 'subsetof', 'supersetof', 'isdistinct'
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
        elif func_name == 'subsetof':
            return self._handle_subsetof(base_expr, func_node)
        elif func_name == 'supersetof':
            return self._handle_supersetof(base_expr, func_node)
        elif func_name == 'isdistinct':
            return self._handle_isdistinct(base_expr, func_node)
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
        # CRITICAL FIX: Disable optimization completely for now to avoid undefined variable references
        # The _create_optimized_expression method creates undefined variable references 
        # TODO: Fix the optimization system to properly handle complex chained expressions
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
        # CRITICAL FIX: Disable optimizations that create undefined variables in nested context
        array_element_expression_generator.max_sql_complexity = 0  # Force simple expressions
        array_element_expression_generator.complex_expr_cache = {}  # Clear cache
        array_element_expression_generator.enable_cte = False  # Disable CTEs in nested generator
        array_element_expression = array_element_expression_generator.visit(func_node.args[0])
        
        # CRITICAL FIX: Merge CTEs and complex_expr_cache from nested generator into main generator
        # This ensures that CTEs created during nested function calls (like upper()) are not lost
        for cte_name, cte_def in array_element_expression_generator.ctes.items():
            if cte_name not in self.generator.ctes:
                self.generator.ctes[cte_name] = cte_def
        
        # CRITICAL FIX: Merge complex_expr_cache to resolve optimized placeholders
        for placeholder, expr in array_element_expression_generator.complex_expr_cache.items():
            if placeholder not in self.generator.complex_expr_cache:
                self.generator.complex_expr_cache[placeholder] = expr
        
        # Generate expression for non-array case (reuse same expression to avoid duplication)
        non_array_element_expression_generator = type(self.generator)(
            table_name=self.generator.table_name,
            json_column=self.generator.json_column,
            resource_type=self.generator.resource_type,
            dialect=self.generator.dialect
        )
        # CRITICAL FIX: Disable optimizations for non-array case too
        non_array_element_expression_generator.max_sql_complexity = 0  # Force simple expressions
        non_array_element_expression_generator.complex_expr_cache = {}  # Clear cache
        non_array_element_expression_generator.enable_cte = False  # Disable CTEs in nested generator
        non_array_element_expression = non_array_element_expression_generator.visit(func_node.args[0])
        
        # Merge CTEs and complex_expr_cache from non-array case too
        for cte_name, cte_def in non_array_element_expression_generator.ctes.items():
            if cte_name not in self.generator.ctes:
                self.generator.ctes[cte_name] = cte_def
        
        # CRITICAL FIX: Merge complex_expr_cache from non-array case too
        for placeholder, expr in non_array_element_expression_generator.complex_expr_cache.items():
            if placeholder not in self.generator.complex_expr_cache:
                self.generator.complex_expr_cache[placeholder] = expr
        
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
        """Handle where() function."""
        # PHASE 4B: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'where'):
            try:
                return self.generator._generate_where_with_cte(func_node, base_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for where(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        if hasattr(func_node, 'args') and len(func_node.args) > 0:
            # Set context for WHERE clause generation
            old_context = self.generator.in_where_context
            self.generator.in_where_context = True
            
            try:
                # CRITICAL FIX: Create a nested generator to properly capture CTEs
                # The where condition might create CTEs (like startsWith) that need to be merged
                nested_generator = type(self.generator)(
                    table_name=self.generator.table_name,
                    json_column="value",  # In where context, we operate on array elements
                    resource_type=self.generator.resource_type,
                    dialect=self.generator.dialect
                )
                # CRITICAL FIX: Disable CTE optimization in nested generator to force inline expressions
                # This ensures functions like startsWith generate LIKE directly instead of CTE references
                nested_generator.enable_cte = False
                # CRITICAL FIX: Set the WHERE context flag on nested generator too
                nested_generator.in_where_context = True
                
                # CRITICAL FIX: Also disable CTE on all function handlers
                if hasattr(nested_generator, 'string_handler'):
                    nested_generator.string_handler.generator.enable_cte = False
                    nested_generator.string_handler.generator.in_where_context = True
                
                where_condition = nested_generator.visit(func_node.args[0])
                
                # CRITICAL FIX: Merge CTEs from nested generator
                for cte_name, cte_def in nested_generator.ctes.items():
                    if cte_name not in self.generator.ctes:
                        self.generator.ctes[cte_name] = cte_def
                
                # CRITICAL FIX: Merge complex_expr_cache too
                for placeholder, expr in nested_generator.complex_expr_cache.items():
                    if placeholder not in self.generator.complex_expr_cache:
                        self.generator.complex_expr_cache[placeholder] = expr
                
                return f"""
                (SELECT {self.generator.aggregate_to_json_array('value')}
                 FROM json_each({base_expr}) 
                 WHERE {where_condition})
                """
            finally:
                self.generator.in_where_context = old_context
        else:
            return base_expr
    
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
        """
        Handle single() function - returns exactly one element or error.
        
        Args:
            base_expr: SQL expression for the base collection
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns single element or raises error
            
        Raises:
            ValueError: If function has arguments (single() takes no arguments)
        """
        # Validate no arguments
        if hasattr(func_node, 'args') and func_node.args:
            raise ValueError("single() function takes no arguments")
        
        # CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'single'):
            try:
                return self._generate_single_with_cte(base_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for single(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        return self._generate_single_inline(base_expr)
    
    def _generate_single_with_cte(self, base_expr: str) -> str:
        """Generate single() function using CTE approach."""
        # This would use the CTE system for optimization
        # For now, delegate to inline implementation
        return self._generate_single_inline(base_expr)
    
    def _generate_single_inline(self, base_expr: str) -> str:
        """Generate single() function using inline approach."""
        # single() function must validate that there's exactly one element
        # Throws error if empty collection or multiple elements
        # FHIRPath spec requires strict validation for single()
        
        # Get dialect-specific error expression
        if self.generator.dialect.name == "POSTGRESQL":
            error_expr = "(1/0)::jsonb"
            array_type = "'array'"
        else:  # DuckDB
            error_expr = "CAST(1/0 AS JSON)"
            array_type = "'ARRAY'"
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN {error_expr}
            WHEN {self.generator.get_json_type(base_expr)} = {array_type} THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN {error_expr}
                    WHEN {self.generator.get_json_array_length(base_expr)} = 1 THEN {self.generator.extract_json_object(base_expr, '$[0]')}
                    ELSE {error_expr}
                END
            ELSE {base_expr}
        END
        """
    
    def _handle_tail(self, base_expr: str, func_node) -> str:
        """
        Handle tail() function - returns all elements except the first.
        
        Args:
            base_expr: SQL expression for the base collection
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns all elements except the first
            
        Raises:
            ValueError: If function has arguments (tail() takes no arguments)
        """
        # Validate no arguments
        if hasattr(func_node, 'args') and func_node.args:
            raise ValueError("tail() function takes no arguments")
        
        # CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'tail'):
            try:
                return self._generate_tail_with_cte(base_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for tail(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        return self._generate_tail_inline(base_expr)
    
    def _generate_tail_with_cte(self, base_expr: str) -> str:
        """Generate tail() function using CTE approach."""
        # This would use the CTE system for optimization
        # For now, delegate to inline implementation
        return self._generate_tail_inline(base_expr)
    
    def _generate_tail_inline(self, base_expr: str) -> str:
        """Generate tail() function using inline approach."""
        # tail() returns all elements except the first
        # For arrays: slice from index 1 to end
        # For single objects: return empty array
        
        index_column = self.get_iteration_index_column()
        index_condition = self.get_zero_based_index_condition(index_column, ">", "0")
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} <= 1 THEN {self.generator.dialect.json_array_function}()
                    ELSE (
                        SELECT {self.generator.aggregate_to_json_array('value')}
                        FROM {self.generator.iterate_json_array(base_expr, "$")}
                        WHERE {index_condition}
                    )
                END
            ELSE {self.generator.dialect.json_array_function}()
        END
        """
    
    def _handle_skip(self, base_expr: str, func_node) -> str:
        """
        Handle skip(num) function - returns all elements after skipping the first num elements.
        
        Args:
            base_expr: SQL expression for the base collection
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns all elements after skipping the first num
            
        Raises:
            ValueError: If function doesn't have exactly one argument
        """
        # Validate exactly one argument
        if not hasattr(func_node, 'args') or len(func_node.args) != 1:
            raise ValueError("skip() function requires exactly one argument")
        
        # Get the skip count argument
        skip_count_expr = self.generator.visit(func_node.args[0])
        
        # CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'skip'):
            try:
                return self._generate_skip_with_cte(base_expr, skip_count_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for skip(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        return self._generate_skip_inline(base_expr, skip_count_expr)
    
    def _generate_skip_with_cte(self, base_expr: str, skip_count_expr: str) -> str:
        """Generate skip() function using CTE approach."""
        # This would use the CTE system for optimization
        # For now, delegate to inline implementation
        return self._generate_skip_inline(base_expr, skip_count_expr)
    
    def _generate_skip_inline(self, base_expr: str, skip_count_expr: str) -> str:
        """Generate skip() function using inline approach."""
        # skip(num) returns all elements after skipping the first num elements
        # For arrays: slice from index num to end
        # For single objects: return empty if num > 0, else return the object
        
        index_column = self.get_iteration_index_column()
        index_condition = self.get_zero_based_index_condition(index_column, ">=", skip_count_expr)
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} <= ({skip_count_expr}) THEN {self.generator.dialect.json_array_function}()
                    ELSE (
                        SELECT {self.generator.aggregate_to_json_array('value')}
                        FROM {self.generator.iterate_json_array(base_expr, "$")}
                        WHERE {index_condition}
                    )
                END
            ELSE 
                CASE 
                    WHEN ({skip_count_expr}) > 0 THEN {self.generator.dialect.json_array_function}()
                    ELSE {self.generator.dialect.json_array_function}({base_expr})
                END
        END
        """
    
    def _handle_take(self, base_expr: str, func_node) -> str:
        """
        Handle take(num) function - returns the first num elements of a collection.
        
        Args:
            base_expr: SQL expression for the base collection
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns the first num elements
            
        Raises:
            ValueError: If function doesn't have exactly one argument
        """
        # Validate exactly one argument
        if not hasattr(func_node, 'args') or len(func_node.args) != 1:
            raise ValueError("take() function requires exactly one argument")
        
        # Get the take count argument
        take_count_expr = self.generator.visit(func_node.args[0])
        
        # CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'take'):
            try:
                return self._generate_take_with_cte(base_expr, take_count_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for take(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        return self._generate_take_inline(base_expr, take_count_expr)
    
    def _generate_take_with_cte(self, base_expr: str, take_count_expr: str) -> str:
        """Generate take() function using CTE approach."""
        # This would use the CTE system for optimization
        # For now, delegate to inline implementation
        return self._generate_take_inline(base_expr, take_count_expr)
    
    def _generate_take_inline(self, base_expr: str, take_count_expr: str) -> str:
        """Generate take() function using inline approach."""
        # take(num) returns the first num elements of a collection
        # For arrays: slice from index 0 to num-1
        # For single objects: return the object if num > 0, else return empty array
        
        index_column = self.get_iteration_index_column()
        index_condition = self.get_zero_based_index_condition(index_column, "<", take_count_expr)
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN ({take_count_expr}) <= 0 THEN {self.generator.dialect.json_array_function}()
                    WHEN {self.generator.get_json_array_length(base_expr)} <= ({take_count_expr}) THEN {base_expr}
                    ELSE (
                        SELECT {self.generator.aggregate_to_json_array('value')}
                        FROM {self.generator.iterate_json_array(base_expr, "$")}
                        WHERE {index_condition}
                    )
                END
            ELSE 
                CASE 
                    WHEN ({take_count_expr}) > 0 THEN {self.generator.dialect.json_array_function}({base_expr})
                    ELSE {self.generator.dialect.json_array_function}()
                END
        END
        """
    
    def _handle_union(self, base_expr: str, func_node) -> str:
        """
        Handle union(other) function - returns union of two collections.
        
        Args:
            base_expr: SQL expression for the base collection
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns the union of two collections
            
        Raises:
            ValueError: If function doesn't have exactly one argument
        """
        # Validate exactly one argument
        if not hasattr(func_node, 'args') or len(func_node.args) != 1:
            raise ValueError("union() function requires exactly one argument")
        
        # Get the other collection argument
        other_expr = self.generator.visit(func_node.args[0])
        
        # CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'union'):
            try:
                return self._generate_union_with_cte(base_expr, other_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for union(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        return self._generate_union_inline(base_expr, other_expr)
    
    def _generate_union_with_cte(self, base_expr: str, other_expr: str) -> str:
        """Generate union() function using CTE approach."""
        # This would use the CTE system for optimization
        # For now, delegate to inline implementation
        return self._generate_union_inline(base_expr, other_expr)
    
    def _generate_union_inline(self, base_expr: str, other_expr: str) -> str:
        """Generate union() function using inline approach."""
        # union(other) returns the union of two collections
        # Convert both to arrays, then combine and deduplicate
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL AND {other_expr} IS NULL THEN NULL
            WHEN {base_expr} IS NULL THEN
                CASE 
                    WHEN {self.generator.get_json_type(other_expr)} = 'ARRAY' THEN {other_expr}
                    ELSE {self.generator.dialect.json_array_function}({other_expr})
                END
            WHEN {other_expr} IS NULL THEN
                CASE 
                    WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN {base_expr}
                    ELSE {self.generator.dialect.json_array_function}({base_expr})
                END
            ELSE (
                SELECT {self.generator.aggregate_to_json_array('value')}
                FROM (
                    SELECT DISTINCT value FROM (
                        SELECT value FROM {self.generator.iterate_json_array(base_expr, "$")}
                        WHERE CASE 
                            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN true
                            ELSE value = {base_expr}
                        END
                        UNION ALL
                        SELECT value FROM {self.generator.iterate_json_array(other_expr, "$")}
                        WHERE CASE 
                            WHEN {self.generator.get_json_type(other_expr)} = 'ARRAY' THEN true
                            ELSE value = {other_expr}
                        END
                    ) combined
                    WHERE value IS NOT NULL
                ) deduplicated
            )
        END
        """
    
    def _handle_combine(self, base_expr: str, func_node) -> str:
        """
        Handle combine(other) function - returns combination of two collections without removing duplicates.
        
        Args:
            base_expr: SQL expression for the base collection
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns the combination of two collections
            
        Raises:
            ValueError: If function doesn't have exactly one argument
        """
        # Validate exactly one argument
        if not hasattr(func_node, 'args') or len(func_node.args) != 1:
            raise ValueError("combine() function requires exactly one argument")
        
        # Get the other collection argument
        other_expr = self.generator.visit(func_node.args[0])
        
        # CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'combine'):
            try:
                return self._generate_combine_with_cte(base_expr, other_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for combine(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        return self._generate_combine_inline(base_expr, other_expr)
    
    def _generate_combine_with_cte(self, base_expr: str, other_expr: str) -> str:
        """Generate combine() function using CTE approach."""
        # This would use the CTE system for optimization
        # For now, delegate to inline implementation
        return self._generate_combine_inline(base_expr, other_expr)
    
    def _generate_combine_inline(self, base_expr: str, other_expr: str) -> str:
        """Generate combine() function using inline approach."""
        # combine(other) returns the combination of two collections without removing duplicates
        # Similar to union() but without DISTINCT
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL AND {other_expr} IS NULL THEN NULL
            WHEN {base_expr} IS NULL THEN
                CASE 
                    WHEN {self.generator.get_json_type(other_expr)} = 'ARRAY' THEN {other_expr}
                    ELSE {self.generator.dialect.json_array_function}({other_expr})
                END
            WHEN {other_expr} IS NULL THEN
                CASE 
                    WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN {base_expr}
                    ELSE {self.generator.dialect.json_array_function}({base_expr})
                END
            ELSE (
                SELECT {self.generator.aggregate_to_json_array('value')}
                FROM (
                    SELECT value FROM {self.generator.iterate_json_array(base_expr, "$")}
                    WHERE CASE 
                        WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN true
                        ELSE value = {base_expr}
                    END
                    UNION ALL
                    SELECT value FROM {self.generator.iterate_json_array(other_expr, "$")}
                    WHERE CASE 
                        WHEN {self.generator.get_json_type(other_expr)} = 'ARRAY' THEN true
                        ELSE value = {other_expr}
                    END
                ) combined
                WHERE value IS NOT NULL
            )
        END
        """
    
    def _handle_intersect(self, base_expr: str, func_node) -> str:
        """
        Handle intersect(other) function - returns intersection of two collections.
        
        Args:
            base_expr: SQL expression for the base collection
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns the intersection of two collections
            
        Raises:
            ValueError: If function doesn't have exactly one argument
        """
        # Validate exactly one argument
        if not hasattr(func_node, 'args') or len(func_node.args) != 1:
            raise ValueError("intersect() function requires exactly one argument")
        
        # Get the other collection argument
        other_expr = self.generator.visit(func_node.args[0])
        
        # CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'intersect'):
            try:
                return self._generate_intersect_with_cte(base_expr, other_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for intersect(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        return self._generate_intersect_inline(base_expr, other_expr)
    
    def _generate_intersect_with_cte(self, base_expr: str, other_expr: str) -> str:
        """Generate intersect() function using CTE approach."""
        # This would use the CTE system for optimization
        # For now, delegate to inline implementation
        return self._generate_intersect_inline(base_expr, other_expr)
    
    def _generate_intersect_inline(self, base_expr: str, other_expr: str) -> str:
        """Generate intersect() function using inline approach."""
        # intersect(other) returns elements that exist in both collections
        # Use EXISTS with subquery to find matching elements
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL OR {other_expr} IS NULL THEN {self.generator.dialect.json_array_function}()
            ELSE (
                SELECT {self.generator.aggregate_to_json_array('DISTINCT base_value')}
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
                    AND EXISTS (
                        SELECT 1 FROM (
                            SELECT CASE 
                                WHEN {self.generator.get_json_type(other_expr)} = 'ARRAY' THEN other_val.value
                                ELSE {other_expr}
                            END as other_value
                            FROM {self.generator.iterate_json_array(other_expr, "$")} other_val
                            WHERE CASE 
                                WHEN {self.generator.get_json_type(other_expr)} = 'ARRAY' THEN true
                                ELSE other_val.value = {other_expr}
                            END
                        ) other_collection
                        WHERE other_collection.other_value = base_value
                    )
                ) intersection
                WHERE base_value IS NOT NULL
            )
        END
        """
    
    def _handle_exclude(self, base_expr: str, func_node) -> str:
        """
        Handle exclude(other) function - returns elements in base collection that are not in other collection.
        
        Args:
            base_expr: SQL expression for the base collection
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns elements in base collection excluding those in other collection
            
        Raises:
            ValueError: If function doesn't have exactly one argument
        """
        # Validate exactly one argument
        if not hasattr(func_node, 'args') or len(func_node.args) != 1:
            raise ValueError("exclude() function requires exactly one argument")
        
        # Get the other collection argument
        other_expr = self.generator.visit(func_node.args[0])
        
        # CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'exclude'):
            try:
                return self._generate_exclude_with_cte(base_expr, other_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for exclude(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        return self._generate_exclude_inline(base_expr, other_expr)
    
    def _generate_exclude_with_cte(self, base_expr: str, other_expr: str) -> str:
        """Generate exclude() function using CTE approach."""
        # This would use the CTE system for optimization
        # For now, delegate to inline implementation
        return self._generate_exclude_inline(base_expr, other_expr)
    
    def _generate_exclude_inline(self, base_expr: str, other_expr: str) -> str:
        """Generate exclude() function using inline approach."""
        # exclude(other) returns elements in base collection that are NOT in other collection
        # Use NOT EXISTS with subquery to find non-matching elements
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {other_expr} IS NULL THEN 
                CASE 
                    WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN {base_expr}
                    ELSE {self.generator.dialect.json_array_function}({base_expr})
                END
            ELSE (
                SELECT {self.generator.aggregate_to_json_array('DISTINCT base_value')}
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
                                WHEN {self.generator.get_json_type(other_expr)} = 'ARRAY' THEN other_val.value
                                ELSE {other_expr}
                            END as other_value
                            FROM {self.generator.iterate_json_array(other_expr, "$")} other_val
                            WHERE CASE 
                                WHEN {self.generator.get_json_type(other_expr)} = 'ARRAY' THEN true
                                ELSE other_val.value = {other_expr}
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
        """
        Handle allTrue() function - returns true if all boolean values are true.
        
        Args:
            base_expr: SQL expression for the base collection
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns true if all values are true, false otherwise
            
        Raises:
            ValueError: If function has arguments (allTrue() takes no arguments)
        """
        # Validate no arguments
        if hasattr(func_node, 'args') and func_node.args:
            raise ValueError("allTrue() function takes no arguments")
        
        # CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'alltrue'):
            try:
                return self._generate_alltrue_with_cte(base_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for allTrue(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        return self._generate_alltrue_inline(base_expr)
    
    def _generate_alltrue_with_cte(self, base_expr: str) -> str:
        """Generate allTrue() function using CTE approach."""
        # This would use the CTE system for optimization
        # For now, delegate to inline implementation
        return self._generate_alltrue_inline(base_expr)
    
    def _generate_alltrue_inline(self, base_expr: str) -> str:
        """Generate allTrue() function using inline approach."""
        # allTrue() returns true if all boolean values in collection are true
        # Returns false if any value is false
        # Returns null if collection is empty or contains null values
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN NULL
                    ELSE (
                        SELECT CASE 
                            WHEN COUNT(CASE WHEN value IS NULL THEN 1 END) > 0 THEN NULL
                            WHEN COUNT(CASE WHEN CAST(value AS BOOLEAN) = false THEN 1 END) > 0 THEN false
                            ELSE true
                        END
                        FROM {self.generator.iterate_json_array(base_expr, "$")}
                    )
                END
            ELSE 
                CASE 
                    WHEN {base_expr} IS NULL THEN NULL
                    WHEN CAST({base_expr} AS BOOLEAN) = true THEN true
                    ELSE false
                END
        END
        """
    
    def _handle_allfalse(self, base_expr: str, func_node) -> str:
        """
        Handle allFalse() function - returns true if all boolean values are false.
        
        Args:
            base_expr: SQL expression for the base collection
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns true if all values are false, false otherwise
            
        Raises:
            ValueError: If function has arguments (allFalse() takes no arguments)
        """
        # Validate no arguments
        if hasattr(func_node, 'args') and func_node.args:
            raise ValueError("allFalse() function takes no arguments")
        
        # CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'allfalse'):
            try:
                return self._generate_allfalse_with_cte(base_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for allFalse(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        return self._generate_allfalse_inline(base_expr)
    
    def _generate_allfalse_with_cte(self, base_expr: str) -> str:
        """Generate allFalse() function using CTE approach."""
        # This would use the CTE system for optimization
        # For now, delegate to inline implementation
        return self._generate_allfalse_inline(base_expr)
    
    def _generate_allfalse_inline(self, base_expr: str) -> str:
        """Generate allFalse() function using inline approach."""
        # allFalse() returns true if all boolean values in collection are false
        # Returns false if any value is true
        # Returns null if collection is empty or contains null values
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN NULL
                    ELSE (
                        SELECT CASE 
                            WHEN COUNT(CASE WHEN value IS NULL THEN 1 END) > 0 THEN NULL
                            WHEN COUNT(CASE WHEN CAST(value AS BOOLEAN) = true THEN 1 END) > 0 THEN false
                            ELSE true
                        END
                        FROM {self.generator.iterate_json_array(base_expr, "$")}
                    )
                END
            ELSE 
                CASE 
                    WHEN {base_expr} IS NULL THEN NULL
                    WHEN CAST({base_expr} AS BOOLEAN) = false THEN true
                    ELSE false
                END
        END
        """
    
    def _handle_anytrue(self, base_expr: str, func_node) -> str:
        """
        Handle anyTrue() function - returns true if any boolean value is true.
        
        Args:
            base_expr: SQL expression for the base collection
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns true if any value is true, false otherwise
            
        Raises:
            ValueError: If function has arguments (anyTrue() takes no arguments)
        """
        # Validate no arguments
        if hasattr(func_node, 'args') and func_node.args:
            raise ValueError("anyTrue() function takes no arguments")
        
        # CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'anytrue'):
            try:
                return self._generate_anytrue_with_cte(base_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for anyTrue(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        return self._generate_anytrue_inline(base_expr)
    
    def _generate_anytrue_with_cte(self, base_expr: str) -> str:
        """Generate anyTrue() function using CTE approach."""
        # This would use the CTE system for optimization
        # For now, delegate to inline implementation
        return self._generate_anytrue_inline(base_expr)
    
    def _generate_anytrue_inline(self, base_expr: str) -> str:
        """Generate anyTrue() function using inline approach."""
        # anyTrue() returns true if any boolean value in collection is true
        # Returns false if all values are false
        # Returns null if collection is empty or contains only null values
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN NULL
                    ELSE (
                        SELECT CASE 
                            WHEN COUNT(CASE WHEN value IS NOT NULL THEN 1 END) = 0 THEN NULL
                            WHEN COUNT(CASE WHEN CAST(value AS BOOLEAN) = true THEN 1 END) > 0 THEN true
                            ELSE false
                        END
                        FROM {self.generator.iterate_json_array(base_expr, "$")}
                    )
                END
            ELSE 
                CASE 
                    WHEN {base_expr} IS NULL THEN NULL
                    WHEN CAST({base_expr} AS BOOLEAN) = true THEN true
                    ELSE false
                END
        END
        """
    
    def _handle_anyfalse(self, base_expr: str, func_node) -> str:
        """
        Handle anyFalse() function - returns true if any boolean value is false.
        
        Args:
            base_expr: SQL expression for the base collection
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns true if any value is false, false otherwise
            
        Raises:
            ValueError: If function has arguments (anyFalse() takes no arguments)
        """
        # Validate no arguments
        if hasattr(func_node, 'args') and func_node.args:
            raise ValueError("anyFalse() function takes no arguments")
        
        # CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'anyfalse'):
            try:
                return self._generate_anyfalse_with_cte(base_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for anyFalse(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        return self._generate_anyfalse_inline(base_expr)
    
    def _generate_anyfalse_with_cte(self, base_expr: str) -> str:
        """Generate anyFalse() function using CTE approach."""
        # This would use the CTE system for optimization
        # For now, delegate to inline implementation
        return self._generate_anyfalse_inline(base_expr)
    
    def _generate_anyfalse_inline(self, base_expr: str) -> str:
        """Generate anyFalse() function using inline approach."""
        # anyFalse() returns true if any boolean value in collection is false
        # Returns false if all values are true
        # Returns null if collection is empty or contains only null values
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN NULL
                    ELSE (
                        SELECT CASE 
                            WHEN COUNT(CASE WHEN value IS NOT NULL THEN 1 END) = 0 THEN NULL
                            WHEN COUNT(CASE WHEN CAST(value AS BOOLEAN) = false THEN 1 END) > 0 THEN true
                            ELSE false
                        END
                        FROM {self.generator.iterate_json_array(base_expr, "$")}
                    )
                END
            ELSE 
                CASE 
                    WHEN {base_expr} IS NULL THEN NULL
                    WHEN CAST({base_expr} AS BOOLEAN) = false THEN true
                    ELSE false
                END
        END
        """
    
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
        """
        Handle children() function - returns all immediate child elements.
        
        Args:
            base_expr: SQL expression for the base collection
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns all immediate child elements
            
        Raises:
            ValueError: If function has arguments (children() takes no arguments)
        """
        # Validate no arguments
        if hasattr(func_node, 'args') and func_node.args:
            raise ValueError("children() function takes no arguments")
        
        # Use inline implementation for reliable functionality
        return self._generate_children_inline(base_expr)
    
    def _generate_children_inline(self, base_expr: str) -> str:
        """Generate children() function using inline approach."""
        # children() returns all immediate child elements of the current collection
        # For FHIR resources, this extracts direct properties/fields
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'OBJECT' THEN (
                SELECT {self.generator.aggregate_to_json_array('child_value')}
                FROM {self.generator.dialect.json_each_function}({base_expr}) as child_table(child_key, child_value)
            )
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN (
                SELECT {self.generator.aggregate_to_json_array('array_element')}
                FROM {self.generator.dialect.json_each_function}({base_expr}) as array_table(array_key, array_element)
            )
            ELSE {self.generator.dialect.json_array_function}()
        END
        """
    
    def _handle_descendants(self, base_expr: str, func_node) -> str:
        """
        Handle descendants() function - returns all descendant elements recursively.
        
        Args:
            base_expr: SQL expression for the base collection
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns all descendant elements
            
        Raises:
            ValueError: If function has arguments (descendants() takes no arguments)
        """
        # Validate no arguments
        if hasattr(func_node, 'args') and func_node.args:
            raise ValueError("descendants() function takes no arguments")
        
        # Use inline implementation for reliable functionality
        return self._generate_descendants_inline(base_expr)
    
    def _generate_descendants_inline(self, base_expr: str) -> str:
        """Generate descendants() function using inline approach."""
        # descendants() returns all descendant elements recursively
        # This is a simplified implementation that flattens all values
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'OBJECT' THEN (
                SELECT {self.generator.aggregate_to_json_array('child_value')}
                FROM {self.generator.dialect.json_each_function}({base_expr}) as child_table(child_key, child_value)
            )
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN (
                SELECT {self.generator.aggregate_to_json_array('array_element')}
                FROM {self.generator.dialect.json_each_function}({base_expr}) as array_table(array_key, array_element)
            )
            ELSE {self.generator.dialect.json_array_function}()
        END
        """    
    def _handle_subsetof(self, base_expr: str, func_node) -> str:
        """
        Handle subsetOf(other) function - returns true if all items in input are in other collection.
        
        Args:
            base_expr: SQL expression for the base collection
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns true if input is subset of other collection
            
        Raises:
            ValueError: If function doesn't have exactly one argument
        """
        # Validate exactly one argument
        if not hasattr(func_node, 'args') or len(func_node.args) != 1:
            raise ValueError("subsetOf() function requires exactly one argument")
        
        # Get the other collection argument
        other_expr = self.generator.visit(func_node.args[0])
        
        return self._generate_subsetof_inline(base_expr, other_expr)
    
    def _generate_subsetof_inline(self, base_expr: str, other_expr: str) -> str:
        """Generate subsetOf() function using inline approach."""
        # subsetOf(other) returns true if all items in base collection exist in other collection
        # Uses NOT EXISTS to check if any base item is not in other collection
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {other_expr} IS NULL THEN false
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' AND {self.generator.get_json_array_length(base_expr)} = 0 THEN true
            ELSE NOT EXISTS (
                SELECT 1
                FROM {self.generator.dialect.json_each_function}({base_expr}) as base_table(base_key, base_value)
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM {self.generator.dialect.json_each_function}({other_expr}) as other_table(other_key, other_value)
                    WHERE base_value = other_value
                )
                AND CASE 
                    WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN true
                    ELSE base_value = {base_expr}
                END
            )
        END
        """
    
    def _handle_supersetof(self, base_expr: str, func_node) -> str:
        """
        Handle supersetOf(other) function - returns true if input collection contains all items in other collection.
        
        Args:
            base_expr: SQL expression for the base collection
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns true if input is superset of other collection
            
        Raises:
            ValueError: If function doesn't have exactly one argument
        """
        # Validate exactly one argument
        if not hasattr(func_node, 'args') or len(func_node.args) != 1:
            raise ValueError("supersetOf() function requires exactly one argument")
        
        # Get the other collection argument
        other_expr = self.generator.visit(func_node.args[0])
        
        return self._generate_supersetof_inline(base_expr, other_expr)
    
    def _generate_supersetof_inline(self, base_expr: str, other_expr: str) -> str:
        """Generate supersetOf() function using inline approach."""
        # supersetOf(other) returns true if base collection contains all items in other collection
        # This is equivalent to other.subsetOf(base), so we reverse the arguments
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN false
            WHEN {other_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(other_expr)} = 'ARRAY' AND {self.generator.get_json_array_length(other_expr)} = 0 THEN true
            ELSE NOT EXISTS (
                SELECT 1
                FROM {self.generator.dialect.json_each_function}({other_expr}) as other_table(other_key, other_value)
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM {self.generator.dialect.json_each_function}({base_expr}) as base_table(base_key, base_value)
                    WHERE other_value = base_value
                )
                AND CASE 
                    WHEN {self.generator.get_json_type(other_expr)} = 'ARRAY' THEN true
                    ELSE other_value = {other_expr}
                END
            )
        END
        """
    
    def _handle_isdistinct(self, base_expr: str, func_node) -> str:
        """
        Handle isDistinct() function - returns true if all items in collection are unique.
        
        Args:
            base_expr: SQL expression for the base collection
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns true if all values are distinct
            
        Raises:
            ValueError: If function has arguments (isDistinct() takes no arguments)
        """
        # Validate no arguments
        if hasattr(func_node, 'args') and func_node.args:
            raise ValueError("isDistinct() function takes no arguments")
        
        return self._generate_isdistinct_inline(base_expr)
    
    def _generate_isdistinct_inline(self, base_expr: str) -> str:
        """Generate isDistinct() function using inline approach."""
        # isDistinct() returns true if all values in collection are unique (no duplicates)
        # Compares total count with distinct count
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} <= 1 THEN true
                    ELSE (
                        SELECT 
                            COUNT(*) = COUNT(DISTINCT value)
                        FROM {self.generator.dialect.json_each_function}({base_expr}) as table_alias(key, value)
                    )
                END
            ELSE true  -- Single values are always distinct
        END
        """
