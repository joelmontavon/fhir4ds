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
        
    def can_handle(self, function_name: str) -> bool:
        """Check if this handler can process the given function."""
        collection_functions = {
            'exists', 'empty', 'first', 'last', 'count', 'length',
            'select', 'where', 'all', 'distinct', 'single', 'tail',
            'skip', 'take', 'union', 'combine', 'intersect', 'exclude',
            'alltrue', 'allfalse', 'anytrue', 'anyfalse', 'contains'
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
        """Handle single() function - placeholder."""
        # Delegate to main generator for now
        return self.generator._handle_function_fallback('single', base_expr, func_node)
    
    def _handle_tail(self, base_expr: str, func_node) -> str:
        """Handle tail() function - placeholder."""
        # Delegate to main generator for now
        return self.generator._handle_function_fallback('tail', base_expr, func_node)
    
    def _handle_skip(self, base_expr: str, func_node) -> str:
        """Handle skip() function - placeholder."""
        # Delegate to main generator for now
        return self.generator._handle_function_fallback('skip', base_expr, func_node)
    
    def _handle_take(self, base_expr: str, func_node) -> str:
        """Handle take() function - placeholder."""
        # Delegate to main generator for now
        return self.generator._handle_function_fallback('take', base_expr, func_node)
    
    def _handle_union(self, base_expr: str, func_node) -> str:
        """Handle union() function - placeholder."""
        # Delegate to main generator for now
        return self.generator._handle_function_fallback('union', base_expr, func_node)
    
    def _handle_combine(self, base_expr: str, func_node) -> str:
        """Handle combine() function - placeholder."""
        # Delegate to main generator for now
        return self.generator._handle_function_fallback('combine', base_expr, func_node)
    
    def _handle_intersect(self, base_expr: str, func_node) -> str:
        """Handle intersect() function - placeholder."""
        # Delegate to main generator for now
        return self.generator._handle_function_fallback('intersect', base_expr, func_node)
    
    def _handle_exclude(self, base_expr: str, func_node) -> str:
        """Handle exclude() function - placeholder."""
        # Delegate to main generator for now
        return self.generator._handle_function_fallback('exclude', base_expr, func_node)
    
    def _handle_alltrue(self, base_expr: str, func_node) -> str:
        """Handle alltrue() function - placeholder."""
        # Delegate to main generator for now
        return self.generator._handle_function_fallback('alltrue', base_expr, func_node)
    
    def _handle_allfalse(self, base_expr: str, func_node) -> str:
        """Handle allfalse() function - placeholder."""
        # Delegate to main generator for now
        return self.generator._handle_function_fallback('allfalse', base_expr, func_node)
    
    def _handle_anytrue(self, base_expr: str, func_node) -> str:
        """Handle anytrue() function - placeholder."""
        # Delegate to main generator for now
        return self.generator._handle_function_fallback('anytrue', base_expr, func_node)
    
    def _handle_anyfalse(self, base_expr: str, func_node) -> str:
        """Handle anyfalse() function - placeholder."""
        # Delegate to main generator for now
        return self.generator._handle_function_fallback('anyfalse', base_expr, func_node)
    
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