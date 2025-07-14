"""
Collection function handlers for FHIRPath expressions - CTEBuilder Integration.

This module handles collection operations like exists, empty, first, last,
count, select, where, and other collection manipulation functions
using the new CTEBuilder architecture for optimal CTE management.
"""

from typing import List, Any, Optional
from ..base_handler import BaseFunctionHandler


class CollectionFunctionHandler(BaseFunctionHandler):
    """
    Handles collection function processing for FHIRPath to SQL conversion.
    
    This updated version inherits from BaseFunctionHandler to leverage the
    CTEBuilder system for optimal CTE generation and management.
    """
    
    def get_supported_functions(self) -> List[str]:
        """Return list of function names this handler supports."""
        return [
            'exists', 'empty', 'first', 'last', 'count', 'length',
            'select', 'where', 'all', 'distinct', 'single', 'tail',
            'skip', 'take', 'union', 'combine', 'intersect', 'exclude',
            'alltrue', 'allfalse', 'anytrue', 'anyfalse', 'contains'
        ]
    
    def can_handle(self, function_name: str) -> bool:
        """Check if this handler can process the given function."""
        return function_name.lower() in self.get_supported_functions()
    
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
        # Validate function call structure
        self.validate_function_call(func_name, func_node)
        
        func_name = func_name.lower()
        
        if func_name == 'where':
            return self._handle_where(base_expr, func_node)
        elif func_name == 'select':
            return self._handle_select(base_expr, func_node)
        elif func_name == 'first':
            return self._handle_first(base_expr, func_node)
        elif func_name == 'exists':
            return self._handle_exists(base_expr, func_node)
        # Add other functions as we migrate them
        else:
            # For now, delegate to original handler for unmigrated functions
            return self.generator._handle_function_fallback(func_name, base_expr, func_node)
    
    def _handle_where(self, base_expr: str, func_node) -> str:
        """
        Handle where() function with CTEBuilder support.
        
        The where() function filters collections based on a condition.
        This implementation uses the CTEBuilder system for complex expressions
        and falls back to inline SQL for simple cases.
        """
        if not hasattr(func_node, 'args') or len(func_node.args) == 0:
            # No condition provided, return original expression
            return base_expr
        
        # Set context for WHERE clause generation
        old_context = getattr(self.generator, 'in_where_context', False)
        self.generator.in_where_context = True
        
        try:
            # Create a nested generator for processing the where condition
            # In where context, we operate on array elements using 'value' from json_each
            nested_generator = type(self.generator)(
                table_name=self.generator.table_name,
                json_column="value",
                resource_type=getattr(self.generator, 'resource_type', None),
                dialect=self.generator.dialect,
                use_new_cte_system=hasattr(self.generator, 'cte_builder') and self.generator.cte_builder is not None
            )
            
            # Configure nested generator for WHERE context
            nested_generator.in_where_context = True
            
            # If using new CTE system, disable CTE in nested generator to force inline expressions
            # This ensures functions like startsWith generate LIKE directly instead of CTE references
            if hasattr(nested_generator, 'cte_builder') and nested_generator.cte_builder is not None:
                # Save original state
                original_enable_cte = getattr(nested_generator, 'enable_cte', True)
                nested_generator.enable_cte = False
                
                # Also disable on function handlers if they exist
                for handler_name in ['string_handler', 'collection_handler', 'math_handler']:
                    handler = getattr(nested_generator, handler_name, None)
                    if handler and hasattr(handler, 'generator'):
                        handler.generator.enable_cte = False
                        handler.generator.in_where_context = True
            
            # Process the where condition
            where_condition = nested_generator.visit(func_node.args[0])
            
            # Merge CTEs and complex expressions from nested generator if using legacy system
            if not hasattr(self.generator, 'cte_builder') or self.generator.cte_builder is None:
                # Legacy CTE system - merge CTEs
                if hasattr(nested_generator, 'ctes'):
                    for cte_name, cte_def in nested_generator.ctes.items():
                        if cte_name not in self.generator.ctes:
                            self.generator.ctes[cte_name] = cte_def
                
                # Merge complex expression cache
                if hasattr(nested_generator, 'complex_expr_cache'):
                    for placeholder, expr in nested_generator.complex_expr_cache.items():
                        if placeholder not in self.generator.complex_expr_cache:
                            self.generator.complex_expr_cache[placeholder] = expr
            
            # Generate the WHERE clause SQL
            where_sql = f"""
            (SELECT {self.generator.aggregate_to_json_array('value')}
             FROM json_each({base_expr}) 
             WHERE {where_condition})
            """
            
            # Use CTEBuilder if beneficial for complex expressions
            if self.should_use_cte(base_expr, 'where'):
                # Generate CTE SQL for where operation
                cte_sql = self.generate_cte_sql(
                    operation='where',
                    main_logic=f"({where_sql}) as where_result"
                )
                
                # Create CTE and return reference
                return self.create_cte_if_beneficial(
                    operation='where_filter',
                    sql=cte_sql,
                    result_column='where_result'
                )
            else:
                # Return inline expression for simple cases
                return where_sql.strip()
                
        finally:
            # Restore original context
            self.generator.in_where_context = old_context
    
    def _handle_select(self, base_expr: str, func_node) -> str:
        """
        Handle select() function with CTEBuilder support.
        
        The select() function transforms each element in a collection.
        """
        if not hasattr(func_node, 'args') or len(func_node.args) == 0:
            # No transformation expression provided, return original
            return base_expr
        
        # Generate expression for array elements (uses 'value' from json_each)
        array_element_expression_generator = type(self.generator)(
            table_name=f"json_each({base_expr})",
            json_column="value",
            resource_type=getattr(self.generator, 'resource_type', None),
            dialect=self.generator.dialect,
            use_new_cte_system=hasattr(self.generator, 'cte_builder') and self.generator.cte_builder is not None
        )
        
        # Disable CTEs in nested generator for simpler expressions
        if hasattr(array_element_expression_generator, 'cte_builder'):
            array_element_expression_generator.enable_cte = False
        
        # Configure for minimal complexity to avoid undefined variables
        array_element_expression_generator.max_sql_complexity = 0
        if hasattr(array_element_expression_generator, 'complex_expr_cache'):
            array_element_expression_generator.complex_expr_cache = {}
        
        array_element_expression = array_element_expression_generator.visit(func_node.args[0])
        
        # Generate expression for non-array case (reuse same expression)
        non_array_element_expression_generator = type(self.generator)(
            table_name=self.generator.table_name,
            json_column=self.generator.json_column,
            resource_type=getattr(self.generator, 'resource_type', None),
            dialect=self.generator.dialect,
            use_new_cte_system=hasattr(self.generator, 'cte_builder') and self.generator.cte_builder is not None
        )
        
        # Configure similar to array case
        if hasattr(non_array_element_expression_generator, 'cte_builder'):
            non_array_element_expression_generator.enable_cte = False
        
        non_array_element_expression_generator.max_sql_complexity = 0
        if hasattr(non_array_element_expression_generator, 'complex_expr_cache'):
            non_array_element_expression_generator.complex_expr_cache = {}
        
        non_array_element_expression = non_array_element_expression_generator.visit(func_node.args[0])
        
        # Merge CTEs from nested generators if using legacy system
        if not hasattr(self.generator, 'cte_builder') or self.generator.cte_builder is None:
            for nested_gen in [array_element_expression_generator, non_array_element_expression_generator]:
                if hasattr(nested_gen, 'ctes'):
                    for cte_name, cte_def in nested_gen.ctes.items():
                        if cte_name not in self.generator.ctes:
                            self.generator.ctes[cte_name] = cte_def
                
                if hasattr(nested_gen, 'complex_expr_cache'):
                    for placeholder, expr in nested_gen.complex_expr_cache.items():
                        if placeholder not in self.generator.complex_expr_cache:
                            self.generator.complex_expr_cache[placeholder] = expr
        
        # Generate the SELECT clause SQL
        select_sql = f"""
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
        
        # Use CTEBuilder if beneficial for complex expressions
        if self.should_use_cte(base_expr, 'select'):
            # Generate CTE SQL for select operation
            cte_sql = self.generate_cte_sql(
                operation='select',
                main_logic=f"({select_sql}) as select_result"
            )
            
            return self.create_cte_if_beneficial(
                operation='select_transform',
                sql=cte_sql,
                result_column='select_result'
            )
        else:
            return select_sql.strip()
    
    def _handle_first(self, base_expr: str, func_node) -> str:
        """
        Handle first() function with CTEBuilder support.
        
        Returns the first element of a collection.
        """
        # Generate first() logic
        first_logic = f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            ELSE COALESCE({self.generator.extract_json_object(base_expr, '$[0]')}, {base_expr})
        END
        """
        
        # Use CTEBuilder if beneficial for complex expressions
        if self.should_use_cte(base_expr, 'first'):
            # Generate CTE SQL for first operation
            cte_sql = self.generate_cte_sql(
                operation='first',
                main_logic=f"({first_logic}) as first_result"
            )
            
            return self.create_cte_if_beneficial(
                operation='first_element',
                sql=cte_sql,
                result_column='first_result'
            )
        else:
            return first_logic.strip()
    
    def _handle_exists(self, base_expr: str, func_node) -> str:
        """
        Handle exists() function with CTEBuilder support.
        
        Checks whether a collection has any elements, optionally with a condition.
        """
        if not hasattr(func_node, 'args') or len(func_node.args) == 0:
            # Simple exists() - check if collection has elements
            exists_logic = f"""
            CASE 
                WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN {self.generator.get_json_array_length(base_expr)} > 0
                ELSE ({base_expr} IS NOT NULL AND NOT ({self.generator.get_json_type(base_expr)} = 'OBJECT' AND {self.generator.get_json_array_length(f"{self.dialect.json_extract_function}({base_expr}, '$.keys()')")} = 0))
            END
            """
        else:
            # exists(criteria) - equivalent to (collection.where(criteria)).exists()
            # Create a mock function node for where clause
            where_node = type('MockNode', (), {'args': [func_node.args[0]]})()
            sql_after_where = self._handle_where(base_expr, where_node)
            
            # Then apply simple exists() to the result
            exists_node = type('MockNode', (), {'args': []})()
            return self._handle_exists(f"({sql_after_where})", exists_node)
        
        # Use CTEBuilder if beneficial for complex expressions
        if self.should_use_cte(base_expr, 'exists'):
            # Generate CTE SQL for exists operation
            cte_sql = self.generate_cte_sql(
                operation='exists',
                main_logic=f"({exists_logic}) as exists_result"
            )
            
            return self.create_cte_if_beneficial(
                operation='exists_check',
                sql=cte_sql,
                result_column='exists_result'
            )
        else:
            return exists_logic.strip()