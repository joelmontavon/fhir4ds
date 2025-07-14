"""
String function handlers for FHIRPath expressions - CTEBuilder Integration.

This module handles string operations like contains, substring, startswith,
endswith, replace, upper/lower case transformations, and other string functions
using the new CTEBuilder architecture for optimal CTE management.
"""

from typing import List, Any, Optional
from ..base_handler import BaseFunctionHandler


class StringFunctionHandler(BaseFunctionHandler):
    """
    Handles string function processing for FHIRPath to SQL conversion.
    
    This updated version inherits from BaseFunctionHandler to leverage the
    CTEBuilder system for optimal CTE generation and management.
    """
    
    def get_supported_functions(self) -> List[str]:
        """Return list of function names this handler supports."""
        return [
            'substring', 'startswith', 'endswith', 'indexof', 
            'replace', 'toupper', 'tolower', 'upper', 'lower', 'trim', 
            'split', 'tochars', 'matches', 'replacematches'
        ]
    
    def can_handle(self, function_name: str) -> bool:
        """Check if this handler can process the given function."""
        return function_name.lower() in self.get_supported_functions()
    
    def handle_function(self, func_name: str, base_expr: str, func_node) -> str:
        """
        Handle string function and return SQL expression.
        
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
        
        if func_name == 'substring':
            return self._handle_substring(base_expr, func_node)
        elif func_name == 'startswith':
            return self._handle_startswith(base_expr, func_node)
        elif func_name == 'endswith':
            return self._handle_endswith(base_expr, func_node)
        elif func_name == 'indexof':
            return self._handle_indexof(base_expr, func_node)
        elif func_name == 'replace':
            return self._handle_replace(base_expr, func_node)
        elif func_name == 'toupper':
            return self._handle_toupper(base_expr, func_node)
        elif func_name == 'tolower':
            return self._handle_tolower(base_expr, func_node)
        elif func_name == 'upper':
            return self._handle_upper(base_expr, func_node)
        elif func_name == 'lower':
            return self._handle_lower(base_expr, func_node)
        elif func_name == 'trim':
            return self._handle_trim(base_expr, func_node)
        elif func_name == 'split':
            return self._handle_split(base_expr, func_node)
        elif func_name == 'tochars':
            return self._handle_tochars(base_expr, func_node)
        elif func_name == 'matches':
            return self._handle_matches(base_expr, func_node)
        elif func_name == 'replacematches':
            return self._handle_replacematches(base_expr, func_node)
        else:
            raise ValueError(f"Unsupported string function: {func_name}")
    
    def _handle_startswith(self, base_expr: str, func_node) -> str:
        """
        Handle startswith() function with CTEBuilder support.
        
        This implementation uses the CTEBuilder system for complex expressions
        and falls back to inline SQL for simple cases.
        """
        # Validate and process arguments
        args = self.handle_function_with_args('startsWith', base_expr, func_node, 1)
        prefix_sql = args[0]
        
        # Prepare base value for string operations
        base_value = self._prepare_string_value(base_expr)
        
        # Generate the LIKE-based comparison logic
        startswith_logic = f"""
        CASE WHEN CAST({base_value} AS VARCHAR) LIKE {self.dialect.string_concat(f'CAST({prefix_sql} AS VARCHAR)', "'%'")} THEN true ELSE false END
        """
        
        # Create full conditional SQL with null handling
        conditional_sql = self.create_case_expression(
            condition=startswith_logic.strip(),
            true_value="true",
            false_value="false", 
            null_check=f"{base_expr} IS NOT NULL"
        )
        
        # Use CTEBuilder if beneficial for complex expressions
        if self.should_use_cte(base_expr, 'startswith'):
            # Generate CTE SQL for startsWith operation
            cte_sql = self.generate_cte_sql(
                operation='startswith',
                main_logic=f"{conditional_sql} as startswith_result"
            )
            
            # Create CTE and return reference
            return self.create_cte_if_beneficial(
                operation='startswith_check',
                sql=cte_sql,
                result_column='startswith_result'
            )
        else:
            # Return inline expression for simple cases
            return conditional_sql
    
    def _handle_endswith(self, base_expr: str, func_node) -> str:
        """
        Handle endswith() function with CTEBuilder support.
        
        Similar to startsWith but checks suffix instead of prefix.
        """
        # Validate and process arguments
        args = self.handle_function_with_args('endsWith', base_expr, func_node, 1)
        suffix_sql = args[0]
        
        # Prepare base value for string operations
        base_value = self._prepare_string_value(base_expr)
        
        # Generate the LIKE-based comparison logic for suffix
        endswith_logic = f"""
        CASE WHEN CAST({base_value} AS VARCHAR) LIKE {self.dialect.string_concat("'%'", f'CAST({suffix_sql} AS VARCHAR)')} THEN true ELSE false END
        """
        
        # Create full conditional SQL with null handling
        conditional_sql = self.create_case_expression(
            condition=endswith_logic.strip(),
            true_value="true",
            false_value="false",
            null_check=f"{base_expr} IS NOT NULL"
        )
        
        # Use CTEBuilder if beneficial for complex expressions
        if self.should_use_cte(base_expr, 'endswith'):
            # Generate CTE SQL for endsWith operation
            cte_sql = self.generate_cte_sql(
                operation='endswith',
                main_logic=f"{conditional_sql} as endswith_result"
            )
            
            # Create CTE and return reference
            return self.create_cte_if_beneficial(
                operation='endswith_check',
                sql=cte_sql,
                result_column='endswith_result'
            )
        else:
            # Return inline expression for simple cases
            return conditional_sql
    
    def _handle_substring(self, base_expr: str, func_node) -> str:
        """Handle substring() function with CTEBuilder support."""
        # Validate and process arguments
        args = self.handle_function_with_args('substring', base_expr, func_node, 2)
        start_sql, length_sql = args
        
        # Prepare base value for string operations
        base_value = self._prepare_string_value(base_expr)
        
        # Generate substring logic using dialect-specific function
        substring_logic = self.dialect.substring(base_value, start_sql, length_sql)
        
        # Create conditional SQL with null handling
        conditional_sql = self.create_case_expression(
            condition=f"{base_expr} IS NOT NULL",
            true_value=substring_logic,
            false_value="NULL"
        )
        
        # Use CTEBuilder if beneficial for complex expressions
        if self.should_use_cte(base_expr, 'substring'):
            # Generate CTE SQL for substring operation
            cte_sql = self.generate_cte_sql(
                operation='substring',
                main_logic=f"{conditional_sql} as substring_result"
            )
            
            return self.create_cte_if_beneficial(
                operation='substring_op',
                sql=cte_sql,
                result_column='substring_result'
            )
        else:
            return conditional_sql
    
    def _handle_indexof(self, base_expr: str, func_node) -> str:
        """Handle indexof() function."""
        # Validate and process arguments
        args = self.handle_function_with_args('indexOf', base_expr, func_node, 1)
        search_sql = args[0]
        
        base_value = self._prepare_string_value(base_expr)
        
        # Use dialect-specific string position function
        position_logic = self.dialect.string_position(search_sql, base_value)
        
        return self.create_case_expression(
            condition=f"{base_expr} IS NOT NULL",
            true_value=position_logic,
            false_value="-1"
        )
    
    def _handle_replace(self, base_expr: str, func_node) -> str:
        """Handle replace() function."""
        # Validate and process arguments
        args = self.handle_function_with_args('replace', base_expr, func_node, 2)
        pattern_sql, replacement_sql = args
        
        base_value = self._prepare_string_value(base_expr)
        
        # Use SQL REPLACE function
        replace_logic = f"REPLACE({base_value}, {pattern_sql}, {replacement_sql})"
        
        return self.create_case_expression(
            condition=f"{base_expr} IS NOT NULL",
            true_value=replace_logic,
            false_value="NULL"
        )
    
    def _handle_toupper(self, base_expr: str, func_node) -> str:
        """Handle toupper() function."""
        if len(func_node.args) != 0:
            raise ValueError("toUpper() function takes no arguments")
        
        # Simple transformation - use inline expression
        return self.create_case_expression(
            condition=f"{base_expr} IS NOT NULL",
            true_value=f"UPPER(CAST({base_expr} AS VARCHAR))",
            false_value="NULL"
        )
    
    def _handle_tolower(self, base_expr: str, func_node) -> str:
        """Handle tolower() function."""
        if len(func_node.args) != 0:
            raise ValueError("toLower() function takes no arguments")
        
        # Simple transformation - use inline expression
        return self.create_case_expression(
            condition=f"{base_expr} IS NOT NULL",
            true_value=f"LOWER(CAST({base_expr} AS VARCHAR))",
            false_value="NULL"
        )
    
    def _handle_upper(self, base_expr: str, func_node) -> str:
        """Handle upper() function."""
        if len(func_node.args) != 0:
            raise ValueError("upper() function takes no arguments")
        
        # Simple transformation - use inline expression
        return self.create_case_expression(
            condition=f"{base_expr} IS NOT NULL",
            true_value=f"UPPER(CAST({base_expr} AS VARCHAR))",
            false_value="NULL"
        )
    
    def _handle_lower(self, base_expr: str, func_node) -> str:
        """Handle lower() function."""
        if len(func_node.args) != 0:
            raise ValueError("lower() function takes no arguments")
        
        # Simple transformation - use inline expression
        return self.create_case_expression(
            condition=f"{base_expr} IS NOT NULL",
            true_value=f"LOWER(CAST({base_expr} AS VARCHAR))",
            false_value="NULL"
        )
    
    def _handle_trim(self, base_expr: str, func_node) -> str:
        """Handle trim() function."""
        if len(func_node.args) != 0:
            raise ValueError("trim() function takes no arguments")
        
        # Simple transformation - use inline expression
        return self.create_case_expression(
            condition=f"{base_expr} IS NOT NULL",
            true_value=f"TRIM(CAST({base_expr} AS VARCHAR))",
            false_value="NULL"
        )
    
    def _handle_split(self, base_expr: str, func_node) -> str:
        """Handle split() function."""
        # Validate and process arguments
        args = self.handle_function_with_args('split', base_expr, func_node, 1)
        separator_sql = args[0]
        
        base_value = self._prepare_string_value(base_expr)
        
        # Use dialect-specific split function
        split_logic = self.dialect.split_string(base_value, separator_sql)
        
        return self.create_case_expression(
            condition=f"{base_expr} IS NOT NULL",
            true_value=split_logic,
            false_value=f"{self.dialect.json_array_function}()"
        )
    
    def _handle_tochars(self, base_expr: str, func_node) -> str:
        """Handle tochars() function - placeholder implementation."""
        # For now, delegate to generator fallback
        # TODO: Implement proper tochars logic
        return self.generator._handle_function_fallback('tochars', base_expr, func_node)
    
    def _handle_matches(self, base_expr: str, func_node) -> str:
        """Handle matches() function - placeholder implementation."""
        # For now, delegate to generator fallback
        # TODO: Implement proper regex matching logic
        return self.generator._handle_function_fallback('matches', base_expr, func_node)
    
    def _handle_replacematches(self, base_expr: str, func_node) -> str:
        """Handle replacematches() function - placeholder implementation."""
        # For now, delegate to generator fallback
        # TODO: Implement proper regex replacement logic
        return self.generator._handle_function_fallback('replacematches', base_expr, func_node)
    
    def _prepare_string_value(self, base_expr: str) -> str:
        """
        Prepare a base expression for string operations by ensuring it's extracted as a string.
        
        This method handles the conversion from JSON extraction to string values
        needed for string operations like LIKE, UPPER, etc.
        
        Args:
            base_expr: Base SQL expression
            
        Returns:
            SQL expression that yields a string value
        """
        # Handle the case where base_expr is already a JSON extracted value
        if 'json_extract(' in base_expr and not 'json_extract_string(' in base_expr:
            # Convert json_extract to json_extract_string to remove quotes for string operations
            import re
            return re.sub(r'json_extract\(', 'json_extract_string(', base_expr)
        elif 'json_extract_string(' in base_expr:
            # Already a string extraction, use directly
            return base_expr
        else:
            # Extract as string using generator method
            return self.generator.extract_json_field(base_expr, '$')