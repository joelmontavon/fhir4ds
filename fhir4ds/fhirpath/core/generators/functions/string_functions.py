"""
String function handlers for FHIRPath expressions.

This module handles string operations like contains, substring, startswith,
endswith, replace, upper/lower case transformations, and other string functions.
"""

from typing import List, Any, Optional
from ..base_handler import BaseFunctionHandler


class StringFunctionHandler(BaseFunctionHandler):
    """Handles string function processing for FHIRPath to SQL conversion."""
    
    def __init__(self, generator, cte_builder=None):
        """
        Initialize the string function handler.
        
        Args:
            generator: Reference to main SQLGenerator for complex operations
            cte_builder: Optional CTEBuilder instance for CTE management
        """
        super().__init__(generator, cte_builder)
        self.generator = generator
        self.dialect = generator.dialect
        
    def get_supported_functions(self) -> List[str]:
        """Return list of string function names this handler supports."""
        return [
            'substring', 'startswith', 'endswith', 'indexof', 
            'replace', 'toupper', 'tolower', 'upper', 'lower', 'trim', 
            'split', 'tochars', 'matches', 'replacematches'
        ]

    def can_handle(self, function_name: str) -> bool:
        """Check if this handler can process the given function."""
        string_functions = {
            'substring', 'startswith', 'endswith', 'indexof', 
            'replace', 'toupper', 'tolower', 'upper', 'lower', 'trim', 
            'split', 'tochars', 'matches', 'replacematches'
        }
        return function_name.lower() in string_functions
    
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
    
    def _handle_substring(self, base_expr: str, func_node) -> str:
        """Handle substring() function."""
        # substring(start, length) function - extract substring from string
        if len(func_node.args) != 2:
            raise ValueError("substring() function requires exactly 2 arguments: start and length")
        
        start_sql = self.generator.visit(func_node.args[0])
        length_sql = self.generator.visit(func_node.args[1])
        
        # PHASE 5D: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'substring'):
            try:
                return self.generator._generate_substring_with_cte(base_expr, start_sql, length_sql)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for substring(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        # Handle the case where base_expr is already a JSON extracted value
        if 'json_extract(' in base_expr and not 'json_extract_string(' in base_expr:
            # Convert json_extract to json_extract_string to remove quotes for string operations
            # This handles the pattern: json_extract(resource, '$.field') -> json_extract_string(resource, '$.field')
            import re
            base_value = re.sub(r'json_extract\(', 'json_extract_string(', base_expr)
        elif 'json_extract_string(' in base_expr:
            # Already a string extraction, use directly
            base_value = base_expr
        else:
            # Extract as string
            base_value = self.generator.extract_json_field(base_expr, '$')
        
        return f"""
        CASE 
            WHEN {base_expr} IS NOT NULL THEN
                {self.dialect.substring(base_value, start_sql, length_sql)}
            ELSE NULL
        END
        """
    
    def _handle_startswith(self, base_expr: str, func_node) -> str:
        """Handle startswith() function."""
        # startsWith(prefix) function - check if string starts with prefix
        if len(func_node.args) != 1:
            raise ValueError("startsWith() function requires exactly 1 argument")
        
        prefix_sql = self.generator.visit(func_node.args[0])
        
        # PHASE 5H: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'startswith'):
            try:
                return self.generator._generate_startswith_with_cte(base_expr, prefix_sql)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for startswith(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        base_value = self._prepare_string_value(base_expr)
        return f"""
        CASE 
            WHEN {base_expr} IS NOT NULL THEN
                CASE WHEN CAST({base_value} AS VARCHAR) LIKE {self.dialect.string_concat(f'CAST({prefix_sql} AS VARCHAR)', "'%'")} THEN true ELSE false END
            ELSE false
        END
        """
    
    def _handle_endswith(self, base_expr: str, func_node) -> str:
        """Handle endswith() function."""
        # endsWith(suffix) function - check if string ends with suffix
        if len(func_node.args) != 1:
            raise ValueError("endsWith() function requires exactly 1 argument")
        
        suffix_sql = self.generator.visit(func_node.args[0])
        
        # PHASE 5I: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'endswith'):
            try:
                return self.generator._generate_endswith_with_cte(base_expr, suffix_sql)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for endswith(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        base_value = self._prepare_string_value(base_expr)
        return f"""
        CASE 
            WHEN {base_expr} IS NOT NULL THEN
                CASE WHEN CAST({base_value} AS VARCHAR) LIKE {self.dialect.string_concat("'%'", f'CAST({suffix_sql} AS VARCHAR)')} THEN true ELSE false END
            ELSE false
        END
        """
    
    def _handle_indexof(self, base_expr: str, func_node) -> str:
        """Handle indexof() function."""
        # indexOf(substring) function - find index of substring
        if len(func_node.args) != 1:
            raise ValueError("indexOf() function requires exactly 1 argument")
        
        search_sql = self.generator.visit(func_node.args[0])
        base_value = self._prepare_string_value(base_expr)
        
        return f"""
        CASE 
            WHEN {base_expr} IS NOT NULL THEN
                {self.dialect.string_position(search_sql, base_value)}
            ELSE -1
        END
        """
    
    def _handle_replace(self, base_expr: str, func_node) -> str:
        """Handle replace() function."""
        # replace(pattern, replacement) function - replace pattern with replacement
        if len(func_node.args) != 2:
            raise ValueError("replace() function requires exactly 2 arguments")
        
        pattern_sql = self.generator.visit(func_node.args[0])
        replacement_sql = self.generator.visit(func_node.args[1])
        base_value = self._prepare_string_value(base_expr)
        
        return f"""
        CASE 
            WHEN {base_expr} IS NOT NULL THEN
                REPLACE({base_value}, {pattern_sql}, {replacement_sql})
            ELSE NULL
        END
        """
    
    def _handle_toupper(self, base_expr: str, func_node) -> str:
        """Handle toupper() function."""
        if len(func_node.args) != 0:
            raise ValueError("toUpper() function takes no arguments")
        
        # Original implementation (must match exact behavior for tests)
        return f"""
        CASE 
            WHEN {base_expr} IS NOT NULL THEN UPPER(CAST({base_expr} AS VARCHAR))
            ELSE NULL
        END
        """
    
    def _handle_tolower(self, base_expr: str, func_node) -> str:
        """Handle tolower() function."""
        if len(func_node.args) != 0:
            raise ValueError("toLower() function takes no arguments")
        
        # Original implementation (must match exact behavior for tests)
        return f"""
        CASE 
            WHEN {base_expr} IS NOT NULL THEN LOWER(CAST({base_expr} AS VARCHAR))
            ELSE NULL
        END
        """
    
    def _handle_upper(self, base_expr: str, func_node) -> str:
        """Handle upper() function."""
        if len(func_node.args) != 0:
            raise ValueError("upper() function takes no arguments")
        
        # Original implementation (must match exact behavior for tests)
        return f"""
        CASE 
            WHEN {base_expr} IS NOT NULL THEN UPPER(CAST({base_expr} AS VARCHAR))
            ELSE NULL
        END
        """
    
    def _handle_lower(self, base_expr: str, func_node) -> str:
        """Handle lower() function."""
        if len(func_node.args) != 0:
            raise ValueError("lower() function takes no arguments")
        
        # Original implementation (must match exact behavior for tests)
        return f"""
        CASE 
            WHEN {base_expr} IS NOT NULL THEN LOWER(CAST({base_expr} AS VARCHAR))
            ELSE NULL
        END
        """
    
    def _handle_trim(self, base_expr: str, func_node) -> str:
        """Handle trim() function."""
        if len(func_node.args) != 0:
            raise ValueError("trim() function takes no arguments")
        
        # Original implementation (must match exact behavior for tests)
        return f"""
        CASE 
            WHEN {base_expr} IS NOT NULL THEN TRIM(CAST({base_expr} AS VARCHAR))
            ELSE NULL
        END
        """
    
    def _handle_split(self, base_expr: str, func_node) -> str:
        """Handle split() function."""
        # split(separator) function - split string into array
        if len(func_node.args) != 1:
            raise ValueError("split() function requires exactly 1 argument")
        
        separator_sql = self.generator.visit(func_node.args[0])
        base_value = self._prepare_string_value(base_expr)
        
        return f"""
        CASE 
            WHEN {base_expr} IS NOT NULL THEN
                {self.dialect.split_string(base_value, separator_sql)}
            ELSE {self.dialect.json_array_function}()
        END
        """
    
    def _handle_tochars(self, base_expr: str, func_node) -> str:
        """
        Handle toChars() function - converts string into collection of individual characters.
        
        Args:
            base_expr: SQL expression for the base string
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns collection of individual characters
            
        Raises:
            ValueError: If function has any arguments (should be no arguments)
        """
        # Validate no arguments
        if hasattr(func_node, 'args') and len(func_node.args) > 0:
            raise ValueError("toChars() function takes no arguments")
        
        return self._generate_tochars_inline(base_expr)
    
    def _generate_tochars_inline(self, base_expr: str) -> str:
        """Generate toChars() function using inline approach."""
        # toChars() converts string into collection of individual characters
        # Use dialect-specific string splitting and character extraction
        
        # Prepare the string value for character extraction
        string_value = self._prepare_string_value(base_expr)
        
        if self.dialect.name == "POSTGRESQL":
            # PostgreSQL approach using string_to_array and unnest
            return f"""
            CASE 
                WHEN {string_value} IS NULL THEN {self.dialect.json_array_function}()
                ELSE (
                    SELECT {self.generator.aggregate_to_json_array('char_value')}
                    FROM (
                        SELECT regexp_split_to_table(CAST({string_value} AS TEXT), '') AS char_value
                        WHERE char_value != ''
                    ) chars_table
                )
            END
            """
        else:  # DuckDB and others
            # DuckDB approach using string splitting and array generation
            return f"""
            CASE 
                WHEN {string_value} IS NULL THEN {self.dialect.json_array_function}()
                WHEN LENGTH(CAST({string_value} AS VARCHAR)) = 0 THEN {self.dialect.json_array_function}()
                ELSE (
                    SELECT {self.generator.aggregate_to_json_array('char_value')}
                    FROM (
                        SELECT substr(CAST({string_value} AS VARCHAR), generate_series.num, 1) AS char_value
                        FROM generate_series(1, LENGTH(CAST({string_value} AS VARCHAR))) AS generate_series(num)
                    ) chars_table
                )
            END
            """
    
    def _handle_matches(self, base_expr: str, func_node) -> str:
        """
        Handle matches(regex) function - returns true if string matches regular expression.
        
        Args:
            base_expr: SQL expression for the base string
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns true if string matches regex pattern
            
        Raises:
            ValueError: If function doesn't have exactly one argument
        """
        # Validate exactly one argument
        if not hasattr(func_node, 'args') or len(func_node.args) != 1:
            raise ValueError("matches() function requires exactly one argument")
        
        # Get the regex pattern argument
        pattern_sql = self.generator.visit(func_node.args[0])
        
        return self._generate_matches_inline(base_expr, pattern_sql)
    
    def _generate_matches_inline(self, base_expr: str, pattern_sql: str) -> str:
        """Generate matches() function using inline approach."""
        # matches(regex) returns true if string matches regular expression pattern
        # Use dialect-specific regex matching
        
        # Prepare the string value for regex matching
        string_value = self._prepare_string_value(base_expr)
        
        if self.dialect.name == "POSTGRESQL":
            # PostgreSQL uses ~ operator for regex matching
            return f"""
            CASE 
                WHEN {string_value} IS NULL OR {pattern_sql} IS NULL THEN NULL
                ELSE (CAST({string_value} AS TEXT) ~ CAST({pattern_sql} AS TEXT))
            END
            """
        else:  # DuckDB and others
            # DuckDB uses regexp_matches function
            return f"""
            CASE 
                WHEN {string_value} IS NULL OR {pattern_sql} IS NULL THEN NULL
                ELSE regexp_matches(CAST({string_value} AS VARCHAR), CAST({pattern_sql} AS VARCHAR))
            END
            """
    
    def _handle_replacematches(self, base_expr: str, func_node) -> str:
        """
        Handle replaceMatches(regex, replacement) function - replaces all regex matches with replacement.
        
        Args:
            base_expr: SQL expression for the base string
            func_node: AST node for the function call
            
        Returns:
            SQL expression that returns string with regex matches replaced
            
        Raises:
            ValueError: If function doesn't have exactly two arguments
        """
        # Validate exactly two arguments
        if not hasattr(func_node, 'args') or len(func_node.args) != 2:
            raise ValueError("replaceMatches() function requires exactly two arguments")
        
        # Get the regex pattern and replacement arguments
        pattern_sql = self.generator.visit(func_node.args[0])
        replacement_sql = self.generator.visit(func_node.args[1])
        
        return self._generate_replacematches_inline(base_expr, pattern_sql, replacement_sql)
    
    def _generate_replacematches_inline(self, base_expr: str, pattern_sql: str, replacement_sql: str) -> str:
        """Generate replaceMatches() function using inline approach."""
        # replaceMatches(regex, replacement) replaces all regex matches with replacement string
        # Use dialect-specific regex replacement
        
        # Prepare the string value for regex replacement
        string_value = self._prepare_string_value(base_expr)
        
        if self.dialect.name == "POSTGRESQL":
            # PostgreSQL uses regexp_replace function
            return f"""
            CASE 
                WHEN {string_value} IS NULL THEN NULL
                WHEN {pattern_sql} IS NULL OR {replacement_sql} IS NULL THEN {string_value}
                ELSE regexp_replace(CAST({string_value} AS TEXT), CAST({pattern_sql} AS TEXT), CAST({replacement_sql} AS TEXT), 'g')
            END
            """
        else:  # DuckDB and others
            # DuckDB uses regexp_replace function (similar to PostgreSQL)
            return f"""
            CASE 
                WHEN {string_value} IS NULL THEN NULL
                WHEN {pattern_sql} IS NULL OR {replacement_sql} IS NULL THEN {string_value}
                ELSE regexp_replace(CAST({string_value} AS VARCHAR), CAST({pattern_sql} AS VARCHAR), CAST({replacement_sql} AS VARCHAR), 'g')
            END
            """
    
    def _prepare_string_value(self, base_expr: str) -> str:
        """
        Prepare a base expression for string operations by ensuring it's extracted as a string.
        
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
            # Extract as string
            return self.generator.extract_json_field(base_expr, '$')