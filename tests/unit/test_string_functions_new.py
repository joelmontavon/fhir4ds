"""
Unit tests for the new CTEBuilder-aware StringFunctionHandler.

Tests the migrated string function handler that inherits from BaseFunctionHandler
and uses the CTEBuilder system for optimal CTE management.
"""

import pytest
from unittest.mock import Mock, MagicMock
from fhir4ds.fhirpath.core.generators.functions.string_functions_new import StringFunctionHandler
from fhir4ds.fhirpath.core.cte_builder import CTEBuilder


class TestStringFunctionHandlerBasic:
    """Test basic functionality of the new string function handler."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = Mock()
        self.generator.table_name = "fhir_resources"
        self.generator.enable_cte = True
        
        # Mock dialect
        self.dialect = Mock()
        self.dialect.string_concat = Mock(side_effect=lambda a, b: f"CONCAT({a}, {b})")
        self.dialect.substring = Mock(side_effect=lambda base, start, length: f"SUBSTRING({base}, {start}, {length})")
        self.dialect.string_position = Mock(side_effect=lambda search, base: f"POSITION({search} IN {base})")
        self.dialect.split_string = Mock(side_effect=lambda base, sep: f"SPLIT({base}, {sep})")
        self.dialect.json_array_function = Mock(return_value="JSON_ARRAY")
        self.generator.dialect = self.dialect
        
        # Mock generator methods
        self.generator.visit = Mock(side_effect=lambda x: f"VISITED({x})")
        self.generator.extract_json_field = Mock(side_effect=lambda expr, path: f"JSON_EXTRACT_STRING({expr}, '{path}')")
        self.generator._handle_function_fallback = Mock(return_value="FALLBACK_SQL")
        
        # Create CTEBuilder
        self.cte_builder = CTEBuilder()
        
        # Create handler
        self.handler = StringFunctionHandler(self.generator, self.cte_builder)
    
    def test_get_supported_functions(self):
        """Test that handler reports correct supported functions."""
        supported = self.handler.get_supported_functions()
        
        expected_functions = [
            'substring', 'startswith', 'endswith', 'indexof', 
            'replace', 'toupper', 'tolower', 'upper', 'lower', 'trim', 
            'split', 'tochars', 'matches', 'replacematches'
        ]
        
        assert set(supported) == set(expected_functions)
    
    def test_can_handle_supported_functions(self):
        """Test can_handle method for supported functions."""
        assert self.handler.can_handle('startswith') is True
        assert self.handler.can_handle('ENDSWITH') is True  # Case insensitive
        assert self.handler.can_handle('substring') is True
        assert self.handler.can_handle('unsupported') is False
    
    def test_validate_function_call_with_valid_node(self):
        """Test function call validation with valid node."""
        func_node = Mock()
        func_node.args = ['arg1', 'arg2']
        
        # Should not raise exception
        self.handler.validate_function_call('test', func_node)
    
    def test_validate_function_call_with_invalid_node(self):
        """Test function call validation with invalid node."""
        # Test with None node
        with pytest.raises(ValueError, match="Function node is required"):
            self.handler.validate_function_call('test', None)
        
        # Test with node missing args
        func_node = Mock(spec=[])
        with pytest.raises(ValueError, match="Function node missing args attribute"):
            self.handler.validate_function_call('test', func_node)


class TestStringFunctionHandlerStartsWith:
    """Test startsWith() function implementation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = Mock()
        self.generator.table_name = "fhir_resources"
        self.generator.enable_cte = True
        
        # Mock dialect
        self.dialect = Mock()
        self.dialect.string_concat = Mock(side_effect=lambda a, b: f"CONCAT({a}, {b})")
        self.generator.dialect = self.dialect
        
        # Mock generator methods
        self.generator.visit = Mock(side_effect=lambda x: f"'{x}'")
        self.generator.extract_json_field = Mock(side_effect=lambda expr, path: f"JSON_EXTRACT_STRING({expr}, '{path}')")
        
        # Create CTEBuilder
        self.cte_builder = CTEBuilder()
        
        # Create handler
        self.handler = StringFunctionHandler(self.generator, self.cte_builder)
    
    def test_startswith_inline_simple_expression(self):
        """Test startsWith with simple expression (should use inline SQL)."""
        # Mock function node
        func_node = Mock()
        func_node.args = ['John']
        
        base_expr = "name"
        result = self.handler._handle_startswith(base_expr, func_node)
        
        # Should return inline CASE expression
        assert "CASE" in result
        assert "WHEN name IS NOT NULL THEN" in result
        assert "LIKE" in result
        assert "CONCAT" in result and "'John'" in result and "'%'" in result
        assert "false" in result
        
        # Should not create any CTEs for simple expression
        assert self.cte_builder.get_cte_count() == 0
    
    def test_startswith_cte_complex_expression(self):
        """Test startsWith with complex expression (should use CTE)."""
        # Mock function node
        func_node = Mock()
        func_node.args = ['prefix']
        
        # Complex base expression that should trigger CTE
        # Make it extra complex to ensure CTE threshold is met
        base_expr = """
        CASE 
            WHEN json_type(json_extract(resource, '$.name')) = 'ARRAY' THEN
                (SELECT json_group_array(
                    json_extract(value, '$.family')
                ) FROM json_each(json_extract(resource, '$.name'), '$'))
            ELSE json_extract(resource, '$.name.family')
        END
        """
        
        result = self.handler._handle_startswith(base_expr, func_node)
        
        # Should create CTE and return reference
        # The exact count may vary based on complexity scoring
        assert self.cte_builder.get_cte_count() >= 1
        assert "(SELECT" in result  # Should be a CTE reference
    
    def test_startswith_argument_validation(self):
        """Test startsWith argument validation."""
        # Test with wrong number of arguments
        func_node = Mock()
        func_node.args = ['arg1', 'arg2']  # Too many args
        
        with pytest.raises(ValueError, match="startsWith\\(\\) function requires exactly 1 argument"):
            self.handler._handle_startswith("base", func_node)
        
        # Test with no arguments
        func_node.args = []
        with pytest.raises(ValueError, match="startsWith\\(\\) function requires exactly 1 argument"):
            self.handler._handle_startswith("base", func_node)
    
    def test_startswith_string_preparation(self):
        """Test string value preparation in startsWith."""
        func_node = Mock()
        func_node.args = ['test']
        
        # Test with json_extract expression
        base_expr = "json_extract(resource, '$.name')"
        result = self.handler._handle_startswith(base_expr, func_node)
        
        # Should convert json_extract to json_extract_string for string operations
        assert "json_extract_string" in result or "JSON_EXTRACT_STRING" in result


class TestStringFunctionHandlerEndsWith:
    """Test endsWith() function implementation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = Mock()
        self.generator.table_name = "fhir_resources"
        self.generator.enable_cte = True
        
        # Mock dialect
        self.dialect = Mock()
        self.dialect.string_concat = Mock(side_effect=lambda a, b: f"CONCAT({a}, {b})")
        self.generator.dialect = self.dialect
        
        # Mock generator methods
        self.generator.visit = Mock(side_effect=lambda x: f"'{x}'")
        self.generator.extract_json_field = Mock(side_effect=lambda expr, path: f"JSON_EXTRACT_STRING({expr}, '{path}')")
        
        # Create CTEBuilder
        self.cte_builder = CTEBuilder()
        
        # Create handler
        self.handler = StringFunctionHandler(self.generator, self.cte_builder)
    
    def test_endswith_inline_simple_expression(self):
        """Test endsWith with simple expression (should use inline SQL)."""
        # Mock function node
        func_node = Mock()
        func_node.args = ['suffix']
        
        base_expr = "name"
        result = self.handler._handle_endswith(base_expr, func_node)
        
        # Should return inline CASE expression
        assert "CASE" in result
        assert "WHEN name IS NOT NULL THEN" in result
        assert "LIKE" in result
        assert "CONCAT" in result and "'%'" in result and "'suffix'" in result
        assert "false" in result
        
        # Should not create any CTEs for simple expression
        assert self.cte_builder.get_cte_count() == 0
    
    def test_endswith_argument_validation(self):
        """Test endsWith argument validation."""
        # Test with wrong number of arguments
        func_node = Mock()
        func_node.args = []  # No args
        
        with pytest.raises(ValueError, match="endsWith\\(\\) function requires exactly 1 argument"):
            self.handler._handle_endswith("base", func_node)


class TestStringFunctionHandlerSubstring:
    """Test substring() function implementation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = Mock()
        self.generator.table_name = "fhir_resources"
        self.generator.enable_cte = True
        
        # Mock dialect
        self.dialect = Mock()
        self.dialect.substring = Mock(side_effect=lambda base, start, length: f"SUBSTRING({base}, {start}, {length})")
        self.generator.dialect = self.dialect
        
        # Mock generator methods
        self.generator.visit = Mock(side_effect=lambda x: f"VISITED({x})")
        self.generator.extract_json_field = Mock(side_effect=lambda expr, path: f"JSON_EXTRACT_STRING({expr}, '{path}')")
        
        # Create CTEBuilder
        self.cte_builder = CTEBuilder()
        
        # Create handler
        self.handler = StringFunctionHandler(self.generator, self.cte_builder)
    
    def test_substring_inline_simple_expression(self):
        """Test substring with simple expression."""
        # Mock function node
        func_node = Mock()
        func_node.args = [1, 5]  # start=1, length=5
        
        base_expr = "name"
        result = self.handler._handle_substring(base_expr, func_node)
        
        # Should return CASE expression with SUBSTRING call
        assert "CASE" in result
        assert "WHEN name IS NOT NULL" in result
        assert "SUBSTRING" in result
        assert "VISITED(1)" in result
        assert "VISITED(5)" in result
        
        # Should not create any CTEs for simple expression
        assert self.cte_builder.get_cte_count() == 0
    
    def test_substring_argument_validation(self):
        """Test substring argument validation."""
        # Test with wrong number of arguments
        func_node = Mock()
        func_node.args = [1]  # Only one arg, need two
        
        with pytest.raises(ValueError, match="substring\\(\\) function requires exactly 2 argument\\(s\\)"):
            self.handler._handle_substring("base", func_node)


class TestStringFunctionHandlerSimpleFunctions:
    """Test simple string functions (upper, lower, trim)."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = Mock()
        self.generator.table_name = "fhir_resources"
        self.generator.enable_cte = True
        self.generator.dialect = Mock()
        
        # Create CTEBuilder
        self.cte_builder = CTEBuilder()
        
        # Create handler
        self.handler = StringFunctionHandler(self.generator, self.cte_builder)
    
    def test_toupper_no_arguments(self):
        """Test toUpper function with no arguments."""
        func_node = Mock()
        func_node.args = []
        
        base_expr = "name"
        result = self.handler._handle_toupper(base_expr, func_node)
        
        # Should return CASE expression with UPPER call
        assert "CASE" in result
        assert "WHEN name IS NOT NULL" in result
        assert "UPPER(CAST(name AS VARCHAR))" in result
        assert "ELSE NULL" in result
    
    def test_toupper_with_arguments_error(self):
        """Test toUpper function with arguments (should error)."""
        func_node = Mock()
        func_node.args = ['invalid']
        
        with pytest.raises(ValueError, match="toUpper\\(\\) function takes no arguments"):
            self.handler._handle_toupper("base", func_node)
    
    def test_tolower_no_arguments(self):
        """Test toLower function."""
        func_node = Mock()
        func_node.args = []
        
        base_expr = "name"
        result = self.handler._handle_tolower(base_expr, func_node)
        
        # Should return CASE expression with LOWER call
        assert "CASE" in result
        assert "WHEN name IS NOT NULL" in result
        assert "LOWER(CAST(name AS VARCHAR))" in result
        assert "ELSE NULL" in result
    
    def test_trim_no_arguments(self):
        """Test trim function."""
        func_node = Mock()
        func_node.args = []
        
        base_expr = "name"
        result = self.handler._handle_trim(base_expr, func_node)
        
        # Should return CASE expression with TRIM call
        assert "CASE" in result
        assert "WHEN name IS NOT NULL" in result
        assert "TRIM(CAST(name AS VARCHAR))" in result
        assert "ELSE NULL" in result


class TestStringFunctionHandlerComplexFunctions:
    """Test more complex string functions."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = Mock()
        self.generator.table_name = "fhir_resources"
        self.generator.enable_cte = True
        
        # Mock dialect
        self.dialect = Mock()
        self.dialect.string_position = Mock(side_effect=lambda search, base: f"POSITION({search} IN {base})")
        self.dialect.split_string = Mock(side_effect=lambda base, sep: f"SPLIT({base}, {sep})")
        self.dialect.json_array_function = Mock(return_value="JSON_ARRAY")
        self.generator.dialect = self.dialect
        
        # Mock generator methods
        self.generator.visit = Mock(side_effect=lambda x: f"VISITED({x})")
        self.generator.extract_json_field = Mock(side_effect=lambda expr, path: f"JSON_EXTRACT_STRING({expr}, '{path}')")
        
        # Create CTEBuilder
        self.cte_builder = CTEBuilder()
        
        # Create handler
        self.handler = StringFunctionHandler(self.generator, self.cte_builder)
    
    def test_indexof_function(self):
        """Test indexOf function."""
        func_node = Mock()
        func_node.args = ['search_term']
        
        base_expr = "text"
        result = self.handler._handle_indexof(base_expr, func_node)
        
        # Should return CASE expression with POSITION call
        assert "CASE" in result
        assert "WHEN text IS NOT NULL" in result
        assert "POSITION(VISITED(search_term)" in result
        assert "ELSE -1" in result
    
    def test_replace_function(self):
        """Test replace function."""
        func_node = Mock()
        func_node.args = ['old', 'new']
        
        base_expr = "text"
        result = self.handler._handle_replace(base_expr, func_node)
        
        # Should return CASE expression with REPLACE call
        assert "CASE" in result
        assert "WHEN text IS NOT NULL" in result
        assert "REPLACE(" in result
        assert "VISITED(old)" in result
        assert "VISITED(new)" in result
        assert "ELSE NULL" in result
    
    def test_split_function(self):
        """Test split function."""
        func_node = Mock()
        func_node.args = [',']
        
        base_expr = "text"
        result = self.handler._handle_split(base_expr, func_node)
        
        # Should return CASE expression with SPLIT call
        assert "CASE" in result
        assert "WHEN text IS NOT NULL" in result
        assert "SPLIT(" in result
        assert "VISITED(,)" in result
        assert "mock.dialect.json_array_function" in result or "JSON_ARRAY" in result


class TestStringFunctionHandlerIntegration:
    """Test integration with CTEBuilder system."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = Mock()
        self.generator.table_name = "fhir_resources"
        self.generator.enable_cte = True
        
        # Mock dialect
        self.dialect = Mock()
        self.dialect.string_concat = Mock(side_effect=lambda a, b: f"CONCAT({a}, {b})")
        self.generator.dialect = self.dialect
        
        # Mock generator methods
        self.generator.visit = Mock(side_effect=lambda x: f"'{x}'")
        self.generator.extract_json_field = Mock(side_effect=lambda expr, path: f"JSON_EXTRACT_STRING({expr}, '{path}')")
        
        # Create CTEBuilder
        self.cte_builder = CTEBuilder()
        
        # Create handler
        self.handler = StringFunctionHandler(self.generator, self.cte_builder)
    
    def test_cte_decision_for_simple_vs_complex(self):
        """Test that CTE decision logic works correctly."""
        func_node = Mock()
        func_node.args = ['test']
        
        # Simple expression should not use CTE
        simple_expr = "name"
        result_simple = self.handler._handle_startswith(simple_expr, func_node)
        simple_cte_count = self.cte_builder.get_cte_count()
        
        # Reset CTEBuilder
        self.cte_builder.clear()
        
        # Complex expression should use CTE
        complex_expr = """
        CASE 
            WHEN json_type(json_extract(resource, '$.name')) = 'ARRAY' THEN
                json_extract(json_extract(resource, '$.name'), '$[0].family')
            ELSE json_extract(resource, '$.name.family')
        END
        """
        result_complex = self.handler._handle_startswith(complex_expr, func_node)
        complex_cte_count = self.cte_builder.get_cte_count()
        
        # Simple should not use CTEs, complex should use CTEs
        assert simple_cte_count == 0
        assert complex_cte_count > 0
        
        # Results should be different (inline vs CTE reference)
        assert result_simple != result_complex
        assert "(SELECT" in result_complex  # CTE reference
    
    def test_handle_function_dispatcher(self):
        """Test the main handle_function method."""
        func_node = Mock()
        func_node.args = ['test']
        
        # Test that dispatcher calls correct handler
        result = self.handler.handle_function('startswith', 'base', func_node)
        
        # Should call _handle_startswith
        assert "CASE" in result
        assert "LIKE" in result
    
    def test_unsupported_function_error(self):
        """Test error handling for unsupported functions."""
        func_node = Mock()
        func_node.args = []
        
        with pytest.raises(ValueError, match="Unsupported string function: unsupported"):
            self.handler.handle_function('unsupported', 'base', func_node)


class TestStringFunctionHandlerEdgeCases:
    """Test edge cases and error conditions."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = Mock()
        self.generator.table_name = "fhir_resources"
        self.generator.enable_cte = True
        self.generator.dialect = Mock()
        self.generator.visit = Mock(side_effect=lambda x: f"VISITED({x})")
        self.generator.extract_json_field = Mock(side_effect=lambda expr, path: f"JSON_EXTRACT_STRING({expr}, '{path}')")
        
        # Create CTEBuilder
        self.cte_builder = CTEBuilder()
        
        # Create handler
        self.handler = StringFunctionHandler(self.generator, self.cte_builder)
    
    def test_prepare_string_value_various_inputs(self):
        """Test _prepare_string_value with different input types."""
        # Test with json_extract expression
        result1 = self.handler._prepare_string_value("json_extract(resource, '$.name')")
        assert "json_extract_string" in result1
        
        # Test with json_extract_string expression
        result2 = self.handler._prepare_string_value("json_extract_string(resource, '$.name')")
        assert result2 == "json_extract_string(resource, '$.name')"
        
        # Test with plain expression
        result3 = self.handler._prepare_string_value("column_name")
        assert "JSON_EXTRACT_STRING" in result3
    
    def test_fallback_functions(self):
        """Test functions that fall back to generator."""
        func_node = Mock()
        func_node.args = []
        
        # Mock generator fallback
        self.generator._handle_function_fallback = Mock(return_value="FALLBACK_RESULT")
        
        # Test tochars fallback
        result = self.handler._handle_tochars("base", func_node)
        assert result == "FALLBACK_RESULT"
        self.generator._handle_function_fallback.assert_called_with('tochars', 'base', func_node)
        
        # Test matches fallback
        result = self.handler._handle_matches("base", func_node)
        assert result == "FALLBACK_RESULT"
        
        # Test replacematches fallback
        result = self.handler._handle_replacematches("base", func_node)
        assert result == "FALLBACK_RESULT"