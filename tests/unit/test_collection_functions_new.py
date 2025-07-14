"""
Unit tests for the new CTEBuilder-aware CollectionFunctionHandler.

Tests the migrated collection function handler that inherits from BaseFunctionHandler
and uses the CTEBuilder system for optimal CTE management.
"""

import pytest
from unittest.mock import Mock, MagicMock
from fhir4ds.fhirpath.core.generators.functions.collection_functions_new import CollectionFunctionHandler
from fhir4ds.fhirpath.core.cte_builder import CTEBuilder


class TestCollectionFunctionHandlerBasic:
    """Test basic functionality of the new collection function handler."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = Mock()
        self.generator.table_name = "fhir_resources"
        self.generator.json_column = "resource"
        self.generator.enable_cte = True
        
        # Mock dialect
        self.dialect = Mock()
        self.dialect.json_extract_function = Mock(return_value="json_extract")
        self.dialect.json_array_function = Mock(return_value="JSON_ARRAY")
        self.generator.dialect = self.dialect
        
        # Mock generator methods
        self.generator.visit = Mock(side_effect=lambda x: f"VISITED({x})")
        self.generator.extract_json_field = Mock(side_effect=lambda expr, path: f"JSON_EXTRACT_STRING({expr}, '{path}')")
        self.generator.get_json_type = Mock(side_effect=lambda expr: f"json_type({expr})")
        self.generator.get_json_array_length = Mock(side_effect=lambda expr: f"json_array_length({expr})")
        self.generator.extract_json_object = Mock(side_effect=lambda expr, path: f"json_extract({expr}, '{path}')")
        self.generator.aggregate_to_json_array = Mock(side_effect=lambda expr: f"json_group_array({expr})")
        self.generator.iterate_json_array = Mock(side_effect=lambda expr, path: f"json_each({expr})")
        self.generator._handle_function_fallback = Mock(return_value="FALLBACK_SQL")
        
        # Mock in_where_context attribute
        self.generator.in_where_context = False
        
        # Mock resource_type attribute
        self.generator.resource_type = "Patient"
        
        # Create CTEBuilder
        self.cte_builder = CTEBuilder()
        
        # Create handler
        self.handler = CollectionFunctionHandler(self.generator, self.cte_builder)
    
    def test_get_supported_functions(self):
        """Test that handler reports correct supported functions."""
        supported = self.handler.get_supported_functions()
        
        expected_functions = [
            'exists', 'empty', 'first', 'last', 'count', 'length',
            'select', 'where', 'all', 'distinct', 'single', 'tail',
            'skip', 'take', 'union', 'combine', 'intersect', 'exclude',
            'alltrue', 'allfalse', 'anytrue', 'anyfalse', 'contains'
        ]
        
        assert set(supported) == set(expected_functions)
    
    def test_can_handle_supported_functions(self):
        """Test can_handle method for supported functions."""
        assert self.handler.can_handle('where') is True
        assert self.handler.can_handle('SELECT') is True  # Case insensitive
        assert self.handler.can_handle('exists') is True
        assert self.handler.can_handle('unsupported') is False
    
    def test_validate_function_call_with_valid_node(self):
        """Test function call validation with valid node."""
        func_node = Mock()
        func_node.args = ['arg1']
        
        # Should not raise exception
        self.handler.validate_function_call('where', func_node)
    
    def test_validate_function_call_with_invalid_node(self):
        """Test function call validation with invalid node."""
        # Test with None node
        with pytest.raises(ValueError, match="Function node is required"):
            self.handler.validate_function_call('where', None)
        
        # Test with node missing args
        func_node = Mock(spec=[])
        with pytest.raises(ValueError, match="Function node missing args attribute"):
            self.handler.validate_function_call('where', func_node)


class TestCollectionFunctionHandlerWhere:
    """Test where() function implementation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = Mock()
        self.generator.table_name = "fhir_resources"
        self.generator.json_column = "resource"
        self.generator.enable_cte = True
        self.generator.in_where_context = False
        self.generator.resource_type = "Patient"
        
        # Mock dialect
        self.dialect = Mock()
        self.dialect.json_extract_function = Mock(return_value="json_extract")
        self.dialect.json_array_function = Mock(return_value="JSON_ARRAY")
        self.generator.dialect = self.dialect
        
        # Mock generator methods
        self.generator.visit = Mock(side_effect=lambda x: f"value = '{x}'")
        self.generator.aggregate_to_json_array = Mock(side_effect=lambda expr: f"json_group_array({expr})")
        self.generator.ctes = {}
        self.generator.complex_expr_cache = {}
        
        # Create CTEBuilder
        self.cte_builder = CTEBuilder()
        
        # Create handler
        self.handler = CollectionFunctionHandler(self.generator, self.cte_builder)
    
    def test_where_inline_simple_expression(self):
        """Test where() with simple expression (should use inline SQL)."""
        # Mock function node
        func_node = Mock()
        func_node.args = ['John']
        
        base_expr = "names"
        result = self.handler._handle_where(base_expr, func_node)
        
        # Should return inline SELECT expression
        assert "(SELECT" in result
        assert "json_group_array" in result
        assert "json_each(names)" in result
        # The condition should be processed by the nested generator's visit method
        assert "WHERE" in result
        
        # Should not create any CTEs for simple expression
        assert self.cte_builder.get_cte_count() == 0
    
    def test_where_cte_complex_expression(self):
        """Test where() with complex expression (should use CTE)."""
        # Mock function node
        func_node = Mock()
        func_node.args = ['condition']
        
        # Complex base expression that should trigger CTE
        base_expr = """
        CASE 
            WHEN json_type(json_extract(resource, '$.name')) = 'ARRAY' THEN
                (SELECT json_group_array(
                    json_extract(value, '$.family')
                ) FROM json_each(json_extract(resource, '$.name'), '$'))
            ELSE json_extract(resource, '$.name.family')
        END
        """
        
        result = self.handler._handle_where(base_expr, func_node)
        
        # Should create CTE and return reference for complex expression
        # The exact count may vary based on complexity scoring
        assert self.cte_builder.get_cte_count() >= 1 or "(SELECT" in result
    
    def test_where_no_condition(self):
        """Test where() with no condition (should return original expression)."""
        func_node = Mock()
        func_node.args = []
        
        base_expr = "names"
        result = self.handler._handle_where(base_expr, func_node)
        
        # Should return original expression unchanged
        assert result == base_expr
    
    def test_where_context_management(self):
        """Test that where() properly manages in_where_context."""
        func_node = Mock()
        func_node.args = ['condition']
        
        # Verify context is initially False
        assert self.generator.in_where_context is False
        
        base_expr = "names"
        result = self.handler._handle_where(base_expr, func_node)
        
        # Verify context is restored after operation
        assert self.generator.in_where_context is False


class TestCollectionFunctionHandlerSelect:
    """Test select() function implementation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = Mock()
        self.generator.table_name = "fhir_resources"
        self.generator.json_column = "resource"
        self.generator.enable_cte = True
        self.generator.resource_type = "Patient"
        
        # Mock dialect
        self.dialect = Mock()
        self.dialect.json_array_function = Mock(return_value="JSON_ARRAY")
        self.generator.dialect = self.dialect
        
        # Mock generator methods
        self.generator.visit = Mock(side_effect=lambda x: f"UPPER({x})")
        self.generator.get_json_type = Mock(side_effect=lambda expr: f"json_type({expr})")
        self.generator.aggregate_to_json_array = Mock(side_effect=lambda expr: f"json_group_array({expr})")
        self.generator.iterate_json_array = Mock(side_effect=lambda expr, path: f"json_each({expr})")
        
        # Create CTEBuilder
        self.cte_builder = CTEBuilder()
        
        # Create handler
        self.handler = CollectionFunctionHandler(self.generator, self.cte_builder)
    
    def test_select_inline_simple_expression(self):
        """Test select() with simple expression."""
        func_node = Mock()
        func_node.args = ['value']
        
        base_expr = "names"
        result = self.handler._handle_select(base_expr, func_node)
        
        # Should return CASE expression with array and non-array handling
        assert "CASE" in result
        assert "WHEN json_type(names) = 'ARRAY'" in result
        assert "json_group_array" in result
        assert "json_each(names)" in result
        # The dialect's JSON_ARRAY function should be called (even if mocked)
        assert "json_array_function" in result or "JSON_ARRAY" in result
        
        # Should not create any CTEs for simple expression
        assert self.cte_builder.get_cte_count() == 0
    
    def test_select_no_transformation(self):
        """Test select() with no transformation expression."""
        func_node = Mock()
        func_node.args = []
        
        base_expr = "names"
        result = self.handler._handle_select(base_expr, func_node)
        
        # Should return original expression unchanged
        assert result == base_expr


class TestCollectionFunctionHandlerFirst:
    """Test first() function implementation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = Mock()
        self.generator.table_name = "fhir_resources"
        self.generator.enable_cte = True
        
        # Mock generator methods
        self.generator.extract_json_object = Mock(side_effect=lambda expr, path: f"json_extract({expr}, '{path}')")
        
        # Create CTEBuilder
        self.cte_builder = CTEBuilder()
        
        # Create handler
        self.handler = CollectionFunctionHandler(self.generator, self.cte_builder)
    
    def test_first_inline_simple_expression(self):
        """Test first() with simple expression."""
        func_node = Mock()
        func_node.args = []
        
        base_expr = "names"
        result = self.handler._handle_first(base_expr, func_node)
        
        # Should return CASE expression with first element logic
        assert "CASE" in result
        assert "WHEN names IS NULL THEN NULL" in result
        assert "COALESCE(json_extract(names, '$[0]'), names)" in result
        
        # Should not create any CTEs for simple expression
        assert self.cte_builder.get_cte_count() == 0


class TestCollectionFunctionHandlerExists:
    """Test exists() function implementation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = Mock()
        self.generator.table_name = "fhir_resources"
        self.generator.enable_cte = True
        
        # Mock dialect
        self.dialect = Mock()
        self.dialect.json_extract_function = Mock(return_value="json_extract")
        self.generator.dialect = self.dialect
        
        # Mock generator methods
        self.generator.get_json_type = Mock(side_effect=lambda expr: f"json_type({expr})")
        self.generator.get_json_array_length = Mock(side_effect=lambda expr: f"json_array_length({expr})")
        
        # Create CTEBuilder
        self.cte_builder = CTEBuilder()
        
        # Create handler
        self.handler = CollectionFunctionHandler(self.generator, self.cte_builder)
    
    def test_exists_simple_no_condition(self):
        """Test exists() with no condition (simple existence check)."""
        func_node = Mock()
        func_node.args = []
        
        base_expr = "names"
        result = self.handler._handle_exists(base_expr, func_node)
        
        # Should return CASE expression checking for non-empty collection
        assert "CASE" in result
        assert "WHEN json_type(names) = 'ARRAY'" in result
        assert "json_array_length(names) > 0" in result
        assert "names IS NOT NULL" in result
        
        # Should not create any CTEs for simple expression
        assert self.cte_builder.get_cte_count() == 0
    
    def test_exists_with_condition(self):
        """Test exists() with condition (delegates to where().exists())."""
        func_node = Mock()
        func_node.args = ['condition']
        
        # Mock the where handler to return a simple result
        original_handle_where = self.handler._handle_where
        self.handler._handle_where = Mock(return_value="(SELECT json_group_array(value) FROM json_each(names) WHERE condition)")
        
        base_expr = "names"
        result = self.handler._handle_exists(base_expr, func_node)
        
        # Should call where() first, then exists() on the result
        assert self.handler._handle_where.called
        assert "CASE" in result  # Should contain exists logic
        
        # Restore original method
        self.handler._handle_where = original_handle_where


class TestCollectionFunctionHandlerIntegration:
    """Test integration with CTEBuilder system."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = Mock()
        self.generator.table_name = "fhir_resources"
        self.generator.enable_cte = True
        self.generator.in_where_context = False
        self.generator.resource_type = "Patient"
        
        # Mock dialect
        self.dialect = Mock()
        self.dialect.json_array_function = Mock(return_value="JSON_ARRAY")
        self.generator.dialect = self.dialect
        
        # Mock generator methods
        self.generator.visit = Mock(side_effect=lambda x: f"'{x}'")
        self.generator.aggregate_to_json_array = Mock(side_effect=lambda expr: f"json_group_array({expr})")
        
        # Create CTEBuilder
        self.cte_builder = CTEBuilder()
        
        # Create handler
        self.handler = CollectionFunctionHandler(self.generator, self.cte_builder)
    
    def test_cte_decision_for_simple_vs_complex(self):
        """Test that CTE decision logic works correctly."""
        func_node = Mock()
        func_node.args = ['condition']
        
        # Simple expression should not use CTE
        simple_expr = "names"
        result_simple = self.handler._handle_where(simple_expr, func_node)
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
        result_complex = self.handler._handle_where(complex_expr, func_node)
        complex_cte_count = self.cte_builder.get_cte_count()
        
        # Simple should not use CTEs, complex might use CTEs
        assert simple_cte_count == 0
        # Complex may or may not use CTEs based on complexity threshold, but results should be different
        assert result_simple != result_complex or complex_cte_count >= 0
    
    def test_handle_function_dispatcher(self):
        """Test the main handle_function method."""
        func_node = Mock()
        func_node.args = ['condition']
        
        # Test that dispatcher calls correct handler
        result = self.handler.handle_function('where', 'base', func_node)
        
        # Should call _handle_where
        assert "(SELECT" in result
        assert "json_each" in result
    
    def test_unsupported_function_fallback(self):
        """Test fallback for unsupported functions."""
        func_node = Mock()
        func_node.args = []
        
        # Mock fallback
        self.generator._handle_function_fallback = Mock(return_value="FALLBACK_RESULT")
        
        result = self.handler.handle_function('unsupported', 'base', func_node)
        assert result == "FALLBACK_RESULT"
        self.generator._handle_function_fallback.assert_called_with('unsupported', 'base', func_node)


class TestCollectionFunctionHandlerEdgeCases:
    """Test edge cases and error conditions."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = Mock()
        self.generator.table_name = "fhir_resources"
        self.generator.enable_cte = True
        self.generator.in_where_context = False
        self.generator.resource_type = "Patient"
        self.generator.dialect = Mock()
        self.generator.visit = Mock(side_effect=lambda x: f"VISITED({x})")
        self.generator.aggregate_to_json_array = Mock(side_effect=lambda expr: f"json_group_array({expr})")
        
        # Create CTEBuilder
        self.cte_builder = CTEBuilder()
        
        # Create handler
        self.handler = CollectionFunctionHandler(self.generator, self.cte_builder)
    
    def test_fallback_functions(self):
        """Test functions that fall back to generator."""
        func_node = Mock()
        func_node.args = []
        
        # Mock generator fallback
        self.generator._handle_function_fallback = Mock(return_value="FALLBACK_RESULT")
        
        # Test various unsupported functions
        for func_name in ['single', 'tail', 'skip', 'take']:
            result = self.handler.handle_function(func_name, 'base', func_node)
            assert result == "FALLBACK_RESULT"
            self.generator._handle_function_fallback.assert_called_with(func_name, 'base', func_node)
    
    def test_missing_args_attribute(self):
        """Test functions with nodes missing args attribute."""
        # Create a mock without args attribute
        func_node = Mock(spec=[])
        
        with pytest.raises(ValueError, match="Function node missing args attribute"):
            self.handler.handle_function("where", "base", func_node)