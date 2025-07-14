"""
Unit tests for BaseFunctionHandler class.

Tests the base functionality for function handlers including:
- CTE decision logic and complexity analysis
- Utility methods for SQL generation
- Common patterns and error handling
- Integration with CTEBuilder
"""

import pytest
from unittest.mock import Mock, MagicMock
from fhir4ds.fhirpath.core.generators.base_handler import BaseFunctionHandler
from fhir4ds.fhirpath.core.cte_builder import CTEBuilder


class MockFunctionHandler(BaseFunctionHandler):
    """Mock implementation of BaseFunctionHandler for testing."""
    
    def get_supported_functions(self):
        return ['test_func', 'mock_func', 'complex_func']
    
    def can_handle(self, function_name: str) -> bool:
        return function_name.lower() in self.get_supported_functions()
    
    def handle_function(self, func_name: str, base_expr: str, func_node) -> str:
        return f"MOCK_{func_name.upper()}({base_expr})"


class TestBaseFunctionHandlerInitialization:
    """Test initialization and configuration."""
    
    def test_basic_initialization(self):
        """Test basic initialization with minimal parameters."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        dialect = Mock()
        generator.dialect = dialect
        
        handler = MockFunctionHandler(generator)
        
        assert handler.generator == generator
        assert handler.dialect == dialect
        assert handler.cte_builder is None
        assert handler.enable_cte is True
    
    def test_initialization_with_cte_builder(self):
        """Test initialization with CTEBuilder."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        cte_builder = CTEBuilder()
        handler = MockFunctionHandler(generator, cte_builder)
        
        assert handler.cte_builder == cte_builder
        assert handler.enable_cte is True
    
    def test_initialization_without_enable_cte_attribute(self):
        """Test initialization when generator doesn't have enable_cte attribute."""
        generator = Mock(spec=['table_name', 'dialect'])  # Mock with only specific attributes
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        handler = MockFunctionHandler(generator)
        
        assert handler.enable_cte is True  # Default value
    
    def test_thresholds_configuration(self):
        """Test that CTE thresholds are properly configured."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        handler = MockFunctionHandler(generator)
        
        assert 'min_expression_length' in handler.cte_thresholds
        assert 'max_simple_operations' in handler.cte_thresholds
        assert 'min_subquery_count' in handler.cte_thresholds
        assert isinstance(handler.cte_thresholds['min_expression_length'], int)


class TestComplexityAnalysis:
    """Test complexity scoring and analysis."""
    
    def test_calculate_complexity_score_empty(self):
        """Test complexity score for empty expression."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        handler = MockFunctionHandler(generator)
        score = handler._calculate_complexity_score("")
        
        assert score == 0.0
    
    def test_calculate_complexity_score_simple(self):
        """Test complexity score for simple expression."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        handler = MockFunctionHandler(generator)
        simple_expr = "SELECT id FROM table"
        score = handler._calculate_complexity_score(simple_expr)
        
        assert score > 0.0
        assert score < 2.0  # Simple expressions should have low scores
    
    def test_calculate_complexity_score_complex(self):
        """Test complexity score for complex expression."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        handler = MockFunctionHandler(generator)
        complex_expr = """
        SELECT 
            CASE 
                WHEN json_type(json_extract(resource, '$.name')) = 'ARRAY' THEN
                    (SELECT json_group_array(
                        CASE 
                            WHEN json_extract(value, '$.use') = 'official' THEN
                                UPPER(CAST(json_extract(value, '$.family') AS VARCHAR))
                            ELSE NULL
                        END
                    ) FROM json_each(json_extract(resource, '$.name'), '$')
                    WHERE json_extract(value, '$.use') = 'official')
                ELSE NULL
            END
        FROM fhir_resources
        WHERE json_extract(resource, '$.resourceType') = 'Patient'
        """
        score = handler._calculate_complexity_score(complex_expr)
        
        assert score > 5.0  # Complex expressions should have high scores
    
    def test_calculate_parentheses_depth(self):
        """Test parentheses depth calculation."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        handler = MockFunctionHandler(generator)
        
        # Test various nesting levels
        assert handler._calculate_parentheses_depth("simple") == 0
        assert handler._calculate_parentheses_depth("func(arg)") == 1
        assert handler._calculate_parentheses_depth("func(nested(arg))") == 2
        assert handler._calculate_parentheses_depth("func(a(b(c)))") == 3
        
        # Test unbalanced parentheses
        assert handler._calculate_parentheses_depth("func(arg") == 1
        assert handler._calculate_parentheses_depth("func)arg") == 0
    
    def test_complexity_score_factors(self):
        """Test individual complexity factors."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        handler = MockFunctionHandler(generator)
        
        # Test length factor
        short_expr = "SELECT 1"
        long_expr = "SELECT " + ", ".join(f"col_{i}" for i in range(100))
        
        short_score = handler._calculate_complexity_score(short_expr)
        long_score = handler._calculate_complexity_score(long_expr)
        assert long_score > short_score
        
        # Test JSON operations
        json_expr = "SELECT json_extract(json_type(json_array_length(data)))"
        non_json_expr = "SELECT id, name, value"
        
        json_score = handler._calculate_complexity_score(json_expr)
        non_json_score = handler._calculate_complexity_score(non_json_expr)
        assert json_score > non_json_score
        
        # Test CASE statements
        case_expr = "SELECT CASE WHEN a THEN b CASE WHEN c THEN d ELSE e END END"
        simple_expr = "SELECT a, b, c"
        
        case_score = handler._calculate_complexity_score(case_expr)
        simple_score = handler._calculate_complexity_score(simple_expr)
        assert case_score > simple_score


class TestCTEDecisionLogic:
    """Test CTE usage decision making."""
    
    def test_should_use_cte_no_cte_builder(self):
        """Test CTE decision when no CTEBuilder available."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        handler = MockFunctionHandler(generator)  # No CTE builder
        
        result = handler.should_use_cte("SELECT complex FROM expression", "filter")
        assert result is False
    
    def test_should_use_cte_disabled(self):
        """Test CTE decision when CTEs are disabled."""
        generator = Mock()
        generator.enable_cte = False
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        cte_builder = CTEBuilder()
        handler = MockFunctionHandler(generator, cte_builder)
        
        result = handler.should_use_cte("SELECT complex FROM expression", "filter")
        assert result is False
    
    def test_should_use_cte_force_enable(self):
        """Test forced CTE usage."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        cte_builder = CTEBuilder()
        handler = MockFunctionHandler(generator, cte_builder)
        
        result = handler.should_use_cte("simple", "filter", force_cte=True)
        assert result is True
    
    def test_should_use_cte_force_disable(self):
        """Test forced CTE disabling."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        cte_builder = CTEBuilder()
        handler = MockFunctionHandler(generator, cte_builder)
        
        complex_expr = "SELECT " + ", ".join(f"complex_col_{i}" for i in range(100))
        result = handler.should_use_cte(complex_expr, "filter", disable_cte=True)
        assert result is False
    
    def test_should_use_cte_complexity_score(self):
        """Test CTE decision based on pre-calculated complexity score."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        cte_builder = CTEBuilder()
        handler = MockFunctionHandler(generator, cte_builder)
        
        # High complexity score should trigger CTE
        result_high = handler.should_use_cte("simple", "filter", complexity_score=10)
        assert result_high is True
        
        # Low complexity score should not trigger CTE
        result_low = handler.should_use_cte("simple", "filter", complexity_score=1)
        assert result_low is False
    
    def test_should_use_cte_operation_multipliers(self):
        """Test operation-specific multipliers in CTE decisions."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        cte_builder = CTEBuilder()
        handler = MockFunctionHandler(generator, cte_builder)
        
        # Complex expression that will trigger CTEs with multipliers
        complex_expr = """
        SELECT 
            CASE 
                WHEN json_type(json_extract(data, '$.field')) = 'ARRAY' THEN
                    json_array_length(json_extract(data, '$.field'))
                ELSE 0
            END as count_result
        FROM table 
        WHERE json_extract(data, '$.active') = true
        """
        
        # Operations that benefit from CTEs (high multipliers)
        assert handler.should_use_cte(complex_expr, "select") == True
        assert handler.should_use_cte(complex_expr, "transform") == True
        
        # Simple expression for operations that don't benefit as much
        simple_expr = "SELECT id FROM table"
        assert handler.should_use_cte(simple_expr, "startswith") == False
        assert handler.should_use_cte(simple_expr, "endswith") == False


class TestCTECreationAndUtilities:
    """Test CTE creation and utility methods."""
    
    def test_create_cte_if_beneficial_with_cte(self):
        """Test CTE creation when beneficial."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        cte_builder = CTEBuilder()
        handler = MockFunctionHandler(generator, cte_builder)
        
        # Complex SQL that should trigger CTE
        complex_sql = """
        SELECT 
            CASE 
                WHEN json_type(json_extract(resource, '$.name')) = 'ARRAY' THEN
                    json_array_length(json_extract(resource, '$.name'))
                ELSE 0
            END as name_count
        FROM fhir_resources
        WHERE json_extract(resource, '$.active') = true
        """
        
        result = handler.create_cte_if_beneficial("complex_filter", complex_sql)
        
        # Should return a CTE reference
        assert result.startswith("(SELECT")
        assert "FROM complex_filter_1)" in result
        assert cte_builder.get_cte_count() == 1
    
    def test_create_cte_if_beneficial_without_cte(self):
        """Test returning original SQL when CTE not beneficial."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        cte_builder = CTEBuilder()
        handler = MockFunctionHandler(generator, cte_builder)
        
        # Simple SQL that shouldn't trigger CTE
        simple_sql = "SELECT id FROM table"
        
        result = handler.create_cte_if_beneficial("simple_op", simple_sql)
        
        # Should return original SQL
        assert result == simple_sql
        assert cte_builder.get_cte_count() == 0
    
    def test_generate_cte_sql(self):
        """Test CTE SQL generation utility."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "fhir_resources"
        generator.dialect = Mock()
        
        handler = MockFunctionHandler(generator)
        
        main_logic = "CASE WHEN active = true THEN 1 ELSE 0 END as is_active"
        result = handler.generate_cte_sql("filter", main_logic)
        
        assert "SELECT" in result
        assert main_logic in result
        assert "FROM fhir_resources" in result
    
    def test_generate_cte_sql_custom_table(self):
        """Test CTE SQL generation with custom table name."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "default_table"
        generator.dialect = Mock()
        
        handler = MockFunctionHandler(generator)
        
        main_logic = "COUNT(*) as total"
        result = handler.generate_cte_sql("count", main_logic, "custom_table")
        
        assert "FROM custom_table" in result
        assert "FROM default_table" not in result
    
    def test_create_case_expression_simple(self):
        """Test simple CASE expression creation."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        handler = MockFunctionHandler(generator)
        
        result = handler.create_case_expression(
            "name = 'John'",
            "true",
            "false"
        )
        
        assert "CASE" in result
        assert "WHEN name = 'John' THEN true" in result
        assert "ELSE false" in result
        assert "END" in result
    
    def test_create_case_expression_with_null_check(self):
        """Test CASE expression with null checking."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        handler = MockFunctionHandler(generator)
        
        result = handler.create_case_expression(
            "name = 'John'",
            "true",
            "false",
            null_check="name IS NOT NULL"
        )
        
        assert "CASE" in result
        assert "WHEN name IS NOT NULL THEN" in result
        assert "WHEN name = 'John' THEN true" in result
        assert result.count("CASE") == 2  # Nested CASE statements


class TestArgumentHandlingAndValidation:
    """Test argument processing and validation utilities."""
    
    def test_handle_function_with_args_correct_count(self):
        """Test successful argument handling with correct count."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        generator.visit = Mock(side_effect=lambda x: f"PROCESSED({x})")
        
        handler = MockFunctionHandler(generator)
        
        # Mock function node with arguments
        func_node = Mock()
        func_node.args = ["arg1", "arg2"]
        
        result = handler.handle_function_with_args("test_func", "base", func_node, 2)
        
        assert len(result) == 2
        assert result[0] == "PROCESSED(arg1)"
        assert result[1] == "PROCESSED(arg2)"
        assert generator.visit.call_count == 2
    
    def test_handle_function_with_args_wrong_count(self):
        """Test error handling with wrong argument count."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        handler = MockFunctionHandler(generator)
        
        # Mock function node with wrong number of arguments
        func_node = Mock()
        func_node.args = ["arg1"]  # Only 1 arg, expect 2
        
        with pytest.raises(ValueError, match="test_func\\(\\) function requires exactly 2 argument\\(s\\), got 1"):
            handler.handle_function_with_args("test_func", "base", func_node, 2)
    
    def test_validate_function_call_valid(self):
        """Test successful function call validation."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        handler = MockFunctionHandler(generator)
        
        # Valid function node
        func_node = Mock()
        func_node.args = ["arg1", "arg2"]
        
        # Should not raise exception
        handler.validate_function_call("test_func", func_node)
    
    def test_validate_function_call_none_node(self):
        """Test validation error with None function node."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        handler = MockFunctionHandler(generator)
        
        with pytest.raises(ValueError, match="Function node is required for test_func\\(\\)"):
            handler.validate_function_call("test_func", None)
    
    def test_validate_function_call_missing_args(self):
        """Test validation error with missing args attribute."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        handler = MockFunctionHandler(generator)
        
        # Function node without args attribute
        func_node = Mock(spec=[])  # Mock with no attributes
        
        with pytest.raises(ValueError, match="Function node missing args attribute for test_func\\(\\)"):
            handler.validate_function_call("test_func", func_node)
    
    def test_validate_function_call_invalid_args_type(self):
        """Test validation error with non-list args."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        handler = MockFunctionHandler(generator)
        
        # Function node with non-list args
        func_node = Mock()
        func_node.args = "not a list"
        
        with pytest.raises(ValueError, match="Function node args must be a list for test_func\\(\\)"):
            handler.validate_function_call("test_func", func_node)


class TestArrayOperations:
    """Test array operation utilities."""
    
    def test_handle_array_operation(self):
        """Test array operation handling."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "fhir_resources"
        generator.dialect = Mock()
        
        cte_builder = CTEBuilder()
        handler = MockFunctionHandler(generator, cte_builder)
        
        base_expr = "json_extract(resource, '$.names')"
        element_logic = "UPPER(CAST(value AS VARCHAR))"
        
        result = handler.handle_array_operation(base_expr, element_logic, "transform")
        
        # Should handle arrays and single values
        assert "json_type" in result
        assert "json_group_array" in result
        assert "json_each" in result
        assert element_logic in result


class TestDebuggingAndInformation:
    """Test debugging and information methods."""
    
    def test_get_debug_info(self):
        """Test debug information gathering."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        dialect = Mock()
        dialect.__class__.__name__ = "MockDialect"
        generator.dialect = dialect
        
        cte_builder = CTEBuilder()
        handler = MockFunctionHandler(generator, cte_builder)
        
        debug_info = handler.get_debug_info()
        
        assert debug_info['handler_type'] == 'MockFunctionHandler'
        assert debug_info['supported_functions'] == ['test_func', 'mock_func', 'complex_func']
        assert debug_info['cte_enabled'] is True
        assert debug_info['cte_builder_available'] is True
        assert debug_info['generator_table'] == 'test_table'
        assert debug_info['dialect'] == 'MockDialect'
        assert 'thresholds' in debug_info
    
    def test_get_debug_info_no_cte_builder(self):
        """Test debug info when no CTE builder available."""
        generator = Mock()
        generator.enable_cte = False
        generator.table_name = "test_table"
        generator.dialect = None
        
        handler = MockFunctionHandler(generator)
        
        debug_info = handler.get_debug_info()
        
        assert debug_info['cte_enabled'] is False
        assert debug_info['cte_builder_available'] is False
        assert debug_info['dialect'] is None
    
    def test_string_representations(self):
        """Test string representation methods."""
        generator = Mock()
        generator.enable_cte = True
        generator.table_name = "test_table"
        generator.dialect = Mock()
        
        cte_builder = CTEBuilder()
        handler = MockFunctionHandler(generator, cte_builder)
        
        # Test __str__
        str_repr = str(handler)
        assert "MockFunctionHandler" in str_repr
        assert "functions=3" in str_repr
        assert "cte_enabled=True" in str_repr
        
        # Test __repr__
        repr_str = repr(handler)
        assert "MockFunctionHandler" in repr_str
        assert "['test_func', 'mock_func', 'complex_func']" in repr_str
        assert "cte_builder=True" in repr_str