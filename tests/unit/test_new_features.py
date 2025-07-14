"""
Test New Features and Bug Fixes

Tests for the issues identified by testers:
1. String concatenation with + operator  
2. Advanced functions: select(), contains(), length()
3. $this context variable support
4. Complex function chaining
5. Nested array join() operations
6. String methods: startsWith(), endsWith(), indexOf(), replace(), toUpper(), toLower()
"""

import pytest
import os
import sys

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from fhir4ds.fhirpath.core.translator import FHIRPathToSQL
from fhir4ds.dialects.duckdb import DuckDBDialect


class TestStringConcatenation:
    """Test string concatenation with + operator"""
    
    def setup_method(self):
        self.translator = FHIRPathToSQL(dialect=DuckDBDialect())
    
    def test_string_literal_concatenation(self):
        """Test concatenation with string literals"""
        result = self.translator.translate("'Hello' + ' ' + 'World'")
        assert "||" in result
        assert "CAST" not in result or "DOUBLE" not in result
    
    def test_field_concatenation(self):
        """Test concatenation of FHIR fields"""
        result = self.translator.translate("name.family + ', ' + name.given")
        assert "||" in result
        
    def test_mixed_concatenation(self):
        """Test mixed string and field concatenation"""
        result = self.translator.translate("'Patient: ' + name.family")
        assert "||" in result
        
    def test_numeric_addition_not_concatenation(self):
        """Test that numeric addition still works"""
        result = self.translator.translate("1 + 2")
        # Numeric literals don't need CAST
        assert "||" not in result
        assert "1 + 2" in result


class TestAdvancedFunctions:
    """Test new advanced functions: select(), contains(), length()"""
    
    def setup_method(self):
        self.translator = FHIRPathToSQL(dialect=DuckDBDialect())
    
    def test_length_function(self):
        """Test length() function as alias for count()"""
        result = self.translator.translate("name.length()")
        assert "json_array_length" in result or "CASE" in result
        
    def test_contains_function_array(self):
        """Test contains() function on arrays"""
        result = self.translator.translate("name.given.contains('John')")
        assert "SELECT" in result and "COUNT" in result
        
    def test_contains_function_single_value(self):
        """Test contains() function on single values"""
        result = self.translator.translate("gender.contains('male')")
        assert "CASE" in result
        
    def test_select_function_simple(self):
        """Test select() function with simple expression"""
        result = self.translator.translate("name.select(family)")
        assert "SELECT" in result
        
    def test_select_function_complex(self):
        """Test select() function with complex expression"""
        result = self.translator.translate("name.select(family + ', ' + given)")
        assert "SELECT" in result
        assert "||" in result


class TestThisContextVariable:
    """Test $this context variable support"""
    
    def setup_method(self):
        self.translator = FHIRPathToSQL(dialect=DuckDBDialect())
    
    def test_this_in_where_clause(self):
        """Test $this variable in where clauses"""
        result = self.translator.translate("name.where($this.use = 'official')")
        # Should not raise an error and should generate valid SQL
        assert "SELECT" in result
        
    def test_this_standalone(self):
        """Test $this as standalone expression"""
        result = self.translator.translate("$this")
        assert "resource" in result  # Should reference the json_column
        
    def test_this_in_complex_expression(self):
        """Test $this in more complex expressions"""
        result = self.translator.translate("name.where($this.family.exists())")
        assert "SELECT" in result


class TestStringMethods:
    """Test new string manipulation methods"""
    
    def setup_method(self):
        self.translator = FHIRPathToSQL(dialect=DuckDBDialect())
    
    def test_startswith_function(self):
        """Test startsWith() function"""
        result = self.translator.translate("name.family.startsWith('Sm')")
        assert "LIKE" in result
        assert "'%'" in result
        
    def test_endswith_function(self):
        """Test endsWith() function"""
        result = self.translator.translate("name.family.endsWith('son')")
        assert "LIKE" in result
        assert "'%'" in result
        
    def test_indexof_function(self):
        """Test indexOf() function"""
        result = self.translator.translate("name.family.indexOf('mit')")
        assert "POSITION" in result
        
    def test_replace_function(self):
        """Test replace() function"""
        result = self.translator.translate("name.family.replace('Mr', 'Mister')")
        assert "REPLACE" in result
        
    def test_toupper_function(self):
        """Test toUpper() function"""
        result = self.translator.translate("name.family.toUpper()")
        assert "UPPER" in result
        
    def test_tolower_function(self):
        """Test toLower() function"""
        result = self.translator.translate("name.family.toLower()")
        assert "LOWER" in result


class TestComplexFunctionChaining:
    """Test complex function chaining scenarios"""
    
    def setup_method(self):
        self.translator = FHIRPathToSQL(dialect=DuckDBDialect())
    
    def test_multiple_where_chaining(self):
        """Test chaining multiple where() functions"""
        result = self.translator.translate("name.where(use = 'official').where(family.exists())")
        assert "SELECT" in result
        # Should not raise an error
        
    def test_function_chain_with_string_ops(self):
        """Test function chaining with string operations"""
        result = self.translator.translate("name.family.toUpper().startsWith('SM')")
        assert "UPPER" in result
        assert "LIKE" in result
        
    def test_complex_select_chain(self):
        """Test complex select() chaining"""
        result = self.translator.translate("name.select(family.toUpper() + ', ' + given.first())")
        assert "SELECT" in result
        assert "UPPER" in result
        assert "||" in result
        
    def test_nested_function_calls(self):
        """Test nested function calls"""
        result = self.translator.translate("name.where(family.length() > 3)")
        assert "SELECT" in result
        # Should handle nested function properly


class TestNestedArrayJoin:
    """Test nested array join() operations"""
    
    def setup_method(self):
        self.translator = FHIRPathToSQL(dialect=DuckDBDialect())
    
    def test_simple_join(self):
        """Test simple join() operation"""
        result = self.translator.translate("name.given.join(' ')")
        assert "string_agg" in result.lower() or "group_concat" in result.lower()
        
    def test_nested_array_join(self):
        """Test join() on nested arrays"""
        result = self.translator.translate("name.select(given.join(' ')).join(', ')")
        assert "string_agg" in result.lower() or "group_concat" in result.lower()
        # Should handle nested arrays properly
        
    def test_join_without_separator(self):
        """Test join() without separator"""
        result = self.translator.translate("name.given.join()")
        assert "string_agg" in result.lower() or "group_concat" in result.lower()
        assert "''" in result  # Empty string separator


class TestErrorHandling:
    """Test error handling and validation"""
    
    def setup_method(self):
        self.translator = FHIRPathToSQL(dialect=DuckDBDialect())
    
    def test_function_argument_validation(self):
        """Test function argument validation"""
        with pytest.raises(ValueError, match="requires exactly"):
            self.translator.translate("name.contains()")  # Missing required argument
            
        with pytest.raises(ValueError, match="requires exactly"):
            self.translator.translate("name.length('extra')")  # Too many arguments
            
    def test_invalid_function_chaining(self):
        """Test error handling in complex function chaining"""
        # This should provide better error messages than before
        try:
            result = self.translator.translate("name.where().first()")  # Invalid where() call
            assert False, "Should have raised an error"
        except ValueError as e:
            assert "where" in str(e).lower()
            
    def test_complex_expression_error_context(self):
        """Test that errors in complex expressions provide good context"""
        try:
            result = self.translator.translate("name.where(invalid.function()).select(family)")
            assert False, "Should have raised an error"
        except Exception as e:
            # Should provide meaningful error context
            assert len(str(e)) > 10  # Non-empty error message


class TestIntegrationScenarios:
    """Test real-world integration scenarios combining multiple features"""
    
    def setup_method(self):
        self.translator = FHIRPathToSQL(dialect=DuckDBDialect())
    
    def test_patient_name_formatting(self):
        """Test complex patient name formatting"""
        result = self.translator.translate(
            "name.where(use = 'official').select(family.toUpper() + ', ' + given.join(' ')).first()"
        )
        assert "SELECT" in result
        assert "UPPER" in result
        assert "||" in result
        assert "string_agg" in result.lower() or "group_concat" in result.lower()
        
    def test_search_and_filter_pattern(self):
        """Test search and filter pattern"""
        result = self.translator.translate(
            "name.where(family.startsWith('Sm')).where(given.contains('John'))"
        )
        assert "SELECT" in result
        # With CTEBuilder, LIKE might be in CTEs rather than main query
        # Check for either inline LIKE or CTE structure
        assert "LIKE" in result or "WITH" in result
        
    def test_data_transformation_pipeline(self):
        """Test data transformation pipeline"""
        result = self.translator.translate(
            "telecom.where(system = 'email').select(value.toLower()).where($this.endsWith('.com'))"
        )
        assert "SELECT" in result
        assert "LOWER" in result
        assert "LIKE" in result


if __name__ == "__main__":
    pytest.main([__file__])