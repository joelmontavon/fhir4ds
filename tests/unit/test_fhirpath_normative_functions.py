"""
Test FHIRPath Normative Functions

Tests for FHIRPath normative specification functions that were added
to ensure compliance with the standard specification.
"""

import pytest
import os
import sys

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from fhir4ds.fhirpath.core.translator import FHIRPathToSQL
from fhir4ds.dialects.duckdb import DuckDBDialect


class TestFHIRPathNormativeStringFunctions:
    """Test FHIRPath normative string functions"""
    
    def setup_method(self):
        self.translator = FHIRPathToSQL(dialect=DuckDBDialect())
    
    def test_upper_function(self):
        """Test upper() function - FHIRPath normative"""
        result = self.translator.translate("name.family.upper()")
        assert "UPPER" in result
        assert "CAST" in result
        
    def test_lower_function(self):
        """Test lower() function - FHIRPath normative"""
        result = self.translator.translate("name.family.lower()")
        assert "LOWER" in result
        assert "CAST" in result
        
    def test_trim_function(self):
        """Test trim() function - FHIRPath normative"""
        result = self.translator.translate("name.family.first().trim()")
        assert "TRIM" in result
        assert "CAST" in result
        
    def test_split_function(self):
        """Test split() function - FHIRPath normative"""
        result = self.translator.translate("telecom.value.split('@')")
        assert "string_split" in result
        assert "CAST" in result
        
    def test_string_function_chaining(self):
        """Test chaining of string functions"""
        result = self.translator.translate("name.family.first().upper().trim()")
        assert "UPPER" in result
        assert "TRIM" in result
        
    def test_string_function_in_concatenation(self):
        """Test string functions in concatenation context"""
        result = self.translator.translate("name.family.upper() + ' - ' + name.given.lower()")
        assert "||" in result  # String concatenation
        assert "UPPER" in result
        assert "LOWER" in result


class TestFHIRPathNormativeCollectionFunctions:
    """Test FHIRPath normative collection functions"""
    
    def setup_method(self):
        self.translator = FHIRPathToSQL(dialect=DuckDBDialect())
    
    def test_distinct_function(self):
        """Test distinct() function - FHIRPath normative"""
        result = self.translator.translate("name.distinct()")
        assert "DISTINCT" in result
        assert "json_group_array" in result
        
    def test_all_function_simple(self):
        """Test all() function with simple criteria"""
        result = self.translator.translate("name.all(family.exists())")
        assert "COUNT" in result
        assert "CASE" in result
        
    def test_all_function_complex(self):
        """Test all() function with complex criteria"""
        result = self.translator.translate("telecom.all(system = 'email')")
        assert "COUNT" in result
        assert "CASE" in result
        
    def test_collection_functions_combination(self):
        """Test combination of collection functions"""
        result = self.translator.translate("name.distinct().where(family.exists())")
        assert "SELECT" in result
        assert "DISTINCT" in result


class TestFHIRPathStandardStringOperations:
    """Test standard FHIRPath string operations that should work"""
    
    def setup_method(self):
        self.translator = FHIRPathToSQL(dialect=DuckDBDialect())
    
    def test_string_concatenation_with_functions(self):
        """Test string concatenation with function results"""
        result = self.translator.translate("name.family.first() + ' (family)'")
        assert "||" in result
        assert "SELECT" in result or "json_extract" in result
        
    def test_literal_plus_field_concat(self):
        """Test literal + field concatenation"""
        result = self.translator.translate("'Hello ' + name.given.first()")
        assert "||" in result
        assert "'Hello '" in result
        
    def test_multiple_concatenation(self):
        """Test multiple string concatenation"""
        result = self.translator.translate("name.family.first() + ', ' + name.given.first()")
        assert "||" in result
        assert "', '" in result
        
    def test_concat_with_empty_string(self):
        """Test concatenation with empty string"""
        result = self.translator.translate("name.family.first() + ''")
        assert "||" in result
        assert "''" in result


class TestFHIRPathWhereClauseSupport:
    """Test FHIRPath where clause functionality"""
    
    def setup_method(self):
        self.translator = FHIRPathToSQL(dialect=DuckDBDialect())
    
    def test_where_with_string_literals(self):
        """Test where with string literals"""
        result = self.translator.translate("name.where(use='official').family.first()")
        assert "SELECT" in result
        assert "'official'" in result
        
    def test_nested_where_operations(self):
        """Test nested where operations"""
        result = self.translator.translate("name.where(use='official').given.where($this = 'John')")
        assert "SELECT" in result
        assert "'official'" in result
        assert "'John'" in result
        
    def test_index_based_filtering(self):
        """Test index-based filtering with $index"""
        result = self.translator.translate("name.given.where($index = 0)")
        assert "SELECT" in result
        # Note: $index support may need special implementation
        
    def test_where_with_string_functions(self):
        """Test where clauses with string functions"""
        result = self.translator.translate("telecom.value.where($this.contains('@'))")
        assert "SELECT" in result
        assert "COUNT" in result  # from contains implementation


class TestFHIRPathSelectFunctionality:
    """Test FHIRPath select function"""
    
    def setup_method(self):
        self.translator = FHIRPathToSQL(dialect=DuckDBDialect())
    
    def test_basic_select(self):
        """Test basic select function"""
        result = self.translator.translate("name.select(family)")
        assert "SELECT" in result
        
    def test_select_with_operations(self):
        """Test select with string operations"""
        result = self.translator.translate("name.select(family + ', ' + given.first())")
        assert "SELECT" in result
        assert "||" in result
        
    def test_select_with_join(self):
        """Test select with join function"""
        result = self.translator.translate("name.select(given.join(' '))")
        assert "SELECT" in result
        # join should use string_agg or similar
        
    def test_select_with_string_functions(self):
        """Test select with string manipulation functions"""
        result = self.translator.translate("name.select(family.upper())")
        assert "SELECT" in result
        assert "UPPER" in result


class TestFHIRPathErrorHandling:
    """Test error handling for function calls"""
    
    def setup_method(self):
        self.translator = FHIRPathToSQL(dialect=DuckDBDialect())
    
    def test_function_argument_validation(self):
        """Test function argument validation"""
        with pytest.raises(ValueError, match="exactly"):
            self.translator.translate("name.upper('extra')")  # upper() takes no args
            
        with pytest.raises(ValueError, match="exactly"):
            self.translator.translate("name.split()")  # split() requires 1 arg
            
    def test_unknown_function_error(self):
        """Test error for unknown functions"""
        with pytest.raises(ValueError, match="Unknown"):
            self.translator.translate("name.unknownFunction()")


class TestFHIRPathIntegrationScenarios:
    """Test real-world integration scenarios"""
    
    def setup_method(self):
        self.translator = FHIRPathToSQL(dialect=DuckDBDialect())
    
    def test_complex_string_processing(self):
        """Test complex string processing pipeline"""
        result = self.translator.translate(
            "name.where(use='official').select(family.upper() + ', ' + given.join(' ').trim())"
        )
        assert "SELECT" in result
        assert "UPPER" in result
        assert "TRIM" in result
        assert "||" in result
        
    def test_email_processing(self):
        """Test email processing with split and contains"""
        result = self.translator.translate(
            "telecom.where(system='email').value.where($this.contains('@')).split('@')"
        )
        assert "SELECT" in result
        assert "'email'" in result
        assert "string_split" in result
        
    def test_name_formatting_pipeline(self):
        """Test complex name formatting"""
        result = self.translator.translate(
            "name.where(use='official').select((family.upper() + ', ' + given.first().trim()).distinct())"
        )
        assert "SELECT" in result
        assert "UPPER" in result
        assert "TRIM" in result
        assert "DISTINCT" in result


if __name__ == "__main__":
    pytest.main([__file__])