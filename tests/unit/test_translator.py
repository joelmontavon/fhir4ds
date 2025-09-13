"""
Unit tests for FHIRPath to SQL translator module

NOTE: These tests are disabled as FHIRPathToSQL is deprecated.
New pipeline architecture uses SQLGenerator instead.
"""

import pytest
# from fhir4ds.fhirpath.core.translator import FHIRPathToSQL

# Skip all tests in this file - testing deprecated FHIRPathToSQL translator
pytestmark = pytest.mark.skip(reason="FHIRPathToSQL translator is deprecated, replaced by new pipeline architecture")


class TestFHIRPathToSQL:
    """Test FHIRPath to SQL translation functionality"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.translator = FHIRPathToSQL(
            table_name="test_resources", 
            json_column="resource_data"
        )
    
    def test_simple_translation(self):
        """Test translation of simple FHIRPath expressions"""
        result = self.translator.translate_to_parts("Patient.name")
        assert 'expression_sql' in result
        assert 'json_extract' in result['expression_sql']
        assert 'resource_data' in result['expression_sql']
        assert '$.Patient' in result['expression_sql'] or '$.name' in result['expression_sql']
    
    def test_function_translation(self):
        """Test translation of function calls"""
        result = self.translator.translate_to_parts("name.exists()")
        assert 'expression_sql' in result
        sql = result['expression_sql']
        # With CTEBuilder, we may get CTE references instead of inline SQL
        # Check for either inline SQL patterns or CTE reference patterns
        assert 'CASE' in sql or 'json_type' in sql or 'SELECT' in sql or 'exists_result' in sql
    
    def test_join_function_translation(self):
        """Test translation of join function"""
        result = self.translator.translate_to_parts("name.given.join(',')")
        assert 'expression_sql' in result
        sql = result['expression_sql']
        assert 'string_agg' in sql or 'json_each' in sql
    
    def test_complex_path_translation(self):
        """Test translation of complex path expressions"""
        result = self.translator.translate_to_parts("name.where(use = 'official').family.first()")
        assert 'expression_sql' in result
        sql = result['expression_sql']
        # With CTEBuilder, we may get CTE references instead of inline json_extract
        # Check for either inline patterns or CTE reference patterns
        assert 'json_extract' in sql or 'SELECT' in sql or 'first_' in sql
        # Note: With CTEBuilder, literal values like 'official' may be in CTE definitions
        # rather than the main expression, so we don't assert their presence here
    
    def test_boolean_expression_translation(self):
        """Test translation of boolean expressions"""
        result = self.translator.translate_to_parts("active = true")
        assert 'expression_sql' in result
        sql = result['expression_sql']
        assert '=' in sql
        assert 'true' in sql.lower()
    
    def test_indexer_translation(self):
        """Test translation of indexer expressions"""
        result = self.translator.translate_to_parts("name[0]")
        assert 'expression_sql' in result
        sql = result['expression_sql']
        assert ('json_extract' in sql or '__OPTIMIZED_INDEX__' in sql)
        assert '$[0]' in sql or '[0]' in sql
    
    def test_literal_translation(self):
        """Test translation of literal values"""
        result = self.translator.translate_to_parts("'test_string'")
        assert 'expression_sql' in result
        assert result['expression_sql'] == "'test_string'"
        
        result = self.translator.translate_to_parts("42")
        assert 'expression_sql' in result
        assert result['expression_sql'] == "42"
        
        result = self.translator.translate_to_parts("true")
        assert 'expression_sql' in result
        assert result['expression_sql'] == "true"
    
    def test_resource_type_context(self):
        """Test translation with resource type context"""
        result = self.translator.translate_to_parts(
            "name.family", 
            resource_type_context="Patient"
        )
        assert 'expression_sql' in result
        assert 'resource_type_filter' in result
        # May or may not have a filter depending on implementation
    
    def test_full_query_translation(self):
        """Test full query translation"""
        expressions = [
            ("id", "patient_id"),
            ("name.family.first()", "family_name"),
            ("active", "is_active")
        ]
        sql = self.translator.translate(expressions, resource_type_context="Patient")
        assert isinstance(sql, str)
        assert 'SELECT' in sql
        assert 'FROM test_resources' in sql
        assert 'patient_id' in sql
        assert 'family_name' in sql
        assert 'is_active' in sql
    
    def test_where_criteria_translation(self):
        """Test translation with WHERE criteria"""
        expressions = [("id", "patient_id")]
        where_criteria = ["active = true"]
        sql = self.translator.translate(
            expressions, 
            resource_type_context="Patient",
            where_criteria=where_criteria
        )
        assert 'WHERE' in sql
        assert 'active' in sql
        assert 'true' in sql.lower()
    
    def test_invalid_expression_handling(self):
        """Test handling of invalid expressions"""
        with pytest.raises(ValueError):
            self.translator.translate_to_parts("invalid..syntax")
    
    def test_debug_information(self):
        """Test that debug information is included in results"""
        result = self.translator.translate_to_parts("Patient.name")
        assert 'processed_ast_type' in result
        assert 'generator_debug_steps' in result
        assert isinstance(result['generator_debug_steps'], list)
    
    def test_empty_expression(self):
        """Test handling of empty expressions"""
        with pytest.raises(Exception):  # Should raise some form of parsing error
            self.translator.translate_to_parts("")
    
    def test_this_expression(self):
        """Test handling of $this expressions"""
        result = self.translator.translate_to_parts("$this")
        assert 'expression_sql' in result
        assert result['expression_sql'] == "resource_data"


class TestFHIRPathToSQLEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_different_table_and_column_names(self):
        """Test with different table and column names"""
        translator = FHIRPathToSQL(
            table_name="custom_table",
            json_column="json_data"
        )
        result = translator.translate_to_parts("Patient.name")
        assert 'json_data' in result['expression_sql']
        
        sql = translator.translate([("id", "patient_id")])
        assert 'FROM custom_table' in sql
    
    def test_complex_nested_expressions(self):
        """Test very complex nested expressions"""
        translator = FHIRPathToSQL()
        result = translator.translate_to_parts(
            "name.where(use = 'official').family.first().exists()"
        )
        assert 'expression_sql' in result
        # Should not crash and should produce some SQL
        assert len(result['expression_sql']) > 10
    
    def test_multiple_function_chaining(self):
        """Test multiple functions chained together"""
        translator = FHIRPathToSQL()
        result = translator.translate_to_parts("name.first().family.exists()")
        assert 'expression_sql' in result
        sql = result['expression_sql']
        # With CTEBuilder, we may get CTE references instead of inline SQL
        # Should handle the chain properly
        assert 'json_extract' in sql or 'CASE' in sql or 'SELECT' in sql or 'exists_result' in sql


if __name__ == '__main__':
    pytest.main([__file__])