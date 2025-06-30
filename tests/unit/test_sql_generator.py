"""
Unit tests for SQL generator module
"""

import pytest
from fhir4ds.fhirpath.core.generator import SQLGenerator
from fhir4ds.fhirpath.parser.ast_nodes import (
    IdentifierNode, LiteralNode, FunctionCallNode, PathNode,
    BinaryOpNode, ThisNode
)


class TestSQLGenerator:
    """Test SQL generation functionality"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.generator = SQLGenerator(
            table_name="test_table", 
            json_column="test_column"
        )
    
    def test_literal_nodes(self):
        """Test SQL generation for literal nodes"""
        # String literal
        node = LiteralNode('hello', 'string')
        sql = self.generator.visit(node)
        assert sql == "'hello'"
        
        # Integer literal
        node = LiteralNode(42, 'integer')
        sql = self.generator.visit(node)
        assert sql == "42"
        
        # Boolean literal
        node = LiteralNode(True, 'boolean')
        sql = self.generator.visit(node)
        assert sql == "true"
        
        # Decimal literal
        node = LiteralNode(3.14, 'decimal')
        sql = self.generator.visit(node)
        assert sql == "3.14"
    
    def test_identifier_node(self):
        """Test SQL generation for identifier nodes"""
        node = IdentifierNode('name')
        sql = self.generator.visit(node)
        # Should generate json_extract call
        assert 'json_extract' in sql
        assert 'test_column' in sql
        assert '$.name' in sql
    
    def test_this_node(self):
        """Test SQL generation for $this nodes"""
        node = ThisNode()
        sql = self.generator.visit(node)
        assert sql == "test_column"
    
    def test_simple_function_calls(self):
        """Test SQL generation for simple function calls"""
        # exists() function
        node = FunctionCallNode('exists', [])
        sql = self.generator.visit(node)
        assert 'CASE' in sql
        assert 'json_type' in sql
        
        # first() function
        node = FunctionCallNode('first', [])
        sql = self.generator.visit(node)
        assert 'COALESCE' in sql or 'CASE' in sql
        
        # count() function
        node = FunctionCallNode('count', [])
        sql = self.generator.visit(node)
        assert 'json_array_length' in sql or 'CASE' in sql
    
    def test_join_function(self):
        """Test SQL generation for join function"""
        # join with separator
        separator_arg = LiteralNode(',', 'string')
        node = FunctionCallNode('join', [separator_arg])
        sql = self.generator.visit(node)
        assert 'string_agg' in sql
        assert "',' " in sql or ',' in sql
        
        # join without separator
        node = FunctionCallNode('join', [])
        sql = self.generator.visit(node)
        assert 'string_agg' in sql
    
    def test_binary_operations(self):
        """Test SQL generation for binary operations"""
        left = IdentifierNode('active')
        right = LiteralNode(True, 'boolean')
        node = BinaryOpNode(left, '=', right)
        sql = self.generator.visit(node)
        assert '=' in sql
        assert 'true' in sql.lower()
        assert 'json_extract' in sql
    
    def test_path_expressions(self):
        """Test SQL generation for path expressions"""
        segments = [
            IdentifierNode('name'),
            IdentifierNode('family')
        ]
        node = PathNode(segments)
        sql = self.generator.visit(node)
        # With CTE architecture, complex paths may generate CTE references
        # Should contain either direct json_extract or CTE reference
        assert ('json_extract' in sql or 
                ('SELECT' in sql and 'FROM' in sql and 'extracted_value' in sql))
        # The field references are now in the CTEs, not the final expression
        # Verify that the SQL generation completed successfully
        assert len(sql) > 0 and sql != 'NULL'
    
    def test_complex_path_with_function(self):
        """Test SQL generation for paths with functions"""
        segments = [
            IdentifierNode('name'),
            FunctionCallNode('first', []),
            IdentifierNode('family')
        ]
        node = PathNode(segments)
        sql = self.generator.visit(node)
        # With CTE architecture, complex paths with functions generate CTE references
        # Should contain either direct json_extract or CTE reference
        assert ('json_extract' in sql or 
                ('SELECT' in sql and 'FROM' in sql and 'extracted_value' in sql))
        # With CTE architecture, function logic is in CTEs, not the final expression
        # Verify that the SQL generation completed successfully
        assert len(sql) > 0 and sql != 'NULL'
    
    def test_where_function_with_criteria(self):
        """Test SQL generation for where function with criteria"""
        criteria = BinaryOpNode(
            IdentifierNode('use'),
            '=',
            LiteralNode('official', 'string')
        )
        node = FunctionCallNode('where', [criteria])
        sql = self.generator.visit(node)
        assert 'json_each' in sql or 'CASE' in sql
        assert "'official'" in sql
    
    def test_extension_function(self):
        """Test SQL generation for extension function"""
        url_arg = LiteralNode('http://example.com/extension', 'string')
        node = FunctionCallNode('extension', [url_arg])
        sql = self.generator.visit(node)
        assert 'json_each' in sql
        assert 'extension' in sql
        assert 'http://example.com/extension' in sql
    
    def test_error_handling(self):
        """Test error handling for unknown functions"""
        node = FunctionCallNode('unknown_function', [])
        with pytest.raises(ValueError, match="Unknown or unsupported standalone function"):
            self.generator.visit(node)
    
    def test_alias_generation(self):
        """Test unique alias generation"""
        alias1 = self.generator.generate_alias()
        alias2 = self.generator.generate_alias()
        assert alias1 != alias2
        assert alias1.startswith('t')
        assert alias2.startswith('t')
    
    def test_resource_type_context(self):
        """Test SQL generation with resource type context"""
        generator = SQLGenerator(
            table_name="test_table",
            json_column="test_column", 
            resource_type="Patient"
        )
        node = IdentifierNode('name')
        sql = generator.visit(node)
        # Should still generate proper SQL regardless of resource type context
        assert 'json_extract' in sql
        assert 'test_column' in sql


class TestSQLGeneratorEdgeCases:
    """Test edge cases and error conditions"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.generator = SQLGenerator()
    
    def test_empty_function_args(self):
        """Test functions that require args but get none"""
        # extension() requires exactly one argument
        node = FunctionCallNode('extension', [])
        with pytest.raises(ValueError):
            self.generator.visit(node)
    
    def test_too_many_function_args(self):
        """Test functions with too many arguments"""
        # join() takes 0 or 1 arguments
        args = [LiteralNode(',', 'string'), LiteralNode(';', 'string')]
        node = FunctionCallNode('join', args)
        with pytest.raises(ValueError):
            self.generator.visit(node)
    
    def test_invalid_literal_type(self):
        """Test handling of invalid literal types"""
        # This should still work, just might not generate optimal SQL
        node = LiteralNode('test', 'unknown_type')
        sql = self.generator.visit(node)
        assert sql == "test"  # Should handle unknown types gracefully


if __name__ == '__main__':
    pytest.main([__file__])