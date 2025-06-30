"""
Unit tests for FHIRPath parser module
"""

import pytest
from fhir4ds.fhirpath.parser import FHIRPathLexer, FHIRPathParser
from fhir4ds.fhirpath.parser.ast_nodes import (
    IdentifierNode, LiteralNode, FunctionCallNode, PathNode,
    BinaryOpNode, IndexerNode, ThisNode
)


class TestFHIRPathLexer:
    """Test FHIRPath lexer functionality"""
    
    def test_simple_identifier(self):
        """Test tokenizing simple identifiers"""
        lexer = FHIRPathLexer("Patient")
        tokens = lexer.tokenize()
        assert len(tokens) == 2  # identifier + EOF
        assert tokens[0].type.name == 'IDENTIFIER'
        assert tokens[0].value == 'Patient'
    
    def test_path_expression(self):
        """Test tokenizing path expressions"""
        lexer = FHIRPathLexer("Patient.name.family")
        tokens = lexer.tokenize()
        # Should have: IDENTIFIER, DOT, IDENTIFIER, DOT, IDENTIFIER, EOF
        assert len(tokens) == 6
        assert tokens[0].value == 'Patient'
        assert tokens[1].type.name == 'DOT'
        assert tokens[2].value == 'name'
        assert tokens[3].type.name == 'DOT'
        assert tokens[4].value == 'family'
    
    def test_function_call(self):
        """Test tokenizing function calls"""
        lexer = FHIRPathLexer("exists()")
        tokens = lexer.tokenize()
        # Should have: IDENTIFIER, LPAREN, RPAREN, EOF
        assert len(tokens) == 4
        assert tokens[0].value == 'exists'
        assert tokens[1].type.name == 'LPAREN'
        assert tokens[2].type.name == 'RPAREN'
    
    def test_string_literals(self):
        """Test tokenizing string literals"""
        lexer = FHIRPathLexer("'hello world'")
        tokens = lexer.tokenize()
        assert len(tokens) == 2
        assert tokens[0].type.name == 'STRING'
        assert tokens[0].value == 'hello world'
    
    def test_numeric_literals(self):
        """Test tokenizing numeric literals"""
        # Integer
        lexer = FHIRPathLexer("42")
        tokens = lexer.tokenize()
        assert tokens[0].type.name == 'INTEGER'
        assert tokens[0].value == '42'
        
        # Decimal
        lexer = FHIRPathLexer("3.14")
        tokens = lexer.tokenize()
        assert tokens[0].type.name == 'DECIMAL'
        assert tokens[0].value == '3.14'
    
    def test_boolean_literals(self):
        """Test tokenizing boolean literals"""
        lexer = FHIRPathLexer("true")
        tokens = lexer.tokenize()
        assert tokens[0].type.name == 'BOOLEAN'
        assert tokens[0].value == 'true'
        
        lexer = FHIRPathLexer("false")
        tokens = lexer.tokenize()
        assert tokens[0].type.name == 'BOOLEAN'
        assert tokens[0].value == 'false'
    
    def test_operators(self):
        """Test tokenizing operators"""
        lexer = FHIRPathLexer("name = 'John'")
        tokens = lexer.tokenize()
        assert any(token.type.name == 'EQUALS' for token in tokens)
        
        lexer = FHIRPathLexer("active and true")
        tokens = lexer.tokenize()
        assert any(token.type.name == 'AND' for token in tokens)


class TestFHIRPathParser:
    """Test FHIRPath parser functionality"""
    
    def test_simple_identifier(self):
        """Test parsing simple identifiers"""
        lexer = FHIRPathLexer("Patient")
        tokens = lexer.tokenize()
        parser = FHIRPathParser(tokens)
        ast = parser.parse()
        assert isinstance(ast, IdentifierNode)
        assert ast.name == 'Patient'
    
    def test_path_expression(self):
        """Test parsing path expressions"""
        lexer = FHIRPathLexer("Patient.name.family")
        tokens = lexer.tokenize()
        parser = FHIRPathParser(tokens)
        ast = parser.parse()
        assert isinstance(ast, PathNode)
        assert len(ast.segments) == 3
        assert all(isinstance(seg, IdentifierNode) for seg in ast.segments)
        assert ast.segments[0].name == 'Patient'
        assert ast.segments[1].name == 'name'
        assert ast.segments[2].name == 'family'
    
    def test_function_call_no_args(self):
        """Test parsing function calls without arguments"""
        lexer = FHIRPathLexer("exists()")
        tokens = lexer.tokenize()
        parser = FHIRPathParser(tokens)
        ast = parser.parse()
        assert isinstance(ast, FunctionCallNode)
        assert ast.name == 'exists'
        assert len(ast.args) == 0
    
    def test_function_call_with_args(self):
        """Test parsing function calls with arguments"""
        lexer = FHIRPathLexer("where(active = true)")
        tokens = lexer.tokenize()
        parser = FHIRPathParser(tokens)
        ast = parser.parse()
        assert isinstance(ast, FunctionCallNode)
        assert ast.name == 'where'
        assert len(ast.args) == 1
        assert isinstance(ast.args[0], BinaryOpNode)
    
    def test_indexer(self):
        """Test parsing indexer expressions"""
        lexer = FHIRPathLexer("name[0]")
        tokens = lexer.tokenize()
        parser = FHIRPathParser(tokens)
        ast = parser.parse()
        assert isinstance(ast, IndexerNode)
        assert isinstance(ast.expression, IdentifierNode)
        assert ast.expression.name == 'name'
        assert isinstance(ast.index, LiteralNode)
        assert ast.index.value == 0
    
    def test_binary_operations(self):
        """Test parsing binary operations"""
        lexer = FHIRPathLexer("active = true")
        tokens = lexer.tokenize()
        parser = FHIRPathParser(tokens)
        ast = parser.parse()
        assert isinstance(ast, BinaryOpNode)
        assert ast.operator == '='
        assert isinstance(ast.left, IdentifierNode)
        assert isinstance(ast.right, LiteralNode)
        assert ast.left.name == 'active'
        assert ast.right.value is True
    
    def test_complex_expression(self):
        """Test parsing complex expressions"""
        lexer = FHIRPathLexer("name.where(use = 'official').family.first()")
        tokens = lexer.tokenize()
        parser = FHIRPathParser(tokens)
        ast = parser.parse()
        assert isinstance(ast, PathNode)
        # Should have: name, where(...), family, first()
        assert len(ast.segments) == 4
        assert isinstance(ast.segments[1], FunctionCallNode)
        assert ast.segments[1].name == 'where'
        assert isinstance(ast.segments[3], FunctionCallNode)
        assert ast.segments[3].name == 'first'
    
    def test_this_node(self):
        """Test parsing $this expressions"""
        lexer = FHIRPathLexer("$this")
        tokens = lexer.tokenize()
        parser = FHIRPathParser(tokens)
        ast = parser.parse()
        assert isinstance(ast, ThisNode)
    
    def test_join_function(self):
        """Test parsing join function calls"""
        lexer = FHIRPathLexer("given.join(',')")
        tokens = lexer.tokenize()
        parser = FHIRPathParser(tokens)
        ast = parser.parse()
        assert isinstance(ast, PathNode)
        assert len(ast.segments) == 2
        assert isinstance(ast.segments[1], FunctionCallNode)
        assert ast.segments[1].name == 'join'
        assert len(ast.segments[1].args) == 1
        assert isinstance(ast.segments[1].args[0], LiteralNode)
        assert ast.segments[1].args[0].value == ','


if __name__ == '__main__':
    pytest.main([__file__])